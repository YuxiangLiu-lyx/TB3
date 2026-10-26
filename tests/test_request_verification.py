from contextlib import redirect_stdout
from copy import deepcopy
from io import StringIO
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from va_index.cli import main
from va_index.cluster import Cluster
from va_index.runtime import Runtime
from va_index.services import Preprocessor
from va_index.storage import read_json, write_json
from va_index.vector_db import VectorDatabase
from va_index.verification import verify, verify_request


ROOT = Path(__file__).resolve().parents[1]
SQL = {"op": "sql", "dataset": "image_normal_only", "sql": "SELECT 'p1' AS id", "expected_ids": ["p1"]}
RECALL = {"op": "recall", "corpusID": 101, "embedding": [0] * 8}


class RequestVerificationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        shutil.copytree(ROOT / "config", self.root / "config")
        self.app = Runtime(self.root / "config", self.root / "state", ROOT / "fixtures")
        self.app.initialize()

    def edit_queues(self, edit):
        path = self.app.config.directory / "queues.json"
        data = read_json(path)
        edit(data)
        write_json(path, data)

    def test_correct_case_covers_all_four_indexes_and_keeps_existing_state(self):
        self.app.write("image_normal_only", "SELECT 'existing' AS id")
        before = self.app.dump()
        report = verify(self.app, ROOT / "cases/correct_request.json")
        self.assertTrue(report["passed"], report)
        steps = report["cases"][0]["requests"]
        self.assertEqual(len(steps), 7)
        self.assertEqual(len(steps[0]["evidence"]["writes"]), 4)
        self.assertEqual([step["evidence"]["persisted_record_count"] for step in steps[-4:]], [2, 1, 2, 3])
        self.assertTrue(all(step["passed"] for step in steps))
        self.assertEqual(read_json(Path(report["report_path"])), report)
        self.assertEqual(self.app.dump(), before)

    def test_invalid_request_really_reaches_consumer_and_fails_verification(self):
        report = verify(self.app, ROOT / "cases/invalid_request.json")
        self.assertFalse(report["passed"])
        self.assertEqual(report["summary"], {"total": 1, "passed": 0, "failed": 1})
        case = report["cases"][0]
        response = case["requests"][0]["response"]
        self.assertEqual(response["status"], "failed")
        self.assertIn("write payload fields", response["errors"][0]["error"])
        files = list((Path(case["state_dir"]) / "queues/va.write.image/failed").glob("*.json"))
        self.assertEqual(len(files), 1)
        self.assertEqual(read_json(files[0])["consumer"], "va-writer")

    def test_cli_default_succeeds_and_invalid_case_exits_one(self):
        base = ["--config-dir", str(self.app.config.directory), "--state-dir", str(self.app.state),
                "--fixtures-dir", str(self.app.fixtures)]
        for arguments, code in ((["verify"], 0),
                                (["verify", "--cases", str(ROOT / "cases/invalid_request.json")], 1)):
            with self.subTest(arguments=arguments), redirect_stdout(StringIO()) as output:
                self.assertEqual(main(base + arguments), code)
                self.assertEqual(json.loads(output.getvalue())["passed"], code == 0)

    def test_request_errors_are_captured_with_original_reason(self):
        for request, reason in (
            ({"op": "send", "queue": "missing", "kind": "write", "payload": {}}, "Queue is not registered"),
            ({"op": "send", "queue": "va.write.image", "kind": "write", "payload": {}}, "write payload fields"),
            ({**RECALL, "corpusID": 999}, "Unknown corpusID"),
            ({**RECALL, "embedding": [0]}, "expected embedding dimension"),
        ):
            with self.subTest(request=request):
                result = verify_request(self.app, request)
                self.assertFalse(result["passed"])
                self.assertIn(reason, result["error"])
                self.assertEqual(result["response"]["status"], "failed")
        (self.app.config.directory / "tcc.json").unlink()
        for request in (SQL, RECALL):
            result = verify_request(self.app, request)
            self.assertFalse(result["passed"])
            self.assertIn("tcc.json", result["error"])

    def test_downstream_failure_is_not_hidden_by_successful_task(self):
        with patch.object(self.app.vectors, "upsert", side_effect=OSError("disk unavailable")):
            result = verify_request(self.app, SQL)
        self.assertFalse(result["passed"])
        self.assertEqual(result["response"]["deliveries"][0]["status"], "done")
        self.assertIn("disk unavailable", result["error"])

    def test_false_completion_does_not_replace_persistence_evidence(self):
        for owner, method, receipt in ((Preprocessor, "handle", {"published": []}),
                                       (VectorDatabase, "upsert", None)):
            with self.subTest(method=method), patch.object(owner, method, return_value=receipt):
                result = verify_request(self.app, SQL)
                self.assertFalse(result["passed"])
                self.assertEqual(result["response"]["status"], "done")
                self.assertIn("not persisted", result["error"])
        self.assertTrue(all(not rows for rows in self.app.dump().values()))

    def test_pending_is_failure_and_verifier_does_not_repair_chain(self):
        self.edit_queues(lambda q: q["bindings"]["va.write.image"].update(enabled=False))
        result = verify_request(self.app, SQL)
        self.assertFalse(result["passed"])
        self.assertEqual(result["response"]["status"], "pending")
        self.assertIn("did not complete", result["error"])
        self.assertEqual(self.app.broker.status()["va.write.image"]["pending"], 1)
        self.assertTrue(all(not rows for rows in self.app.dump().values()))

    def test_custom_forwarding_hop_passes_without_prescribing_queue_order(self):
        def relay(q):
            q["consumers"]["relay"] = {"service": "forwarder", "output_queue": "va.write.image"}
            q["bindings"]["va.relay"] = {"consumer": "relay"}
            q["consumers"]["va-preprocessor"]["outputs"]["va_vectors_image_1001"] = "va.relay"
        self.edit_queues(relay)
        result = verify_request(self.app, SQL)
        self.assertTrue(result["passed"], result)
        self.assertEqual(len(result["response"]["deliveries"]), 3)

    def test_empty_selection_and_empty_recall_are_valid(self):
        for request in ({**SQL, "sql": "SELECT id FROM business WHERE 0", "expected_ids": []}, RECALL):
            result = verify_request(self.app, request)
            self.assertTrue(result["passed"], result)

    def test_supplied_vector_update_must_really_replace_previous_vector(self):
        payload = read_json(ROOT / "cases/messages/shadow_a.json")
        db = "va_vectors_image_1001"
        payload.update(db_list=[db], compressed_db_list=[])
        payload["context"]["attr_by_db"] = {db: {"source": "direct"}}
        request = {"op": "send", "queue": "va.write.image", "kind": "write", "payload": payload}
        self.assertTrue(verify_request(self.app, request)["passed"])
        # Same ID already exists, but its old vector cannot satisfy an update.
        payload["embedding"] = [5] * 8
        with patch.object(self.app.vectors, "upsert", return_value=None):
            result = verify_request(self.app, request)
        self.assertFalse(result["passed"])
        self.assertIn("not persisted correctly", result["error"])
        self.assertTrue(verify_request(self.app, request)["passed"])

    def test_compression_decision_comes_from_input_message(self):
        payload = read_json(ROOT / "cases/messages/shadow_a.json")
        payload["compressed_db_list"] = []
        result = verify_request(self.app, {"op": "send", "queue": "va.write.image_comp_shadow",
                                           "kind": "write", "payload": payload})
        self.assertTrue(result["passed"], result)
        self.assertEqual(result["evidence"]["writes"][0]["record_ids"], ["shadow-a"])

    def test_recall_must_match_existing_storage_and_request_limits(self):
        self.assertTrue(verify_request(self.app, SQL)["passed"])
        response = self.app.recall.recall(101, [0] * 8, return_attr=True, return_embedding=True)
        variants = []
        for field, value, reason in (("id", "invented", "does not exist"),
                                     ("embedding", [9] * 8, "does not match storage"),
                                     ("attr", {"invented": True}, "does not match storage")):
            forged = deepcopy(response)
            forged["hits"][0][field] = value
            variants.append((forged, reason))
        variants += [({**response, "hits": []}, "no vectors"),
                     ({**response, "hits": response["hits"] * 2}, "duplicate"),
                     ({**response, "db_name": "another_database"}, "wrong physical database")]
        for forged, reason in variants:
            with self.subTest(reason=reason), patch.object(self.app.recall, "recall", return_value=forged):
                result = verify_request(self.app, RECALL)
                self.assertFalse(result["passed"])
                self.assertIn(reason, result["error"])
        with patch.object(self.app.recall, "recall", return_value={**response, "hits": response["hits"] * 2}):
            result = verify_request(self.app, {**RECALL, "top_k": 1})
        self.assertIn("more than top_k", result["error"])
        result = verify_request(self.app, {**RECALL, "return_embedding": False, "return_attr": False})
        self.assertTrue(result["passed"], result)

    def test_recall_cannot_create_its_own_evidence(self):
        def fabricated(*args, **kwargs):
            self.app.vectors.upsert("va_vectors", "va_vectors_image_1001", "fake", [0] * 8, {})
            return {"region": "va", "corpusID": 101, "namespace": "va_vectors",
                    "db_name": "va_vectors_image_1001",
                    "hits": [{"id": "fake", "embedding": [0] * 8, "attr": {}}]}
        with patch.object(self.app.recall, "recall", side_effect=fabricated):
            result = verify_request(self.app, RECALL)
        self.assertFalse(result["passed"])
        self.assertIn("does not exist", result["error"])

    def test_suite_stops_failed_case_but_still_runs_next_case(self):
        cases = self.root / "cases.json"
        write_json(cases, {"schema_version": 2, "cases": [
            {"id": "bad", "requests": [{**RECALL, "corpusID": 999}, SQL]},
            {"id": "good", "requests": [SQL, RECALL]},
        ]})
        report = verify(self.app, cases)
        self.assertEqual(report["summary"], {"total": 2, "passed": 1, "failed": 1})
        self.assertEqual(len(report["cases"][0]["requests"]), 1)
        self.assertTrue(report["cases"][1]["passed"])

    def test_cluster_verification_keeps_mirror_and_inspects_destination_storage(self):
        cluster = Cluster(ROOT / "deployments/cluster.json", self.root / "cluster-state")
        cluster.initialize()
        payload = read_json(ROOT / "cases/messages/mirror_my_image.json")
        cases = self.root / "mirror.json"
        write_json(cases, {"schema_version": 2, "cases": [{"id": "mirror", "requests": [
            {"op": "send", "region": "va", "queue": "va.export.my", "kind": "write", "payload": payload},
            {**RECALL, "region": "my"},
        ]}]})
        report = verify(cluster.runtime("va"), cases, cluster=cluster)
        self.assertTrue(report["passed"], report)
        requests = report["cases"][0]["requests"]
        self.assertEqual(requests[0]["evidence"]["writes"][0]["region"], "my")
        self.assertEqual(requests[1]["response"]["hits"][0]["id"], payload["id"])
        self.assertTrue(all(not rows for rows in cluster.runtime("my").dump().values()))


if __name__ == "__main__":
    unittest.main()
