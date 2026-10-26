from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from va_index.cli import main
from va_index.runtime import Runtime
from va_index.services import RecallProxy
from va_index.storage import SimulationError, read_json
from va_index.vector_db import VectorDatabase
from va_index.verification import verify


ROOT = Path(__file__).resolve().parents[1]
CASES = ROOT / "cases" / "va_cases.json"


class DeliveryAndVerificationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        shutil.copytree(ROOT / "config", self.root / "config")
        self.app = Runtime(self.root / "config", self.root / "state", ROOT / "fixtures")
        self.app.initialize()

    def test_sql_write_waits_for_all_target_writes(self):
        result = self.app.write("products", "SELECT id FROM business WHERE id = 'p1'", "forward")
        self.assertEqual(result["status"], "done")
        self.assertEqual(len(result["writes"]), 4)
        self.assertEqual(len(result["deliveries"]), 5)
        for receipt in result["writes"]:
            rows = self.app.vectors.records(receipt["namespace"], receipt["db_name"])
            self.assertEqual(rows[0]["id"], receipt["id"])

    def test_downstream_write_failure_is_returned_even_when_task_is_done(self):
        with patch.object(self.app.vectors, "upsert", side_effect=OSError("simulated write failure")):
            result = self.app.write("image_normal_only", "SELECT 'p1' AS id")
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["deliveries"][0]["status"], "done")
        self.assertIn("simulated write failure", result["errors"][0]["error"])
        self.assertEqual(result["writes"], [])

    def test_send_checks_queue_then_consumer_checks_message(self):
        with self.assertRaisesRegex(SimulationError, "Queue is not registered"):
            self.app.send("absent-queue", "write", {})
        self.assertFalse((self.app.state / "queues" / "absent-queue").exists())
        for payload in ({"id": "bad"}, None, ["bad"]):
            result = self.app.send("va.write.image", "write", payload)
            self.assertEqual(result["status"], "failed")
            self.assertTrue(result["errors"])
        self.assertTrue(all(not rows for rows in self.app.dump().values()))
        self.assertEqual(self.app.broker.status()["va.write.image"]["failed"], 3)

    def test_direct_write_uses_sent_vector_and_recall_returns_persisted_vector(self):
        payload = read_json(ROOT / "cases" / "messages" / "shadow_a.json")
        with patch.object(self.app.features, "compute", side_effect=AssertionError("unexpected model call")):
            result = self.app.send("va.write.image_comp_shadow", "write", payload)
        self.assertEqual(result["status"], "done")
        restarted = Runtime(self.app.config.directory, self.app.state, self.app.fixtures)
        recalled = restarted.recall.recall(103, payload["embedding"], return_attr=True, return_embedding=True)
        self.assertEqual(recalled["hits"][0]["embedding"], payload["embedding"])
        self.assertEqual(recalled["hits"][0]["attr"], ["shadow-a"])

    def test_unrelated_queue_failure_does_not_fail_this_delivery(self):
        self.app.send("va.write.image", "write", {}, wait=False)
        payload = read_json(ROOT / "cases" / "messages" / "text_task.json")
        result = self.app.send("va.tasks", "task", payload)
        self.assertEqual(result["status"], "done")
        self.assertEqual(len(result["writes"]), 2)
        self.assertEqual(result["errors"], [])
        self.assertEqual(self.app.broker.status()["va.write.image"]["failed"], 1)

    def test_processing_limit_does_not_report_unfinished_writes_as_done(self):
        result = self.app.write("products", "SELECT 'p1' AS id", limit=1)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(result["writes"], [])
        resumed = self.app.wait_delivery(result["queue"], result["message_id"])
        self.assertEqual(resumed["status"], "done")
        self.assertEqual(len(resumed["writes"]), 4)

    def test_existing_runtime_really_reads_tcc_for_write_and_recall(self):
        (self.app.config.directory / "tcc.json").unlink()
        with self.assertRaisesRegex(SimulationError, "tcc.json"):
            self.app.write("image_normal_only", "SELECT 'p1' AS id")
        with self.assertRaisesRegex(SimulationError, "tcc.json"):
            self.app.recall.recall(101, [0] * 8)

    def test_acceptance_cases_run_and_preserve_existing_state_and_config(self):
        self.app.write("image_normal_only", "SELECT 'existing' AS id")
        before = self.app.dump()
        report = verify(self.app, CASES)
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["summary"], {"total": 10, "passed": 10, "failed": 0})
        self.assertEqual(read_json(Path(report["report_path"])), report)
        self.assertEqual(self.app.dump(), before)
        self.assertTrue((self.app.config.directory / "tcc.json").exists())

    def test_verifier_rejects_successful_receipt_without_actual_storage(self):
        with patch.object(VectorDatabase, "upsert", return_value=None):
            report = verify(self.app, CASES, case_id="normal_image_sql")
        self.assertFalse(report["passed"])
        self.assertEqual(report["cases"][0]["requests"][0]["response"]["status"], "done")
        self.assertIn("persisted records", report["cases"][0]["error"])

    def test_verifier_rejects_recall_of_nonexistent_vector(self):
        forged = {"namespace": "va_vectors", "db_name": "va_vectors_image_1001",
                  "hits": [{"id": "invented-vector", "embedding": [0.0] * 8, "attr": {}}]}
        with patch.object(RecallProxy, "recall", return_value=forged):
            report = verify(self.app, CASES, case_id="normal_image_sql")
        self.assertFalse(report["passed"])
        self.assertIn("does not exist", report["cases"][0]["error"])

    def test_verifier_rejects_wrong_embedding_for_existing_vector(self):
        original = RecallProxy.recall
        def tampered(proxy, *args, **kwargs):
            response = original(proxy, *args, **kwargs)
            response["hits"][0]["embedding"] = [123.0] * 8
            return response
        with patch.object(RecallProxy, "recall", tampered):
            report = verify(self.app, CASES, case_id="normal_image_sql")
        self.assertFalse(report["passed"])
        self.assertIn("does not match its stored vector", report["cases"][0]["error"])

    def test_cli_exit_codes_reflect_success_and_failures(self):
        base = ["--config-dir", str(self.app.config.directory), "--state-dir", str(self.app.state),
                "--fixtures-dir", str(self.app.fixtures)]
        for arguments, expected in (
            (["write", "--dataset", "image_normal_only", "--sql", "SELECT 'cli-id' AS id"], 0),
            (["send", "--queue", "unknown", "--kind", "write", "--payload", "{}"], 1),
            (["send", "--queue", "va.write.image", "--kind", "write", "--payload", "{}"], 1),
            (["recall", "--corpus-id", "99999", "--embedding", "[0]"], 1),
        ):
            with self.subTest(arguments=arguments), redirect_stdout(StringIO()) as output, redirect_stderr(StringIO()) as error:
                self.assertEqual(main(base + arguments), expected)
                result = json.loads(output.getvalue() or error.getvalue())
                if expected:
                    self.assertTrue(result.get("error") or result.get("errors"))
        with patch.object(VectorDatabase, "upsert", return_value=None), redirect_stdout(StringIO()) as output:
            self.assertEqual(main(base + ["verify", "--cases", str(CASES), "--case", "normal_image_sql"]), 1)
            self.assertFalse(json.loads(output.getvalue())["passed"])


if __name__ == "__main__":
    unittest.main()
