from copy import deepcopy
from pathlib import Path
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from va_index.cluster import Cluster
from va_index.regions import region_scope
from va_index.runtime import Runtime
from va_index.storage import SimulationError, read_json, write_json


ROOT = Path(__file__).resolve().parents[1]


class QueueTopologyTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        shutil.copytree(ROOT / "config", self.root / "config")
        self.app = Runtime(self.root / "config", self.root / "state", ROOT / "fixtures")
        self.app.initialize()

    def edit(self, change):
        path = self.app.config.directory / "queues.json"
        value = read_json(path)
        change(value)
        write_json(path, value)

    def test_disabled_writer_keeps_message_pending_until_it_is_enabled(self):
        self.edit(lambda q: q["bindings"]["va.write.image"].update(enabled=False))
        result = self.app.write("image_normal_only", "SELECT 'p1' AS id")
        self.assertEqual(result["status"], "pending")
        self.assertEqual(self.app.broker.status()["va.tasks"]["done"], 1)
        self.assertEqual(self.app.broker.status()["va.write.image"]["pending"], 1)
        self.assertTrue(all(not rows for rows in self.app.dump().values()))
        self.edit(lambda q: q["bindings"]["va.write.image"].update(enabled=True))
        self.assertEqual(self.app.wait_delivery(result["queue"], result["message_id"])["status"], "done")

    def test_queue_binding_not_message_kind_selects_consumer(self):
        self.edit(lambda q: q["bindings"]["va.tasks"].update(consumer="va-writer"))
        with patch.object(self.app.preprocessor, "handle", side_effect=AssertionError("must not run")):
            result = self.app.write("image_normal_only", "SELECT 'p1' AS id")
        self.assertEqual(result["status"], "failed")
        self.assertIn("Writer accepts only write", result["errors"][0]["error"])
        message = self.app.broker.messages("va.tasks", "failed")[0]
        self.assertEqual(message["consumer"], "va-writer")

    def test_no_downstream_publication_means_no_automatic_write(self):
        with patch.object(self.app.preprocessor, "handle", return_value={"published": []}):
            result = self.app.write("products", "SELECT 'p1' AS id")
        self.assertEqual(result["status"], "done")  # Consumer completed, but produced nothing.
        self.assertEqual(result["writes"], [])
        self.assertTrue(all(not rows for rows in self.app.dump().values()))
        self.assertEqual(len(result["deliveries"]), 1)

    def test_missing_output_route_fails_instead_of_using_index_queue_implicitly(self):
        self.edit(lambda q: q["consumers"]["va-preprocessor"]["outputs"].pop("va_vectors_image_1001"))
        result = self.app.write("image_normal_only", "SELECT 'p1' AS id")
        self.assertEqual(result["status"], "failed")
        self.assertIn("No configured preprocessor output", result["errors"][0]["error"])
        self.assertEqual(self.app.broker.status()["va.write.image"]["pending"], 0)

    def test_configured_intermediate_queue_is_a_real_separate_hop(self):
        def add_hop(q):
            q["consumers"]["relay"] = {"service": "forwarder", "output_queue": "va.write.image"}
            q["bindings"]["va.image.staging"] = {"consumer": "relay"}
            q["consumers"]["va-preprocessor"]["outputs"]["va_vectors_image_1001"] = "va.image.staging"
        self.edit(add_hop)
        submitted = self.app.trigger.submit("image_normal_only", "SELECT 'p1' AS id")
        self.app.process(queue="va.tasks")
        staged = self.app.broker.messages("va.image.staging")[0]
        self.assertEqual(self.app.broker.status()["va.write.image"]["pending"], 0)
        self.assertTrue(all(not rows for rows in self.app.dump().values()))
        self.app.process(queue="va.image.staging")
        forwarded = self.app.broker.messages("va.write.image")[0]
        self.assertEqual(staged["payload"], forwarded["payload"])
        self.assertTrue(all(not rows for rows in self.app.dump().values()))
        result = self.app.wait_delivery(submitted["queue"], submitted["message_id"])
        self.assertEqual(result["status"], "done")
        self.assertEqual([row["queue"] for row in result["deliveries"]],
                         ["va.tasks", "va.image.staging", "va.write.image"])

    def test_unbound_queue_is_not_silently_assigned_a_writer(self):
        self.edit(lambda q: q["bindings"].pop("va.write.image"))
        with self.assertRaisesRegex(SimulationError, "explicit consumer binding"):
            self.app.send("va.write.image", "write", {})

    def test_unknown_consumer_and_local_cycles_are_rejected(self):
        original = read_json(self.app.config.directory / "queues.json")
        self.edit(lambda q: q["bindings"]["va.tasks"].update(consumer="absent-consumer"))
        with self.assertRaisesRegex(SimulationError, "unknown consumer"):
            self.app.config.snapshot()
        write_json(self.app.config.directory / "queues.json", original)
        def cycle(q):
            q["consumers"]["relay"] = {"service": "forwarder", "output_queue": "va.tasks"}
            q["bindings"]["va.tasks"]["consumer"] = "relay"
        self.edit(cycle)
        with self.assertRaisesRegex(SimulationError, "cycle"):
            self.app.config.snapshot()


class RegionTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        shutil.copytree(ROOT / "deployments", self.root / "deployments")
        manifest = self.root / "deployments" / "cluster.json"
        config = read_json(manifest)
        for deployment in config["regions"].values():
            deployment["fixtures_dir"] = str(ROOT / "fixtures")
        write_json(manifest, config)
        self.cluster = Cluster(manifest, self.root / "state")
        self.cluster.initialize()
        self.va, self.my = self.cluster.runtime("va"), self.cluster.runtime("my")
        self.payload = read_json(ROOT / "cases" / "messages" / "mirror_my_image.json")

    def edit(self, region, filename, change):
        path = self.cluster.runtime(region).config.directory / filename
        data = read_json(path)
        change(data)
        write_json(path, data)

    def test_regions_have_separate_state_and_only_va_has_forward_data(self):
        self.assertNotEqual(self.va.state, self.my.state)
        self.assertTrue(self.va.features.forward_path.exists())
        self.assertFalse(self.my.features.forward_path.exists())
        with self.assertRaisesRegex(SimulationError, "another region"):
            Runtime(self.my.config.directory, self.va.state, ROOT / "fixtures", region="my")

    def test_cross_region_service_calls_are_rejected(self):
        attempts = [
            self.va.config.snapshot,
            lambda: self.va.features.compute("p1", "image_embedding"),
            lambda: self.va.compressed.lookup("va_vectors_image_comp_2002", "h"),
            lambda: self.va.vectors.upsert("va_vectors", "va_vectors_image_1001", "x", [0]*8, {}),
            lambda: self.va.recall.recall(101, [0]*8),
            lambda: self.va.broker.publish("va.tasks", "task", {}),
            lambda: self.va.trigger.select_ids("SELECT 'p1' AS id"),
            lambda: self.cluster.runtime("va"),
            self.cluster.process,
        ]
        with region_scope("my"):
            for attempt in attempts:
                with self.subTest(call=attempt), self.assertRaises(SimulationError):
                    attempt()
        self.assertTrue(all(not rows for rows in self.va.dump().values()))

    def test_remote_client_injected_into_service_is_still_rejected(self):
        self.my.recall.vectors = self.va.vectors
        with self.assertRaisesRegex(SimulationError, "Cross-region"):
            self.my.recall.recall(101, [0]*8)

    def test_my_cannot_compute_features_or_maintain_compressed_lists(self):
        for mode in ("realtime", "forward"):
            with self.subTest(mode=mode), self.assertRaisesRegex(SimulationError, "only in va"):
                self.my.features.compute("p1", "image_embedding", mode)
        with self.assertRaisesRegex(SimulationError, "only in va"):
            self.my.compressed.lookup("my_vectors_image_comp_86204", "h")
        payload = deepcopy(self.payload)
        db = "my_vectors_image_comp_86204"
        payload.update(db_list=[db], compressed_db_list=[db])
        payload["context"]["attr_by_db"] = {db: {}}
        result = self.my.send("my.write.image_comp", "write", payload)
        self.assertEqual(result["status"], "failed")
        self.assertIn("only in va", result["errors"][0]["error"])
        self.assertFalse((self.my.state / "compressed").exists())

    def test_my_sql_is_not_automatically_routed_to_va(self):
        result = self.my.write("image_normal_only", "SELECT 'p1' AS id", "forward")
        self.assertEqual(result["status"], "failed")
        self.assertIn("not configured", result["errors"][0]["error"])
        self.assertEqual(self.va.broker.status()["va.tasks"]["done"], 0)
        self.assertTrue(all(not rows for rows in self.my.dump().values()))

    def test_mirror_preserves_message_and_destination_consumes_separately(self):
        self.edit("my", "queues.json", lambda q: q["bindings"]["my.write.image"].update(enabled=False))
        result = self.va.send("va.export.my", "write", self.payload)
        self.assertEqual(result["status"], "pending")
        source = self.va.broker.messages("va.export.my", "done")[0]
        target = self.my.broker.messages("my.write.image", "pending")[0]
        for field in ("schema_version", "message_id", "kind", "created_at", "payload"):
            self.assertEqual(source[field], target[field], field)
        self.assertEqual(target["payload"], self.payload)
        self.assertTrue(all(not rows for rows in self.my.dump().values()))
        self.edit("my", "queues.json", lambda q: q["bindings"]["my.write.image"].update(enabled=True))
        completed = self.va.wait_delivery(result["queue"], result["message_id"])
        self.assertEqual(completed["status"], "done")
        self.assertEqual([row["region"] for row in completed["deliveries"]], ["va", "my"])
        recalled = self.my.recall.recall(101, [0]*8, return_embedding=True)
        self.assertEqual(recalled["namespace"], "my_vectors")
        self.assertEqual(recalled["hits"][0]["embedding"], self.payload["embedding"])
        self.assertTrue(all(not rows for rows in self.va.dump().values()))

    def test_local_processing_does_not_consume_destination_queue(self):
        queued = self.va.send("va.export.my", "write", self.payload, wait=False)
        local = self.va.process(queue="va.export.my")
        self.assertEqual(local["processed"], 1)
        self.assertEqual(self.my.broker.status()["my.write.image"]["pending"], 1)
        self.assertTrue(all(not rows for rows in self.my.dump().values()))
        self.my.process(queue="my.write.image")
        self.assertEqual(self.va.wait_delivery(queued["queue"], queued["message_id"])["status"], "done")

    def test_mirror_does_not_translate_va_physical_database_into_my_database(self):
        payload = deepcopy(self.payload)
        payload["db_list"] = ["va_vectors_image_1001"]
        payload["context"]["attr_by_db"] = {"va_vectors_image_1001": {}}
        result = self.va.send("va.export.my", "write", payload)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["errors"][0]["region"], "my")
        self.assertIn("Unknown physical db_name", result["errors"][0]["error"])
        self.assertTrue(all(not rows for rows in self.my.dump().values()))

    def test_mirror_retry_after_lost_ack_does_not_duplicate_destination_message(self):
        accept = self.my.broker._accept_mirror
        def lost_ack(queue, envelope):
            accept(queue, envelope)
            raise OSError("simulated lost mirror acknowledgement")
        with patch.object(self.my.broker, "_accept_mirror", side_effect=lost_ack):
            result = self.va.send("va.export.my", "write", self.payload)
        self.assertEqual(result["status"], "failed")
        self.va.broker.retry("va.export.my")
        completed = self.va.wait_delivery(result["queue"], result["message_id"])
        self.assertEqual(completed["status"], "done")
        self.assertEqual(self.my.broker.status()["my.write.image"]["done"], 1)
        self.assertEqual(len(self.my.dump()["my_vectors_image_71931"]), 1)

    def test_conf_owner_and_preprocessor_deployment_are_checked(self):
        self.edit("my", "conf.json", lambda c: c.update(region="va"))
        with self.assertRaisesRegex(SimulationError, "own Conf"):
            self.my.config.snapshot()
        self.edit("my", "conf.json", lambda c: c.update(region="my"))
        self.edit("my", "queues.json", lambda q: q["consumers"].update(
            {"illegal-compute": {"service": "preprocessor", "outputs": {}}}))
        with self.assertRaisesRegex(SimulationError, "only be deployed in va"):
            self.my.config.snapshot()

    def test_cluster_rejects_cross_region_forwarding_cycles(self):
        self.edit("my", "queues.json", lambda q: q["bindings"]["my.write.image"].update(consumer="my-to-va"))
        self.edit("my", "queues.json", lambda q: q["consumers"]["my-to-va"].update(target_queue="va.export.my"))
        with self.assertRaisesRegex(SimulationError, "cycle"):
            self.cluster.validate()

    def test_same_message_id_cannot_hide_different_payload(self):
        message_id = self.my.broker.publish("my.write.image", "write", self.payload)
        altered = deepcopy(self.payload)
        altered["id"] = "different-id"
        with self.assertRaisesRegex(SimulationError, "collision"):
            self.my.broker.publish("my.write.image", "write", altered, message_id)

    def test_cli_preserves_separate_mirror_and_destination_consumption(self):
        base = [sys.executable, "-m", "va_index", "--cluster",
                str(self.root / "deployments" / "cluster.json"), "--state-dir", str(self.root / "state")]
        def call(region, *arguments):
            result = subprocess.run(base + ["--region", region, *arguments], cwd=ROOT,
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
            return json.loads(result.stdout)
        topology = call("va", "topology")
        self.assertEqual(topology["va"]["queues"]["va.export.my"]["service"], "mirror")
        queued = call("va", "send", "--queue", "va.export.my", "--kind", "write",
                      "--payload-file", str(ROOT / "cases/messages/mirror_my_image.json"), "--enqueue-only")
        self.assertEqual(queued["status"], "pending")
        call("va", "process", "--queue", "va.export.my")
        self.assertTrue(all(not rows for rows in call("my", "dump").values()))
        pending = call("my", "queues", "--queue", "my.write.image")
        self.assertEqual(pending[0]["payload"], self.payload)
        call("my", "process", "--queue", "my.write.image")
        recalled = call("my", "recall", "--corpus-id", "101", "--embedding", "[0,0,0,0,0,0,0,0]",
                        "--return-embedding")
        self.assertEqual(recalled["region"], "my")
        self.assertEqual(recalled["hits"][0]["embedding"], self.payload["embedding"])


if __name__ == "__main__":
    unittest.main()
