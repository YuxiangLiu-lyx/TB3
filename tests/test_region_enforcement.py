"""Region errors must come from the running chain, without the verifier."""

from copy import deepcopy
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from va_index.cluster import Cluster
from va_index.regions import RegionalService, region_scope
from va_index.storage import SimulationError, read_json, write_json
from va_index.vector_db import VectorDatabase


ROOT = Path(__file__).resolve().parents[1]


class RegionEnforcementTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        shutil.copytree(ROOT / "deployments", self.root / "deployments")
        manifest = self.root / "deployments/cluster.json"
        config = read_json(manifest)
        for deployment in config["regions"].values():
            deployment["fixtures_dir"] = str(ROOT / "fixtures")
        write_json(manifest, config)
        self.cluster = Cluster(manifest, self.root / "state")
        self.cluster.initialize()
        self.va, self.my = self.cluster.runtime("va"), self.cluster.runtime("my")
        self.payload = read_json(ROOT / "cases/messages/mirror_my_image.json")

    def test_every_component_has_fixed_region_and_topology_exposes_it(self):
        for app in (self.va, self.my):
            components = {name: component for name, component in vars(app).items()
                          if isinstance(component, RegionalService)}
            self.assertEqual(set(components), {"config", "broker", "features", "vectors", "compressed",
                                               "trigger", "preprocessor", "writer", "forwarder", "recall", "scheduler"})
            for component in (app, *components.values()):
                self.assertEqual(component.region, app.region)
                with self.assertRaises(AttributeError):
                    component.region = "elsewhere"
            topology = app.topology()
            self.assertTrue(all(row["region"] == app.region for row in topology["services"].values()))
            self.assertTrue(all(row["region"] == app.region for row in topology["queues"].values()))
            self.assertEqual(topology["services"]["features"]["available"], app.region == "va")
            self.assertEqual(topology["services"]["compressed"]["available"], app.region == "va")

    def test_replacing_config_cannot_change_service_identity(self):
        attempts = [
            (self.va.recall, lambda: self.va.recall.recall(101, [0] * 8)),
            (self.va.features, lambda: self.va.features.compute("p1", "image_embedding")),
            (self.va.vectors, lambda: self.va.vectors.records("my_vectors", "my_vectors_image_71931")),
            (self.va.broker, lambda: self.va.broker.publish("my.tasks", "task", {})),
            (self.va.trigger, lambda: self.va.trigger.submit("image_normal_only", "SELECT 'p1' AS id")),
        ]
        for service, attempt in attempts:
            with self.subTest(service=type(service).__name__), patch.object(service, "config", self.my.config):
                self.assertEqual(service.region, "va")
                with self.assertRaisesRegex(SimulationError, "va -> my FileConfig.snapshot"):
                    attempt()
        self.assertTrue(all(not rows for rows in self.my.dump().values()))
        self.assertTrue(all(row["pending"] == 0 for row in self.my.broker.status().values()))

    def test_recall_rejects_remote_config_even_if_remote_vector_client_also_matches(self):
        with patch.multiple(self.va.recall, config=self.my.config, vectors=self.my.vectors):
            with self.assertRaisesRegex(SimulationError, "Cross-region.*va -> my"):
                self.va.recall.recall(101, [0] * 8)
        self.assertEqual(self.va.recall.recall(101, [0] * 8)["region"], "va")

    def test_cross_region_dependencies_fail_inside_actual_queue_consumption(self):
        for service, field, remote, dataset, failed_queue in (
            (self.va.preprocessor, "features", self.my.features, "image_normal_only", "va.tasks"),
            (self.va.preprocessor, "broker", self.my.broker, "image_normal_only", "va.tasks"),
            (self.va.writer, "vectors", self.my.vectors, "image_normal_only", "va.write.image"),
            (self.va.writer, "compressed", self.my.compressed, "image_compressed_only", "va.write.image_comp"),
            (self.va.writer, "config", self.my.config, "image_normal_only", "va.write.image"),
        ):
            with self.subTest(field=field), patch.object(service, field, remote):
                result = self.va.write(dataset, "SELECT 'p1' AS id")
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["errors"][0]["queue"], failed_queue)
                self.assertIn("Cross-region access denied: va -> my", result["errors"][0]["error"])
            message = self.va.broker.delivery(failed_queue, result["errors"][0]["message_id"])
            self.assertEqual(message["status"], "failed")
            self.assertEqual(message["message"]["region"], "va")
        self.assertTrue(all(not rows for rows in self.va.dump().values()))
        self.assertTrue(all(not rows for rows in self.my.dump().values()))

    def test_mirror_does_not_authorize_destination_to_call_source_services(self):
        with patch.object(self.my.writer, "vectors", self.va.vectors):
            result = self.va.send("va.export.my", "write", self.payload)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["deliveries"][0]["status"], "done")
        self.assertEqual(result["errors"][0]["region"], "my")
        self.assertIn("my -> va VectorDatabase.upsert", result["errors"][0]["error"])
        self.assertTrue(all(not rows for rows in self.my.dump().values()))

    def test_business_payload_cannot_override_execution_region(self):
        payload = deepcopy(self.payload)
        db = "my_vectors_image_comp_86204"
        payload.update(db_list=[db], compressed_db_list=[db])
        payload["context"].update(region="va", caller_region="va", attr_by_db={db: {}})
        with patch.object(self.my.writer, "compressed", self.va.compressed):
            result = self.my.send("my.write.image_comp", "write", payload)
        self.assertEqual(result["status"], "failed")
        self.assertIn("my -> va CompressedStore.write", result["errors"][0]["error"])
        self.assertFalse(self.va.compressed.root.exists())

    def test_remote_state_paths_and_symlinks_are_rejected_by_resource_apis(self):
        attempts = [
            (self.va.vectors, "root", self.my.vectors.root,
             lambda: self.va.vectors.upsert("va_vectors", "va_vectors_image_1001", "x", [0] * 8, {})),
            (self.va.broker, "root", self.my.broker.root,
             lambda: self.va.broker.publish("va.tasks", "task", {})),
            (self.my.trigger, "business_path", self.va.trigger.business_path,
             lambda: self.my.trigger.select_ids("SELECT id FROM business")),
            (self.va.features, "forward_path", self.my.state / "forward.json",
             lambda: self.va.features.compute("p1", "image_embedding", "forward")),
            (self.va.compressed, "root", self.my.compressed.root,
             lambda: self.va.compressed.lookup("va_vectors_image_comp_2002", "hash")),
            (self.va.scheduler, "state_path", self.my.scheduler.state_path,
             lambda: self.va.scheduler.tick(now=100)),
        ]
        for service, field, path, attempt in attempts:
            with self.subTest(resource=path), patch.object(service, field, path):
                with self.assertRaisesRegex(SimulationError, "Cross-region storage access denied"):
                    attempt()
        alias = self.root / "apparently-local"
        alias.symlink_to(self.my.state, target_is_directory=True)
        vectors = VectorDatabase(alias / "vectors", self.va.config)
        with self.assertRaisesRegex(SimulationError, "Cross-region storage access denied"):
            vectors.records("va_vectors", "va_vectors_image_1001")

    def test_mismatched_queue_or_consumer_region_is_rejected_before_publication(self):
        path = self.va.config.directory / "queues.json"
        original = read_json(path)
        for collection, name in (("bindings", "va.tasks"), ("consumers", "va-preprocessor")):
            with self.subTest(collection=collection):
                config = deepcopy(original)
                config[collection][name]["region"] = "my"
                write_json(path, config)
                with self.assertRaisesRegex(SimulationError, "Cross-region.*binding denied"):
                    self.va.write("image_normal_only", "SELECT 'p1' AS id")
        write_json(path, original)
        self.assertEqual(self.va.broker.status()["va.tasks"]["pending"], 0)

    def test_runtime_checks_region_before_using_replaced_broker(self):
        with patch.object(self.va, "broker", self.my.broker):
            with self.assertRaisesRegex(SimulationError, "va -> my FileBroker.publish"):
                self.va.send("my.write.image", "write", self.payload)
        self.assertEqual(self.my.broker.status()["my.write.image"]["pending"], 0)

    def test_sync_control_plane_entry_is_rejected_before_enqueuing(self):
        with region_scope("va"):
            for attempt in (lambda: self.va.write("image_normal_only", "SELECT 'p1' AS id"),
                            lambda: self.va.send("va.tasks", "task", {})):
                with self.assertRaisesRegex(SimulationError, "control plane"):
                    attempt()
        self.assertEqual(self.va.broker.status()["va.tasks"]["pending"], 0)

    def test_jobs_config_is_region_scoped_and_my_preprocessor_is_unavailable(self):
        with patch.object(self.my.scheduler, "jobs_path", self.va.scheduler.jobs_path):
            with self.assertRaisesRegex(SimulationError, "own jobs config"):
                self.my.scheduler.tick(now=100)
        with self.assertRaisesRegex(SimulationError, "only in va"):
            self.my.preprocessor.handle({"kind": "task", "payload": {
                "task_id": "empty", "dataset": "products", "ids": [], "mode": "forward"}}, outputs={})


if __name__ == "__main__":
    unittest.main()
