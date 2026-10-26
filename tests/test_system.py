from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from va_index.messages import WriteMessage
from va_index.runtime import Runtime
from va_index.storage import SimulationError, read_json, stable_hash, write_json


ROOT = Path(__file__).resolve().parents[1]


class SystemTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copytree(ROOT / "config", self.root / "config")
        self.runtime = self.restart()
        self.runtime.initialize()

    def restart(self):
        return Runtime(self.root / "config", self.root / "state", ROOT / "fixtures")

    def edit(self, filename, transform):
        path = self.root / "config" / filename
        data = read_json(path)
        transform(data)
        write_json(path, data)

    def index(self, corpus_id):
        return self.runtime.config.snapshot().index(corpus_id)

    def records(self, corpus_id):
        index = self.index(corpus_id)
        return self.runtime.vectors.records(index.namespace, index.db_name)

    def submit(self, dataset="products", sql="SELECT id FROM business WHERE active=1 ORDER BY id", mode="forward"):
        return self.runtime.trigger.submit(dataset, sql, mode)

    def process(self, **kwargs):
        result = self.runtime.process(**kwargs)
        self.assertEqual(result["failed"], 0, result)
        return result

    def test_full_pipeline_and_recall_contract(self):
        result = self.submit()
        self.assertEqual(result["selected_ids"], ["p1", "p2", "p3"])
        self.assertEqual(self.process()["processed"], 13)
        self.assertEqual([len(self.records(key)) for key in (101, 102, 103, 104)], [3, 2, 2, 3])
        self.assertEqual(self.records(101)[0]["attr"], {})
        self.assertEqual(self.records(104)[0]["attr"], {"source": "catalog"})
        embedding = self.runtime.features.compute("p1", "image_embedding", "forward")
        result = self.runtime.recall.recall(102, embedding)
        self.assertEqual(result["db_name"], self.index(102).db_name)
        hit = next(hit for hit in result["hits"] if "p1" in hit["attr"])
        self.assertEqual(hit["id"], stable_hash(embedding))
        self.assertEqual(hit["attr"], ["p1", "p2"])
        result = self.runtime.recall.recall(102, embedding, return_attr=False, top_k=1)
        self.assertEqual(len(result["hits"]), 1)
        self.assertNotIn("attr", result["hits"][0])

    def test_same_feature_fans_out_to_independent_queues(self):
        self.submit(sql="SELECT 'p1' AS id")
        with patch.object(self.runtime.features, "compute", wraps=self.runtime.features.compute) as compute:
            self.process(stage="preprocess")
        self.assertEqual(compute.call_count, 2)  # One calculation per feature, not per index.
        snapshot = self.runtime.config.snapshot()
        image_vectors = []
        for key in (101, 102, 103, 104):
            index = snapshot.index(key)
            queue = snapshot.index_queues[index.db_name]
            messages = self.runtime.broker.messages(queue)
            self.assertEqual(len(messages), 1)
            payload = messages[0]["payload"]
            self.assertEqual(payload["db_list"], [index.db_name])
            self.assertEqual(payload["compressed_db_list"], [index.db_name] if index.is_compressed else [])
            WriteMessage.parse(payload)
            if key != 104:
                image_vectors.append(payload["embedding"])
        self.assertTrue(all(vector == image_vectors[0] for vector in image_vectors))
        self.process(stage="write", queue="va.write.image")
        self.assertEqual(len(self.records(101)), 1)
        self.assertEqual(self.records(102), [])
        self.assertEqual(self.runtime.broker.status()["va.write.image_comp"]["pending"], 1)

    def test_compressed_auxiliary_is_scoped_to_physical_database(self):
        self.submit("image_only", "SELECT 'p1' AS id")
        self.submit("shadow_only", "SELECT 'p2' AS id")
        self.process()
        first, second = self.records(102)[0], self.records(103)[0]
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["attr"], ["p1"])
        self.assertEqual(second["attr"], ["p2"])
        self.assertEqual(self.runtime.compressed.lookup(self.index(102).db_name, first["id"]), ["p1"])
        self.assertEqual(self.runtime.compressed.lookup(self.index(103).db_name, first["id"]), ["p2"])

    def test_auxiliary_lists_are_really_read_after_restart(self):
        self.submit("image_only", "SELECT 'p1' AS id")
        self.process()
        for path in (self.root / "state" / "compressed").glob("*.json"):
            data = read_json(path)
            for ids in data["records"].values():
                ids.append("external-id")
            write_json(path, data)
        self.runtime = self.restart()
        self.submit("image_only", "SELECT 'p2' AS id")
        self.process()
        self.assertEqual(self.records(102)[0]["attr"], ["p1", "external-id", "p2"])

    def test_repeated_delivery_does_not_duplicate_compressed_ids_or_fanout(self):
        self.submit("image_only")
        self.process()
        task = self.runtime.broker.messages("va.tasks", "done")[0]
        self.runtime.preprocessor.handle(task)
        self.assertEqual(self.runtime.broker.status()["va.write.image"]["pending"], 0)
        self.submit("image_only")  # Distinct task, same business IDs.
        self.process()
        self.assertEqual(len(self.records(101)), 3)
        self.assertEqual(len(self.records(102)), 2)
        ids = [id for row in self.records(102) for id in row["attr"]]
        self.assertCountEqual(ids, ["p1", "p2", "p3"])

    def test_partial_preprocessing_failure_and_retry(self):
        forward_path = self.runtime.features.forward_path
        original = read_json(forward_path)
        broken = deepcopy(original)
        del broken["p2"]
        write_json(forward_path, broken)
        self.submit("image_only", "SELECT id FROM business WHERE id IN ('p1', 'p2') ORDER BY id")
        result = self.runtime.process()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(self.records(101)), 1)
        write_json(forward_path, original)
        self.assertEqual(self.runtime.broker.retry("va.tasks"), 1)
        self.process()
        self.assertEqual(len(self.records(101)), 2)
        self.assertEqual(self.records(102)[0]["attr"], ["p1", "p2"])
        self.assertEqual(self.runtime.broker.status()["va.write.image"]["done"], 2)
        self.assertEqual(self.runtime.broker.messages("va.tasks", "done")[0]["attempts"], 2)

    def test_failed_final_write_can_be_repaired_from_saved_auxiliary_list(self):
        self.submit("image_only", "SELECT 'p1' AS id")
        self.process(stage="preprocess")
        with patch.object(self.runtime.vectors, "upsert", side_effect=OSError("simulated disk failure")):
            result = self.runtime.process(stage="write", queue="va.write.image_comp")
        self.assertEqual(result["failed"], 1)
        vector = self.runtime.features.compute("p1", "image_embedding", "forward")
        self.assertEqual(self.runtime.compressed.lookup(self.index(102).db_name, stable_hash(vector)), ["p1"])
        self.assertEqual(self.records(102), [])
        self.runtime = self.restart()
        self.runtime.broker.retry("va.write.image_comp")
        self.process()
        self.assertEqual(self.records(102)[0]["attr"], ["p1"])

    def test_wrong_queue_fails_without_writing_and_other_queues_progress(self):
        self.submit("image_only", "SELECT 'p1' AS id")
        self.process(stage="preprocess")
        payload = self.runtime.broker.messages("va.write.image")[0]["payload"]
        self.runtime.broker.publish("va.write.text", "write", payload)
        result = self.runtime.process(stage="write")
        self.assertEqual(result["failed"], 1)
        self.assertEqual(len(self.records(101)), 1)
        self.assertEqual(len(self.records(102)), 1)
        self.assertEqual(self.records(104), [])
        failure = self.runtime.broker.messages("va.write.text", "failed")[0]
        self.assertIn("Wrong queue", failure["error"])
        self.assertEqual(failure["payload"], payload)

    def test_invalid_contracts_are_quarantined(self):
        self.submit("image_only", "SELECT 'p1' AS id")
        self.process(stage="preprocess")
        valid = self.runtime.broker.messages("va.write.image")[0]["payload"]
        bad_payloads = []
        bad = deepcopy(valid)
        bad["compressed_db_list"] = ["unrelated_db"]
        bad_payloads.append(bad)
        bad = deepcopy(valid)
        bad["embedding"] = [True] * 8
        bad_payloads.append(bad)
        bad = deepcopy(valid)
        bad["embedding"] = [1.0]
        bad_payloads.append(bad)
        bad = deepcopy(valid)
        bad["context"]["feature_name"] = "text_embedding"
        bad_payloads.append(bad)
        for payload in bad_payloads:
            self.runtime.broker.publish("va.write.image", "write", payload)
        result = self.runtime.process(stage="write", queue="va.write.image")
        self.assertEqual(result["failed"], len(bad_payloads))
        self.assertEqual(len(self.records(101)), 1)

    def test_compression_decision_is_carried_in_message(self):
        self.submit("image_only", "SELECT 'p1' AS id")
        self.process(stage="preprocess")
        self.edit("conf.json", lambda data: data["indexes"][1].update(is_compressed=False))
        self.process(stage="write")
        self.assertEqual(self.records(102)[0]["attr"], ["p1"])
        self.assertNotEqual(self.records(102)[0]["id"], "p1")

    def test_forward_data_and_tcc_are_reloaded(self):
        realtime = self.runtime.features.compute("p1", "image_embedding", "realtime")
        before = self.runtime.features.compute("p1", "image_embedding", "forward")
        forward = read_json(self.runtime.features.forward_path)
        forward["p1"]["image_embedding"] = "modified-photo"
        write_json(self.runtime.features.forward_path, forward)
        after = self.runtime.features.compute("p1", "image_embedding", "forward")
        self.assertNotEqual(before, after)
        self.assertEqual(realtime, self.runtime.features.compute("p1", "image_embedding", "realtime"))
        self.edit("tcc.json", lambda data: data["features"]["image_embedding"].update(seed="model-v2"))
        self.assertNotEqual(after, self.runtime.features.compute("p1", "image_embedding", "forward"))
        self.submit()
        self.process()
        self.edit("tcc.json", lambda data: data["recall"].update(top_k=1, return_attr=True))
        result = self.runtime.recall.recall(101, realtime)
        self.assertEqual(len(result["hits"]), 1)
        self.assertIn("attr", result["hits"][0])

    def test_add_index_and_dataset_through_files_without_restarting(self):
        db_name = "va_vectors_brand_new_987654"
        def add_conf(data):
            data["indexes"].append({"corpusID": 999, "feature_name": "image_embedding",
                                    "db_name": db_name, "namespace": "va_vectors",
                                    "is_compressed": True, "extra_config": {}})
            data["datasets"]["new_dataset"] = [999]
        self.edit("conf.json", add_conf)
        def add_queue(data):
            data["index_queues"][db_name] = "va.write.new"
            data["bindings"]["va.write.new"] = {"consumer": "va-writer"}
            data["consumers"]["va-preprocessor"]["outputs"][db_name] = "va.write.new"
        self.edit("queues.json", add_queue)
        self.submit("new_dataset", "SELECT 'p1' AS id")
        self.process()
        self.assertEqual(self.records(999)[0]["attr"], ["p1"])
        self.assertEqual(self.records(101), [])
        self.assertEqual(self.runtime.broker.status()["va.write.new"]["done"], 1)

    def test_scheduler_periodic_and_one_off_state_survives_restart(self):
        self.edit("jobs.json", lambda data: data["jobs"][1].update(enabled=True))
        first = self.runtime.scheduler.tick(now=100)
        self.assertEqual(len(first), 2)
        self.assertEqual(self.runtime.scheduler.tick(now=101), [])
        self.runtime = self.restart()
        self.assertEqual(self.runtime.scheduler.tick(now=3699), [])
        due = self.runtime.scheduler.tick(now=3700)
        self.assertEqual(len(due), 1)
        self.assertEqual(due[0]["job"], "active-products")
        self.assertNotEqual(due[0]["task_id"], first[0]["task_id"])

    def test_scheduler_failed_job_does_not_block_other_jobs(self):
        def edit(data):
            data["jobs"][0]["sql"] = "SELECT id FROM missing_table"
            data["jobs"][1]["enabled"] = True
        self.edit("jobs.json", edit)
        results = self.runtime.scheduler.tick(now=100)
        self.assertIn("error", results[0])
        self.assertEqual(results[1]["count"], 4)
        self.assertEqual(self.runtime.broker.status()["va.tasks"]["pending"], 1)

    def test_sql_is_real_readonly_and_deduplicated(self):
        result = self.submit(sql="SELECT id FROM business WHERE category='shoes' UNION ALL SELECT 'p1'")
        self.assertEqual(result["selected_ids"], ["p1", "p2"])
        for sql in ("DELETE FROM business", "SELECT category FROM business", "SELECT NULL AS id",
                    "ATTACH DATABASE ':memory:' AS other", "SELECT id FROM business; DELETE FROM business"):
            with self.subTest(sql=sql), self.assertRaises(SimulationError):
                self.submit(sql=sql)
        with closing(sqlite3.connect(self.runtime.trigger.business_path)) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM business").fetchone()[0], 4)

    def test_empty_selection_is_successful_noop(self):
        result = self.submit(sql="SELECT id FROM business WHERE 0")
        self.assertEqual(result["count"], 0)
        self.assertEqual(self.process()["processed"], 1)
        self.assertTrue(all(not rows for rows in self.runtime.dump().values()))

    def test_invalid_regions_and_namespace_are_rejected(self):
        with self.assertRaises(SimulationError):
            self.runtime.vectors.upsert("my_vectors", self.index(101).db_name, "p1", [0.0] * 8, {})
        for filename in ("tcc.json", "conf.json", "queues.json"):
            original = read_json(self.root / "config" / filename)
            self.edit(filename, lambda data: data.update(region="my"))
            with self.subTest(filename=filename), self.assertRaises(SimulationError):
                self.runtime.config.snapshot()
            write_json(self.root / "config" / filename, original)

    def test_conf_requires_unique_identity_and_independent_queues(self):
        original = read_json(self.root / "config" / "conf.json")
        for key in ("corpusID", "db_name"):
            self.edit("conf.json", lambda data: data["indexes"][1].update({key: data["indexes"][0][key]}))
            with self.subTest(key=key), self.assertRaises(SimulationError):
                self.runtime.config.snapshot()
            write_json(self.root / "config" / "conf.json", original)
        self.edit("queues.json", lambda data: data["index_queues"].update(
            {original["indexes"][1]["db_name"]: "va.write.image"}))
        with self.assertRaises(SimulationError):
            self.runtime.config.snapshot()

    def test_invalid_recall_inputs_fail_clearly(self):
        for corpus_id, vector, options in ((99999, [0.0] * 8, {}),
                                           (101, [0.0], {}),
                                           (101, [float("nan")] * 8, {}),
                                           (101, [0.0] * 8, {"top_k": 0}),
                                           (101, [0.0] * 8, {"return_attr": "yes"})):
            with self.subTest(corpus_id=corpus_id, options=options), self.assertRaises(SimulationError):
                self.runtime.recall.recall(corpus_id, vector, **options)

    def test_concurrent_compressed_updates_do_not_lose_ids(self):
        index = self.index(102)
        vector = [0.25] * 8
        def write(position):
            self.runtime.compressed.write(index.db_name, f"item-{position}", vector,
                lambda hash_id, ids: self.runtime.vectors.upsert(index.namespace, index.db_name,
                                                                 hash_id, vector, ids))
        with ThreadPoolExecutor(max_workers=4) as executor:
            list(executor.map(write, range(12)))
        self.assertEqual(len(self.records(102)), 1)
        self.assertCountEqual(self.records(102)[0]["attr"], [f"item-{n}" for n in range(12)])

    def test_cli_demo_in_a_fresh_process(self):
        completed = subprocess.run([sys.executable, "-m", "va_index", "--config-dir",
                                    str(self.root / "config"), "--state-dir", str(self.root / "cli-state"),
                                    "demo"], cwd=ROOT, text=True, capture_output=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(result["processed"]["failed"], 0)
        self.assertEqual(result["vector_counts"][self.index(102).db_name], 2)


if __name__ == "__main__":
    unittest.main()
