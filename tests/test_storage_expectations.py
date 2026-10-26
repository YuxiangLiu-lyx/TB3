from copy import deepcopy
from pathlib import Path
import shutil
import tempfile
import unittest

from va_index.runtime import Runtime
from va_index.storage import SimulationError, read_json, write_json
from va_index.verification import VerificationFailure, verify, verify_storage


ROOT = Path(__file__).resolve().parents[1]


class StorageExpectationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        shutil.copytree(ROOT / "config", self.root / "config")
        self.app = Runtime(self.root / "config", self.root / "state", ROOT / "fixtures")
        self.app.initialize()
        self.expected = {"region": "va", "corpusID": 102, "business_ids": ["p1", "p2", "p3"],
                         "record_count": 2, "id_groups": [["p1", "p2"], ["p3"]]}

    def seed(self):
        result = self.app.write("image_compressed_only", "SELECT id FROM business WHERE active = 1", "forward")
        self.assertEqual(result["status"], "done")

    def test_compressed_groups_are_checked_without_hash_or_order_requirements(self):
        self.seed()
        expected = deepcopy(self.expected)
        expected["id_groups"] = [["p3"], ["p2", "p1"]]
        evidence = verify_storage(self.app, [expected])
        self.assertEqual(evidence[0]["business_ids"], ["p1", "p2", "p3"])
        self.assertEqual(evidence[0]["record_count"], 2)

    def test_unexpected_business_id_in_compressed_attr_is_rejected(self):
        self.seed()
        path = next((self.app.state / "vectors").glob("*.json"))
        data = read_json(path)
        next(iter(data["records"].values()))["attr"].append("wrong-index-id")
        write_json(path, data)
        with self.assertRaisesRegex(VerificationFailure, "business IDs differ"):
            verify_storage(self.app, [self.expected])

    def test_duplicate_compressed_ids_are_rejected(self):
        self.seed()
        path = next((self.app.state / "vectors").glob("*.json"))
        data = read_json(path)
        ids = next(iter(data["records"].values()))["attr"]
        ids.append(ids[0])
        write_json(path, data)
        with self.assertRaisesRegex(SimulationError, "duplicates"):
            verify_storage(self.app, [self.expected])

    def test_correct_id_union_with_wrong_compressed_grouping_is_rejected(self):
        self.seed()
        path = next((self.app.state / "vectors").glob("*.json"))
        data = read_json(path)
        records = list(data["records"].values())
        records[0]["attr"], records[1]["attr"] = ["p1", "p3"], ["p2"]
        write_json(path, data)
        with self.assertRaisesRegex(VerificationFailure, "ID groups differ"):
            verify_storage(self.app, [self.expected])

    def test_wrong_record_count_fails_and_known_empty_database_passes(self):
        self.seed()
        with self.assertRaisesRegex(VerificationFailure, "Expected 3 persisted records"):
            verify_storage(self.app, [{**self.expected, "record_count": 3,
                                      "id_groups": [["p1"], ["p2"], ["p3"]]}])
        evidence = verify_storage(self.app, [{"corpusID": 101, "business_ids": [],
                                             "record_count": 0, "id_groups": []}])
        self.assertEqual(evidence[0]["record_count"], 0)

    def test_suite_saves_final_storage_assertions_and_reports_failures(self):
        case = {"id": "groups", "requests": [{"op": "sql", "dataset": "image_compressed_only",
                 "sql": "SELECT id FROM business WHERE active = 1", "mode": "forward",
                 "expected_ids": ["p1", "p2", "p3"]}], "expect_storage": [self.expected]}
        path = self.root / "cases.json"
        write_json(path, {"schema_version": 2, "cases": [case]})
        report = verify(self.app, path)
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["cases"][0]["storage_evidence"][0]["record_count"], 2)
        case["expect_storage"][0]["id_groups"] = [["p1", "p3"], ["p2"]]
        write_json(path, {"schema_version": 2, "cases": [case]})
        report = verify(self.app, path)
        self.assertFalse(report["passed"])
        self.assertTrue(report["cases"][0]["requests"][0]["passed"])
        self.assertIn("ID groups differ", report["cases"][0]["error"])

    def test_inconsistent_expectations_are_configuration_errors(self):
        for expected in ({**self.expected, "record_count": -1},
                         {**self.expected, "id_groups": [["p1"], ["p2"]]},
                         {**self.expected, "unexpected": True}):
            with self.subTest(expected=expected), self.assertRaises(SimulationError):
                verify_storage(self.app, [expected])


if __name__ == "__main__":
    unittest.main()
