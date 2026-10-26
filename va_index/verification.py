"""Behavioral acceptance checks using actual APIs and independent disk inspection.

No golden embeddings, hash algorithm or recall ordering are prescribed here.
The verifier does not call the writer's read API to decide whether it wrote data.
"""

from datetime import datetime, timezone
from pathlib import Path
import re
import shutil
import uuid

from .runtime import Runtime
from .messages import TaskMessage, WriteMessage, embedding_value, string_list
from .regions import require_control_plane
from .storage import SimulationError, object_value, read_json, write_json


class VerificationFailure(AssertionError):
    pass


def check(condition, message):
    if not condition:
        raise VerificationFailure(message)


def read_persisted(state):
    """Read the actual storage files, not consumer receipts or recall responses."""
    databases = {}
    for path in sorted((state / "vectors").glob("*.json")):
        data = read_json(path)
        key = (data["namespace"], data["db_name"])
        check(key not in databases, f"Duplicate physical storage for {key}")
        rows = object_value(data["records"], f"persisted records in {path}")
        for record_id, record in rows.items():
            check(record.get("id") == record_id, f"Stored record ID mismatch in {path}")
            embedding_value(record.get("embedding"))
        databases[key] = rows
    return databases


def request_payload(request, directory):
    return (read_json(directory / request["payload_file"]) if "payload_file" in request
            else request["payload"])


def execute(runtime, request, directory):
    op = request["op"]
    if op == "sql":
        return runtime.write(request["dataset"], request["sql"], request.get("mode", "realtime"),
                             limit=request.get("limit", 10000))
    if op == "send":
        return runtime.send(request["queue"], request["kind"], request_payload(request, directory),
                            limit=request.get("limit", 10000))
    if op == "recall":
        return runtime.recall.recall(request["corpusID"], request["embedding"],
                                     top_k=request.get("top_k"),
                                     return_attr=request.get("return_attr", True),
                                     return_embedding=request.get("return_embedding", True))
    raise SimulationError(f"Unknown case operation: {op}")


def verify_target(runtime, target, case, case_directory):
    # Recreate all service objects so an in-memory write cannot pass persistence checks.
    restarted = Runtime(runtime.config.directory, runtime.state, runtime.fixtures, region=runtime.region)
    all_records = read_persisted(runtime.state)
    identity = (target["namespace"], target["db_name"])
    records = all_records.get(identity, {})
    check(len(records) == case["expected_record_count"],
          f"Expected {case['expected_record_count']} persisted records in {identity}, got {len(records)}")
    check(all(not rows for key, rows in all_records.items() if key != identity),
          "Data was also written to an unrelated physical database")
    business_records = {}
    for record in records.values():
        if target["is_compressed"]:
            check(isinstance(record.get("attr"), list), "Compressed record is missing business IDs")
            ids = record["attr"]
        else:
            ids = [record["id"]]
        for business_id in ids:
            check(business_id not in business_records, f"Business ID {business_id} was stored more than once")
            business_records[business_id] = record
    check(set(business_records) == set(case["expected_ids"]),
          f"Persisted business IDs differ: expected {case['expected_ids']}, got {sorted(business_records)}")

    # For direct writes, verify that the supplied vector really arrived unchanged.
    for request in case["requests"]:
        if request["op"] == "send" and request["kind"] == "write":
            payload = request_payload(request, case_directory)
            check(business_records[payload["id"]]["embedding"] == payload["embedding"],
                  f"Directly submitted vector for {payload['id']} was not persisted correctly")

    query = next(iter(records.values()))["embedding"]
    recalled = restarted.recall.recall(target["corpusID"], query, top_k=10,
                                       return_attr=True, return_embedding=True)
    check((recalled.get("namespace"), recalled.get("db_name")) == identity,
          "Recall proxy selected the wrong physical database")
    hits = recalled.get("hits")
    check(isinstance(hits, list) and hits, "Recall returned no vectors after successful writes")
    hit_ids = set()
    for hit in hits:
        record_id = hit.get("id")
        check(record_id in records, f"Recalled vector {record_id} does not exist in {identity}")
        check(record_id not in hit_ids, f"Recall returned duplicate record {record_id}")
        hit_ids.add(record_id)
        check(hit.get("embedding") == records[record_id]["embedding"],
              f"Recalled embedding {record_id} does not match its stored vector")
        check(hit.get("attr") == records[record_id]["attr"],
              f"Recalled Attr {record_id} does not match storage")
    return {"persisted_record_count": len(records), "business_ids": sorted(business_records),
            "recall": recalled}


def _storage(apps):
    return {(region, *identity): rows for region, app in apps.items()
            for identity, rows in read_persisted(app.state).items()}


def _write_targets(app, request, response, apps):
    """Derive intended writes from inputs/Conf, never from writer receipts or routes."""
    if request["op"] == "send" and request["kind"] == "write":
        message = WriteMessage.parse(request["payload"])
        targets = []
        for db in message.db_list:
            matches = [(region, index) for region, candidate in apps.items()
                       for index in candidate.config.snapshot().indexes.values() if index.db_name == db]
            check(len(matches) == 1, f"Cannot uniquely locate intended database {db}")
            region, index = matches[0]
            if "target_region" in request:
                check(region == request["target_region"], f"{db} is not in expected region {request['target_region']}")
            targets.append((region, index, [message.id], db in message.compressed_db_list,
                            message.embedding, message.context["attr_by_db"][db]))
        return targets
    if request["op"] == "sql":
        # Selection is the SQL ingress result; downstream completion/writes are
        # not evidence of persistence. Cases can also pin the selected ID set.
        ids = string_list(response.get("selected_ids"), "selected_ids", nonempty=False)
        if "expected_ids" in request:
            expected = string_list(request["expected_ids"], "expected_ids", nonempty=False)
            check(set(ids) == set(expected), f"SQL selected IDs differ: expected {expected}, got {ids}")
        dataset = request["dataset"]
    else:
        check(request["kind"] == "task", f"Unsupported message kind: {request['kind']}")
        message = TaskMessage.parse(request["payload"])
        ids, dataset = message.ids, message.dataset
    region = request.get("target_region", app.region)
    check(region in apps, f"Region is not deployed: {region}")
    return [(region, index, ids, index.is_compressed, None, index.extra_config.get("attr", {}))
            for index in apps[region].config.snapshot().dataset(dataset)]


def _check_writes(targets, before, after, apps):
    evidence, allowed = [], set()
    for region, index, ids, compressed, embedding, attr in targets:
        identity = (region, index.namespace, index.db_name)
        if ids:
            allowed.add(identity)
        records = after.get(identity, {})
        dimension = apps[region].config.snapshot().tcc["features"][index.feature_name]["dimension"]
        matched = set()
        for business_id in ids:
            candidates = ([record for record in records.values()
                           if isinstance(record.get("attr"), list) and business_id in record["attr"]]
                          if compressed else [records[business_id]] if business_id in records else [])
            check(candidates, f"Business ID {business_id} was not persisted in {identity}")
            if embedding is not None:
                candidates = [record for record in candidates if record["embedding"] == embedding]
                check(candidates, f"Submitted vector for {business_id} was not persisted correctly in {identity}")
            check(all(len(record["embedding"]) == dimension for record in candidates),
                  f"Stored vector dimension is wrong for {business_id} in {identity}")
            if not compressed:
                check(candidates[0].get("attr") == attr, f"Stored Attr differs for {business_id} in {identity}")
            matched.update(record["id"] for record in candidates)
        evidence.append({"region": region, "namespace": index.namespace, "db_name": index.db_name,
                         "business_ids": ids, "record_ids": sorted(matched),
                         "persisted_record_count": len(records)})
    for identity in before.keys() | after.keys():
        if identity not in allowed:
            check(before.get(identity, {}) == after.get(identity, {}),
                  f"Unrelated database changed: {identity}")
    return {"writes": evidence}


def _check_recall(app, request, response, before, after):
    snapshot = app.config.snapshot()
    index = snapshot.index(request["corpusID"])
    identity = (app.region, index.namespace, index.db_name)
    check((response.get("region"), response.get("namespace"), response.get("db_name")) == identity
          and response.get("corpusID") == index.corpus_id, "Recall proxy selected the wrong physical database")
    # Compare against storage BEFORE recall: a fabricated hit cannot validate
    # itself by being inserted into the database by the recall implementation.
    records = before.get(identity, {})
    hits = response.get("hits")
    check(isinstance(hits, list), "Recall hits must be a list")
    options = {**snapshot.tcc["recall"], **index.extra_config.get("recall", {})}
    top_k = request.get("top_k")
    top_k = options["top_k"] if top_k is None else top_k
    check(len(hits) <= top_k, f"Recall returned more than top_k={top_k} vectors")
    check(not records or hits, "Recall returned no vectors from a nonempty database")
    seen = set()
    for hit in hits:
        record_id = hit.get("id")
        check(record_id in records, f"Recalled vector {record_id} does not exist in {identity}")
        check(record_id not in seen, f"Recall returned duplicate record {record_id}")
        seen.add(record_id)
        for field, option in (("embedding", "return_embedding"), ("attr", "return_attr")):
            enabled = request.get(option, True)
            if enabled is None:
                enabled = options.get(option, False)
            if enabled or field in hit:
                check(field in hit and hit[field] == records[record_id][field],
                      f"Recalled {field} {record_id} does not match storage")
    check(before == after, "Recall changed persisted vector data")
    return {"region": app.region, "namespace": index.namespace, "db_name": index.db_name,
            "persisted_record_count": len(records), "recalled_record_ids": sorted(seen)}


def verify_request(runtime, request, directory=Path("."), *, cluster=None):
    """Execute one real request, returning passed=False and the error on failure.

    Uses the supplied state (no reset). Each write is checked immediately, so a
    later request cannot conceal an earlier missing write. No service is invoked
    to repair a failed chain, and no ranking/model/hash implementation is assumed.
    """
    require_control_plane()
    outcome = {"input": request, "passed": False}
    try:
        object_value(request, "request")
        fields = {
            "sql": {"dataset", "sql", "mode", "limit", "expected_ids", "target_region"},
            "send": {"queue", "kind", "payload", "payload_file", "limit", "target_region"},
            "recall": {"corpusID", "embedding", "top_k", "return_attr", "return_embedding"},
        }
        op = request.get("op")
        check(op in fields, f"Unknown case operation: {op}")
        check(not set(request) - fields[op] - {"op", "region"}, "Unsupported request fields")
        if op == "send":
            check(("payload" in request) != ("payload_file" in request),
                  "send requires exactly one of payload or payload_file")
        apps = cluster.runtimes if cluster is not None else {runtime.region: runtime}
        check(cluster is not None or runtime.transport is None,
              "Pass cluster= when verifying a connected regional runtime")
        region = request.get("region", runtime.region)
        check(region in apps, f"Region is not deployed: {region}")
        app = apps[region]
        prepared = dict(request)
        if request.get("op") == "send":
            prepared["payload"] = request_payload(request, Path(directory))
            prepared.pop("payload_file", None)
        before = _storage(apps)
        try:
            response = execute(app, prepared, Path(directory))
        except Exception as exc:
            outcome["response"] = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
            raise
        outcome["response"] = response
        check(isinstance(response, dict), "Service response must be an object")
        check(not response.get("error") and not response.get("errors") and response.get("status") != "failed",
              f"Request failed: {response}")
        after = _storage(apps)
        if request["op"] == "recall":
            outcome["evidence"] = _check_recall(app, prepared, response, before, after)
            outcome["checks"] = ["recalled_vectors_exist_in_target", "returned_fields_match_storage",
                                 "recall_did_not_change_storage"]
        else:
            check(response.get("status") == "done", f"Write did not complete: {response}")
            targets = _write_targets(app, prepared, response, apps)
            outcome["evidence"] = _check_writes(targets, before, after, apps)
            outcome["checks"] = ["write_completed", "business_ids_persisted", "other_indexes_unchanged"]
        outcome["passed"] = True
    except Exception as exc:
        outcome["error"] = f"{type(exc).__name__}: {exc}"
    return outcome


def _validate_storage_expectations(expectations):
    if not isinstance(expectations, list):
        raise SimulationError("expect_storage must be a list")
    for expected in expectations:
        object_value(expected, "storage expectation")
        if set(expected) - {"region", "corpusID", "business_ids", "record_count", "id_groups"}:
            raise SimulationError("Unsupported storage expectation fields")
        if type(expected.get("corpusID")) is not int or expected["corpusID"] <= 0:
            raise SimulationError("storage expectation requires a positive corpusID")
        ids = string_list(expected.get("business_ids"), "expected business_ids", nonempty=False)
        count = expected.get("record_count")
        if type(count) is not int or count < 0:
            raise SimulationError("expected record_count must be a nonnegative integer")
        if "id_groups" in expected:
            groups = expected["id_groups"]
            if not isinstance(groups, list) or len(groups) != count:
                raise SimulationError("expected id_groups must have record_count entries")
            for group in groups:
                string_list(group, "expected ID group")
            if {id for group in groups for id in group} != set(ids):
                raise SimulationError("expected id_groups must cover exactly business_ids")


def verify_storage(runtime, expectations, *, cluster=None):
    """Assert final storage outcomes without fixing hashes, vectors or ranking.

    Used for exact business-ID sets, compressed grouping and repeated requests.
    Region enforcement remains in service/resource APIs, not in this oracle.
    """
    require_control_plane()
    _validate_storage_expectations(expectations)
    apps = cluster.runtimes if cluster is not None else {runtime.region: runtime}
    persisted, evidence = _storage(apps), []
    for expected in expectations:
        region = expected.get("region", runtime.region)
        check(region in apps, f"Region is not deployed: {region}")
        index = apps[region].config.snapshot().index(expected["corpusID"])
        identity = (region, index.namespace, index.db_name)
        records = persisted.get(identity, {})
        check(len(records) == expected["record_count"],
              f"Expected {expected['record_count']} persisted records in {identity}, got {len(records)}")
        groups = []
        for record in records.values():
            ids = record.get("attr") if index.is_compressed else [record["id"]]
            string_list(ids, f"stored business IDs in {identity}")
            groups.append(sorted(ids))
        business_ids = sorted({id for group in groups for id in group})
        check(business_ids == sorted(expected["business_ids"]),
              f"Persisted business IDs differ in {identity}: expected {expected['business_ids']}, got {business_ids}")
        if "id_groups" in expected:
            check(sorted(groups) == sorted(sorted(group) for group in expected["id_groups"]),
                  f"Stored ID groups differ in {identity}: expected {expected['id_groups']}, got {groups}")
        evidence.append({"region": region, "corpusID": index.corpus_id, "namespace": index.namespace,
                         "db_name": index.db_name, "record_count": len(records),
                         "business_ids": business_ids, "id_groups": sorted(groups)})
    return evidence


def _isolated_runtime(runtime, root, cluster):
    if cluster is None:
        check(runtime.transport is None, "Pass cluster= when verifying a connected regional runtime")
        shutil.copytree(runtime.config.directory, root / "config")
        app = Runtime(root / "config", root / "state", runtime.fixtures, region=runtime.region)
        app.initialize()
        return app, None
    from .cluster import Cluster
    deployments = {}
    for region, source in cluster.runtimes.items():
        shutil.copytree(source.config.directory, root / "config" / region)
        deployments[region] = {"config_dir": f"config/{region}",
                               "fixtures_dir": str(source.fixtures.resolve())}
    write_json(root / "cluster.json", {"regions": deployments})
    copied = Cluster(root / "cluster.json", root / "state")
    copied.initialize()
    return copied.runtime(runtime.region), copied


def _verify_requests(runtime, suite, cases_path, *, case_id, cluster):
    require_control_plane()
    cases = suite.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SimulationError("Verification cases must be a nonempty list")
    names = set()
    for case in cases:
        object_value(case, "case")
        name = case.get("id")
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9_]+", name) or name in names:
            raise SimulationError(f"Invalid or duplicate case ID: {name}")
        names.add(name)
        if set(case) - {"id", "description", "requests", "expect_storage"}:
            raise SimulationError(f"{name}: schema 2 accepts only id, description, requests, expect_storage; errors always fail")
        _validate_storage_expectations(case.get("expect_storage", []))
        requests = case.get("requests")
        if not isinstance(requests, list) or not requests:
            raise SimulationError(f"{name}: requests must not be empty")
        for request in requests:
            if not isinstance(request, dict) or request.get("op") not in ("sql", "send", "recall"):
                raise SimulationError(f"{name}: unsupported request")
    if case_id is not None:
        if case_id not in names:
            raise SimulationError(f"Unknown verification case: {case_id}")
        cases = [case for case in cases if case["id"] == case_id]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    run_dir = runtime.state.resolve() / "verification" / run_id
    results = []
    for case in cases:
        root = run_dir / case["id"]
        outcome = {"id": case["id"], "description": case.get("description", ""), "passed": False,
                   "state_dir": str(root / "state"), "requests": []}
        try:
            app, copied_cluster = _isolated_runtime(runtime, root, cluster)
            for request in case["requests"]:
                result = verify_request(app, request, cases_path.parent, cluster=copied_cluster)
                outcome["requests"].append(result)
                check(result["passed"], f"Request {len(outcome['requests'])}: {result.get('error')}")
            if "expect_storage" in case:
                outcome["storage_evidence"] = verify_storage(app, case["expect_storage"], cluster=copied_cluster)
            outcome["passed"] = True
        except Exception as exc:
            outcome["error"] = f"{type(exc).__name__}: {exc}"
        results.append(outcome)
    passed = sum(result["passed"] for result in results)
    report = {"schema_version": 2, "passed": passed == len(results),
              "summary": {"total": len(results), "passed": passed, "failed": len(results) - passed},
              "report_path": str(run_dir / "report.json"), "cases": results}
    write_json(run_dir / "report.json", report)
    return report


def verify(runtime, cases_path=Path("cases/correct_request.json"), *, case_id=None, cluster=None):
    cases_path = Path(cases_path)
    suite = object_value(read_json(cases_path), "verification suite")
    if suite.get("schema_version") == 2:
        return _verify_requests(runtime, suite, cases_path, case_id=case_id, cluster=cluster)
    if suite.get("schema_version") != 1:
        raise SimulationError("Unsupported verification schema_version")
    if cluster is not None:
        raise SimulationError("Cluster verification requires schema_version 2")
    indexes = object_value(suite.get("indexes"), "verification indexes")
    cases = suite.get("cases")
    if not isinstance(cases, list) or not cases:
        raise SimulationError("Verification cases must be a nonempty list")
    ids = set()
    for case in cases:
        object_value(case, "case")
        name = case.get("id")
        if not isinstance(name, str) or not re.fullmatch(r"[a-z0-9_]+", name) or name in ids:
            raise SimulationError(f"Invalid or duplicate case ID: {name}")
        ids.add(name)
        requests = case.get("requests")
        if not isinstance(requests, list) or not requests:
            raise SimulationError(f"{name}: requests must not be empty")
        for request in requests:
            if not isinstance(request, dict) or request.get("op") not in ("sql", "send", "recall"):
                raise SimulationError(f"{name}: unsupported request")
        if case.get("fault") not in (None, "missing_tcc"):
            raise SimulationError(f"{name}: unsupported fault")
        if case.get("expect_error"):
            if len(requests) != 1:
                raise SimulationError(f"{name}: error cases must contain exactly one request")
        elif case.get("target") not in indexes or not case.get("expected_ids"):
            raise SimulationError(f"{name}: target and expected_ids are required")
    if case_id is not None:
        if case_id not in ids:
            raise SimulationError(f"Unknown verification case: {case_id}")
        cases = [case for case in cases if case["id"] == case_id]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    run_dir = runtime.state.resolve() / "verification" / run_id
    run_dir.mkdir(parents=True)
    results = []
    for case in cases:
        case_root = run_dir / case["id"]
        outcome = {"id": case["id"], "description": case.get("description", ""),
                   "state_dir": str(case_root / "state"), "requests": []}
        try:
            shutil.copytree(runtime.config.directory, case_root / "config")
            app = Runtime(case_root / "config", case_root / "state", runtime.fixtures, region=runtime.region)
            app.initialize()
            # Detect broken deployment fixtures independently of runtime routing.
            snapshot = app.config.snapshot()
            for expected in indexes.values():
                actual = snapshot.index(expected["corpusID"])
                check((actual.namespace, actual.db_name, actual.feature_name, actual.is_compressed) ==
                      (expected["namespace"], expected["db_name"], expected["feature_name"], expected["is_compressed"]),
                      f"Conf differs for corpusID {expected['corpusID']}")
                check(snapshot.index_queues[actual.db_name] == expected["queue"],
                      f"Queue differs for corpusID {expected['corpusID']}")
            if case.get("fault") == "missing_tcc":
                (app.config.directory / "tcc.json").unlink()
            for request in case["requests"]:
                try:
                    response = execute(app, request, cases_path.parent)
                except (SimulationError, OSError) as exc:
                    response = {"status": "failed", "error": str(exc)}
                outcome["requests"].append({"input": request, "response": response})
                if case.get("expect_error"):
                    check(response.get("status") == "failed" and
                          bool(response.get("error") or response.get("errors")),
                          "Expected a real error but the system reported success or no error details")
                else:
                    check(response.get("status") == "done", f"Write did not complete: {response}")
            if case.get("expect_error"):
                check(not any(read_persisted(app.state).values()), "Rejected request still wrote vector records")
                outcome["checks"] = ["request_returned_error", "no_unexpected_vector_writes"]
            else:
                outcome["evidence"] = verify_target(app, indexes[case["target"]], case, cases_path.parent)
                outcome["checks"] = ["written_to_target_storage", "business_ids_present",
                                     "other_indexes_unchanged", "recalled_vectors_exist_in_target"]
            outcome["passed"] = True
        except Exception as exc:
            outcome["passed"] = False
            outcome["error"] = f"{type(exc).__name__}: {exc}"
        results.append(outcome)
    passed = sum(result["passed"] for result in results)
    report = {"passed": passed == len(results), "summary": {"total": len(results), "passed": passed,
              "failed": len(results) - passed}, "report_path": str(run_dir / "report.json"), "cases": results}
    write_json(run_dir / "report.json", report)
    return report
