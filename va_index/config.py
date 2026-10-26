"""Conf and TCC are external files, reloaded on each service operation."""

from dataclasses import dataclass
from pathlib import Path
import re

from .storage import SimulationError, object_value, positive_int, read_json, text_value
from .regions import RegionalService, local_service, region_name


@dataclass(frozen=True)
class IndexConfig:
    corpus_id: int
    feature_name: str
    db_name: str
    namespace: str
    is_compressed: bool
    extra_config: dict


@dataclass(frozen=True)
class Snapshot:
    tcc: dict
    indexes: dict[int, IndexConfig]
    datasets: dict[str, list[int]]
    task_queue: str
    index_queues: dict[str, str]
    consumers: dict[str, dict]
    bindings: dict[str, dict]

    def consumer(self, queue):
        if queue not in self.bindings:
            raise SimulationError(f"Queue is not registered: {queue}")
        return self.consumers[self.bindings[queue]["consumer"]]

    def index(self, corpus_id: int) -> IndexConfig:
        if type(corpus_id) is not int or corpus_id not in self.indexes:
            raise SimulationError(f"Unknown corpusID: {corpus_id!r}")
        return self.indexes[corpus_id]

    def dataset(self, name: str) -> list[IndexConfig]:
        if not isinstance(name, str) or name not in self.datasets:
            raise SimulationError(f"Unknown dataset: {name!r}")
        return [self.indexes[key] for key in self.datasets[name]]

    def by_db(self, name: str) -> IndexConfig:
        for index in self.indexes.values():
            if index.db_name == name:
                return index
        raise SimulationError(f"Unknown physical db_name: {name}")


class FileConfig(RegionalService):
    def __init__(self, directory: Path, region="va"):
        super().__init__(region)
        self.directory = Path(directory)

    @local_service
    def snapshot(self) -> Snapshot:
        tcc = object_value(read_json(self.directory / "tcc.json"), "TCC")
        if tcc.get("region") != self.region:
            raise SimulationError(f"{self.region} services can only read their own TCC")
        namespace = text_value(tcc.get("namespace"), "TCC namespace")
        features = object_value(tcc.get("features"), "TCC features")
        for name, feature in features.items():
            text_value(name, "feature name")
            feature = object_value(feature, "feature settings")
            positive_int(feature.get("dimension"), "feature dimension")
            text_value(feature.get("seed"), "feature seed")
        recall = object_value(tcc.get("recall"), "TCC recall")
        positive_int(recall.get("top_k"), "recall top_k")
        if type(recall.get("return_attr")) is not bool:
            raise SimulationError("TCC recall.return_attr must be boolean")
        if "return_embedding" in recall and type(recall["return_embedding"]) is not bool:
            raise SimulationError("TCC recall.return_embedding must be boolean")

        conf = object_value(read_json(self.directory / "conf.json"), "Conf")
        if conf.get("region") != self.region:
            raise SimulationError(f"{self.region} services can only read their own Conf")
        rows = conf.get("indexes")
        if not isinstance(rows, list) or not rows:
            raise SimulationError("Conf indexes must be a nonempty list")
        indexes, physical_names = {}, set()
        for row in rows:
            row = object_value(row, "index")
            corpus_id = positive_int(row.get("corpusID"), "corpusID")
            db = text_value(row.get("db_name"), "db_name")
            feature = text_value(row.get("feature_name"), "feature_name")
            if corpus_id in indexes or db in physical_names:
                raise SimulationError("corpusID and physical db_name must be unique")
            if row.get("namespace") != namespace:
                raise SimulationError(f"{db}: index must use the local deployment namespace")
            if feature not in features:
                raise SimulationError(f"{db}: missing TCC feature {feature}")
            if type(row.get("is_compressed")) is not bool:
                raise SimulationError(f"{db}: is_compressed must be boolean")
            extra = object_value(row.get("extra_config", {}), "extra_config")
            if "attr" in extra:
                object_value(extra["attr"], "extra_config.attr")
            if "recall" in extra:
                overrides = object_value(extra["recall"], "extra_config.recall")
                if set(overrides) - {"top_k", "return_attr", "return_embedding"}:
                    raise SimulationError("Unsupported Conf recall parameter")
                if "top_k" in overrides:
                    positive_int(overrides["top_k"], "Conf recall.top_k")
                if "return_attr" in overrides and type(overrides["return_attr"]) is not bool:
                    raise SimulationError("Conf recall.return_attr must be boolean")
                if "return_embedding" in overrides and type(overrides["return_embedding"]) is not bool:
                    raise SimulationError("Conf recall.return_embedding must be boolean")
            indexes[corpus_id] = IndexConfig(corpus_id, feature, db, namespace,
                                            row["is_compressed"], extra)
            physical_names.add(db)

        datasets = object_value(conf.get("datasets"), "Conf datasets")
        for name, ids in datasets.items():
            text_value(name, "dataset name")
            if not isinstance(ids, list) or not ids:
                raise SimulationError(f"Dataset {name} must contain corpusIDs")
            for key in ids:
                if type(key) is not int or key not in indexes:
                    raise SimulationError(f"Dataset {name}: unknown corpusID {key!r}")
            if len(ids) != len(set(ids)):
                raise SimulationError(f"Dataset {name}: duplicate corpusIDs")

        queues = object_value(read_json(self.directory / "queues.json"), "queues")
        if queues.get("region") != self.region:
            raise SimulationError(f"{self.region} services can only use their own queues")
        task_queue = text_value(queues.get("task_queue"), "task_queue")
        routes = object_value(queues.get("index_queues"), "index_queues")
        if set(routes) != physical_names:
            raise SimulationError("Each Conf physical database needs exactly one queue route")
        names = [task_queue, *routes.values()]
        for name in names:
            if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name):
                raise SimulationError(f"Invalid queue name: {name!r}")
        if len(set(names)) != len(names):
            raise SimulationError("Every physical database requires an independent queue")
        consumers = object_value(queues.get("consumers"), "consumers")
        bindings = object_value(queues.get("bindings"), "bindings")
        if not set(names) <= set(bindings):
            raise SimulationError("Every task/index queue requires an explicit consumer binding")
        for name, binding in bindings.items():
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", name):
                raise SimulationError(f"Invalid queue name: {name!r}")
            object_value(binding, "queue binding")
            binding_region = region_name(binding.setdefault("region", self.region))
            if binding_region != self.region:
                raise SimulationError(f"Cross-region queue binding denied: {self.region} -> {binding_region}: {name}")
            consumer = text_value(binding.get("consumer"), "binding.consumer")
            if consumer not in consumers:
                raise SimulationError(f"{name}: unknown consumer {consumer}")
            if type(binding.get("enabled", True)) is not bool:
                raise SimulationError(f"{name}: enabled must be boolean")
        for name, consumer in consumers.items():
            text_value(name, "consumer name")
            object_value(consumer, "consumer")
            consumer_region = region_name(consumer.setdefault("region", self.region))
            if consumer_region != self.region:
                raise SimulationError(f"Cross-region consumer binding denied: {self.region} -> {consumer_region}: {name}")
            service = consumer.get("service")
            if service not in ("preprocessor", "writer", "forwarder", "mirror", "reject"):
                raise SimulationError(f"{name}: unknown service {service}")
            if service == "preprocessor":
                if self.region != "va":
                    raise SimulationError("preprocessor/feature computation can only be deployed in va")
                outputs = object_value(consumer.get("outputs"), "preprocessor.outputs")
                for db, target_queue in outputs.items():
                    if db not in physical_names or not isinstance(target_queue, str) or target_queue not in bindings:
                        raise SimulationError(f"{name}: invalid output route {db} -> {target_queue}")
            elif service == "forwarder":
                target = text_value(consumer.get("output_queue"), "forwarder.output_queue")
                if target not in bindings:
                    raise SimulationError(f"{name}: output queue is not registered: {target}")
            elif service == "mirror":
                target_region = region_name(consumer.get("target_region"))
                if target_region == self.region:
                    raise SimulationError("Mirror requires a different destination region")
                text_value(consumer.get("target_queue"), "mirror.target_queue")
            elif service == "reject":
                text_value(consumer.get("reason"), "reject.reason")
        # Consumer callbacks publish while holding the source queue lock. Reject
        # static local cycles instead of allowing recursive delivery/deadlocks.
        edges = {}
        for queue, binding in bindings.items():
            consumer = consumers[binding["consumer"]]
            edges[queue] = (list(consumer["outputs"].values()) if consumer["service"] == "preprocessor"
                            else [consumer["output_queue"]] if consumer["service"] == "forwarder" else [])
        visiting, visited = set(), set()
        def visit(queue):
            if queue in visiting:
                raise SimulationError(f"Queue forwarding cycle at {queue}")
            if queue in visited:
                return
            visiting.add(queue)
            for target in edges[queue]:
                visit(target)
            visiting.remove(queue)
            visited.add(queue)
        for queue in bindings:
            visit(queue)
        return Snapshot(tcc, indexes, datasets, task_queue, routes, consumers, bindings)
