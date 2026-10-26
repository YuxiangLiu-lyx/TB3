"""A regional queue worker. Only configured consumers advance business messages."""

from contextlib import closing
from pathlib import Path
import sqlite3

from .broker import FileBroker
from .config import FileConfig
from .features import FeatureService
from .scheduler import Scheduler, TaskTrigger
from .services import Forwarder, Preprocessor, RecallProxy, Writer
from .regions import RegionalService, claim_state, local_service, region_scope, require_control_plane, require_state_path
from .storage import SimulationError, file_lock, positive_int, read_json, write_json
from .vector_db import CompressedStore, VectorDatabase


class Runtime(RegionalService):
    def __init__(self, config_dir="config", state_dir="state", fixtures_dir="fixtures", *, region="va"):
        super().__init__(region)
        self.config = FileConfig(Path(config_dir), region=self.region)
        self.state = Path(state_dir)
        claim_state(self.state, self.region)
        self.fixtures = Path(fixtures_dir)
        self.broker = FileBroker(self.state / "queues", self.config)
        self.features = FeatureService(self.config, self.state / "forward.json")
        self.vectors = VectorDatabase(self.state / "vectors", self.config)
        self.compressed = CompressedStore(self.state / "compressed", region=self.region)
        self.trigger = TaskTrigger(self.config, self.broker, self.state / "business.sqlite3")
        self.preprocessor = Preprocessor(self.config, self.broker, self.features)
        self.writer = Writer(self.config, self.vectors, self.compressed)
        self.forwarder = Forwarder(self.broker)
        self.recall = RecallProxy(self.config, self.vectors)
        self.scheduler = Scheduler(self.config.directory / "jobs.json",
                                   self.state / "scheduler.json", self.trigger)
        self.transport = None
        self._pump = self.process
        self._delivery_reader = self._local_delivery
        self._services = {
            "preprocessor": lambda q, e, c: self.preprocessor.handle(e, outputs=c["outputs"]),
            "writer": lambda q, e, c: self.writer.handle(q, e),
            "forwarder": lambda q, e, c: self.forwarder.handle(q, e, c["output_queue"]),
            "mirror": self._mirror,
            "reject": self._reject,
        }

    def _mirror(self, queue, envelope, consumer):
        if self.transport is None:
            raise SimulationError("Mirror transport is not connected; open a cluster deployment")
        reference = self.transport.copy(self.region, consumer["target_region"],
                                        consumer["target_queue"], envelope)
        return {"published": [reference]}

    def _reject(self, queue, envelope, consumer):
        raise SimulationError(consumer["reason"])

    def _local_delivery(self, region, queue, message_id):
        if region != self.region:
            raise SimulationError("Remote receipts require the cluster control plane")
        return self.broker.delivery(queue, message_id)

    @local_service
    def initialize(self):
        self.config.snapshot()
        require_state_path(self.state, self.region)
        require_state_path(self.trigger.business_path, self.region)
        self.state.mkdir(parents=True, exist_ok=True)
        with file_lock(self.state / ".init.lock"):
            if not self.trigger.business_path.exists():
                records = read_json(self.fixtures / "business.json")
                with closing(sqlite3.connect(self.trigger.business_path)) as connection:
                    with connection:
                        connection.execute("CREATE TABLE business (id TEXT PRIMARY KEY, category TEXT, active INTEGER)")
                        connection.executemany("INSERT INTO business VALUES (:id, :category, :active)", records)
            if self.region == "va" and not self.features.forward_path.exists():
                require_state_path(self.features.forward_path, self.region)
                write_json(self.features.forward_path, read_json(self.fixtures / "forward.json"))
        return {"region": self.region, "state_dir": str(self.state.resolve()), "queues": self.broker.status()}

    def process(self, stage="all", queue=None, limit=10000):
        with region_scope(self.region):
            return self._process(stage, queue, limit)

    def _process(self, stage, queue, limit):
        positive_int(limit, "limit")
        snapshot = self.config.snapshot()
        if stage not in ("all", "preprocess", "write"):
            raise SimulationError("stage must be all, preprocess or write")
        service_filter = {"preprocess": "preprocessor", "write": "writer"}
        selected = [name for name in snapshot.bindings
                    if stage == "all" or snapshot.consumer(name)["service"] == service_filter[stage]]
        if queue is not None:
            if queue not in selected:
                raise SimulationError(f"Queue {queue} is not available for stage {stage}")
            selected = [queue]
        results = []
        while len(results) < limit:
            progressed = False
            for name in selected:
                if len(results) >= limit:
                    break
                current = self.config.snapshot()
                binding = current.bindings[name]
                if not binding.get("enabled", True):
                    continue
                consumer = current.consumer(name)
                handler = self._services[consumer["service"]]
                outcome = self.broker.consume_one(
                    name, lambda envelope, q=name, c=consumer, h=handler: h(q, envelope, c),
                    consumer=binding["consumer"],
                )
                if outcome is not None:
                    results.append(outcome)
                    progressed = True
            if not progressed:
                break
        return {"region": self.region, "processed": len(results), "failed": sum(row["status"] == "failed" for row in results),
                "messages": results, "queues": self.broker.status()}

    @local_service
    def dump(self):
        snapshot = self.config.snapshot()
        return {index.db_name: self.vectors.records(index.namespace, index.db_name)
                for index in snapshot.indexes.values()}

    def write(self, dataset, sql, mode="realtime", *, limit=10000):
        """SQL -> task queue -> preprocessing -> index queues -> persisted writes."""
        require_control_plane()
        positive_int(limit, "limit")
        with region_scope(self.region):
            submitted = self.trigger.submit(dataset, sql, mode)
        return {**submitted, **self.wait_delivery(submitted["queue"], submitted["message_id"], limit=limit)}

    def send(self, queue, kind, payload, *, wait=True, limit=10000):
        """Publish to a registered queue; its actual consumer validates the payload."""
        if wait:
            require_control_plane()
        positive_int(limit, "limit")
        with region_scope(self.region):
            message_id = self.broker.publish(queue, kind, payload)
        if not wait:
            return {"region": self.region, "queue": queue, "message_id": message_id, "status": "pending"}
        return self.wait_delivery(queue, message_id, limit=limit)

    def wait_delivery(self, queue, message_id, *, limit=10000):
        require_control_plane()
        # Validate the requested receipt before processing other queued work.
        with region_scope(self.region):
            self.broker.delivery(queue, message_id)
        self._pump(limit=limit)
        deliveries, writes, errors = [], [], []
        pending = [(self.region, queue, message_id)]
        seen = set()
        while pending:
            current_region, current_queue, current_id = pending.pop(0)
            if (current_region, current_queue, current_id) in seen:
                continue
            seen.add((current_region, current_queue, current_id))
            delivery = self._delivery_reader(current_region, current_queue, current_id)
            envelope = delivery.pop("message")
            deliveries.append(delivery)
            if delivery["status"] == "failed":
                errors.append({**delivery, "error": envelope["error"]})
            elif delivery["status"] == "done":
                result = envelope.get("result")
                if not isinstance(result, dict):
                    errors.append({**delivery, "error": "Consumer did not return a completion receipt"})
                    continue
                writes.extend(result.get("writes", []))
                pending.extend((child.get("region", current_region), child["queue"], child["message_id"])
                               for child in result.get("published", []))
        status = ("failed" if errors else
                  "pending" if any(row["status"] == "pending" for row in deliveries) else "done")
        return {"region": self.region, "queue": queue, "message_id": message_id, "status": status,
                "writes": writes, "errors": errors, "deliveries": deliveries}

    @local_service
    def topology(self):
        snapshot = self.config.snapshot()
        return {"region": self.region, "namespace": snapshot.tcc["namespace"],
                "services": {name: {"service": type(service).__name__, "region": service.region,
                                    "available": service.available}
                             for name, service in vars(self).items() if isinstance(service, RegionalService)},
                "queues": {queue: {**binding, **snapshot.consumer(queue)}
                           for queue, binding in snapshot.bindings.items()}}
