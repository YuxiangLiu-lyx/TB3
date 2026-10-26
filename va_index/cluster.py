"""Control-plane event loop and the sole cross-region message transport.

Neither class implements an indexing pipeline or rewrites business identities.
"""

from pathlib import Path

from .regions import _mirror_handoff, region_name, require_control_plane, require_local
from .runtime import Runtime
from .storage import SimulationError, object_value, positive_int, read_json


class MirrorTransport:
    def __init__(self, brokers):
        self._brokers = dict(brokers)

    def copy(self, source_region, target_region, target_queue, envelope):
        require_local(source_region, "Mirror source")
        if source_region not in self._brokers or target_region not in self._brokers:
            raise SimulationError("Mirror source/destination region is not deployed")
        if source_region == target_region:
            raise SimulationError("Mirror must cross a region boundary")
        with _mirror_handoff(target_region):
            message_id = self._brokers[target_region]._accept_mirror(target_queue, envelope)
        return {"region": target_region, "queue": target_queue, "message_id": message_id}


class Cluster:
    def __init__(self, deployment_path, state_dir="state"):
        require_control_plane()
        path = Path(deployment_path).resolve()
        deployments = object_value(read_json(path), "cluster").get("regions")
        object_value(deployments, "cluster.regions")
        if not deployments:
            raise SimulationError("Cluster requires at least one region")
        self.runtimes = {}
        for region, deployment in deployments.items():
            region_name(region)
            object_value(deployment, "deployment")
            for field in ("config_dir", "fixtures_dir"):
                if not isinstance(deployment.get(field), str) or not deployment[field]:
                    raise SimulationError(f"{region}: missing {field}")
            self.runtimes[region] = Runtime(
                path.parent / deployment["config_dir"], Path(state_dir) / region,
                path.parent / deployment["fixtures_dir"], region=region,
            )
        self.transport = MirrorTransport({name: app.broker for name, app in self.runtimes.items()})
        for app in self.runtimes.values():
            app.transport = self.transport
            app._pump = self.process
            app._delivery_reader = self.delivery

    def runtime(self, region):
        require_control_plane()
        if region not in self.runtimes:
            raise SimulationError(f"Region is not deployed: {region}")
        return self.runtimes[region]

    def validate(self):
        require_control_plane()
        snapshots = {region: app.config.snapshot() for region, app in self.runtimes.items()}
        namespaces, physical_dbs = set(), set()
        edges = {}
        for region, snapshot in snapshots.items():
            namespace = snapshot.tcc["namespace"]
            if namespace in namespaces:
                raise SimulationError("Different regions must use different namespaces")
            namespaces.add(namespace)
            for index in snapshot.indexes.values():
                if index.db_name in physical_dbs:
                    raise SimulationError("Physical database names must differ between regions")
                physical_dbs.add(index.db_name)
            for queue in snapshot.bindings:
                consumer = snapshot.consumer(queue)
                service = consumer["service"]
                destinations = []
                if service == "preprocessor":
                    destinations = [(region, target) for target in consumer["outputs"].values()]
                elif service == "forwarder":
                    destinations = [(region, consumer["output_queue"])]
                elif service == "mirror":
                    destination = (consumer["target_region"], consumer["target_queue"])
                    if destination[0] not in snapshots or destination[1] not in snapshots[destination[0]].bindings:
                        raise SimulationError(f"Mirror destination is not registered: {destination}")
                    destinations = [destination]
                edges[(region, queue)] = destinations
        active, visited = set(), set()
        def visit(node):
            if node in active:
                raise SimulationError(f"Cross-region forwarding cycle at {node}")
            if node in visited:
                return
            active.add(node)
            for destination in edges[node]:
                visit(destination)
            active.remove(node)
            visited.add(node)
        for node in edges:
            visit(node)
        return {"valid": True, "regions": list(snapshots)}

    def initialize(self):
        self.validate()
        return {region: app.initialize() for region, app in self.runtimes.items()}

    def process(self, *, limit=10000):
        require_control_plane()
        positive_int(limit, "limit")
        self.validate()
        messages = []
        while len(messages) < limit:
            progressed = False
            for app in self.runtimes.values():
                if len(messages) >= limit:
                    break
                result = app.process(limit=1)
                if result["processed"]:
                    messages.extend(result["messages"])
                    progressed = True
            if not progressed:
                break
        return {"processed": len(messages), "failed": sum(row["status"] == "failed" for row in messages),
                "messages": messages,
                "queues": {region: app.broker.status() for region, app in self.runtimes.items()}}

    def delivery(self, region, queue, message_id):
        require_control_plane()
        return self.runtime(region).broker.delivery(queue, message_id)

    def topology(self):
        self.validate()
        return {region: app.topology() for region, app in self.runtimes.items()}
