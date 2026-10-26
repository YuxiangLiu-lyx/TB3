"""File-backed vector API. Writes are real; recall ranking is deliberately fake."""

from copy import deepcopy

from .messages import embedding_value
from .regions import RegionalService, local_service, require_state_path
from .storage import SimulationError, file_lock, positive_int, read_json, stable_hash, text_value, write_json


class VectorDatabase(RegionalService):
    def __init__(self, root, config):
        super().__init__(config.region)
        self.root = root
        self.config = config

    def _target(self, namespace, db_name, embedding=None):
        snapshot = self.config.snapshot()
        if namespace != snapshot.tcc["namespace"]:
            raise SimulationError(f"{self.region} vector database cannot access another namespace")
        index = snapshot.by_db(db_name)
        if index.namespace != namespace:
            raise SimulationError("Namespace does not match Conf")
        if embedding is not None:
            dimension = snapshot.tcc["features"][index.feature_name]["dimension"]
            if len(embedding_value(embedding)) != dimension:
                raise SimulationError(f"{db_name}: expected embedding dimension {dimension}")
        path = self.root / f"{stable_hash([namespace, db_name])}.json"
        require_state_path(path, self.region)
        return path

    def _read(self, path, namespace, db_name):
        data = read_json(path) if path.exists() else {
            "namespace": namespace, "db_name": db_name, "records": {}}
        if data.get("namespace") != namespace or data.get("db_name") != db_name:
            raise SimulationError("Vector storage identity mismatch")
        return data

    @local_service
    def upsert(self, namespace: str, db_name: str, id: str, embedding: list, attr):
        text_value(id, "vector id")
        vector = embedding_value(embedding)
        path = self._target(namespace, db_name, vector)
        with file_lock(path.with_suffix(".lock")):
            data = self._read(path, namespace, db_name)
            data["records"][id] = {"id": id, "embedding": vector, "attr": deepcopy(attr)}
            write_json(path, data)

    @local_service
    def records(self, namespace: str, db_name: str) -> list[dict]:
        path = self._target(namespace, db_name)
        with file_lock(path.with_suffix(".lock")):
            data = self._read(path, namespace, db_name)
            return [data["records"][key] for key in sorted(data["records"])]

    @local_service
    def recall(self, namespace, db_name, embedding, top_k=3, return_attr=False, return_embedding=False):
        self._target(namespace, db_name, embedding)
        positive_int(top_k, "top_k")
        if type(return_attr) is not bool:
            raise SimulationError("return_attr must be boolean")
        if type(return_embedding) is not bool:
            raise SimulationError("return_embedding must be boolean")
        # Select persisted records in ID order. Scores do not measure similarity.
        hits = []
        for offset, record in enumerate(self.records(namespace, db_name)[:top_k]):
            hit = {"id": record["id"], "score": round(1 / (offset + 1), 6)}
            if return_attr:
                hit["attr"] = record["attr"]
            if return_embedding:
                hit["embedding"] = record["embedding"]
            hits.append(hit)
        return hits


class CompressedStore(RegionalService):
    """Auxiliary IDs keyed by the actual physical db_name AND embedding hash."""

    va_only = True

    def __init__(self, root, region="va"):
        super().__init__(region)
        self.root = root

    def _path(self, db_name):
        path = self.root / f"{stable_hash(db_name)}.json"
        require_state_path(path, self.region)
        return path

    def _read(self, path, db_name):
        data = read_json(path) if path.exists() else {"db_name": db_name, "records": {}}
        if data.get("db_name") != db_name:
            raise SimulationError("Compressed storage identity mismatch")
        return data

    @local_service
    def write(self, db_name, business_id, embedding, upsert):
        hash_id = stable_hash(embedding_value(embedding))
        path = self._path(db_name)
        # Keep auxiliary update and final write ordered even across processes.
        # If upsert fails, retry reads the saved list and repairs the vector attr.
        with file_lock(path.with_suffix(".lock")):
            data = self._read(path, db_name)
            ids = data["records"].setdefault(hash_id, [])
            if business_id not in ids:
                ids.append(business_id)
            write_json(path, data)
            upsert(hash_id, list(ids))
        return hash_id

    @local_service
    def lookup(self, db_name, hash_id) -> list[str]:
        path = self._path(db_name)
        with file_lock(path.with_suffix(".lock")):
            return list(self._read(path, db_name)["records"].get(hash_id, []))
