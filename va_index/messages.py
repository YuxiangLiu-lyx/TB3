"""Versioned task and write contracts; validate again at consumer boundaries."""

from dataclasses import asdict, dataclass
import math

from .storage import SimulationError, object_value, text_value


def fields(value: dict, required: set[str], name: str):
    object_value(value, name)
    if set(value) != required:
        raise SimulationError(f"{name} fields must be {sorted(required)}")


def string_list(value, name: str, *, nonempty=True) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise SimulationError(f"{name} must be {'a nonempty' if nonempty else 'a'} list")
    for item in value:
        text_value(item, name)
    if len(set(value)) != len(value):
        raise SimulationError(f"{name} must not contain duplicates")
    return value


def embedding_value(value) -> list[float]:
    if not isinstance(value, list) or not value:
        raise SimulationError("embedding must be a nonempty list")
    if any(type(x) not in (float, int) or not math.isfinite(x) for x in value):
        raise SimulationError("embedding must contain finite numbers")
    # Canonicalize numerical representation so 1 and 1.0 (and signed zero) hash alike.
    return [0.0 if x == 0 else float(x) for x in value]


@dataclass(frozen=True)
class TaskMessage:
    task_id: str
    dataset: str
    ids: list[str]
    mode: str

    @classmethod
    def parse(cls, raw):
        fields(raw, {"task_id", "dataset", "ids", "mode"}, "task payload")
        text_value(raw["task_id"], "task_id")
        text_value(raw["dataset"], "dataset")
        string_list(raw["ids"], "ids", nonempty=False)
        if raw["mode"] not in ("realtime", "forward"):
            raise SimulationError("mode must be realtime or forward")
        return cls(**raw)

    def payload(self):
        return asdict(self)


@dataclass(frozen=True)
class WriteMessage:
    id: str
    embedding: list[float]
    db_list: list[str]
    compressed_db_list: list[str]
    context: dict

    @classmethod
    def parse(cls, raw):
        fields(raw, {"id", "embedding", "db_list", "compressed_db_list", "context"},
               "write payload")
        text_value(raw["id"], "id")
        embedding = embedding_value(raw["embedding"])
        dbs = string_list(raw["db_list"], "db_list")
        compressed = string_list(raw["compressed_db_list"], "compressed_db_list", nonempty=False)
        if not set(compressed) <= set(dbs):
            raise SimulationError("compressed_db_list must be a subset of db_list")
        context = object_value(raw["context"], "context")
        for key in ("task_id", "dataset", "feature_name"):
            text_value(context.get(key), f"context.{key}")
        if context.get("mode") not in ("realtime", "forward"):
            raise SimulationError("context.mode must be realtime or forward")
        object_value(context.get("attr_by_db"), "context.attr_by_db")
        if set(context["attr_by_db"]) != set(dbs):
            raise SimulationError("attr_by_db must contain exactly the target databases")
        for attr in context["attr_by_db"].values():
            object_value(attr, "attr")
        return cls(raw["id"], embedding, dbs, compressed, context)

    def payload(self):
        return asdict(self)
