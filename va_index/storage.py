"""Small file storage primitives shared by simulated services."""

from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile


class SimulationError(ValueError):
    """An invalid configuration, request or simulated service operation."""


def read_json(path: Path):
    try:
        with path.open(encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, ValueError) as exc:
        raise SimulationError(f"Cannot read JSON {path}: {exc}") from exc


def write_json(path: Path, value):
    """Atomic replacement: readers see either the old or the complete new file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def stable_hash(value) -> str:
    serialized = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(serialized.encode()).hexdigest()


def object_value(value, name: str) -> dict:
    if not isinstance(value, dict):
        raise SimulationError(f"{name} must be an object")
    return value


def text_value(value, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SimulationError(f"{name} must be a nonempty string")
    return value


def positive_int(value, name: str) -> int:
    if type(value) is not int or value <= 0:
        raise SimulationError(f"{name} must be a positive integer")
    return value
