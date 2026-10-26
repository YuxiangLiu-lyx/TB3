"""Independent file mailboxes, with visible acknowledgements and failures.

There is no broker server. A queue lock serializes local consumers. A process
crash leaves the message pending, so handlers must tolerate redelivery.
"""

from datetime import datetime, timezone
from copy import deepcopy
from pathlib import Path
import re
import uuid

from .storage import SimulationError, file_lock, object_value, read_json, write_json
from .regions import RegionalService, local_service, require_state_path


class FileBroker(RegionalService):
    def __init__(self, root: Path, config):
        super().__init__(config.region)
        self.root = root
        self.config = config

    @local_service
    def names(self):
        config = self.config.snapshot()
        return list(config.bindings)

    def _queue(self, queue: str) -> Path:
        if queue not in self.names():
            raise SimulationError(f"Queue is not registered: {queue}")
        directory = self.root / queue
        require_state_path(directory, self.region)
        for status in ("pending", "done", "failed"):
            require_state_path(directory / status, self.region)
            (directory / status).mkdir(parents=True, exist_ok=True)
        return directory

    @local_service
    def publish(self, queue: str, kind: str, payload: dict, message_id=None) -> str:
        self._queue(queue)
        message_id = message_id or uuid.uuid4().hex
        if not isinstance(message_id, str) or not re.fullmatch(r"[a-f0-9]{32,64}", message_id):
            raise SimulationError("message_id must contain 32 to 64 lowercase hex digits")
        envelope = {
            "schema_version": 1,
            "message_id": message_id,
            "kind": kind,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "attempts": 0,
            "payload": payload,
        }
        return self._persist(queue, envelope)

    def _persist(self, queue, envelope):
        directory = self._queue(queue)
        message_id = envelope["message_id"]
        with file_lock(directory / ".lock"):
            # Retained acknowledgements make producer replay idempotent.
            for status in ("pending", "done", "failed"):
                path = directory / status / f"{message_id}.json"
                if path.exists():
                    existing = read_json(path)
                    if existing.get("kind") != envelope["kind"] or existing.get("payload") != envelope["payload"]:
                        raise SimulationError(f"Message ID collision in {queue}: {message_id}")
                    return message_id
            write_json(directory / "pending" / f"{message_id}.json", envelope)
        return message_id

    @local_service
    def _accept_mirror(self, queue, envelope):
        # Message identity, schema, kind, creation time and payload stay unchanged.
        # Attempts/result/error/consumer belong to the destination delivery, not
        # the business message, so each broker maintains them independently.
        copied = deepcopy(envelope)
        for delivery_field in ("result", "error", "consumer", "region"):
            copied.pop(delivery_field, None)
        copied["attempts"] = 0
        return self._persist(queue, copied)

    @local_service
    def consume_one(self, queue: str, handler, *, consumer=None) -> dict | None:
        directory = self._queue(queue)
        with file_lock(directory / ".lock"):
            pending = sorted((directory / "pending").glob("*.json"),
                             key=lambda path: (path.stat().st_mtime_ns, path.name))
            if not pending:
                return None
            path = pending[0]
            envelope = None
            try:
                envelope = object_value(read_json(path), "message envelope")
                if type(envelope.get("schema_version")) is not int or envelope["schema_version"] != 1:
                    raise SimulationError("Unsupported message schema_version")
                if envelope.get("message_id") != path.stem:
                    raise SimulationError("message_id does not match its queue file")
                if type(envelope.get("attempts")) is not int or envelope["attempts"] < 0:
                    raise SimulationError("Invalid message attempts")
                envelope["attempts"] += 1
                envelope["consumer"] = consumer
                envelope["region"] = self.region
                write_json(path, envelope)
                envelope["result"] = handler(envelope)
                status = "done"
                envelope.pop("error", None)
            except Exception as exc:
                status = "failed"
                if not isinstance(envelope, dict):
                    envelope = {"raw_text": path.read_text(encoding="utf-8", errors="replace")}
                envelope["error"] = f"{type(exc).__name__}: {exc}"
            write_json(directory / status / path.name, envelope)
            path.unlink()
            return {"region": self.region, "queue": queue, "consumer": consumer,
                    "message_id": path.stem, "status": status,
                    **({"error": envelope["error"]} if status == "failed" else {})}

    @local_service
    def status(self) -> dict:
        result = {}
        for queue in self.names():
            directory = self._queue(queue)
            result[queue] = {status: len(list((directory / status).glob("*.json")))
                             for status in ("pending", "done", "failed")}
        return result

    @local_service
    def delivery(self, queue: str, message_id: str) -> dict:
        directory = self._queue(queue)
        if not isinstance(message_id, str) or not re.fullmatch(r"[a-f0-9]{32,64}", message_id):
            raise SimulationError("Invalid message_id")
        with file_lock(directory / ".lock"):
            for status in ("pending", "failed", "done"):
                path = directory / status / f"{message_id}.json"
                if path.exists():
                    return {"region": self.region, "queue": queue, "message_id": message_id, "status": status,
                            "message": read_json(path)}
        raise SimulationError(f"Message {message_id} does not exist in queue {queue}")

    @local_service
    def messages(self, queue: str, status="pending") -> list[dict]:
        if status not in ("pending", "done", "failed"):
            raise SimulationError("status must be pending, done or failed")
        directory = self._queue(queue)
        with file_lock(directory / ".lock"):
            return [read_json(path) for path in sorted((directory / status).glob("*.json"))]

    @local_service
    def retry(self, queue: str) -> int:
        directory = self._queue(queue)
        with file_lock(directory / ".lock"):
            paths = list((directory / "failed").glob("*.json"))
            for path in paths:
                path.replace(directory / "pending" / path.name)
            return len(paths)
