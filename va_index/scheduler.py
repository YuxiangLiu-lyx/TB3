"""Real read-only SQL selection and a small file-backed polling scheduler."""

import sqlite3
import time
import uuid

from .messages import TaskMessage
from .regions import RegionalService, local_service, require_state_path
from .storage import SimulationError, file_lock, object_value, positive_int, read_json, stable_hash, text_value, write_json


class TaskTrigger(RegionalService):
    def __init__(self, config, broker, business_path):
        super().__init__(config.region)
        self.config, self.broker, self.business_path = config, broker, business_path

    @local_service
    def select_ids(self, sql: str) -> list[str]:
        text_value(sql, "sql")
        require_state_path(self.business_path, self.region)
        if not self.business_path.exists():
            raise SimulationError("Business data is missing; run init first")
        try:
            connection = sqlite3.connect(self.business_path.resolve().as_uri() + "?mode=ro", uri=True)
            try:
                connection.execute("PRAGMA query_only = ON")
                allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ,
                           sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
                connection.set_authorizer(lambda action, *args: sqlite3.SQLITE_OK
                                          if action in allowed else sqlite3.SQLITE_DENY)
                deadline = time.monotonic() + 5
                connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
                cursor = connection.execute(sql)
                if cursor.description is None:
                    raise SimulationError("SQL must select a column named id")
                columns = [column[0] for column in cursor.description]
                if columns.count("id") != 1:
                    raise SimulationError("SQL must select exactly one column named id")
                position = columns.index("id")
                ids = []
                seen = set()
                for row in cursor:
                    value = row[position]
                    if type(value) not in (str, int) or not str(value).strip():
                        raise SimulationError("SQL id values must be nonempty strings or integers")
                    value = str(value)
                    if value not in seen:
                        ids.append(value)
                        seen.add(value)
                return ids
            finally:
                connection.close()
        except sqlite3.Error as exc:
            raise SimulationError(f"SQL selection failed: {exc}") from exc

    @local_service
    def submit(self, dataset, sql, mode="realtime", *, task_id=None):
        snapshot = self.config.snapshot()
        snapshot.dataset(dataset)
        ids = self.select_ids(sql)
        task_id = task_id or uuid.uuid4().hex
        task = TaskMessage.parse({"task_id": task_id, "dataset": dataset, "ids": ids, "mode": mode})
        message_id = self.broker.publish(snapshot.task_queue, "task", task.payload(),
                                         stable_hash(["task", task_id]))
        return {"task_id": task_id, "message_id": message_id, "queue": snapshot.task_queue,
                "selected_ids": ids, "count": len(ids)}


class Scheduler(RegionalService):
    def __init__(self, jobs_path, state_path, trigger):
        super().__init__(trigger.region)
        self.jobs_path, self.state_path, self.trigger = jobs_path, state_path, trigger

    @local_service
    def tick(self, now=None):
        now = time.time() if now is None else now
        require_state_path(self.state_path, self.region)
        config = object_value(read_json(self.jobs_path), "jobs config")
        if config.get("region", "va") != self.region:
            raise SimulationError(f"{self.region} scheduler can only read its own jobs config")
        jobs = config.get("jobs")
        if not isinstance(jobs, list):
            raise SimulationError("jobs must be a list")
        names = set()
        # Validate all jobs before triggering any, so invalid config fails clearly.
        for job in jobs:
            object_value(job, "job")
            name = text_value(job.get("name"), "job.name")
            if name in names:
                raise SimulationError(f"Duplicate job: {name}")
            names.add(name)
            if type(job.get("enabled")) is not bool:
                raise SimulationError(f"{name}: enabled must be boolean")
            if job.get("kind") not in ("once", "periodic"):
                raise SimulationError(f"{name}: kind must be once or periodic")
            if job["kind"] == "periodic":
                positive_int(job.get("interval_seconds"), "interval_seconds")
            text_value(job.get("dataset"), "job.dataset")
            text_value(job.get("sql"), "job.sql")
            if job.get("mode") not in ("realtime", "forward"):
                raise SimulationError(f"{name}: mode must be realtime or forward")
        results = []
        with file_lock(self.state_path.with_suffix(".lock")):
            state = read_json(self.state_path) if self.state_path.exists() else {}
            for job in jobs:
                if not job["enabled"]:
                    continue
                previous = state.get(job["name"])
                if previous and (job["kind"] == "once" or
                                 now < previous["last_run"] + job["interval_seconds"]):
                    continue
                runs = previous["runs"] if previous else 0
                task_id = stable_hash(["scheduled", job, runs])
                try:
                    result = self.trigger.submit(job["dataset"], job["sql"], job["mode"], task_id=task_id)
                except SimulationError as exc:
                    results.append({"job": job["name"], "error": str(exc)})
                    continue
                state[job["name"]] = {"last_run": now, "runs": runs + 1}
                write_json(self.state_path, state)
                results.append({"job": job["name"], **result})
        return results
