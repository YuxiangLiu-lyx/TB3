import argparse
import json
from pathlib import Path
import sys
import time

from .runtime import Runtime
from .storage import SimulationError, read_json


def parser():
    root = argparse.ArgumentParser(description="Regional file-backed vector index simulator")
    root.add_argument("--config-dir", default="config")
    root.add_argument("--state-dir", default="state")
    root.add_argument("--fixtures-dir", default="fixtures")
    root.add_argument("--region", default="va", help="Calling deployment region")
    root.add_argument("--cluster", type=Path, help="Region deployment manifest; enables Mirror transport")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="Seed local SQL/forward data; existing data is preserved")
    commands.add_parser("validate", help="Read and validate current Conf, TCC and queue topology")
    commands.add_parser("topology", help="Show actual queue bindings, consumers and forwarding routes")
    submit = commands.add_parser("submit", help="Select business IDs with SQL and enqueue a task")
    submit.add_argument("--dataset", required=True)
    submit.add_argument("--sql", required=True)
    submit.add_argument("--mode", choices=["realtime", "forward"], default="realtime")
    write = commands.add_parser("write", help="Submit SQL and wait for actual writes or errors")
    write.add_argument("--dataset", required=True)
    write.add_argument("--sql", required=True)
    write.add_argument("--mode", choices=["realtime", "forward"], default="realtime")
    write.add_argument("--limit", type=int, default=10000)
    send = commands.add_parser("send", help="Deliver a message to its configured consumer")
    send.add_argument("--queue", required=True)
    send.add_argument("--kind", choices=["task", "write"], required=True)
    payload = send.add_mutually_exclusive_group(required=True)
    payload.add_argument("--payload", type=json.loads)
    payload.add_argument("--payload-file", type=Path)
    send.add_argument("--enqueue-only", action="store_true")
    send.add_argument("--limit", type=int, default=10000)
    process = commands.add_parser("process", help="Run local consumers until empty or the limit")
    process.add_argument("--stage", choices=["all", "preprocess", "write"], default="all")
    process.add_argument("--queue")
    process.add_argument("--limit", type=int, default=10000)
    process.add_argument("--all-regions", action="store_true", help="Pump the cluster's configured consumers")
    queues = commands.add_parser("queues", help="Queue counts or actual message envelopes")
    queues.add_argument("--queue")
    queues.add_argument("--status", choices=["pending", "done", "failed"], default="pending")
    retry = commands.add_parser("retry", help="Move a queue's failed messages back to pending")
    retry.add_argument("--queue", required=True)
    feature = commands.add_parser("feature", help="Call the mock VA feature service")
    feature.add_argument("--id", required=True)
    feature.add_argument("--feature", required=True)
    feature.add_argument("--mode", choices=["realtime", "forward"], default="realtime")
    recall = commands.add_parser("recall", help="Query by corpusID; ranking is a mock")
    recall.add_argument("--corpus-id", type=int, required=True)
    recall.add_argument("--embedding", type=json.loads, required=True)
    recall.add_argument("--top-k", type=int)
    recall.add_argument("--return-attr", action=argparse.BooleanOptionalAction, default=None)
    recall.add_argument("--return-embedding", action=argparse.BooleanOptionalAction, default=None)
    verify = commands.add_parser("verify", help="Verify actual write/recall outcomes in fresh state directories")
    verify.add_argument("--cases", type=Path, default=Path("cases/correct_request.json"))
    verify.add_argument("--case", dest="case_id", help="Run one named case")
    commands.add_parser("dump", help="Inspect persisted vector records by physical database")
    commands.add_parser("tick", help="Submit currently due scheduled jobs once")
    worker = commands.add_parser("worker", help="Poll scheduled jobs and process queues until Ctrl-C")
    worker.add_argument("--poll-seconds", type=float, default=1)
    commands.add_parser("demo", help="Initialize, submit active products, process and recall")
    return root


def emit(value):
    print(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), flush=True)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        cluster = None
        if args.cluster:
            from .cluster import Cluster
            cluster = Cluster(args.cluster, args.state_dir)
            runtime = cluster.runtime(args.region)
        else:
            runtime = Runtime(args.config_dir, args.state_dir, args.fixtures_dir, region=args.region)
        command = args.command
        exit_code = 0
        if command == "init":
            result = cluster.initialize() if cluster else runtime.initialize()
        elif command == "validate":
            if cluster:
                result = cluster.validate()
            else:
                snapshot = runtime.config.snapshot()
                result = {"valid": True, "region": runtime.region, "indexes": len(snapshot.indexes),
                          "datasets": list(snapshot.datasets), "queues": runtime.broker.names()}
        elif command == "topology":
            result = cluster.topology() if cluster else runtime.topology()
        elif command == "submit":
            result = runtime.trigger.submit(args.dataset, args.sql, args.mode)
        elif command == "write":
            result = runtime.write(args.dataset, args.sql, args.mode, limit=args.limit)
            exit_code = 0 if result["status"] == "done" else 1
        elif command == "send":
            payload = read_json(args.payload_file) if args.payload_file else args.payload
            result = runtime.send(args.queue, args.kind, payload, wait=not args.enqueue_only, limit=args.limit)
            exit_code = 0 if args.enqueue_only or result["status"] == "done" else 1
        elif command == "verify":
            from .verification import verify
            result = verify(runtime, args.cases, case_id=args.case_id, cluster=cluster)
            exit_code = 0 if result["passed"] else 1
        elif command == "process":
            if args.all_regions:
                if not cluster or args.queue or args.stage != "all":
                    raise SimulationError("--all-regions requires --cluster and cannot filter queue/stage")
                result = cluster.process(limit=args.limit)
            else:
                result = runtime.process(args.stage, args.queue, args.limit)
            exit_code = 1 if result["failed"] else 0
        elif command == "queues":
            result = (runtime.broker.messages(args.queue, args.status) if args.queue
                      else runtime.broker.status())
        elif command == "retry":
            result = {"queue": args.queue, "retried": runtime.broker.retry(args.queue)}
        elif command == "feature":
            result = {"embedding": runtime.features.compute(args.id, args.feature, args.mode)}
        elif command == "recall":
            result = runtime.recall.recall(args.corpus_id, args.embedding, top_k=args.top_k,
                                           return_attr=args.return_attr, return_embedding=args.return_embedding)
        elif command == "dump":
            result = runtime.dump()
        elif command == "tick":
            result = runtime.scheduler.tick()
            exit_code = 1 if any("error" in row for row in result) else 0
        elif command == "worker":
            if not 0.05 <= args.poll_seconds <= 3600:
                raise SimulationError("poll-seconds must be between 0.05 and 3600")
            cluster.initialize() if cluster else runtime.initialize()
            while True:
                scheduled = runtime.scheduler.tick()
                processed = cluster.process() if cluster else runtime.process()
                if scheduled or processed["processed"]:
                    emit({"scheduled": scheduled, "processed": processed})
                time.sleep(args.poll_seconds)
        elif command == "demo":
            if runtime.region != "va":
                raise SimulationError("demo is the VA baseline; MY migration is an unfinished exercise")
            runtime.initialize()
            submitted = runtime.trigger.submit("products", "SELECT id FROM business WHERE active = 1 ORDER BY id", "forward")
            processed = runtime.process()
            vector = runtime.features.compute("p1", "image_embedding", "forward")
            result = {"submitted": submitted, "processed": processed,
                      "vector_counts": {name: len(rows) for name, rows in runtime.dump().items()},
                      "compressed_recall": runtime.recall.recall(102, vector, return_attr=True)}
            exit_code = 1 if processed["failed"] else 0
        emit(result)
        return exit_code
    except (SimulationError, OSError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
