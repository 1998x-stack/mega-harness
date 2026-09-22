"""Minimal local queue management CLI (trusted handlers via Python API)."""
from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict

from .store import TaskStore
from .worker import Worker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mega-harness")
    parser.add_argument("--db", default=".mega-harness/tasks.sqlite3", help="Local SQLite database path")
    sub = parser.add_subparsers(dest="command", required=True)
    submit = sub.add_parser("submit", help="Submit a built-in echo task")
    submit.add_argument("--payload", required=True, help="JSON object")
    submit.add_argument("--key", help="Idempotency key")
    submit.add_argument("--priority", type=int, default=0)
    submit.add_argument("--max-attempts", type=int, default=3)
    worker = sub.add_parser("worker", help="Run the built-in echo worker")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--poll", type=float, default=0.5)
    status = sub.add_parser("status", help="Show a task")
    status.add_argument("id")
    events = sub.add_parser("events", help="Show durable task events")
    events.add_argument("id")
    cancel = sub.add_parser("cancel", help="Cancel a queued or running task")
    cancel.add_argument("id")
    sub.add_parser("list", help="List recent tasks")
    args = parser.parse_args(argv)
    store = TaskStore(args.db)
    if args.command == "submit":
        try:
            payload = json.loads(args.payload)
            task_id = store.submit("echo", payload, priority=args.priority,
                                   max_attempts=args.max_attempts, idempotency_key=args.key)
        except (ValueError, TypeError) as exc:
            parser.error(str(exc))
        print(task_id)
    elif args.command == "worker":
        if args.poll <= 0:
            parser.error("--poll must be positive")
        logging.basicConfig(level=logging.INFO)
        runner = Worker(store, {"echo": lambda payload: payload})
        try:
            while True:
                did_work = runner.run_once()
                if args.once:
                    break
                if not did_work:
                    time.sleep(args.poll)
        except KeyboardInterrupt:
            return 130
    elif args.command == "status":
        task = store.get(args.id)
        if task is None:
            parser.error("unknown task id")
        print(json.dumps(asdict(task), ensure_ascii=False))
    elif args.command == "events":
        print(json.dumps(store.events(args.id), ensure_ascii=False))
    elif args.command == "cancel":
        print("cancelled" if store.cancel(args.id) else "not-cancellable")
    elif args.command == "list":
        print(json.dumps([asdict(task) for task in store.list_tasks()], ensure_ascii=False))
    return 0
