"""Explicitly registered handlers; no task payload is executed as code."""
from __future__ import annotations

import logging
from threading import Event, Thread
from typing import Any, Callable, Mapping
from uuid import uuid4

from .store import LostLease, TaskStore

LOG = logging.getLogger(__name__)
Handler = Callable[[dict[str, Any]], Any]


class Worker:
    def __init__(self, store: TaskStore, handlers: Mapping[str, Handler], *,
                 worker_id: str | None = None, lease_seconds: float = 30):
        if not 0 < lease_seconds <= 86400:
            raise ValueError("lease_seconds must be in (0, 86400]")
        self.store = store
        self.handlers = dict(handlers)
        self.worker_id = worker_id or uuid4().hex
        self.lease_seconds = lease_seconds

    def run_once(self) -> bool:
        """Process one available task. Return False when the queue is idle."""
        task = self.store.claim(self.worker_id, lease_seconds=self.lease_seconds)
        if task is None:
            return False
        stop, lost = Event(), Event()

        def keep_alive() -> None:
            while not stop.wait(self.lease_seconds / 3):
                try:
                    if not self.store.heartbeat(task.id, task.lease_token, lease_seconds=self.lease_seconds):
                        lost.set()
                        return
                except Exception:
                    LOG.exception("Heartbeat failed for task %s", task.id)

        thread = Thread(target=keep_alive, name=f"heartbeat-{task.id}", daemon=True)
        thread.start()
        try:
            try:
                if task.kind not in self.handlers:
                    raise ValueError(f"No registered handler for task kind: {task.kind}")
                result = self.handlers[task.kind](task.payload)
            except Exception as exc:
                failure = f"{type(exc).__name__}: {exc}"[:2000]
                result = None
            else:
                failure = None
        finally:
            stop.set()
            thread.join()
        if lost.is_set():
            LOG.warning("Discarding result after losing lease for task %s", task.id)
            return True
        try:
            if failure is None:
                self.store.succeed(task.id, task.lease_token, result)
            else:
                self.store.fail(task.id, task.lease_token, failure)
        except LostLease:
            LOG.warning("Task %s lease changed before completion", task.id)
        return True
