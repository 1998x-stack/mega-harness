"""Transactional, single-host SQLite task queue with fenced leases."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable, Iterator
from uuid import uuid4


class LostLease(RuntimeError):
    """The job was cancelled or the worker no longer owns its lease."""


@dataclass(frozen=True)
class Task:
    id: str
    kind: str
    payload: dict[str, Any]
    status: str
    attempts: int
    max_attempts: int
    priority: int
    available_at: float
    lease_owner: str | None
    lease_token: str | None
    lease_expires: float | None
    result: Any
    error: str | None


def _encode(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _task(row: sqlite3.Row) -> Task:
    return Task(
        id=row["id"], kind=row["kind"], payload=json.loads(row["payload"]),
        status=row["status"], attempts=row["attempts"],
        max_attempts=row["max_attempts"], priority=row["priority"],
        available_at=row["available_at"], lease_owner=row["lease_owner"],
        lease_token=row["lease_token"], lease_expires=row["lease_expires"],
        result=json.loads(row["result"]) if row["result"] is not None else None,
        error=row["error"],
    )


class TaskStore:
    """A SQLite-backed queue for workers on the same host.

    Each operation uses its own connection and explicit transaction; never share
    a connection across threads. The caller owns idempotency of handler effects.
    """

    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time):
        self.path = str(path)
        self.clock = clock
        if self.path != ":memory:":
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY, kind TEXT NOT NULL, payload TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed','cancelled')),
                    attempts INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0, available_at REAL NOT NULL,
                    lease_owner TEXT, lease_token TEXT, lease_expires REAL,
                    result TEXT, error TEXT, idempotency_key TEXT UNIQUE,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS tasks_ready ON tasks(status, available_at, priority DESC, created_at);
                CREATE INDEX IF NOT EXISTS tasks_expired ON tasks(status, lease_expires);
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
                    event TEXT NOT NULL, details TEXT NOT NULL, created_at REAL NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(id)
                );
                CREATE INDEX IF NOT EXISTS events_task ON events(task_id, seq);
            """)

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=5000")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except BaseException:
                db.rollback()
                raise
            else:
                db.commit()

    def _event(self, db: sqlite3.Connection, task_id: str, event: str, details: Any = None) -> None:
        db.execute("INSERT INTO events(task_id,event,details,created_at) VALUES(?,?,?,?)",
                   (task_id, event, _encode(details if details is not None else {}), self.clock()))

    def submit(self, kind: str, payload: dict[str, Any], *, max_attempts: int = 3,
               priority: int = 0, idempotency_key: str | None = None) -> str:
        if not kind or not isinstance(kind, str) or not isinstance(payload, dict):
            raise ValueError("kind must be a nonempty string and payload must be an object")
        if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
            raise ValueError("max_attempts must be an integer between 1 and 100")
        if type(priority) is not int:
            raise ValueError("priority must be an integer")
        if idempotency_key is not None and (not isinstance(idempotency_key, str) or not idempotency_key):
            raise ValueError("idempotency_key must be a nonempty string")
        encoded = _encode(payload)
        now, task_id = self.clock(), uuid4().hex
        with self._write() as db:
            if idempotency_key is not None:
                existing = db.execute("SELECT * FROM tasks WHERE idempotency_key=?", (idempotency_key,)).fetchone()
                if existing:
                    if (existing["kind"], existing["payload"], existing["max_attempts"], existing["priority"]) != (kind, encoded, max_attempts, priority):
                        raise ValueError("idempotency_key already used with different task arguments")
                    return existing["id"]
            db.execute("""INSERT INTO tasks(id,kind,payload,status,max_attempts,priority,available_at,
                         idempotency_key,created_at,updated_at) VALUES(?,?,?,'queued',?,?,?,?,?,?)""",
                       (task_id, kind, encoded, max_attempts, priority, now, idempotency_key, now, now))
            self._event(db, task_id, "submitted")
        return task_id

    def get(self, task_id: str) -> Task | None:
        with self._connection() as db:
            row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        return _task(row) if row else None

    def list_tasks(self, limit: int = 50) -> list[Task]:
        if not 1 <= limit <= 500:
            raise ValueError("limit must be between 1 and 500")
        with self._connection() as db:
            rows = db.execute("SELECT * FROM tasks ORDER BY created_at DESC, id DESC LIMIT ?", (limit,)).fetchall()
        return [_task(row) for row in rows]

    def events(self, task_id: str) -> list[dict[str, Any]]:
        with self._connection() as db:
            rows = db.execute("SELECT seq,event,details,created_at FROM events WHERE task_id=? ORDER BY seq", (task_id,)).fetchall()
        return [dict(seq=r["seq"], event=r["event"], details=json.loads(r["details"]), created_at=r["created_at"]) for r in rows]

    def claim(self, worker_id: str, *, lease_seconds: float = 30) -> Task | None:
        if not worker_id or not isinstance(worker_id, str) or not 0 < lease_seconds <= 86400:
            raise ValueError("worker_id must be nonempty and lease_seconds must be in (0, 86400]")
        with self._write() as db:
            now = self.clock()
            expired = db.execute("SELECT id,attempts,max_attempts FROM tasks WHERE status='running' AND lease_expires<=?", (now,)).fetchall()
            for row in expired:
                next_status = "failed" if row["attempts"] >= row["max_attempts"] else "queued"
                db.execute("""UPDATE tasks SET status=?,lease_owner=NULL,lease_token=NULL,lease_expires=NULL,
                              available_at=?,error='worker lease expired',updated_at=? WHERE id=?""",
                           (next_status, now, now, row["id"]))
                self._event(db, row["id"], "lease_expired", {"next_status": next_status})
            row = db.execute("""SELECT id FROM tasks WHERE status='queued' AND available_at<=?
                              ORDER BY priority DESC,created_at ASC,id ASC LIMIT 1""", (now,)).fetchone()
            if not row:
                return None
            token = uuid4().hex
            db.execute("""UPDATE tasks SET status='running',attempts=attempts+1,
                          lease_owner=?,lease_token=?,lease_expires=?,updated_at=? WHERE id=?""",
                       (worker_id, token, now + lease_seconds, now, row["id"]))
            self._event(db, row["id"], "claimed", {"worker_id": worker_id})
            claimed = db.execute("SELECT * FROM tasks WHERE id=?", (row["id"],)).fetchone()
            return _task(claimed)

    def heartbeat(self, task_id: str, token: str, *, lease_seconds: float = 30) -> bool:
        if not 0 < lease_seconds <= 86400:
            raise ValueError("lease_seconds must be in (0, 86400]")
        with self._write() as db:
            now = self.clock()
            result = db.execute("""UPDATE tasks SET lease_expires=?,updated_at=? WHERE id=?
                                AND status='running' AND lease_token=? AND lease_expires>?""",
                                (now + lease_seconds, now, task_id, token, now))
            return result.rowcount == 1

    def _finish(self, task_id: str, token: str, *, result: Any = None, error: str | None = None) -> None:
        encoded = _encode(result) if error is None else None
        with self._write() as db:
            now = self.clock()
            row = db.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
            if not row or row["status"] != "running" or row["lease_token"] != token or row["lease_expires"] <= now:
                raise LostLease(task_id)
            if error is None:
                status, available_at = "succeeded", row["available_at"]
            elif row["attempts"] >= row["max_attempts"]:
                status, available_at = "failed", row["available_at"]
            else:
                status, available_at = "queued", now + min(60, 2 ** (row["attempts"] - 1))
            db.execute("""UPDATE tasks SET status=?,available_at=?,result=?,error=?,
                          lease_owner=NULL,lease_token=NULL,lease_expires=NULL,updated_at=? WHERE id=?""",
                       (status, available_at, encoded, error, now, task_id))
            self._event(db, task_id, "succeeded" if error is None else "attempt_failed",
                        {"status": status, "error": error} if error is not None else {})

    def succeed(self, task_id: str, token: str, result: Any) -> None:
        self._finish(task_id, token, result=result)

    def fail(self, task_id: str, token: str, error: str) -> None:
        self._finish(task_id, token, error=str(error))

    def cancel(self, task_id: str) -> bool:
        with self._write() as db:
            now = self.clock()
            result = db.execute("""UPDATE tasks SET status='cancelled',lease_owner=NULL,lease_token=NULL,
                                lease_expires=NULL,updated_at=? WHERE id=? AND status IN ('queued','running')""",
                                (now, task_id))
            if result.rowcount:
                self._event(db, task_id, "cancelled")
            return result.rowcount == 1
