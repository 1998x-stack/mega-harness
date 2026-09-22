import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from mega_harness import LostLease, TaskStore, Worker
from mega_harness.cli import main


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.now = [1000.0]
        self.path = Path(self.temp.name) / "tasks.db"
        self.store = TaskStore(self.path, clock=lambda: self.now[0])

    def test_persists_across_instances(self):
        task_id = self.store.submit("echo", {"hello": "world"})
        other = TaskStore(self.path)
        self.assertEqual(other.get(task_id).payload, {"hello": "world"})

    def test_claim_and_success(self):
        task_id = self.store.submit("echo", {"x": 1})
        claimed = self.store.claim("worker")
        self.assertEqual(claimed.attempts, 1)
        self.assertEqual(claimed.lease_owner, "worker")
        self.assertIsNone(self.store.claim("other"))
        self.store.succeed(task_id, claimed.lease_token, {"done": True})
        self.assertEqual(self.store.get(task_id).result, {"done": True})
        self.assertEqual(self.store.get(task_id).status, "succeeded")
        self.assertEqual([e["event"] for e in self.store.events(task_id)], ["submitted", "claimed", "succeeded"])

    def test_idempotency_and_conflict(self):
        task_id = self.store.submit("echo", {"x": 1}, idempotency_key="k")
        self.assertEqual(task_id, self.store.submit("echo", {"x": 1}, idempotency_key="k"))
        with self.assertRaises(ValueError):
            self.store.submit("echo", {"x": 2}, idempotency_key="k")
        self.assertEqual(len(self.store.events(task_id)), 1)

    def test_priority(self):
        low = self.store.submit("echo", {}, priority=0)
        high = self.store.submit("echo", {}, priority=10)
        self.assertEqual(self.store.claim("w").id, high)
        self.assertEqual(self.store.claim("w").id, low)

    def test_retry_backoff_and_limit(self):
        task_id = self.store.submit("echo", {}, max_attempts=2)
        first = self.store.claim("w")
        self.store.fail(task_id, first.lease_token, "failed")
        self.assertEqual(self.store.get(task_id).status, "queued")
        self.assertIsNone(self.store.claim("w"))
        self.now[0] += 1
        second = self.store.claim("w")
        self.assertEqual(second.attempts, 2)
        self.store.fail(task_id, second.lease_token, "failed again")
        self.assertEqual(self.store.get(task_id).status, "failed")
        self.assertIsNone(self.store.claim("w"))

    def test_lease_expiry_fences_old_worker(self):
        task_id = self.store.submit("echo", {})
        old = self.store.claim("old", lease_seconds=3)
        self.now[0] += 4
        new = self.store.claim("new", lease_seconds=3)
        self.assertEqual(new.id, task_id)
        self.assertNotEqual(old.lease_token, new.lease_token)
        with self.assertRaises(LostLease):
            self.store.succeed(task_id, old.lease_token, "stale")
        self.store.succeed(task_id, new.lease_token, "fresh")
        self.assertEqual(self.store.get(task_id).result, "fresh")

    def test_expired_final_attempt_fails(self):
        task_id = self.store.submit("echo", {}, max_attempts=1)
        self.store.claim("lost", lease_seconds=2)
        self.now[0] += 3
        self.assertIsNone(self.store.claim("next"))
        self.assertEqual(self.store.get(task_id).status, "failed")

    def test_heartbeat_extends_lease(self):
        task_id = self.store.submit("echo", {})
        claim = self.store.claim("w", lease_seconds=3)
        self.now[0] += 2
        self.assertTrue(self.store.heartbeat(task_id, claim.lease_token, lease_seconds=3))
        self.now[0] += 2
        self.assertIsNone(self.store.claim("other"))
        self.now[0] += 2
        self.assertFalse(self.store.heartbeat(task_id, claim.lease_token))

    def test_cancel_fences_active_worker(self):
        task_id = self.store.submit("echo", {})
        claim = self.store.claim("w")
        self.assertTrue(self.store.cancel(task_id))
        self.assertFalse(self.store.cancel(task_id))
        with self.assertRaises(LostLease):
            self.store.fail(task_id, claim.lease_token, "late")
        self.assertEqual(self.store.get(task_id).status, "cancelled")

    def test_parallel_claims_are_unique(self):
        ids = {self.store.submit("echo", {"i": i}) for i in range(20)}
        def claim(i):
            return TaskStore(self.path, clock=lambda: self.now[0]).claim(f"worker-{i}")
        with ThreadPoolExecutor(max_workers=8) as pool:
            claimed = list(pool.map(claim, range(20)))
        self.assertEqual({t.id for t in claimed}, ids)
        self.assertTrue(all(t is not None for t in claimed))

    def test_worker_success_and_unknown_handler(self):
        ok = self.store.submit("echo", {"a": 2})
        missing = self.store.submit("unknown", {}, max_attempts=1)
        worker = Worker(self.store, {"echo": lambda payload: payload})
        self.assertTrue(worker.run_once())
        self.assertTrue(worker.run_once())
        self.assertFalse(worker.run_once())
        self.assertEqual(self.store.get(ok).result, {"a": 2})
        self.assertEqual(self.store.get(missing).status, "failed")

    def test_worker_exception_retries(self):
        task_id = self.store.submit("explode", {}, max_attempts=2)
        worker = Worker(self.store, {"explode": lambda _: 1 / 0})
        self.assertTrue(worker.run_once())
        self.assertEqual(self.store.get(task_id).status, "queued")
        self.now[0] += 1
        self.assertTrue(worker.run_once())
        self.assertEqual(self.store.get(task_id).status, "failed")

    def test_inputs_and_json(self):
        for invalid in ([], "text", 12):
            with self.assertRaises(ValueError):
                self.store.submit("echo", invalid)
        with self.assertRaises(ValueError):
            self.store.submit("echo", {"nan": float("nan")})
        with self.assertRaises(ValueError):
            self.store.submit("echo", {}, max_attempts=0)
        with self.assertRaises(ValueError):
            self.store.submit("echo", {}, priority=True)

    def test_cli_round_trip(self):
        import contextlib
        import io
        output = io.StringIO()
        args = ["--db", str(self.path)]
        with contextlib.redirect_stdout(output):
            self.assertEqual(main(args + ["submit", "--payload", '{"message":"hello"}']), 0)
        task_id = output.getvalue().strip()
        self.assertTrue(task_id)
        self.assertEqual(main(args + ["worker", "--once"]), 0)
        self.assertEqual(self.store.get(task_id).status, "succeeded")


if __name__ == "__main__":
    unittest.main()
