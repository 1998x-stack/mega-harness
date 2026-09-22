# Architecture and rollout plan

## M1 (implemented): local durable coordination

`TaskStore` stores JSON tasks and a monotonic event journal in SQLite. `BEGIN IMMEDIATE`
serializes claims, lease transitions, cancellation, and completion. Each task gets a
fresh random lease token on claim; only the current unexpired holder can finalize
its result. `Worker` runs explicitly registered handlers and renews the lease in a
background thread. Crashed workers are recovered on the next claim. Failed handlers
retry with bounded exponential backoff up to `max_attempts`.

**Delivery semantics:** at least once. A handler can produce side effects before
its result is committed. Lease expiry or cancellation does not stop handler code
already executing; stale completion is rejected, not rolled back. External tools
must use idempotency keys and compensating actions when appropriate.

**Scope:** one host with local filesystem storage, not SQLite over NFS, not a
multi-host distributed queue. The SQLite database is not a security boundary;
use a trusted directory. Handlers run as trusted local code. No untrusted task
payload is executed or imported as Python code.

## M2: package lifecycle and Agent integration (not implemented)

Define a versioned handler/extension manifest, permission allowlist, provenance
checks, and explicit registration. Provide an adapter for `mid_harness.Agent` that
isolates workspaces, scopes tool permissions, persists the resumable session
boundary, and maps cancellation to cooperative handler signals. Add integration
and fault-injection tests for actual provider/agent execution.

## M3: real distributed execution (not implemented)

Introduce a `QueueBackend` contract and a transactional shared backend (for example
PostgreSQL), remote worker authentication, registration, capability-aware routing,
heartbeats, bounded concurrency, dead-letter/replay policy, schema migration,
metrics/tracing, and an operator dashboard. Revisit fencing at the external side
effect boundary and test network partitions, clock skew, and rolling upgrades.
