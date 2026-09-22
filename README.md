# mega-harness

**Generation 3 harness — make the loop distributable.** Current milestone:
**M1, durable local multi-worker orchestration**. This is an implementation
foundation, not yet a network-distributed agent runtime.

`mega-harness` coordinates task execution above an Agent loop rather than
reimplementing model reasoning. Python 3.11+; no runtime third-party dependencies.
It is designed to later integrate `mid-harness` Agents and its hooks/MCP/skills.

## Quick start

```bash
python -m pip install -e .
mega-harness --db ./tasks.sqlite3 submit --payload '{"message":"hello"}' --key example-1
mega-harness --db ./tasks.sqlite3 worker --once
mega-harness --db ./tasks.sqlite3 list
# Inspect a task by ID returned from submit:
mega-harness --db ./tasks.sqlite3 status TASK_ID
mega-harness --db ./tasks.sqlite3 events TASK_ID
```

The CLI intentionally runs only the built-in, non-executing `echo` handler.
For a custom handler, explicitly register trusted Python callables:

```python
from mega_harness import TaskStore, Worker

store = TaskStore("./tasks.sqlite3")
task_id = store.submit("summarize", {"text": "Hello"}, idempotency_key="summary-1")
worker = Worker(store, {"summarize": lambda payload: {"length": len(payload["text"])}})
worker.run_once()
print(store.get(task_id).result)
```

Launch multiple worker processes against the same **local** database to process
tasks concurrently. A task is claimed by only one worker at a time; heartbeat
renewal and expiring leases allow recovery when a worker disappears. Use
`python -m unittest discover -s tests -v` to run the offline regression suite.

## Guarantees and limitations

- Durable queue, task priority, submit idempotency keys, retries with exponential
  backoff, expiring fenced leases, heartbeat, cancellation, and replayable events.
- **At least once, not exactly once:** a handler may run twice after an interruption.
  Task submission idempotency does not make external handler side effects idempotent.
- Cancelling a running task fences its result but cannot terminate already-running
  Python code. Code must implement cooperative cancellation where necessary.
- SQLite is intended for trusted workers on **one machine** and a local filesystem;
  it is not the M3 remote distributed backend. Avoid untrusted handlers and secrets
  in task payloads or event messages.
- The CLI is a deliberately narrow demonstration. No arbitrary commands or remote
  task code are evaluated. Full Agent/MCP/skill integration and remote workers are
  future milestones, not shipped functionality.

See [architecture and phased rollout](docs/architecture.md) for the M2/M3 plan.

## Harness lineage

| Project | Focus |
| --- | --- |
| tiny-harness | Make the loop work |
| mid-harness | Make the loop extensible (hooks, MCP, skills) |
| **mega-harness** | Make the loop distributable, starting with durable coordination |

## License

MIT. See [LICENSE](LICENSE).
