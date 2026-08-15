---
title: tarsk
---

# tarsk

A Python task queue whose workers hold a memory ceiling you set — without losing a task.

`task` with `rust` through the middle. The scheduler, retry state machine, lease tracking and
child supervision are Rust; the only Python in the hot path is your handler.

```bash
pip install tarsk        # not yet: see the status note in the README
```

```python
from tarsk import App

app = App(broker="redis://localhost:6379/0")

@app.task(retries=3, timeout=30, queue="heavy")
def embed_document(doc_id: str) -> dict:
    ...

task_id = embed_document.send("abc")
```

```bash
tarsk worker --app myapp:app --broker redis://localhost:6379/0 \
             --queues heavy --children 4 --max-rss 400MB --metrics 0.0.0.0:9090
```

**Handlers must be idempotent.** Delivery is at-least-once. This is a contract, not a footnote:
a worker killed mid-task will run that task again.

## Pages

| | |
|---|---|
| [How it works](how-it-works) | The shape of a worker, where your imports land, what runs where |
| [Writing tasks](tasks) | Timeouts, soft timeouts, retries, expiry, deduplication, rate limits, concurrency caps |
| [Routing and scheduling](routing) | Queue priority, brokers, chains and groups, cron, the async producer |
| [Operating](operating) | The console, metrics, the backlog, dead letters, cancellation, the test suites |
| [Benchmarks](benchmarks) | Against Celery and taskiq, including every column where tarsk loses |

## The claim, in one chart

![child RSS over one hour under a leaky handler, and the supervisor holding it](https://raw.githubusercontent.com/rahmadafandi/tarsk/main/demo/one-hour.svg)

One hour, 72,001 tasks, a handler that never frees anything. 66 recycles, peak 400 MB against a
400 MB ceiling, no sample over it, nothing killed, nothing lost.

The sawtooth is the children being held under the ceiling. The flat line beneath it, on the
same axis, is the supervisor doing the holding — 28.32 to 28.40 MB across the hour, a drift of
82 KB. A ceiling enforced from inside a process with the same problem would only defer it, so
the enforcer staying put is half the design.

Reproduce both with `python demo/run.py`, which exits non-zero if the supervisor drifts.

## Source

[github.com/rahmadafandi/tarsk](https://github.com/rahmadafandi/tarsk)
