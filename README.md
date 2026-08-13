# tarsk

A Python task queue whose workers hold a memory ceiling you set — without losing a task.

`task` with `rust` through the middle. The scheduler, retry state machine, lease tracking and
child supervision are Rust; the only Python in the hot path is your handler.

> **Status: 0.1.0, not yet on PyPI.** The core works and is tested; the wheel builds and
> installs. See [What is missing](#what-is-missing).

```python
from tarsk import App

app = App(broker="redis://localhost:6379/0")

@app.task(retries=3, timeout=30, queue="heavy")
def embed_document(doc_id: str) -> dict:
    ...

task_id = embed_document.send("abc")
```

`send()` hands back an id, not a future. Nothing is written anywhere unless the task asks for
it — `@app.task(result_ttl=3600)`, then `app.result(task_id).get(timeout=30)`. Writing every
result to a broker nobody reads from is most of why Celery feels heavy, so it is off by
default, and a stored result always expires.

```bash
tarsk worker --app myapp:app --broker redis://localhost:6379/0 \
             --queues heavy --children 4 --max-rss 400MB --metrics 0.0.0.0:9090
```

**Handlers must be idempotent.** Delivery is at-least-once. This is a contract, not a footnote:
a worker killed mid-task will run that task again.

![child RSS over one hour under a leaky handler](demo/one-hour.svg)

One hour, 72,001 tasks, a handler that never frees anything. 66 recycles, peak 400 MB against a
400 MB ceiling, nothing killed, nothing lost. Reproduce with `python demo/run.py`; the full
write-up, including the columns where tarsk loses, is in [demo/show-hn.md](demo/show-hn.md).

## What it does that others do not

Every Python task queue leaks, because leaks come from the code they run rather than from the
queue. The difference is what the runtime does about it.

`--max-rss` is a byte budget. Celery's `--max-tasks-per-child` is a task count, which only
bounds bytes if you already know how many bytes a task costs — and stays wrong once that
changes. From [`bench/`](bench/README.md), same configuration, different workloads:

| | leak 20MB/task | leak 40MB/task | payload-dependent 2–80MB |
|---|---|---|---|
| celery `--max-tasks-per-child=6` | **170 MB** | 290 MB | 335 MB |
| tarsk `--max-rss=200MB` | 205 MB | **225 MB** | **247 MB** |

Celery wins the first column, and that is the honest result: when the leak per task is known
and constant, dividing the budget by it works. It is the other two columns that tarsk was
built for — nothing was reconfigured between them.

Recycling is also free, which it usually is not. A replacement child is started before the
trigger fires, so the slot never goes empty:

| | p50 gap | p99 gap | max gap |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 5.7 ms | 156 ms | 157 ms |
| tarsk `--max-tasks=20` | 5.4 ms | 9 ms | **13 ms** |
| taskiq (no recycling at all) | 6.2 ms | 7 ms | 7 ms |

And the worker that runs your code is smaller, because it imports your tasks and nothing else —
no broker driver, no scheduler: 16 MB against Celery's 49 MB and taskiq's 58 MB.

## What it does not claim

**Not faster.** Dispatch costs microseconds; real handlers run for 50ms to minutes. With a 50ms
handler tarsk, Celery and taskiq all reach 19 tasks/s — identical, as they should be. Anyone
selling a task queue on throughput benchmarks is selling the wrong thing.

**Not a hard ceiling regardless of task size.** The ceiling is read while a child is idle, so a
child never *starts* a task it cannot afford — but a handler that allocates 300MB will allocate
it. Overshoot is bounded by one task's peak, not by zero. A ceiling that killed running work
could not make an oversized task fit; it would only turn "completes, briefly over budget" into
"never completes".

**Not durable execution.** That is Temporal's category, and it is much heavier.

## How it works

```
supervisor (Rust)                    children (Python)
  broker driver, leases                imports ONLY your task modules
  retry + dead-letter                  loop: recv → call → reply
  RSS monitoring, recycling          
  Prometheus endpoint                  RSS = interpreter + your code
        └── unix socket, length-prefixed msgpack ──┘
```

The supervisor never imports your app module and the children never import the broker. That is
what makes both footprints numbers worth publishing — and why `--broker` is a flag rather than
something read off your `App`.

Recycle triggers: `--max-rss`, `--max-tasks`, `--max-lifetime`, whichever comes first. A child
is drained, never killed outright: it finishes what it is holding.

`--hard-max-rss` is the exception, and it is off by default. Set it and a child that reaches it
mid-task is killed rather than allowed to keep growing — the task is retried, and if it cannot
fit it is dead-lettered instead of retried forever. It cannot make an oversized task succeed;
it decides who loses when one task threatens the box. It must sit above `--max-rss`, or the
graceful ceiling could never fire.

## Scheduling

`send_in(60, …)` runs a task once, later. `@app.task(cron="*/5 * * * *")` runs it on a
schedule — five fields, evaluated in UTC.

UTC is the whole timezone story, deliberately. A local schedule has to answer what "02:30
daily" means on the night a clock jumps, and every answer is someone's outage.

The schedule lives with the supervisor, because that is the only process holding the registry
and no user code. Firing is elected per minute through the broker, so running ten workers still
runs the task once.

## Brokers

Redis Streams and Postgres. Both at-least-once, both leasing per task rather than per worker,
neither needing lease renewal — a task's timeout is capped, so a lease cannot outlive a known
ceiling.

Failures follow the task's own `retries` and `backoff`, and neither backend needed a delay
queue for it: Postgres already has a visibility timer, and Redis has the idle clock its pending
sweep reads. What runs out of retries lands in `tarsk:{queue}:dead` or the `tarsk_dead` table,
with the error and traceback.

## Metrics

`--metrics HOST:PORT` serves Prometheus text from the supervisor. Nothing is sampled for the
sake of metrics — the counters sit on paths that already existed and child RSS is the reading
the supervision loop takes anyway, so a Python worker never learns it is being observed.

Including `tarsk_supervisor_rss_bytes`, so the constant this project calls constant can be
checked rather than believed.

## What is missing

- Not on PyPI yet — the wheel builds and installs, it just has not been uploaded
- No cron or recurring schedule — `send_in(60, ...)` covers one-off delays only
- No chains, groups or chords
- Linux and macOS only; Windows needs a decision about the IPC transport

## Running the tests

```bash
maturin develop --release   # after switching to abi3, delete any stale
                            # tarsk/_core.cpython-*.so first: CPython prefers
                            # the version-tagged file over _core.abi3.so
python tests/test_ipc.py        # protocol, timeouts, retries
python tests/test_recycle.py    # the memory ceiling and overlap replacement
python tests/test_brokers.py    # redis and postgres, including lease expiry
python demo/run.py --minutes 3 --ceiling 200MB --rate 25   # the sawtooth
```

The broker tests start their own `redis-server` and Postgres cluster and skip whichever is not
installed.

## License

MIT — see [LICENSE](LICENSE).
