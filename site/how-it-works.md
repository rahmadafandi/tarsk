---
title: How it works
---

# How it works

The shape of a worker, where your imports land, and what runs where.

[← Back to the index](./)

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

`--slots N` lets one child hold N tasks at once, and it costs precision in exactly the place
this project claims it. At one slot the ceiling is read while the child has nothing running, so
overshoot is bounded by a single task's peak. At 64 it is bounded by whatever 64 tasks are
holding, and a hard kill takes all 64 down together.

**The default is 100 slots**, whatever else is set — taskiq's number, and tarsk is tuned out
of the box for handlers that wait. 2,000 awaiting tasks with no flags at all drain in 0.96s on
74MB, against taskiq's 1.31s on 145MB. The same default holds 40 tasks leaking 20MB each to
123MB under a `--max-rss=200MB` ceiling, where `--slots 1` gives 183MB — lower at a hundred
slots than at one, because a child holding several tasks climbs faster between two RSS readings
and so meets the ceiling's projection sooner.

**This used to be the flag you had to know about.** A hundred slots handed a child a hundred
tasks before any of them had allocated anything, the ceiling was next read long after the
damage, and that row said 824MB. The supervisor now learns what a task costs in bytes and
refuses to fill a slot the ceiling cannot afford, so the same default fits both workloads
without being told which one it is running. `--max-rss` still does not move `--slots`: one flag
quietly changing another's default is worse than either number being wrong. It budgets against
it instead.

Raise it for handlers that *wait* and allocate little; set it to 1 for handlers that allocate,
which is the case the ceiling exists for. For a mix, run one worker per queue rather than
splitting the difference:

```
tarsk worker --queues io    --slots 64 --max-rss 400MB
tarsk worker --queues heavy --slots 1  --max-rss 400MB
```

The safe ceiling for slots is arithmetic: `(max_rss − child baseline) / peak allocation per
task`. A 27MB child under a 400MB ceiling running tasks that peak at 5MB gives about 74.

Sync handlers get slots too — they run in threads, and the pool is sized to the slot count
rather than left at asyncio's default of `min(32, cpu + 4)`, which would otherwise cap
`--slots 64` at twenty without saying so. Threads only overlap *waiting*: a sync handler that
computes holds the GIL and gains nothing.

## Where setup belongs

`@app.on_start` runs in each worker process before it takes work, and `@app.on_stop` as it
drains. Opening a connection pool at import time instead puts it in every worker's baseline
RSS — the number `--max-rss` is measured against — so it spends the budget the ceiling exists
to protect, in every worker, forever.

**Once per process, not once per deployment.** Workers are replaced whenever they hit a
recycle limit, so both hooks run again every time — in the hour-long run above that would have
been 66 times. Pools and clients belong here; migrations and "we are up" notifications do not.

Hooks run before the worker registers, so whatever they open is inside the baseline the
supervisor checks against the ceiling. A pool that does not fit is refused at startup rather
than discovered as a worker that recycles instantly.

## Platforms

Linux, macOS and Windows, each running the test suite in CI on every push, across 3.11 to 3.14
including the free-threaded build.

The supervisor-to-child channel is a Unix socket where there is one and a named pipe on
Windows. Not a loopback port on either: it carries task payloads and is believed when it says a
job finished, and a port is reachable by every process on the machine — inside a Kubernetes Pod
that includes every sidecar. Both of the chosen ones are protected, by directory permissions
and by an ACL.

Two differences worth knowing. Windows has no SIGTERM, so the polite step before terminating a
child is skipped; the supervisor has already sent a `Drain` frame and waited by then, so this
was the second escalation rather than the first. And on Windows the ceiling measures the worker
*and its descendants*, because a venv's `python.exe` there can be a launcher that runs the real
interpreter as a child — measured against only the pid it spawned, the supervisor watched a
four-megabyte stub while the worker held three hundred, and never recycled.

Windows CI skips the broker suites, since neither Redis nor Postgres ships for it. The channel,
the protocol, recycling and the ceiling are all covered.


---

[← Back to the index](./)
