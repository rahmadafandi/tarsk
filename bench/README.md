# Benchmarks: tarsk vs Celery vs taskiq

```bash
python bench/run.py                    # every scenario
python bench/run.py memory oom gap     # a subset
```

Linux only: RSS comes from `/proc`, the hard-limit scenario uses `systemd-run --user --scope`.

By default the harness starts its own `redis-server` on a free port and tears it down, so it
never touches anything you are running. To use your own instead:

```bash
BENCH_REDIS=redis://127.0.0.1:6379/10 python bench/run.py
```

Nothing here calls `FLUSHALL` — which empties every database in an instance no matter which
one is selected, measured: a key in db 0 does not survive a `FLUSHALL` issued from db 10. The
harness uses `FLUSHDB`, so a database number in the URL is respected and the rest of your
instance is left alone. A throwaway server is still the better measurement, since it runs
without persistence and without other traffic.

## What is being claimed

Per spec §2, tarsk does **not** claim to be faster. The claims are about memory and about not
losing work. Scenarios `memory`, `shift`, `variable`, `oom` and `gap` test those. `throughput`
and `footprint` are here to show the cost and the baseline, not to win.

## Fairness rules this harness follows

- **Identical handler code.** All three workers import the same `bench/handlers.py`. The task
  body, the allocation, and the completion record are the same instructions everywhere.
- **Same concurrency.** One worker process, one task at a time: Celery `-c 1 -P prefork`,
  taskiq `--workers 1 --max-async-tasks 1 --max-threadpool-threads 1`, tarsk `children=1`.
- **No result backend anywhere.** Celery runs `task_ignore_result=True` because tarsk has no
  result backend yet; writing results to Redis for one runtime and not the others would be a
  free penalty.
- **Queue is preloaded.** Every task is submitted before the worker starts, so submission cost
  is outside the measurement and all runtimes begin against a full queue.
- **Completion is measured the same way.** Each handler appends one line to a shared file.
  No framework's own result plumbing is trusted or paid for.
- **Celery gets its best configuration, not its default one.** Where a Celery setting exists
  that answers the scenario — `--max-tasks-per-child`, `task_acks_late` — it gets its own row,
  tuned for the workload being run.
- **Warm-up is discarded** in the gap scenario: the first 10% of completions cover worker
  startup, which is not a recycling stall.

## Results

Linux 7.0.0, i7-13620H ×16, Python 3.14.3, Celery 5.6.3, taskiq 0.12.4, Redis 8.10.0.
One worker process, concurrency 1 throughout. Single run per row — these are not medians,
and anything inside ~10% of another number should be read as a tie.

Peak worker RSS is the larger of two lower bounds: the worker's own `ru_maxrss` high-water
mark, and `/proc` sampled every 50ms. Neither alone is trustworthy — `ru_maxrss` stops at the
last completed task, so it under-reports a process killed mid-task, and sampling misses any
spike shorter than the interval, which a fast handover makes likely.

### Where your imports land

`python bench/run.py imports`

The same two app modules under each runtime — one trivial, one importing celery + taskiq +
redis, which costs 47MB in a bare interpreter. The importing process names itself in a log
rather than being guessed at as "the biggest child", which picks wrong the moment a runtime
keeps several around.

| runtime | coordinator (light → heavy) | runs your code (light → heavy) | coordinator imports it | whole tree, heavy (Rss / Pss) |
|---|---|---|---|---|
| celery | 55 → 77 MB (+22) | 55 → 77 MB (+22) | **yes** | 141 / **99** MB |
| taskiq | 51 → 51 MB (+0) | 58 → 58 MB (+0) † | no | 144 / **91** MB |
| tarsk | 27 → 27 MB (+0) | 24 → 58 MB (+33) | no | 85 / **59** MB |

Celery's master imports your app and grows with it, then forks, so its children start from that
memory — which is why both its columns are the same number. taskiq's coordinator does not
import your code, and neither does tarsk's supervisor: **this is not unique to tarsk, and an
earlier draft of these notes implied it was.**

What differs is what the property is spent on. taskiq's coordinator starts workers and restarts
them when they die; it reads nobody's RSS and enforces nothing. tarsk's supervisor is the
enforcer — it reads each child's RSS from outside, decides when one retires, holds the leases
so a dead child's work comes back, and runs the retries and the schedule. That has to live
somewhere that outlives every child *and does not itself grow*: a memory ceiling cannot be held
from inside a process with the same problem. Celery's master could not do it at 77MB of your
imports, forking children from them.

**Two things this scenario taught us that correct the numbers elsewhere on this page.**

*Summed Rss over a process tree double-counts.* Every page shared between processes is counted
in full for each of them, and forking shares almost everything. Pss divides each shared page
among its mappers, which is why the tree columns above show both. Celery's tree is 141MB by Rss
and 99MB by Pss — 42MB of double counting — against tarsk's 85 and 59. The gap between the two
runtimes is real but roughly 40% smaller than summed Rss suggests, and the *tree RSS* column in
the footprint table below overstates in exactly this way.

*The cost of an import depends on what is already loaded.* Instrumenting the taskiq worker
directly showed the heavy module costing it +7MB, not +47: it already had 514 modules, so most
of celery's dependency tree was a re-import. The same code costs a fresh tarsk child +33MB
because a fresh tarsk child has almost nothing in it. Neither number is wrong; they answer
different questions, and "how much does my dependency tree cost" only has an answer relative to
a starting point.

† The taskiq worker cell does not move because steady-state RSS does not isolate the import
for it: measured *at import*, the heavy module costs that process +7MB (52.3 → 59.4MB, 514 →
581 modules), but its worker allocates a comparable amount after import regardless, so both
configurations settle near 58MB a few seconds later. tarsk's child does almost nothing after
importing, so its delta survives to steady state. Two runtimes, one measurement, different
things measured — worth knowing before reading the column as a ranking.

**A compatibility bug this scenario surfaced, since it changes how taskiq is run here.** With a
plain URL, taskiq-redis 1.2.3 on redis-py 8.1.0 kills its worker every five seconds while idle.
`ListQueueBroker.listen` calls `brpop()` with no timeout, intending to block; redis-py 8.x now
applies a default 5-second socket read timeout even when the URL sets none, and raises
`redis.exceptions.TimeoutError` — which `listen` does not catch, because it catches
`ConnectionError` and in redis-py 8.x `TimeoutError` descends from `RedisError` instead. The
generator dies, the worker dies, the process manager respawns it.

`socket_timeout=None` fixes it (0 errors across 14 idle seconds, against a crash every 5), and
this harness now passes it. Anything else would be benchmarking a version mismatch. The earlier
scenarios on this page never saw it because they preload the queue, so the worker is never idle
long enough.

### Footprint, after one trivial task### Footprint, after one trivial task

| runtime | worker RSS | tree RSS |
|---|---|---|
| tarsk | **27 MB** | 52 MB |
| celery | 49 MB | 104 MB |
| taskiq | 57 MB | 138 MB |

The tarsk child is CPython plus the user's task module and nothing else — no broker driver,
no scheduler (spec §4.1). The tree column includes the supervisor / master, and is summed Rss,
so it double-counts pages shared by forking — see the Pss figures above for how much.

**This table said 16 MB for tarsk until it was measured properly.** A child that runs one
trivial task lives about a second, and the sampler took the largest of a handful of 50ms reads
— often missing the peak entirely. The figure now comes from the worker's own `ru_maxrss`,
which cannot. A bare CPython 3.14 is 12.6 MB here and `tarsk` plus msgpack adds 11.8, so 27 was
always the honest number; 16 was never reachable. The ratio against Celery is 1.8×, not the 3×
this page used to claim.

### Bounded memory, 40 tasks each retaining 20MB

| runtime | peak worker RSS |
|---|---|
| celery (no recycling) | 849 MB |
| taskiq (no recycling available) | 858 MB |
| celery `--max-tasks-per-child=6` | **170 MB** |
| tarsk `--max-rss=200MB --slots 1` | 205 MB |
| tarsk `--max-rss=200MB` (default 100 slots) | **826 MB** |

Celery wins this one. When the leak per task is known and constant, dividing the budget by it
is a perfectly good bound — and a tighter one, because it stops short of the budget rather
than at it.

**The two tarsk rows are the same ceiling at one slot and at the default hundred**, and the
second one is not a looser bound — it is barely a bound at all. 826MB against 849MB for no
recycling whatsoever. With a hundred slots the child is handed a hundred tasks before any of
them has allocated anything, so the ceiling is first read long after the damage. A slot is a
task that can be allocating when the ceiling is read; the default is chosen for handlers that
wait, and this workload does the opposite. `--slots 1` is what the rest of this page measures.

### The same configurations, different workloads

| | leak raised to 40MB | payload-dependent 2–80MB |
|---|---|---|
| celery `--max-tasks-per-child=6` | 290 MB | 335 MB |
| tarsk `--max-rss=200MB` | 224 MB | 247 MB |

Nothing was reconfigured between this table and the last one. A task count encodes an
assumption about bytes per task that nothing enforces; when the assumption expires, so does
the bound. tarsk overshoots its ceiling by at most one task's peak allocation, which is the
floor for any design that refuses to kill running work.

### Task loss under a hard 400MB cgroup limit, 30 tasks leaking 20MB

| runtime | completed | lost | executions | restarts | max gap | peak RSS |
|---|---|---|---|---|---|---|
| celery (defaults) | 16/30 | **14** | 16 | 0 | 8 ms | 368 MB |
| taskiq (list, no ack) | 15/30 | **15** | 15 | 0 | 9 ms | 358 MB |
| taskiq (streams, acked) | 15/30 | **15** | 15 | 0 | 8 ms | 358 MB |
| celery `task_acks_late=True` | 16/30 | **14** | 16 | 0 | 12 ms | 368 MB |
| celery `task_acks_late=True` + restarted | 29/30 | **1** | 29 | 1 | 2452 ms | 369 MB |
| celery `--max-tasks-per-child=6` (tuned) | 30/30 | 0 | 30 | 0 | 171 ms | 170 MB |
| tarsk `--max-rss=200MB` | 30/30 | 0 | 30 | 0 | 13 ms | 204 MB |

**An acknowledgement only helps if something survives to notice.** taskiq's streams broker acks
and still loses 15 of 30 here, exactly as its unacked list broker does, for the same reason
Celery's `task_acks_late` changes nothing: the OOM killer takes the whole cgroup, coordinator
included, so there is nobody left to redeliver to. With an orchestrator restarting the worker it very
nearly recovers — the one task still missing was in flight when the kill landed, and the Redis
transport's default visibility timeout (1 hour) puts its redelivery outside the run. The price
is 2.8 seconds of dead air.

A correctly tuned Celery ties tarsk here. The difference is what you had to know in advance.

### Recycling stalls, 120 × 5ms tasks, recycled every 20

| runtime | p50 gap | p99 gap | max gap |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 5.7 ms | 156 ms | 162 ms |
| tarsk `--max-tasks=20` | 6.0 ms | 10 ms | 12 ms |
| taskiq (no recycling available) | 6.1 ms | 7 ms | 7 ms |

Recycling costs tarsk essentially nothing: it sits with taskiq, which never recycles at all.
Getting there took three fixes, not one — starting the replacement early enough (projected from
the child's trajectory against a measured spawn cost, not a fixed percentage), retiring the old
child off the critical path, and never launching a second replacement while the first is still
booting. The supervisor exports `recycles_prewarmed` and `wasted_spares` so this is checkable
rather than assertable.

### Draining a queue, by size

`python bench/run.py scale` — four worker processes each, five runs per cell, no-op handler.
Shaped after [s3rius's taskiq benchmark](https://gist.github.com/s3rius/91c39494fe1b96ad467cee671dfdf5ec),
with startup pulled out of the figure rather than folded into it.

| runtime | 10 | 100 | 1,000 | 10,000 | startup |
|---|---|---|---|---|---|
| celery | 0.005 | 0.048 | 0.494 | 5.106 | 0.33 s |
| taskiq (list, no ack) | 0.002 | 0.012 | 0.090 | 0.853 | 0.43 s |
| taskiq (streams, acked) | 0.005 | 0.030 | 0.134 | 1.479 | 0.47 s |
| tarsk (streams, acked) | 0.005 | 0.014 | 0.120 | **0.945** | 0.16 s |

Median seconds from first completion to last, five runs a cell.

The two `streams, acked` rows are the like-for-like pair — same guarantee, same process count,
same handler. **tarsk is the faster one at ten thousand**, 0.95s against 1.48s, and the two
sets of five runs do not overlap: tarsk's slowest is 1.27s, taskiq's fastest 1.44s.

That is a change, and the cause is not a clever optimisation. This page used to report a tie,
and the reason for the tie was a mutex the Redis driver held round its own connection — every
command from every child queued behind one lock on a single-threaded runtime. A
`MultiplexedConnection` is built to be used concurrently. The lock was a bottleneck the driver
had invented for itself, and removing it is the entire difference between 1.55s and 0.95s.

Two earlier versions of this page read an ordering out of five noisy runs and had to retract
it, so the separation being clean this time is worth stating plainly rather than assuming.
What has not changed: a no-op handler is the case most favourable to whoever dispatches
fastest, and nobody runs one.

**Acking is not free for either.** taskiq's own list broker against its own stream broker is
the cleanest measure — 0.85s against 1.48s at ten thousand — and tarsk pays that cost too,
plus a Unix socket round trip per task, about 63µs, because the handler runs in a child the
supervisor can meter and replace. That cost did not go away. It stopped being buried under a
larger one.

taskiq's default `ListQueueBroker` is `BRPOP` with no acknowledgement — at-most-once, and a
killed worker loses what it held. Comparing it against an acked broker compares different
promises, which is why both of its rows are here.

What is held equal is **processes, not concurrency**. taskiq can await thousands of tasks
inside one process and would pull away on anything I/O-bound; tarsk runs one task per child and
cannot. That is a real limit rather than a benchmark choice — see the one-slot note in
`tarsk/_child.py` — and a no-op handler is precisely the case that hides it.

**The harness was on the scale.** These figures come from runs with RSS sampling switched off.
Walking `/proc` for a whole process tree twenty times a second is affordable when RSS is the
measurement and a thumb on the scale when speed is — and it costs more for a runtime with more
processes, which taskiq has. With sampling on, tarsk's thousand-task cell read 0.44s instead of
0.18s.

#### How big a batch

The Redis driver used to claim one message per `XREADGROUP`. Batching helps — measured on 5,000
no-ops across four children, seven runs each:

| messages per read | median | spread |
|---|---|---|
| 1 | 0.56 s | 0.50–0.68 |
| 4 (one per child) | 0.48 s | 0.42–0.96 |
| 64 | 0.46 s | 0.44–0.74 |

Claiming one at a time is measurably worse. Four and sixty-four are not distinguishable — the
spread within either is wider than the gap between them — so the cap stays at the child count
for a reason that is not speed: a claimed message holds a lease whether or not a child is free
to run it, and a buffer deeper than the children can drain is just leases ageing in memory.

This table used to say sixty-four was **slower than not batching at all**, at 1.18s against
0.96s. That was true, and it was an artefact of the mutex described above: a large batch parsed
in one burst blocked every other Redis command. The mutex is gone and so is the effect. The
conclusion happened to survive — the cap is still the child count — but the reason given for it
was wrong.

### Waiting, not working — 500 tasks that each await 100ms

Nothing computes; everything waits, which is what most real task queue work does — call an API,
query a database, wait for a webhook.

| runtime | wall | vs. the 4-way floor | peak tree RSS | completed |
|---|---|---|---|---|
| celery, 4 processes | 12.63s | 1.01× | 251 MB | 500/500 |
| taskiq, 4 processes × 1 | 12.64s | 1.01× | 318 MB | 500/500 |
| tarsk, 4 children | 12.70s | 1.02× | **125 MB** | 500/500 |
| celery, 32 processes | 1.58s | 0.13× | 1,628 MB | 500/500 |
| taskiq, 4 processes × 64 | 0.20s | 0.02× | 319 MB | 500/500 |
| tarsk, 32 children | 1.64s | 0.13× | 812 MB | 500/500 |
| **tarsk, 4 children × 64 slots** | **0.15s** | 0.01× | **127 MB** | 500/500 |

Held to one task per process, all three land on the arithmetic floor of 12.5s and the only
difference is footprint. Buying concurrency with processes is the expensive way: 32 children
costs 812MB to reach 1.64s, and Celery's 32 processes cost 1,628MB to reach much the same.

`--slots 64` buys it with coroutines instead, and that row is the whole point of running this
table: **0.15s on 127MB against taskiq's 0.20s on 319MB.** Same concurrency model now, and
tarsk's advantage is the one it always had — a child that imports your tasks and no broker
driver is a smaller process to have 64 of. Three runs each: tarsk 0.15 / 0.16 / 0.16, taskiq
0.20 / 0.20 / 0.20.

**This is not free, and what it costs is the thing this page is otherwise about.** At one slot
the ceiling is read while the child has nothing running, so overshoot is bounded by a single
task's peak. At 64 it is bounded by whatever 64 tasks are holding, and `--hard-max-rss` takes
all 64 down together rather than one. That is why the default is 1: handlers that wait should
raise it, handlers that allocate are exactly the case the precision exists for.

An earlier version of this section reported this row as a loss tarsk could not fix, and called
one task per child "the design, not a tuning gap". The measurement was right and the conclusion
was wrong — the one-slot loop was a default, and the note left in `tarsk/_child.py` already
said what multi-slot needed — N Readys and a reader task. It took about an hour.

### Out of the box — 2,000 tasks that each await 100ms

No `-c`, no `--workers`, no `--children`, no `--slots`: whatever each runtime does when told
nothing. Every other table here pins concurrency so one axis is held equal. This one asks what
a person gets for typing the documented command.

| runtime | median wall | peak tree RSS | completed |
|---|---|---|---|
| celery (`-c` = cores, 16 here) | 12.72s | 841 MB | 2000/2000 |
| taskiq (2 workers × 100) | 1.36s | 210 MB | 2000/2000 |
| **tarsk (2 children × 100 slots)** | **0.98s** | **78 MB** | 2000/2000 |

The I/O handler is deliberate: a no-op would make this a measure of dispatch rather than of the
default concurrency, and the default concurrency is the whole question.

Read this against the two tarsk rows in the memory table above. The same default that wins here
is the one that lets a leaky workload reach 826MB under a 200MB ceiling. tarsk ships tuned for
handlers that wait, and a handler that allocates needs `--slots 1` — which is the configuration
every other table on this page uses.

### Throughput, single worker

| runtime | no-op | 50 ms handler | startup |
|---|---|---|---|
| celery | 1,696/s | 20/s | 0.29 s |
| taskiq | 2,488/s | 19/s | 0.50 s |
| tarsk | 2,726/s | 19/s | 0.14 s |

All three read the same Redis. Rate and wall cover first completion to last; **startup is
separate on purpose**. Folding it in turns a 500-task row into a boot-time contest — it was 51%
of Celery's figure, 59% of tarsk's and 77% of taskiq's when this table still did that.

With a 50 ms handler all three land on 19–20/s, which is the row that matters and the whole of
spec §2's argument: dispatch overhead disappears behind any handler doing real work.

## When these numbers were taken

The speed tables (`scale`, `throughput`) and the memory tables came from different runs,
because two full runs in a row produced throughput a third of the first for all three runtimes
at once and scale figures three times worse. Nothing had changed in any of them — the machine
was in use. Memory figures are unaffected by CPU contention and the sleep-bound tables barely
notice it; dispatch throughput notices nothing else.

`bench/run.py` now times an identical pure-Python loop before each case and says so under the
tables when the spread exceeds 1.25×. Neither contaminated run had it yet, which is why this
section exists rather than a footnote. On this machine the probe is 20ms when idle and reached
30ms while a browser and a Kubernetes node were running.

Take these on an idle machine. Google Colab is not one — beyond the shared CPU, it has no
systemd user session for the OOM table's `systemd-run --scope MemoryMax=400M` and runs as root,
which `initdb` refuses, so two tables would silently not exist.

## Known limits of these numbers

- **Every runtime here reads the same Redis.** That was not true until recently: tarsk was fed
  from an in-memory list while the others paid a round trip, which flattered every throughput
  figure on this page. The tables above are from after that was fixed.
- **Single machine, single worker.** Nothing here says anything about scaling out.
- **Peak RSS is a lower bound, not the true peak.** Both measurement methods under-report in
  different ways (see Results), and taking the larger of the two narrows the gap without
  closing it.
- **Leaks here are synthetic** — one `bytearray` per task, retained. Real leaks fragment the
  allocator and are messier; that generally makes RSS worse, not better.
