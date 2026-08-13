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
| tarsk | **27 MB** | 47 MB |
| celery | 49 MB | 104 MB |
| taskiq | 58 MB | 144 MB |

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
| tarsk `--max-rss=200MB` | 205 MB |

Celery wins this one. When the leak per task is known and constant, dividing the budget by it
is a perfectly good bound — and a tighter one, because it stops short of the budget rather
than at it.

### The same configurations, different workloads

| | leak raised to 40MB | payload-dependent 2–80MB |
|---|---|---|
| celery `--max-tasks-per-child=6` | 290 MB | 335 MB |
| tarsk `--max-rss=200MB` | 225 MB | 247 MB |

Nothing was reconfigured between this table and the last one. A task count encodes an
assumption about bytes per task that nothing enforces; when the assumption expires, so does
the bound. tarsk overshoots its ceiling by at most one task's peak allocation, which is the
floor for any design that refuses to kill running work.

### Task loss under a hard 400MB cgroup limit, 30 tasks leaking 20MB

| runtime | completed | lost | executions | restarts | max gap | peak RSS |
|---|---|---|---|---|---|---|
| celery (defaults) | 16/30 | **14** | 16 | 0 | 14 ms | 369 MB |
| taskiq | 15/30 | **15** | 15 | 0 | 17 ms | 358 MB |
| celery `task_acks_late=True` | 16/30 | **14** | 16 | 0 | 19 ms | 369 MB |
| celery `task_acks_late=True` + restarted | 29/30 | **1** | 29 | 1 | 2757 ms | 377 MB |
| celery `--max-tasks-per-child=6` (tuned) | 30/30 | 0 | 30 | 0 | 203 ms | 170 MB |
| tarsk `--max-rss=200MB` | 30/30 | 0 | 30 | 0 | 21 ms | 204 MB |

`task_acks_late` alone changes nothing: the OOM killer takes the whole cgroup, master included,
so nothing survives to receive a redelivery. With an orchestrator restarting the worker it very
nearly recovers — the one task still missing was in flight when the kill landed, and the Redis
transport's default visibility timeout (1 hour) puts its redelivery outside the run. The price
is 2.8 seconds of dead air.

A correctly tuned Celery ties tarsk here. The difference is what you had to know in advance.

### Recycling stalls, 120 × 5ms tasks, recycled every 20

| runtime | p50 gap | p99 gap | max gap |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 6.2 ms | 171 ms | 173 ms |
| tarsk `--max-tasks=20` | 6.0 ms | 10 ms | 11 ms |
| taskiq (no recycling available) | 6.7 ms | 7 ms | 7 ms |

Recycling costs tarsk essentially nothing: it sits with taskiq, which never recycles at all.
Getting there took three fixes, not one — starting the replacement early enough (projected from
the child's trajectory against a measured spawn cost, not a fixed percentage), retiring the old
child off the critical path, and never launching a second replacement while the first is still
booting. The supervisor exports `recycles_prewarmed` and `wasted_spares` so this is checkable
rather than assertable.

### Throughput, single worker

| runtime | noop | 50ms handler |
|---|---|---|
| celery | 359/s | 18/s |
| taskiq | 445/s | 18/s |
| tarsk | 850/s (no broker yet) | 19/s |

The noop row for tarsk is not a comparison: it has no broker to talk to yet. The 50ms row is
the one that matters, and all three are identical — which is exactly what spec §2 predicts.
Dispatch overhead disappears behind any handler doing real work.

## Known limits of these numbers

- **tarsk has no broker yet (step 3).** Its jobs come from an in-memory list, so throughput
  rows skip a Redis round-trip that Celery and taskiq pay. Those rows are an upper bound on
  tarsk, not a comparison. Memory, OOM and gap results are unaffected — none of them depend on
  where the job came from.
- **Single machine, single worker.** Nothing here says anything about scaling out.
- **Peak RSS is a lower bound, not the true peak.** Both measurement methods under-report in
  different ways (see Results), and taking the larger of the two narrows the gap without
  closing it.
- **Leaks here are synthetic** — one `bytearray` per task, retained. Real leaks fragment the
  allocator and are messier; that generally makes RSS worse, not better.
