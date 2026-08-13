# Benchmarks: tarsk vs Celery vs taskiq

```bash
python bench/run.py                    # every scenario
python bench/run.py memory oom gap     # a subset
```

Linux only: RSS comes from `/proc`, the hard-limit scenario uses `systemd-run --user --scope`.
Needs `redis-server` on `PATH`; the harness starts its own instance on port 6399 and tears it down.

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

### Footprint, after one trivial task

| runtime | worker RSS | tree RSS |
|---|---|---|
| tarsk | 16 MB | 42 MB |
| celery | 49 MB | 104 MB |
| taskiq | 58 MB | 144 MB |

The tarsk child is CPython plus the user's task module and nothing else — no broker driver,
no scheduler (spec §4.1). The tree column includes the supervisor / master.

### Bounded memory, 40 tasks each retaining 20MB

| runtime | peak worker RSS |
|---|---|
| celery (no recycling) | 848 MB |
| taskiq (no recycling available) | 858 MB |
| celery `--max-tasks-per-child=6` | **170 MB** |
| tarsk `--max-rss=200MB` | 204 MB |

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
| celery (defaults) | 16/30 | **14** | 16 | 0 | 7 ms | 368 MB |
| taskiq | 15/30 | **15** | 15 | 0 | 8 ms | 358 MB |
| celery `task_acks_late=True` | 16/30 | **14** | 16 | 0 | 10 ms | 368 MB |
| celery `task_acks_late=True` + restarted | 29/30 | **1** | 29 | 1 | 2440 ms | 368 MB |
| celery `--max-tasks-per-child=6` (tuned) | 30/30 | 0 | 30 | 0 | 167 ms | 170 MB |
| tarsk `--max-rss=200MB` | 30/30 | 0 | 30 | 0 | 14 ms | 204 MB |

`task_acks_late` alone changes nothing: the OOM killer takes the whole cgroup, master included,
so nothing survives to receive a redelivery. With an orchestrator restarting the worker it very
nearly recovers — the one task still missing was in flight when the kill landed, and the Redis
transport's default visibility timeout (1 hour) puts its redelivery outside the run. The price
is 2.4 seconds of dead air.

A correctly tuned Celery ties tarsk here. The difference is what you had to know in advance.

### Recycling stalls, 120 × 5ms tasks, recycled every 20

| runtime | p50 gap | p99 gap | max gap |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 5.7 ms | 156 ms | 157 ms |
| tarsk `--max-tasks=20` | 5.4 ms | 9 ms | 13 ms |
| taskiq (no recycling available) | 6.2 ms | 7 ms | 7 ms |

Recycling costs tarsk essentially nothing: it sits with taskiq, which never recycles at all.
Getting there took three fixes, not one — starting the replacement early enough (projected from
the child's trajectory against a measured spawn cost, not a fixed percentage), retiring the old
child off the critical path, and never launching a second replacement while the first is still
booting. The supervisor exports `recycles_prewarmed` and `wasted_spares` so this is checkable
rather than assertable.

### Throughput, single worker

| runtime | noop | 50ms handler |
|---|---|---|
| celery | 856/s | 19/s |
| taskiq | 778/s | 19/s |
| tarsk | 2553/s (no broker yet) | 19/s |

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
