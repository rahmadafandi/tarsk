# Show HN draft

Not posted. The tables come from a GitHub Actions runner via `.github/workflows/bench.yml`, so
anyone with the repository can reproduce them; the hour-long trace comes from a laptop, because
a CI job that runs for an hour is a different kind of favour to ask. `python demo/run.py` for
the trace, `python bench/run.py` for the tables.

The tables keep the columns where tarsk loses. A Show HN thread finds them within the hour
whether or not the post does, and the difference is whether that comment reads as a correction
or as agreement. The first objection below is one this project created for itself and would
rather answer than be handed.

---

## The post

```text
Show HN: Tarsk – a Python task queue whose workers hold a memory ceiling

Every Python task queue leaks, because the leaks come from the code they run, not
from the queue. What differs is what the runtime does about it.

Celery has --max-tasks-per-child. It's a task count, so it only bounds bytes if you
already know how many bytes a task costs, and it stays wrong once that changes.
taskiq has no recycling at all. Both leave RSS enforcement to Kubernetes, where the
answer is an OOMKill.

Tarsk takes --max-rss in bytes. A Rust supervisor watches each child's RSS from the
parent and retires it when it crosses, starting the replacement early enough that
the slot never goes empty. Children import your task modules and nothing else -- no
broker driver, no scheduler -- so a worker is 27 MB against Celery's 41 MB.

One hour, 72,001 tasks, a handler that frees nothing: 66 recycles, peak 400 MB
against a 400 MB ceiling, zero tasks lost, nothing killed. The trough held at 27.9 MB
over the first half hour and 28.1 MB over the second, so nothing survives a recycle.

That run used --slots 1, which is not the default and should be whenever your
handlers allocate. The default is 100 tasks in flight per child: right for handlers
that wait on other services, ruinous for ones that allocate. The same 40 leaky tasks
peak at 203 MB under a 200 MB ceiling at one slot, and at 824 MB at a hundred. The
ceiling is exact when one task is running while it is read, and that is something
you configure rather than something you get.

What it does not claim: speed. An earlier draft of this claimed 1.45x over taskiq
on a no-op queue. That was measured on a two-core runner; those runners now have
four cores and the same workflow on the same commit reads 1.06x, and a sixteen-core
laptop reads 1.1x. tarsk is ahead in every run and by 6%, which is a tie by the
rule at the bottom of this page. With a 50 ms handler all three reach 20 tasks/s,
identical, as they should be. What is not a tie is startup, 0.20s against 0.42s,
and footprint, everywhere on this page. Nor is the ceiling absolute: it is read between
tasks, so a child never starts one it cannot afford, but a handler that allocates
300 MB will allocate it. Overshoot is one task's peak, not zero. A ceiling that
killed running work could not make an oversized task fit -- it would only turn
"completes, briefly over budget" into "never completes".

And a correctly tuned Celery ties it on task loss. The difference is what you had to
know in advance.

Redis Streams and Postgres brokers, per-task leases, retries with backoff, a
dead-letter store you can read and replay from the CLI, opt-in results with a
required TTL, UTC cron elected through the broker, chains and groups, rate limits
and concurrency caps held in the broker rather than per worker, cancellation,
expiry, send deduplication, an async producer, and a Prometheus endpoint that
reports queue depth and the supervisor's own RSS so both constants can be checked
rather than believed.

Pre-release: no published wheel yet. Linux, macOS and Windows, all three tested
in CI on 3.11 through 3.14 including the free-threaded build.
```

---

## The trace

![child RSS over one hour under a leaky handler](one-hour.svg)

One hour at 20 tasks/s, leak 100–600 KB per task, `--slots 1`, sampled once a second from the
supervisor's own `/metrics`.

| | |
|---|---|
| tasks completed | 72,001 |
| tasks lost | **0** |
| peak child RSS | 400 MB against a 400 MB ceiling |
| recycles | 66, all handed over pre-warmed |
| killed / crashed | 0 / 0 |
| trough drift | 27.9 MB → 28.1 MB |
| samples above the ceiling | 1 of 3,599 |

The flat trough is the part that needed an hour. A baseline creeping from 28 MB toward 50 would
mean something survives each recycle and the ceiling is only deferring the leak.

---

## Evidence

From a four-core `ubuntu-24.04` runner unless noted. Absolute figures on another machine will
differ, and so — this draft learned the hard way — will some of the ratios. The memory figures
held on every machine they were run on. The speed ones did not.

**Peak worker RSS under a leaky handler.** Same two configurations throughout — only the
workload changes between columns.

| runtime | leak 20 MB/task | leak 40 MB/task | payload 2–80 MB |
|---|---|---|---|
| celery, no recycling | 841 MB | — | — |
| taskiq, none available | 845 MB | — | — |
| celery `--max-tasks-per-child=6` | **162 MB** | 282 MB | 327 MB |
| tarsk `--max-rss=200MB --slots 1` | 203 MB | **223 MB** | **277 MB** |
| tarsk `--max-rss=200MB`, default slots | 824 MB | — | — |

**Gap between consecutive completions**, recycling every 20 tasks — the cost of recycling at all.

| runtime | p50 | p99 | max |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 5.3 ms | 139 ms | 143 ms |
| tarsk `--max-tasks=20` | 5.6 ms | 7 ms | **7 ms** |
| taskiq, never recycles | 5.4 ms | 6 ms | 6 ms |

**30 leaky tasks under a hard 400 MB cgroup limit** — what Kubernetes does to a worker that grows.

| runtime | completed | lost | restarts | longest stall |
|---|---|---|---|---|
| celery, defaults | 17/30 | **13** | 0 | 4 ms |
| taskiq (list, no ack) | 17/30 | **13** | 0 | 3 ms |
| taskiq (streams, acked) | 17/30 | **13** | 0 | 4 ms |
| celery `task_acks_late` | 17/30 | **13** | 0 | 4 ms |
| celery `task_acks_late` + restarted | 29/30 | **1** | 1 | 2,466 ms |
| celery `--max-tasks-per-child=6`, tuned | **30/30** | 0 | 0 | 139 ms |
| tarsk `--max-rss=200MB` | **30/30** | 0 | 0 | 44 ms |

**Throughput**, single worker, concurrency 1. Reported to be honest about it, not as a claim.

| runtime | no-op handler | 50 ms handler | startup |
|---|---|---|---|
| celery | 1,305/s | 20/s | 0.36s |
| taskiq | 2,611/s | 20/s | 0.42s |
| tarsk | 2,688/s | 20/s | **0.20s** |

Draining a 10,000-task queue across four processes, same guarantee for both: taskiq 1.78s,
tarsk 1.68s. Celery takes 7.8s. tarsk is ahead in all five runs and by 6% — a tie by the rule
at the bottom of this page.

Both numbers in that pair are retractions. This draft claimed 1.45× from a two-core runner;
those runners have four cores now and the same commit reads 1.06×. It also claimed the ratio
held on a sixteen-core laptop, from a tarsk figure of 0.95s that does not reproduce — three
commits were rebuilt and re-run, including the one that first published it, and all three
measure 1.2–1.3s. That is not a regression, and it is not a faster machine either, because
taskiq's figure from the same old run reproduces exactly. It has no explanation, and it was
taken before the harness had a CPU probe, which is the part worth generalising: a benchmark
number nobody tried to break is not evidence, and three of the ones on this page broke.

**Out of the box**, no concurrency flags at all, 2,000 tasks that each await 100 ms: tarsk
0.94s on 74 MB, taskiq 1.23s on 145 MB, Celery 50.3s on 216 MB. Celery's `-c` defaults to the
core count, and the runner has four.

The 50 ms row is the one that matters, and all three are the same number.

---

## Objections to expect

**Your own default breaks your headline.**
It does, and the table above says so rather than waiting to be asked: 824 MB under a 200 MB
ceiling at the default hundred slots, 203 MB at one. The default is tuned for handlers that
wait, because that is most task queue work, and because tarsk with no flags beats taskiq with
no flags on exactly that workload. A ceiling is a request for the other thing, and the worker
prints a line saying so when both are set. The other default would have made the common case
slow instead of making this case loose; no single number is right for both, which is why it is
a flag.

**Just use `--max-tasks-per-child`.**
Correct, when you know the bytes per task and they stay put — it bounds tighter than tarsk does
in that case, 162 MB against 203. The second and third columns above are the same flag,
unchanged, against a workload that moved.

**Celery doesn't lose tasks, you configured it wrong.**
Tuned correctly it ties: 30/30, and at a lower peak. `task_acks_late` alone does not — the OOM
killer takes the whole cgroup, master included, so nothing survives to receive the redelivery.
With an orchestrator restarting the pod it recovers all but the in-flight one, and pays 2.5
seconds of dead air. taskiq's acked streams broker loses the same 13 as its unacked one, for
the same reason: an acknowledgement only helps if something survives to redeliver to.

**This is useless for I/O-bound work.**
It was, when one slot per child was the only option. `--slots N` runs N tasks in one child, and
500 tasks each awaiting 100 ms take 0.13s on 120 MB against taskiq's 0.21s on 226 MB — the same
concurrency model, in a smaller process, because the child still imports no broker driver. What
it costs is the precision above.

**A ceiling that can be exceeded isn't a ceiling.**
It is read between tasks, so overshoot is bounded by what is running rather than by zero.
Killing running work to hold the number exactly cannot make an oversized task fit — it converts
"completes, briefly over budget" into "never completes". `--hard-max-rss` exists for people who
would rather lose the task, and it is off by default.

**Your benchmarks are from your laptop.**
They were, twice, and both runs were thrown away: a twenty-minute CPU-bound benchmark heats the
machine it runs on, and an identical CPU probe drifted from 11 ms to 27 ms with nothing else
competing. The harness now takes that probe before every row and marks the row when it reads
more than 25% slower than the run's fastest. These come from a runner anyone can dispatch.

The laptop numbers that survived from before that guard existed did not survive being checked.
One of them, tarsk draining ten thousand tasks in 0.95s, is retracted above: three rebuilt
commits all measure 1.2–1.3s and none of them explains where 0.95s came from. A benchmark page
is only worth as much as the numbers on it that someone has tried to break.

**Why not contribute this to Celery?**
The supervision model is the whole design — pull-based children, a registry that crosses IPC so
the supervisor never imports user code, per-task leases. Bolting an RSS ceiling onto prefork
gets the byte budget without the handover, which is the 139 ms column.

---

## Not done yet

- Not on PyPI yet — the wheel builds and installs, it just has not been uploaded
- No chord; chains and groups are there, fanning back in is not
- No soft timeout — a task is stopped at its deadline rather than warned before it
- The hour-long trace records child RSS but not the supervisor's own gauge — the other constant
  this project claims, still unproven at that length
- No strict priority. `--queues high,low` prefers the first within a read, which is not a
  priority queue

---

Tables from a GitHub Actions `ubuntu-24.04` runner, four cores, Python 3.12. The trace and the
laptop comparisons are Linux 7.0, i7-13620H ×16, Python 3.14.3. Celery 5.6.3, taskiq 0.12.4,
Redis 8.10, Postgres 18.4. Anything within ~10% of another number is a tie.
