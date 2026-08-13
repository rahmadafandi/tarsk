# Show HN draft

Not posted. Every number here is measured on one machine and reproducible from this repo —
`python demo/run.py` for the trace, `python bench/run.py` for the tables.

The tables keep the columns where tarsk loses. A Show HN thread finds them within the hour
whether or not the post does, and the difference is whether that comment reads as a correction
or as agreement.

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
broker driver, no scheduler -- so a worker is 27 MB against Celery's 49 MB.

One hour, 72,001 tasks, a handler that frees nothing: 66 recycles, peak 400 MB
against a 400 MB ceiling, zero tasks lost, nothing killed. The trough held at 27.9 MB
over the first half hour and 28.1 MB over the second, so nothing survives a recycle.

What it does not claim: it is not faster. With a 50 ms handler, Tarsk, Celery and
taskiq all reach 19-20 tasks/s -- identical, as they should be. Dispatch costs
microseconds and real handlers do not. Nor is the ceiling absolute: it is read while
a child is idle, so a child never starts a task it cannot afford, but a handler that
allocates 300 MB will allocate it. Overshoot is one task's peak, not zero. A ceiling
that killed running work could not make an oversized task fit -- it would only turn
"completes, briefly over budget" into "never completes".

And a correctly tuned Celery ties it on task loss. The difference is what you had to
know in advance.

Redis Streams and Postgres brokers, per-task leases, retries with backoff, a
dead-letter store, opt-in results with a required TTL, UTC cron elected through the
broker, and a Prometheus endpoint that reports the supervisor's own RSS so the
constant can be checked rather than believed.

Pre-release: no published wheel yet. Linux and macOS.
```

---

## The trace

![child RSS over one hour under a leaky handler](one-hour.svg)

One hour at 20 tasks/s, leak 100–600 KB per task, sampled once a second from the supervisor's
own `/metrics`.

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

**Peak worker RSS under a leaky handler.** Same two configurations throughout — only the
workload changes between columns.

| runtime | leak 20 MB/task | leak 40 MB/task | payload 2–80 MB |
|---|---|---|---|
| celery, no recycling | 848 MB | — | — |
| taskiq, none available | 858 MB | — | — |
| celery `--max-tasks-per-child=6` | **170 MB** | 290 MB | 335 MB |
| tarsk `--max-rss=200MB` | 205 MB | **225 MB** | **247 MB** |

**Gap between consecutive completions**, recycling every 20 tasks — the cost of recycling at all.

| runtime | p50 | p99 | max |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 6.2 ms | 171 ms | 173 ms |
| tarsk `--max-tasks=20` | 6.0 ms | 10 ms | **11 ms** |
| taskiq, never recycles | 6.7 ms | 7 ms | 7 ms |

**30 leaky tasks under a hard 400 MB cgroup limit** — what Kubernetes does to a worker that grows.

| runtime | completed | lost | restarts | longest stall |
|---|---|---|---|---|
| celery, defaults | 16/30 | **14** | 0 | 14 ms |
| taskiq | 15/30 | **15** | 0 | 17 ms |
| celery `task_acks_late` | 16/30 | **14** | 0 | 19 ms |
| celery `task_acks_late` + restarted | 29/30 | **1** | 1 | 2,757 ms |
| celery `--max-tasks-per-child=6`, tuned | **30/30** | 0 | 0 | 203 ms |
| tarsk `--max-rss=200MB` | **30/30** | 0 | 0 | 21 ms |

**Throughput**, single worker, concurrency 1. Reported to be honest about it, not as a claim.

| runtime | no-op handler | 50 ms handler | startup |
|---|---|---|---|
| celery | 1,773/s | 20/s | 0.30s |
| taskiq | 2,571/s | 19/s | 0.45s |
| tarsk | 6,138/s *(no broker yet)* | 20/s | 0.16s |

The no-op row for tarsk is not a comparison: it has no broker to talk to yet, so it skips a hop
the others pay. The 50 ms row is the one that matters, and all three are the same number.

---

## Objections to expect

**Just use `--max-tasks-per-child`.**
Correct, when you know the bytes per task and they stay put — it bounds tighter than tarsk does
in that case, 170 MB against 205. The second and third columns above are the same flag,
unchanged, against a workload that moved.

**Celery doesn't lose tasks, you configured it wrong.**
Tuned correctly it ties: 30/30, and at a lower peak. `task_acks_late` alone does not — the OOM
killer takes the whole cgroup, master included, so nothing survives to receive the redelivery.
With an orchestrator restarting the pod it recovers all but the in-flight one, and pays 2.8
seconds of dead air.

**Your throughput numbers are missing the broker.**
They are, and the post says so. Redis Streams and Postgres drivers exist; the benchmark harness
still feeds tarsk from memory, so treat 6,138/s as an upper bound rather than a result. It
changes nothing above 50 ms, which is where handlers live.

**A ceiling that can be exceeded isn't a ceiling.**
It is read while a child is idle, so overshoot is bounded by one task's peak allocation rather
than by zero. Killing running work to hold the number exactly cannot make an oversized task fit
— it converts "completes, briefly over budget" into "never completes". `--hard-max-rss` exists
for people who would rather lose the task, and it is off by default.

**Why not contribute this to Celery?**
The supervision model is the whole design — pull-based children, a registry that crosses IPC so
the supervisor never imports user code, per-task leases. Bolting an RSS ceiling onto prefork
gets the byte budget without the handover, which is the 173 ms column.

---

## Not done yet

- Not on PyPI yet — the wheel builds and installs, it just has not been uploaded
- No chains, groups or chords
- Linux and macOS; the macOS wheel builds in CI but nothing runs the tests there
- The hour-long trace records child RSS but not the supervisor's own gauge — the other constant
  this project claims, still unproven at that length

---

Numbers from Linux 7.0, i7-13620H ×16, Python 3.14.3, Celery 5.6.3, taskiq 0.12.4, Redis 8.10,
Postgres 18.4. Single run per row; anything within ~10% of another number is a tie.
