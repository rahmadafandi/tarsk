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
against a 400 MB ceiling, zero samples over it, zero tasks lost, nothing killed.
The trough held at 28.5 MB over the first half hour and 28.6 MB over the second, so
nothing survives a recycle. The supervisor enforcing all of that moved 82 KB across
the hour -- 28.32 to 28.40 MB -- which matters because a ceiling held from inside a
process with the same problem would only defer it.

That run used --slots 1. The default is 100 tasks in flight per child, and it used
to be a trap: the same 40 leaky tasks peaked at 824 MB under a 200 MB ceiling,
because a child is handed every slot before any of them has allocated anything. The
supervisor now learns what a task costs in bytes and will not fill a slot the ceiling
cannot afford, so those tasks peak at 123 MB at a hundred slots and 183 MB at one.
Lower at a hundred, because a child holding several climbs faster between two
readings and meets the ceiling's projection sooner. What more slots still cost is
precision: overshoot is the peak of whatever is in flight, not of one task.

What it does not claim: speed. An earlier draft of this claimed 1.45x over taskiq
on a no-op queue. That was measured on a two-core runner; those runners now have
four cores and two runs on them read 1.06x and 1.04x, and a sixteen-core laptop
reads 1.1x. tarsk is ahead in every run and by 4%, which is a tie by the rule at
the bottom of this page. With a 50 ms handler all three reach 20 tasks/s,
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
expiry per task or per send, deduplication, soft timeouts that ask a handler to
stop before the hard one takes it, strict queue priority, an async producer, and
a Prometheus endpoint that reports
queue depth and the supervisor's own RSS so both constants can be checked rather
than believed.

pip install tarsk. Wheels for Linux (glibc and musl, x86_64 and aarch64), macOS
universal2 and Windows x64, on 3.11 through 3.14 including the free-threaded
build, which needs its own wheel because abi3 stops at the GIL.
```

---

## The trace

![child RSS over one hour under a leaky handler, and the supervisor holding it](one-hour.svg)

One hour at 20 tasks/s, leak 100–600 KB per task, `--slots 1`, sampled once a second from the
supervisor's own `/metrics`. Two lines on one axis: the children sawtoothing under the ceiling,
and the process enforcing it. The second line is flat against the same scale, which is the
point of drawing them together rather than giving the small one a scale that flatters it.

| | |
|---|---|
| tasks completed | 72,001 |
| tasks lost | **0** |
| peak child RSS | 400 MB against a 400 MB ceiling |
| recycles | 66, all handed over pre-warmed |
| killed / crashed | 0 / 0 |
| trough drift | 28.5 MB → 28.6 MB |
| samples above the ceiling | **0** of 3,599 |
| supervisor RSS | 28.32 – 28.40 MB, drift **+0.1%** |

The flat trough is the part that needed an hour. A baseline creeping from 28 MB toward 50 would
mean something survives each recycle and the ceiling is only deferring the leak.

**The supervisor line is the half that went unchecked the longest.** A ceiling has to be held
from outside the process that grows, so the whole design rests on the holder not having the
problem — and the trace recorded only the children, which is to say it measured the claim and
not the assumption underneath it. Over 72,001 tasks the enforcing process moved 82 KB, between
28.32 and 28.40 MB. `demo/run.py` now exits non-zero if it drifts more than 25%, so CI checks
this on every push rather than once when someone thinks to look.

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
| tarsk `--max-rss=200MB --slots 1` | 183 MB | **183 MB** | **187 MB** |
| tarsk `--max-rss=200MB`, default slots | **123 MB** | — | — |

**Gap between consecutive completions**, recycling every 20 tasks — the cost of recycling at all.

| runtime | p50 | p99 | max |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 5.4 ms | 139 ms | 140 ms |
| tarsk `--max-tasks=20` | 5.7 ms | 7 ms | **7 ms** |
| taskiq, never recycles | 5.4 ms | 6 ms | 6 ms |

**30 leaky tasks under a hard 400 MB cgroup limit** — what Kubernetes does to a worker that grows.

| runtime | completed | lost | restarts | longest stall |
|---|---|---|---|---|
| celery, defaults | 17/30 | **13** | 0 | 3 ms |
| taskiq (list, no ack) | 17/30 | **13** | 0 | 4 ms |
| taskiq (streams, acked) | 17/30 | **13** | 0 | 4 ms |
| celery `task_acks_late` | 17/30 | **13** | 0 | 9 ms |
| celery `task_acks_late` + restarted | 29/30 | **1** | 1 | 2,530 ms |
| celery `--max-tasks-per-child=6`, tuned | **30/30** | 0 | 0 | 139 ms |
| tarsk `--max-rss=200MB` | **30/30** | 0 | 0 | 63 ms |

**Throughput**, single worker, concurrency 1. Reported to be honest about it, not as a claim.

| runtime | no-op handler | 50 ms handler | startup |
|---|---|---|---|
| celery | 965/s | 20/s | 0.45s |
| taskiq | 1,784/s | 20/s | 0.51s |
| tarsk | 2,007/s | 20/s | **0.21s** |

Draining a 10,000-task queue across four processes, same guarantee for both: taskiq 2.27s,
tarsk 2.18s. Celery takes 10.0s. tarsk is ahead in all five runs and by 4% — a tie by the rule
at the bottom of this page.

Both numbers in that pair are retractions. This draft claimed 1.45× from a two-core runner;
those runners have four cores now and two runs on them read 1.06× and 1.04×. It also claimed the ratio
held on a sixteen-core laptop, from a tarsk figure of 0.95s that does not reproduce — three
commits were rebuilt and re-run, including the one that first published it, and all three
measure 1.2–1.3s. That is not a regression, and it is not a faster machine either, because
taskiq's figure from the same old run reproduces exactly. It has no explanation, and it was
taken before the harness had a CPU probe, which is the part worth generalising: a benchmark
number nobody tried to break is not evidence, and three of the ones on this page broke.

**Out of the box**, no concurrency flags at all, 2,000 tasks that each await 100 ms: tarsk
0.96s on 74 MB, taskiq 1.31s on 145 MB, Celery 50.3s on 215 MB. Celery's `-c` defaults to the
core count, and the runner has four.

The 50 ms row is the one that matters, and all three are the same number.

---

## Objections to expect

**Your own default breaks your headline.**
It did, and this draft said so for weeks before it was fixed: 824 MB under a 200 MB ceiling at
the default hundred slots, against 841 MB for no recycling at all. A child is handed every slot
at once, so a hundred tasks were dispatched while its RSS still read baseline and the ceiling
was next consulted long after all hundred had allocated. The answer was not a smaller default.
The supervisor now learns what a task costs in bytes — the same way it already learned what a
spawn costs in milliseconds — and refuses to fill a slot the ceiling cannot afford. That row is
123 MB now, and the flag it used to require you to know about is gone from this page.

Worth stating plainly because the objection was real and the table carried it: a benchmark page
that reports the case where your own defaults lose is how you find out they lose.

**Just use `--max-tasks-per-child`.**
Correct, when you know the bytes per task and they stay put — it bounds tighter than tarsk does
in that case, 162 MB against 183. The second and third columns above are the same flag,
unchanged, against a workload that moved.

**Celery doesn't lose tasks, you configured it wrong.**
Tuned correctly it ties: 30/30, and at a lower peak. `task_acks_late` alone does not — the OOM
killer takes the whole cgroup, master included, so nothing survives to receive the redelivery.
With an orchestrator restarting the pod it recovers all but the in-flight one, and pays 2.5
seconds of dead air. taskiq's acked streams broker loses the same 13 as its unacked one, for
the same reason: an acknowledgement only helps if something survives to redeliver to.

**This is useless for I/O-bound work.**
It was, when one slot per child was the only option. `--slots N` runs N tasks in one child, and
500 tasks each awaiting 100 ms take 0.14s on 119 MB against taskiq's 0.23s on 225 MB — the same
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

- No chord; chains and groups are there, fanning back in is not

---

Tables from a GitHub Actions `ubuntu-24.04` runner, four cores, Python 3.12. The trace and the
laptop comparisons are Linux 7.0, i7-13620H ×16, Python 3.14.3. Celery 5.6.3, taskiq 0.12.4,
Redis 8.10, Postgres 18.4. Anything within ~10% of another number is a tie.
