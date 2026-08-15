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

A GitHub Actions `ubuntu-24.04` runner: AMD EPYC 9V74, four cores, Python 3.12, Celery 5.6.3,
taskiq 0.12.4, Redis 8.10.0. One worker process, concurrency 1 throughout. Single run per row —
these are not medians, and anything inside ~10% of another number should be read as a tie.

**These runners had two cores until recently and now have four.** Every absolute number below
moved when that happened, and one conclusion moved with it; see the drain section. A figure on
this page is a reading from a specific machine, and the machine is not a constant either.

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
| celery | 50 → 69 MB (+19) | 50 → 69 MB (+19) | **yes** | 123 / **66** MB |
| taskiq | 47 → 47 MB (-0) | 44 → 44 MB (+0) † | no | 91 / **61** MB |
| tarsk | 27 → 27 MB (+0) | 23 → 53 MB (+30) | no | 80 / **56** MB |

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
among its mappers, which is why the tree columns above show both. Celery's tree is 123MB by Rss
and 66MB by Pss — 57MB of double counting — against tarsk's 80 and 56. The gap between the two
runtimes is real but far smaller than summed Rss suggests, and the *tree RSS* column in the
footprint table below overstates in exactly this way.

*The cost of an import depends on what is already loaded.* Instrumenting the taskiq worker
directly showed the heavy module costing it +7MB, not +47: it already had 514 modules, so most
of celery's dependency tree was a re-import. The same code costs a fresh tarsk child +33MB
because a fresh tarsk child has almost nothing in it. Neither number is wrong; they answer
different questions, and "how much does my dependency tree cost" only has an answer relative to
a starting point.

**The footprint advantage is largest when your app is small.** Read the heavy column as a
ranking and tarsk loses it — 53MB against taskiq's 44MB. Read the footnote below and it does
not, because taskiq's 44MB is an under-read that settles near 58MB. Either way the honest shape
is this: at a trivial app tarsk's process is 27MB against taskiq's 44MB, and by the time your
dependency tree is 47MB the two are 53 and 58. What tarsk saves is the broker driver and the
scheduler, which is a fixed amount. It does not save you from your own imports, and those are
usually the larger number.

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

### Footprint, after one trivial task

| runtime | worker RSS | tree Rss | tree Pss |
|---|---|---|---|
| tarsk | **27 MB** | 51 MB | **31 MB** |
| celery | 41 MB | 91 MB | 52 MB |
| taskiq | 44 MB | 92 MB | 56 MB |

The tarsk child is CPython plus the user's task module and nothing else — no broker driver, no
scheduler (spec §4.1). The tree columns include the supervisor / master.

**Read the Pss column.** Summed Rss counts a shared page once per process mapping it, so a
supervisor and its child double-count libpython, libc and the extension module. Pss divides
each page by its sharers. On the four-core runner the correction is much the same for all three
— summed Rss inflates tarsk 1.65×, Celery 1.75× and taskiq 1.64×. An earlier version of this
paragraph said the correction favoured tarsk, from a run where those figures read 1.93×, 1.51×
and 1.66×. It does not favour anyone; it is an artefact of counting, and the ordering between
the three runtimes is the same in both columns.

Rss is also the column that will not hold still — 47 and 51 MB on two runs of the same commit
on the same runner a day apart, where Pss gave 28 and 31. Earlier machines gave 49, 50, 52, 52.
An earlier version of this table said 44 MB and I could not account for it.
Checking out the commit that produced it, rebuilding and measuring again gave the same 51 MB as
today, so nothing in tarsk had changed between them; how much of libpython happened to be
resident had. A number that moves with the page cache should not have been the headline.

**This table said 16 MB for tarsk until it was measured properly.** A child that runs one
trivial task lives about a second, and the sampler took the largest of a handful of 50ms reads
— often missing the peak entirely. The figure now comes from the worker's own `ru_maxrss`,
which cannot. A bare CPython 3.14 is 12.6 MB here and `tarsk` plus msgpack adds 11.8, so 27 was
always the honest number; 16 was never reachable. The ratio against Celery is 1.5×, not the 3×
this page used to claim.

### Bounded memory, 40 tasks each retaining 20MB

| runtime | peak worker RSS |
|---|---|
| celery (no recycling) | 841 MB |
| taskiq (no recycling available) | 844 MB |
| celery `--max-tasks-per-child=6` | **162 MB** |
| tarsk `--max-rss=200MB --slots 1` | 183 MB |
| tarsk `--max-rss=200MB` (default 100 slots) | **123 MB** |

Celery still bounds the single-slot row tighter, 162MB against 183MB, and when the leak per
task is known and constant that is a perfectly good bound. It stops short of the budget rather
than at it. Hold on to how it got there: someone divided 200MB by 20MB and typed 6.

**Both tarsk rows now land under the ceiling, and the hundred-slot row lands lowest.** That
reads backwards and is not. The supervisor budgets against what a task costs, so a child
holding several tasks at once climbs faster between two RSS readings and its projection meets
the ceiling sooner — it retires at a lower number than a child taking them one at a time. More
concurrency buys a tighter bound here, not a looser one.

**This table used to read 824MB in that row**, against 841MB for no recycling whatsoever — the
ceiling barely bound at all above one slot. A child advertises every slot at once, so a hundred
Readys arrived while its RSS still read baseline; all hundred were given a task, and the
ceiling was next consulted long after all hundred had allocated. The fix is not a smaller
default: the supervisor now learns what a task costs in bytes and refuses to fill a slot the
ceiling cannot afford. `--slots N` still means N wherever no ceiling is set.

### The same configurations, different workloads

| | leak raised to 40MB | payload-dependent 2–80MB |
|---|---|---|
| celery `--max-tasks-per-child=6` | 282 MB | 327 MB |
| tarsk `--max-rss=200MB` | **183 MB** | **187 MB** |

Nothing was reconfigured between this table and the last one. A task count encodes an
assumption about bytes per task that nothing enforces; when the assumption expires, so does
the bound, and Celery's 162MB becomes 282MB and then 327MB without anyone touching a flag.

tarsk holds at 183 and 187 because the assumption is not encoded anywhere — the supervisor
measures what a task costs and keeps measuring. That is the whole argument for a byte budget
over a task count, and it is the only table on this page where the two answer the same
question and give different answers.

The payload-dependent column is the one to read. Memory follows the payload there, so no task
count maps to a byte budget at all, and the estimate has to survive a workload whose tasks
cost 2MB and 80MB by turns. It does because it keeps the running maximum rather than the mean:
a mean fits under any ceiling, and the tail is what overshoots it.

### Task loss under a hard 400MB cgroup limit, 30 tasks leaking 20MB

| runtime | completed | lost | executions | restarts | max gap | peak RSS |
|---|---|---|---|---|---|---|
| celery (defaults) | 17/30 | **13** | 17 | 0 | 3 ms | 381 MB |
| taskiq (list, no ack) | 17/30 | **13** | 17 | 0 | 4 ms | 384 MB |
| taskiq (streams, acked) | 17/30 | **13** | 17 | 0 | 4 ms | 384 MB |
| celery `task_acks_late=True` | 17/30 | **13** | 17 | 0 | 9 ms | 381 MB |
| celery `task_acks_late=True` + restarted | 29/30 | **1** | 29 | 1 | 2530 ms | 381 MB |
| celery `--max-tasks-per-child=6` (tuned) | 30/30 | 0 | 30 | 0 | 139 ms | 162 MB |
| tarsk `--max-rss=200MB` | 30/30 | 0 | 30 | 0 | 63 ms | 183 MB |

**An acknowledgement only helps if something survives to notice.** taskiq's streams broker acks
and still loses 13 of 30 here, exactly as its unacked list broker does, for the same reason
Celery's `task_acks_late` changes nothing: the OOM killer takes the whole cgroup, coordinator
included, so there is nobody left to redeliver to. With an orchestrator restarting the worker it very
nearly recovers — the one task still missing was in flight when the kill landed, and the Redis
transport's default visibility timeout (1 hour) puts its redelivery outside the run. The price
is 2.5 seconds of dead air.

A correctly tuned Celery ties tarsk here. The difference is what you had to know in advance.

### Recycling stalls, 120 × 5ms tasks, recycled every 20

| runtime | p50 gap | p99 gap | max gap |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 5.4 ms | 139 ms | 140 ms |
| tarsk `--max-tasks=20` | 5.7 ms | 7 ms | **7 ms** |
| taskiq (no recycling available) | 5.4 ms | 6 ms | 6 ms |

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
| celery | 0.010 | 0.103 | 1.007 | 9.986 | 0.47 s |
| taskiq (list, no ack) | 0.004 | 0.027 | 0.204 | 2.088 | 0.48 s |
| taskiq (streams, acked) | 0.007 | 0.057 | 0.239 | 2.271 | 0.52 s |
| tarsk (streams, acked) | 0.003 | 0.024 | 0.217 | **2.181** | 0.21 s |

Median seconds from first completion to last, five runs a cell.

The two `streams, acked` rows are the like-for-like pair — same guarantee, same process count,
same handler. tarsk is the faster one at ten thousand, 2.18s against 2.27s, and the two sets of
five runs do not overlap: tarsk's slowest is 2.192s, taskiq's fastest 2.253s. Consistent, and
**4%** — inside the ~10% tie rule at the top of this page. Faster in every run, by an amount
this page's own standard says not to call a win.

**This page claimed 1.45× here, and that is retracted.** The claim came from a two-core runner;
these runners now have four cores, and two separate runs on them read 1.06× and 1.04×. On a
16-core laptop it reads 1.1×, across four runs ranging 1.03× to 1.18×. Three machines, one
answer everywhere except the one that no longer exists: at four cores and above these two
brokers drain a no-op queue at the same speed.

Those two four-core runs are worth reading against each other. Every absolute figure in the
second is about 25% slower than the first — 9.99s against 7.76s for Celery, 2.27s against 1.78s
for taskiq — and the ratio between the two brokers moved from 1.06× to 1.04×. That is what this
page means when it says to compare within a table: the same shared runner, a day apart, cannot
be trusted to give the same seconds, and does give the same ordering.

That has a plausible shape — on two cores taskiq's dispatch loop is CPU-bound and a Rust
supervisor pulls ahead; give both machines enough cores and they queue behind Redis instead —
but it is a story fitted to three points after the fact, and the two-core measurement can no
longer be re-run to test it. It is offered as a guess and not as a finding.

The 0.95s this page used to report for tarsk on the laptop does not reproduce either, and that
one is not a regression. Three commits were rebuilt in worktrees and re-run on the same machine
within minutes of each other — the one that first published 0.95s, the one 27 later that
restated it, and HEAD — and they measure 1.32s, 1.22s and 1.31s. Whatever produced 0.95s, it
was not code that has since been lost. What stays unexplained is worth recording: taskiq's
figure from that same old run, 1.48s, reproduces exactly, so a machine that was simply faster
that day does not fit. The run predates this harness having a CPU probe, which is the only
thing about it that can still be checked, and it fails that check by not having one.

The mutex story this section used to end on still stands as history and no longer as an
explanation of a gap. The Redis driver did hold a lock round its own connection, every command
from every child did queue behind it on a single-threaded runtime, and removing it did move
these rows. What it bought is no longer visible as a lead over taskiq at four cores; it is
visible in the 10 and 100 columns, and in the startup column, where tarsk is still half.

Three versions of this page have now read an ordering out of five runs and had to retract it.
The pattern is not noise in the runs — the separations were clean each time — it is reading a
ratio as a property of the software when it is a property of the software *and the machine*.

**What acking costs.** taskiq's own list broker against its own stream broker is the cleanest
measure of it: 2.09s against 2.27s at ten thousand, a 9% difference. On a 16-core laptop the
same pair reads 1.21s against 1.42s — 17%. Two earlier figures for this are withdrawn: 73%,
from a laptop run that does not reproduce, and 1.5%, from the two-core runner that no longer
exists. The cost is real and somewhere under a fifth; the three numbers this page has printed
for it span fifty-fold, which says more about single runs than about acking. What tarsk pays on
top of either is a Unix socket round trip per task, about 63µs, because the handler runs in a
child the supervisor can meter and replace.

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
| celery, 4 processes | 12.52s | 1.00× | 214 MB | 500/500 |
| taskiq, 4 processes × 1 | 12.55s | 1.00× | 224 MB | 500/500 |
| tarsk, 4 children | 12.64s | 1.01× | **117 MB** | 500/500 |
| celery, 32 processes | 1.54s | 0.12× | 1,368 MB | 500/500 |
| taskiq, 4 processes × 64 | 0.23s | 0.02× | 225 MB | 500/500 |
| tarsk, 32 children `slow` | 1.76s | 0.14× | 757 MB | 500/500 |
| **tarsk, 4 children × 64 slots** | **0.14s** | 0.01× | **119 MB** | 500/500 |

Held to one task per process, all three land on the arithmetic floor of 12.5s and the only
difference is footprint. Buying concurrency with processes is the expensive way: 32 children
cost 757MB to reach 1.76s, and Celery's 32 processes cost 1,368MB to reach much the same. That
tarsk row carries a `slow` mark: the probe read high while it was measured, so read the RSS
column of it and not the seconds.

`--slots 64` buys it with coroutines instead, and that row is the whole point of running this
table: **0.14s on 119MB against taskiq's 0.23s on 225MB.** Same concurrency model now, and
tarsk's advantage is the one it always had — a child that imports your tasks and no broker
driver is a smaller process to have 64 of. The same pair on a 16-core laptop reads 0.13s
against 0.20s. This is the one comparison on the page that has read the same on every machine
it has been run on, which is worth more than the size of it.

**This is not free, and what it costs is the thing this page is otherwise about.** At one slot
the ceiling is read while the child has nothing running, so overshoot is bounded by a single
task's peak. At 64 it is bounded by whatever 64 tasks are holding, and `--hard-max-rss` takes
all 64 down together rather than one. The default is 100, tuned for handlers that wait, which
is what most task queue work is; a handler that allocates wants `--slots 1`, and that is the
configuration every memory table on this page uses.

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
| celery (`-c` = cores, 4 here) | 50.34s | 215 MB | 2000/2000 |
| taskiq (2 workers × 100) | 1.31s | 145 MB | 2000/2000 |
| **tarsk (2 children × 100 slots)** | **0.96s** | **74 MB** | 2000/2000 |

The I/O handler is deliberate: a no-op would make this a measure of dispatch rather than of the
default concurrency, and the default concurrency is the whole question.

Read this against the two tarsk rows in the memory table above. Those two used to be in
tension: the same default that wins here was the one that let a leaky workload reach 824MB
under a 200MB ceiling, and this page told you to set `--slots 1` if your handlers allocate.
It no longer does. The ceiling is now budgeted against what a task costs, so the default holds
a hundred tasks in flight when they wait and as few as the ceiling affords when they allocate,
without being told which workload it is running.

### Throughput, single worker

| runtime | no-op | 50 ms handler | startup |
|---|---|---|---|
| celery | 965/s | 20/s | 0.45 s |
| taskiq | 1,784/s | 20/s | 0.51 s |
| tarsk | 2,007/s | 20/s | **0.21 s** |

All three read the same Redis. Rate and wall cover first completion to last; **startup is
separate on purpose**. Folding it in turns a 500-task row into a boot-time contest — it was 51%
of Celery's figure, 59% of tarsk's and 77% of taskiq's when this table still did that.

tarsk and taskiq are 12% apart on the no-op row, and the previous run of this table on the same
four-core runner put them 3% apart. Neither is a claim: the two figures disagree by more than
either gap, on the same machine and the same commit. The column that does not move is startup,
where tarsk is half in every run on every machine. On two cores this row read 2,859/s against
2,398/s and was printed here in bold, which was the same mistake as the drain table — an
ordering read as a property of the software.

With a 50 ms handler all three land on 19–20/s, which is the row that matters and the whole of
spec §2's argument: dispatch overhead disappears behind any handler doing real work.

## Where these numbers came from

A GitHub Actions `ubuntu-24.04` runner, four cores, AMD EPYC 9V74, via
`.github/workflows/bench.yml`. Anyone with the repository can reproduce them by dispatching
that workflow, which is the reason to prefer them over the ones this laptop produces: a 16-core
machine nobody else has is not evidence.

The core count is why Celery's out-of-the-box row is fifty seconds. `-c` defaults to it, so
Celery gets four workers where taskiq's defaults get two hundred concurrent tasks. That is what
the flag does, not a handicap applied to it — and on the two-core runner this row read a
hundred seconds, so it is a measure of the runner as much as of the default.

**Almost nothing here travelled.** Ten thousand tasks across four processes, tarsk against
taskiq's stream broker: 1.45× on a two-core runner, 1.06× on the four-core one that replaced
it, 1.1× on a 16-core laptop. Single-worker no-op throughput: 1.19× then 1.03×. Acking cost:
1.5%, then 8%, and 17% on the laptop. Three separate conclusions on this page rested on the
two-core runner and none of them survived it being replaced.

What did travel is the I/O row — 0.13s against 0.21s here, 0.13s against 0.20s on the laptop —
and every memory figure on the page. That is the shape of it: the footprint claims are
properties of the software, and the speed claims were properties of a machine. Compare rows
within a table, do not carry a ratio across, and treat any ordering here as provisional until
it has held on a machine that was not the one it was found on.

Every run prints a CPU probe taken before each row and marks the row `slow` when that reading
sits more than 25% above the run's fastest, so a table can be trusted cell by cell rather than
run by run. It compared against the median until a run warned that it had drifted 1.7× and then
marked nothing — the warning measures fastest against slowest and the mark measured against the
middle, so the two could disagree about the same run and the guard said nothing useful. On a
laptop the usual cause is the benchmark heating the machine it is running on: a twenty-minute
local run drifted from 11ms to 27ms with nothing else competing, which is most of why these
came from a runner instead. The run above drifted 19ms to 25ms and marked no rows.

The probe catches a machine that changes during a run. It does not catch a machine that is
simply a different machine than last time, which is what actually invalidated the numbers here.

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
