---
title: Operating
---

# Operating

Watching a running worker, and reaching into it.

[← Back to the index](./)

## Cancelling, and what is in the dead letters

```
tarsk cancel <id>                     # stops it before it starts
tarsk dead list                       # what the retries gave up on
tarsk dead show <id>                  # its traceback
tarsk dead replay <id>                # put it back on the queue
tarsk dead purge
```

`app.cancel(task_id)` is the same thing from Python, and `AsyncResult.cancel()` from a handle.
It takes effect within a second: the supervisor pulls cancellations as a set on a timer rather
than asking the broker about every job it is about to run, which would put a round trip on the
dispatch path of every task to answer "no" for almost all of them.

**A cancellation finds its job by id.** `--queue` on the command line and `queue=` in Python
are still accepted and decide nothing. They used to: a cancel that named the wrong queue —
including the default, for a job on any other — reported success and the job ran anyway. The
one thing a cancellation still needs from you is `ttl`, how long to remember it, which has to
outlive the job: something scheduled for next week needs a week. The console works that out
from the job's due time.

**A job that has already started is not interrupted, unless you ask.** The supervisor could tell the child to
drop it, but a handler that has begun has usually begun doing the thing you wanted stopped —
written the row, called the API — and a queue that reports "cancelled" for work that
half-happened is worse than one that admits it finished.

`app.cancel(task_id, interrupt=True)` asks. An async handler is cancelled where it is waiting:
it sees `asyncio.CancelledError` with `ctx.cancelled` set, which is what tells this from a
deadline or a worker shutting down, and whatever it had already done stays done — so it is for
work that can be abandoned halfway. A sync handler runs in a thread, which cannot be
interrupted, and finishes regardless.

A cancelled job is settled and gone, started or not: not retried, and not left in the dead
letters where a replay would bring back something its caller asked to be rid of. If it keeps a
result, the result says `Cancelled`.

Neither command needs `--app`. A cancellation is an id and a dead letter is a name, a payload
and a traceback; none of them needs your code to be importable, which matters exactly when the
reason the tasks died is that it is not.

## Pausing a queue

```
tarsk pause --queue reports      # every worker stops taking from it, within a second
tarsk resume --queue reports
```

`app.pause("reports")` and `app.resume("reports")` from Python, and a button beside each queue
in the console. For when whatever a queue's tasks call is down: the jobs wait where they are
instead of spending their retries against it, and the workers' other queues carry on. Jobs
already running finish, sending to the queue still works, and `tarsk status` marks it
`paused`. It is a property of the queue, held in the broker, so it survives a worker restart
and applies to workers started later — remember to resume it.

## Which task is leaking

The ceiling tells you a worker grew. It does not tell you what grew it, and that is the next
question. `/metrics` and the console answer it per task, with two numbers:

- **Objects kept** — how many objects each run left the interpreter holding, counted inside
  the worker around every call. A task that cleans up after itself comes out near zero, and
  one that does not is the leak. `tarsk_task_kept_objects{task=…}`.
- **Growth** — how much the worker's memory rose while the task ran, read by the supervisor
  whenever a ceiling is set. `tarsk_task_rss_growth_bytes_total{task=…}`.

Read the first for *who* and the second for *how much*. They can disagree, and when they do
the first is right: an allocator hands the memory one task freed to the next task that asks,
so a leak is paid for by whoever allocates after it. Measured with a leaking task and a tidy
one taking turns, the tidy one was charged 40 MB of growth and the leak 16 — and the leak kept
738 objects to the tidy task's 42.

Both are exact with `--slots 1`. Above that, tasks that overlap are in each other's numbers:
good for ranking, not for accounting. Objects are a count, not a size, so a task that leaks
one enormous buffer scores low on it and high on growth.

## Stopping a worker

SIGTERM, or ctrl-C, and the worker stops taking work at once: what its children are running
finishes, nothing new is claimed, and it exits. Anything it had read ahead and not started is
handed back for another worker rather than held until its lease runs out.

The signal can go to the supervisor alone, as an orchestrator sends it, or to the whole process
group, as a terminal and systemd's default `KillMode` do. Children ignore the one meant for the
supervisor and wait to be told to drain, so in both cases a running task ends because it
finished and not because it was cut off.

How long that takes is the longest task in flight, up to its own `timeout`. Give the
orchestrator's grace period at least that — `terminationGracePeriodSeconds`,
`TimeoutStopSec` — or the kill it sends at the end of it lands on a task mid-run, and that task
comes back to another worker when its lease expires.

## When the broker goes away

A failover, a restart, a connection idle too long for some middlebox: workers and producers
both reconnect. A worker keeps trying for as long as it is up and prints what the broker said,
at most one line every five seconds:

```
tarsk: broker error during claim: db error: FATAL: terminating connection due to administrator command
```

`TARSK_LOG_FORMAT=json` turns every line a worker prints into one JSON object with a level
and a timestamp, for a log pipeline that cannot do anything with prose. `broker_reconnects`
in the stats and on `/metrics` counts how often Postgres or RabbitMQ had to be connected to
again; a number that climbs is a network worth looking at before it is an outage.

A producer raises `tarsk.BrokerError` — its own type, so the code around a `send()` can retry
or queue locally on an outage without also catching its own mistakes, which are still
`ValueError`:

```python
from tarsk import BrokerError

try:
    task_id = send_receipt.send(order.id)
except BrokerError:
    outbox.keep(order.id)       # the broker is away; this is not a bug in the call
```

On Redis and Postgres a producer notices a dropped connection before it sends on it, so the
drop costs no call. On Redis that is a PING, sent first on any connection that has been quiet
for a second — one extra round trip for a producer that sends rarely, none for one that is
busy. A command that was already in flight when the connection died fails either way, and is
not retried for you: whether the server acted on it is not something a client can know.

## Seeing the backlog

```
$ tarsk status --broker redis://localhost:6379/0 --queues default,reports
queue       ready  running  delayed     dead
default        25        0        5        0
reports         0        1        0        2

$ tarsk jobs --broker redis://localhost:6379/0 --state running
70cd39698ff0f529  running  rebuild_report  14m ago  attempt 2  on tarsk-25530
```

`tarsk status` says how far behind; `tarsk jobs` says behind on what. The second question is
the one asked when something is stuck, and a count cannot answer it. `--state ready`,
`running` or `delayed` narrows the list; a delayed job reports the time until it is due rather
than the time it has waited, because it has not started waiting. A running job names the worker
holding it, which is the question after "what is stuck": stuck *where*.

```python
send_invoice.options(meta={"trace": trace_id, "tenant": tenant.id}).send(invoice.id)
```

`meta` is anything the sender wants to carry alongside a job. The handler reads it from
`Context.meta` without having declared a parameter for it, and `tarsk jobs --meta` prints it,
so a trace id survives from the send to the listing. It travels beside the arguments rather
than inside them: a listing can read it without unpacking a call it does not understand, and
nothing in the supervisor ever looks at what is in it.

And as `tarsk_queue_jobs{queue,state}` on the metrics endpoint, refreshed by the worker on the
same timer that pulls cancellations.

This is the question an operator asks first — *how far behind are we* — and until this existed
the metrics could not answer it. They counted tasks finished, which is throughput; a queue can
be perfectly healthy on throughput while an hour deep in backlog.

`running` is work handed to a worker and not yet settled, `delayed` is enqueued for later and
not yet due. On Postgres those two are both a lease in the future and only `run_lease` tells
them apart — a delayed job has never been claimed.

## The console

A worker already runs an HTTP listener for `/metrics`. The same one serves an admin page at
`/`, so there is nothing extra to deploy:

```
tarsk worker --app myapp:app --broker redis://… --metrics 127.0.0.1:9090
```

Queues and their depth, the individual jobs with their state and age and which worker holds
them, and the dead letters with their tracebacks. With actions enabled, buttons to cancel a
job, replay a dead letter, or purge the lot. Server-rendered from the standard library — no
framework, no JavaScript build, and tarsk's only dependency is still msgpack.

**It is not `/metrics`.** That endpoint serves numbers; this one serves task payloads and can
destroy work. Three defaults follow from that:

- Binding anywhere but loopback **refuses to start** without `TARSK_ADMIN_TOKEN`. The flag that
  was harmless for metrics yesterday exposes an admin console today, and a warning nobody reads
  is not a control.
- Without a token it answers only when addressed as `127.0.0.1`, `localhost` or `[::1]`. A
  loopback bind keeps other machines out and does nothing about DNS rebinding, where a page on
  another site points its own hostname at your loopback and the browser then treats this
  console as part of that site. The request still arrives under the attacker's name.
- With a token set, the page asks for it over HTTP Basic — user `tarsk`, password the token —
  compared in constant time.
- Cancel, replay and purge are **off** until `TARSK_ADMIN_ACTIONS=1`. A console reached by
  accident should be a viewer.

`/metrics` itself stays unauthenticated. It was before this existed, it carries no payloads,
and breaking every scrape config to add a console would be a poor trade.

And because it is a web page that can destroy work:

- **Cross-site posts are refused.** Basic credentials are attached by the browser to any
  request to an origin it holds them for, including a form on someone else's page — so without
  this, a page you visit while logged in here could press purge. Cookies have `SameSite`; Basic
  auth has nothing, so `Sec-Fetch-Site` and `Origin` stand in. A request with neither header is
  not a browser and is allowed: it supplied the token itself.
- **Everything rendered is escaped** — task names, ids, tracebacks, queue and worker names all
  come from outside this process.
- **A strict `Content-Security-Policy`** on every response: no scripts, no images, no external
  anything, so an escaping bug is a rendering bug rather than an execution one. Plus
  `frame-ancestors 'none'` and `X-Frame-Options: DENY`, because a page with a purge button on
  it should not be frameable, and `Cache-Control: no-store`, because the page shows payloads.
- **A quarter-second pause on a wrong token**, and at most 32 connections at once. Constant-time
  comparison stops a timing attack and does nothing about volume; the pause turns millions of
  guesses a second into four. The connection cap is there because these tasks live in the
  supervisor, and an unbounded accept loop lets a stranger spend its memory from outside.
- **Every destructive action is logged** to the worker's stderr with the queue, the id and the
  address it came from. Basic auth gives one account, so the address is as close to *who* as
  this gets — but a purge with no record at all is the thing noticed a week later.

**Two things this cannot fix for you.**

The token crosses the network in Basic auth, which is base64, which is readable. Requiring a
token off loopback stops the accident of an open console; it does not make the link private.
Either keep it on `127.0.0.1` and reach it through an SSH tunnel, or put it behind something
that terminates TLS.

And a dead letter's traceback is shown in full, because that is what makes it worth keeping. If
your handlers put credentials in exception messages, the console will display them.

**Keep the broker password out of `ps`.** `--broker postgres://user:pass@host` is visible to
every process on the machine. `TARSK_BROKER` in the environment is not.

Every `tarsk` subcommand resolves the broker the same way: `--broker` if given, else
`TARSK_BROKER` from the environment, else `TARSK_BROKER` from a `./.env` file in the working
directory — the precedence every dotenv tool trains people to expect, so a shell export can
always override a project file and a flag can always override both. Only `TARSK_`-prefixed
keys are read from `.env`: a queue CLI has no business exporting the rest of a project's
secrets into its own process. The library half matches: `App()` with no URL reads
`TARSK_BROKER` too, though only the variable — a library that reads files at import time is a
surprise, so `.env` belongs to the CLI.

```
tarsk web --broker redis://… --addr 127.0.0.1:9099
```

The same pages with no worker attached, for when every worker is down — which is when a queue
most needs looking at, and the one case a console living inside the worker cannot cover.

## Metrics

`--metrics HOST:PORT` serves Prometheus text from the supervisor. Nothing is sampled for the
sake of metrics — the counters sit on paths that already existed and child RSS is the reading
the supervision loop takes anyway, so a Python worker never learns it is being observed.

Including `tarsk_supervisor_rss_bytes`, so the constant this project calls constant can be
checked rather than believed.

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


---

[← Back to the index](./)
