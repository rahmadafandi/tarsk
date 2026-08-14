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

```python
@app.on_start
def connect():
    global pool
    pool = psycopg.ConnectionPool(...)   # not at import: see below
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
| celery `--max-tasks-per-child=6` | **162 MB** | 282 MB | 327 MB |
| tarsk `--max-rss=200MB --slots 1` | 202 MB | **222 MB** | **276 MB** |

Celery wins the first column, and that is the honest result: when the leak per task is known
and constant, dividing the budget by it works. It is the other two columns that tarsk was
built for — nothing was reconfigured between them.

Recycling is also free, which it usually is not. A replacement child is started before the
trigger fires, so the slot never goes empty:

| | p50 gap | p99 gap | max gap |
|---|---|---|---|
| celery `--max-tasks-per-child=20` | 5.4 ms | 41 ms | 139 ms |
| tarsk `--max-tasks=20` | 5.6 ms | 8 ms | **8 ms** |
| taskiq (no recycling at all) | 5.4 ms | 6 ms | 7 ms |

And the worker that runs your code is smaller, because it imports your tasks and nothing else —
no broker driver, no scheduler: 26 MB against Celery's 41 MB and taskiq's 44 MB.

Your imports land in one process here, and the supervisor is not it:

| runtime | coordinator (trivial app → heavy app) | coordinator imports your code |
|---|---|---|
| celery | 50 → 69 MB | **yes**, and it forks children from that |
| taskiq | 47 → 47 MB | no |
| tarsk | 26 → 26 MB | no |

(`python bench/run.py imports`. The heavy app imports celery + taskiq + redis.)

taskiq keeps its coordinator clean too, so that part is not a tarsk invention. The difference
is what the clean process then does: taskiq's starts and restarts workers, while tarsk's reads
their RSS, retires them, holds their leases and runs the schedule. A memory ceiling has to be
held by something that does not have the problem — which is also why Celery could not enforce
one from a master carrying 77 MB of your dependencies.

Draining 10,000 no-op tasks across four worker processes, same Redis and same at-least-once
guarantee, startup excluded: Celery 7.6s, taskiq 2.7s, tarsk 1.9s. That gap is recent and it
is not a clever optimisation — the Redis driver used to hold a mutex round its own connection,
serialising every command from every child, and removing it is the whole difference. A no-op
handler is also the case most favourable to whoever dispatches fastest, and nobody runs one.
Full tables in [`bench/`](bench/README.md), measured on a GitHub Actions runner so anyone can
reproduce them — the same 1.45× holds on a 16-core laptop where both figures are half these.

## What it does not claim

**Not faster.** Dispatch costs microseconds; real handlers run for 50ms to minutes. With a 50ms
handler tarsk, Celery and taskiq all reach 19–20 tasks/s — identical, as they should be. tarsk
drains a no-op queue faster than both, and anyone selling a task queue on that number is
selling the wrong thing.

**Not precise and concurrent at once.** One task per child is the default, and it is what makes
the ceiling exact — a child with nothing running cannot grow. For handlers that wait rather
than allocate, `--slots N` runs N at a time in one child: 500 tasks each awaiting 100ms take
0.19s on 116MB, against taskiq's 0.23s on 225MB and 12.5s for tarsk at one slot. The cost is
the precision above — overshoot becomes the peak of whatever is in flight, not of one task. Set
it high for work that waits, leave it at 1 for work that allocates.

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

`--slots N` lets one child hold N tasks at once, and it costs precision in exactly the place
this project claims it. At one slot the ceiling is read while the child has nothing running, so
overshoot is bounded by a single task's peak. At 64 it is bounded by whatever 64 tasks are
holding, and a hard kill takes all 64 down together.

**The default is 100 slots**, whatever else is set — taskiq's number, and tarsk is tuned out
of the box for handlers that wait. 2,000 awaiting tasks with no flags at all drain in 0.98s on
78MB, against taskiq's 1.36s on 210MB. The same default lets 40 tasks leaking 20MB each reach
826MB under a `--max-rss=200MB` ceiling, where `--slots 1` holds them to 205MB: a hundred slots
hands a child a hundred tasks before any of them has allocated anything, so the ceiling is
first read long after the damage. **If your handlers allocate, set `--slots 1`.** One flag quietly changing another flag's default is worse
than either number being wrong, so `--max-rss` does not move this one. What the worker does
print, when a ceiling and several slots are both in play, is what that combination means: the
ceiling still fires, but several tasks are running when it does, so overshoot is their peak
rather than one task's.

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

`--hard-max-rss` is the exception, and it is off by default. Set it and a child that reaches it
mid-task is killed rather than allowed to keep growing — the task is retried, and if it cannot
fit it is dead-lettered instead of retried forever. It cannot make an oversized task succeed;
it decides who loses when one task threatens the box. It must sit above `--max-rss`, or the
graceful ceiling could never fire.

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

## Middleware and injection

```python
class Trace:
    async def execute(self, ctx, call):
        with tracer.span(ctx.name):
            return await call()

app.middleware(Trace())

@app.task()
def charge(order_id, db=Depends(pool)):
    ...
```

Middleware is an onion, not a pair of callbacks: you hold the call open, so a span or a
transaction is the obvious shape rather than something reconstructed from two hooks. The
timeout covers the middleware too — a tracing layer that hangs holds the lease exactly as a
handler would.

`execute` may be sync or async. A sync one runs in a thread and is handed a `call()` that
blocks it until the inner layers finish, so a plain `with` block wraps the task the way it
looks like it should. That costs a thread for the length of the task — the same trade sync
handlers already make.

Everything else takes either kind too. Handlers, `on_start`, `on_stop` and `Depends` providers
may be sync or async, but only sync *handlers* and sync *middleware* are moved to a thread. A
sync hook or provider runs on the worker's event loop, which is harmless while a worker runs
one task at a time and is the first thing to revisit if that changes.

`Depends(provider)` resolves once per worker process by default, which is what a module-level
global would have been, except `app.override(provider, fake)` can replace it in a test. Pass
`scope="task"` to resolve per call.

`execute` runs in the worker; `before_send(ctx)` runs in the producer before the job is
serialised and may add to `ctx.kwargs`, which is how a trace id gets attached to work that has
not left the process yet. Neither can run in the supervisor:
that process never imports your code, which is what keeps its footprint constant. For the same
reason there is no "result stored" hook — the supervisor is what stores it.

## Saying no, and saying how far along

```python
@app.task(retries=5)
def parse(blob, ctx=Depends(Context)):
    if not looks_like_json(blob):
        raise Reject("this will never parse")      # straight to the dead letters
    if upstream.is_cold():
        raise Retry("warming up", delay=30)        # back to the queue, one attempt spent
    ctx.set_progress({"rows": 0})
```

`Reject` skips whatever retries are left — spending five attempts to reach the same answer only
delays the moment someone looks at it. `Retry` is still charged an attempt: a retry that costs
nothing is an infinite loop with extra steps, and the thing being waited on is usually the
thing least able to absorb one.

`ctx.set_progress(value)` is readable through `app.result(task_id).progress()`, kept under the
same TTL as the result. The worker has no broker connection — that is what keeps its imports
down — so progress travels over the same socket as everything else, which also means a
progress frame from a handler thread and an Ack from the event loop are serialised rather than
interleaved.

## Scheduling

`send_in(60, …)` runs a task once, later; `send_at(when)` takes a timezone-aware datetime.
`task.options(queue=…, timeout=…, task_id=…)` overrides any of it for one send — keyword-only
and on its own object, so nothing can collide with an argument the task itself takes. A
caller-supplied id doubles as an idempotency key.

 `@app.task(cron="*/5 * * * *")` runs it on a
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

Both speak TLS, which every managed instance requires. `rediss://` for Redis, and for Postgres
whatever `sslmode` says — `require`, `verify-ca` and `verify-full` all verify the chain and the
hostname, since rustls does that on every handshake. The last two are spellings tokio-postgres
does not know and are mapped to `require` rather than refused, because a connection string
copied from a provider usually says one of them. `sslmode=disable`, or no `sslmode` against a
server without TLS, still connects in the clear.

Point `SSL_CERT_FILE` at your provider's CA bundle if it is not one the system already trusts.

`rediss://` connects over TLS, which every managed Redis requires — verified against the
system trust store, so an untrusted certificate is refused rather than accepted. Append
`#insecure` to skip that check for a self-signed certificate you control; nothing else turns
it off.

Failures follow the task's own `retries` and `backoff`, and neither backend needed a delay
queue for it: Postgres already has a visibility timer, and Redis has the idle clock its pending
sweep reads. What runs out of retries lands in `tarsk:{queue}:dead` or the `tarsk_dead` table,
with the error and traceback.

## Enqueueing from async code

```python
job = await send_email.send_async(user.id)
answer = await app.result(job).get_async(timeout=30)
```

`send()` and `get()` are blocking, and in a web handler that matters more than the microseconds
suggest. Measured on 500 sequential enqueues against a local Redis: the blocking pair held the
event loop for 45ms of the 46ms it took, while the async pair kept it responsive at 103µs
median lag. `get()` is worse than either — it polls, so awaiting a result on the loop holds it
for up to the timeout, thirty seconds by default.

The Rust producer releases the GIL for the round trip, so the async versions hand it to a
thread and the loop is free for the whole wait rather than the parts Python was not holding.
That is a thread hop rather than a native async client: 500 concurrent enqueues take 57ms, and
the ceiling is asyncio's default executor rather than the network.

## Chains and groups

```python
from tarsk import chain, group

pipeline = chain(fetch.s(url), parse.s(), store.si("bucket"))
result_id = pipeline.send()          # only the first step is queued now
answer = app.result(result_id).get() # the id of the last step, known up front

ids = group(resize.s(f) for f in files).send()
```

`t.s(...)` is fed the previous step's result as its first argument; `t.si(...)` is not, for
steps that take only what they were given. Every step's id is minted by the client before
anything is sent, which is why a handle to the end of a chain exists while its first step is
still being written.

**The chain travels as data on the job record**, a list of `[id, name, payload, timeout, queue,
feed]`. The supervisor splices the result into the next step's arguments and queues it; it never
learns what any of those names mean, which is the same rule that keeps it from importing your
app (§4.1). The next step is queued before the current one is acked, so a crash in between
redelivers the step rather than dropping the rest of the chain — at-least-once, as everywhere
else here.

A chain that fails partway stops there: the remaining steps were never queued, so there is
nothing to cancel. A group is deliberately thin — the queue already runs what it can in
parallel, and what `group` adds is handles that exist before the sending does.

**No chord.** Fanning back in needs a counter in the broker and every member's result gathered
before the callback can run, which is a different kind of machinery from either of these. Ask
if you want it.

## Expiry

```python
@app.task(expires=300)
def refresh_dashboard(user_id): ...
```

Work that waited too long is dropped at the moment a worker would have started it, rather than
run late. That is what an outage leaves behind: a backlog where the nine o'clock reminder is
now being sent at three, and the abandoned-cart email is going to someone who checked out
hours ago.

**Measured from when the job became runnable, not from when it was enqueued.** Otherwise
`send_in(3600)` on a task with `expires=300` would be dead before it was ever due — a trap
worth not building. On Redis that costs nothing: a stream id already carries the millisecond it
entered, and a delayed job only enters when the sweep promotes it.

Checked at every dispatch, so a retry after the deadline is dropped too — stale work is still
stale on the second attempt. A job already running is not interrupted. Drops are counted as
`tarsk_tasks_expired_total` rather than passing for success.

There is no default and no app-wide setting. How long a task stays worth doing is domain
knowledge, and the asymmetry is one-sided: without expiry, work runs late and you can see it;
with the wrong expiry, work disappears and you cannot.

## Not sending the same thing twice

```python
@app.task(unique=30)              # identical arguments collapse for 30 seconds
def refresh(user_id): ...

refresh.options(dedup_key=f"user:{uid}", dedup_ttl=60).send(uid, force=True)
```

The second send is dropped and **hands back the first job's id**, so the caller waits on the
same answer rather than on a job that was never queued. With no explicit key the arguments are
the key, hashed from the same bytes that go on the wire.

This is a window, not "until it has run": the reservation expires on its own timer and nothing
clears it early. That is the honest shape for "don't refresh this more than once a minute", and
it is the wrong tool for "run this exactly once ever" — say so with a database constraint,
where exactly-once actually lives.

**`task_id=` is not this.** It files two sends' results under one key; the queue still takes
both jobs and runs them both, and only the answers collide. This README used to imply
otherwise.

## Concurrency caps

```python
@app.task(max_concurrency=2)
def rebuild_report(account_id): ...
```

At most this many in progress at once, across every worker. That is a different question from
the rate limit above: `"5/s"` bounds how often something *starts* and does nothing to stop a
hundred slow ones piling onto the database at the same time.

**The slot carries its own expiry** rather than being a counter, and its expiry is the job's own
lease. A worker that dies holding a slot would otherwise throttle that task forever, and the
symptom — a task that quietly stops running, with nothing in the logs — is the worst kind to
debug. Here the slot lapses exactly when the job it was holding does, and the job comes back
through the same reclaim path as any other lost lease.

Refused jobs are handed back with a short delay, so the worker slot goes to other work rather
than idling. Measured with sixteen worker slots free and `max_concurrency=2`: two ran at a time.

## What a task can be sent

JSON-shaped values, plus `datetime`, `date`, `time`, `timedelta`, `Decimal`, `UUID`, `set` and
`frozenset` — as msgpack extension types, so they come back as themselves rather than as
strings you have to parse. A `Decimal` that returned as a float would be the exact bug
`Decimal` exists to avoid.

Anything else raises at the call site, where you can still fix it:

```
TypeError: cannot send a Session: tarsk carries JSON-shaped values plus datetime, date,
time, timedelta, Decimal, UUID, set and frozenset. Convert it at the call site, or send
an id and load it in the handler.
```

**There is no pickle option, and there will not be.** Unpickling is arbitrary code execution
by design: anyone who can write to your broker owns every worker. A supervisor built to contain
a leaking process is not much use if the channel into it runs whatever it is sent. Sending an
id and loading the object in the handler costs one query and closes that door.

A return value is treated differently from an argument: it degrades to `repr` rather than
raising, because by then the task has run and its side effects are already spent.

## Rate limits

```python
@app.task(rate_limit="10/s")     # also "100/m", "2/h", "30/5m"
def call_their_api(order_id): ...
```

**The bucket lives in the broker, not the worker.** Celery's `rate_limit` is per worker, so three
workers each allowing ten a second is thirty a second arriving at the API you were protecting.
Here they share one bucket, and the limit means what it says however many workers you run —
measured at five a second across four processes with eight slots each, which is thirty-two
things that could otherwise have run at once.

Burst is the numerator: `"10/s"` lets ten go at once and then refills at ten a second, because
that is what someone protecting a quota of ten per second means. A task over its limit is handed
back with the wait the bucket calculated, so the slot goes to other work rather than idling.

Only tasks that ask for a limit pay for one — the check is a broker round trip, and a task
without `rate_limit` never makes it. If the bucket cannot be read at all, the task is deferred
rather than run: a limit exists because exceeding it hurts something outside this process.

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

**A job that has already started is not interrupted.** The supervisor could tell the child to
drop it, but a handler that has begun has usually begun doing the thing you wanted stopped —
written the row, called the API — and a queue that reports "cancelled" for work that
half-happened is worse than one that admits it finished.

Neither command needs `--app`. A cancellation is an id and a dead letter is a name, a payload
and a traceback; none of them needs your code to be importable, which matters exactly when the
reason the tasks died is that it is not.

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

## What is missing

- Not on PyPI yet — the wheel builds and installs, it just has not been uploaded
- No chord. `chain` and `group` are here; fanning back in to a callback is not
- No strict priority. A worker reads `--queues high,low` in that order within one claim, which
  prefers the first without being a priority queue
- No soft timeout — a task is stopped at its deadline rather than warned before it
- Windows runs the suites that need no broker, since neither Redis nor Postgres ships for it.
  Everything else — the channel, recycling, the memory ceiling — is tested there

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
