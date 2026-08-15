---
title: Writing tasks
---

# Writing tasks

Timeouts, retries, expiry, deduplication, rate limits — everything a handler can ask for.

[← Back to the index](./)

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

## Being asked before being stopped

```python
@app.task(timeout=30, soft_timeout=25)
async def render(doc_id: str, ctx=Depends(Context)):
    try:
        return await slow_work(doc_id)
    except asyncio.CancelledError:
        if ctx.soft_expired:
            save_partial(doc_id)      # five seconds to get this out
        raise
```

`timeout` stops a task. `soft_timeout` asks it to stop first, leaving the gap between the two
to save partial work, release something, or raise `Retry` to hand the job back. Ignore the ask
and `timeout` takes it anyway — the soft one is a request, not a second enforcer.

**The handler catches `asyncio.CancelledError`, not a tarsk exception.** Asking is cancellation,
and asyncio gives no way to raise a type of your choosing into a running coroutine; anything
else here would be describing a different runtime. `ctx.soft_expired` is what separates a passed
deadline from a worker shutting down, which is the distinction worth acting on. The job is
reported as `SoftTimeout` so a dead letter says it ran long rather than that it was stopped
cold.

Two limits, stated because both are real:

- **Async handlers only.** A sync handler runs in a thread that cannot be interrupted, so there
  is nowhere to deliver the ask. `soft_timeout` on one is a startup error rather than a flag
  that silently does nothing.
- **A sync middleware swallows the ask.** It runs in a thread holding a blocking call into the
  loop, so the cancellation lands on the middleware and the handler never sees it. The job is
  still reported as `SoftTimeout`; the handler just never got its chance.

`soft_timeout` must be less than `timeout`, checked at startup, because one at or past it would
never fire.

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

## Expiry

```python
@app.task(expires=300)
def refresh_dashboard(user_id): ...

refresh_dashboard.options(expires=30).send(uid)     # this one, sooner
send_report.options(expires=3600).send(month)       # no registered deadline; ask for one
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

**A send can set its own.** Staleness usually belongs to the request rather than the task: the
nightly rebuild can wait an hour and the same task fired from a page load cannot, and the
registration cannot know which one it is looking at. `options(expires=…)` decides for one send
and wins over the registration.

Omitting it means "the sender did not say", which leaves the registration in charge — so a send
can shorten a deadline or supply one, and cannot remove one the task registered. A request
should not be able to overrule a policy by leaving a field out.

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


---

[← Back to the index](./)
