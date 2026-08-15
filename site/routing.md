---
title: Routing and scheduling
---

# Routing and scheduling

Which worker runs what, in which order, and when.

[← Back to the index](./)

## Priority

```
tarsk worker --queues high,low
```

The order is a priority, not a preference: nothing from `low` is claimed while `high` has work.
Postgres sorts by it in the same statement that claims the row, so it costs nothing there.
Redis needs a read per queue — the highest with anything waiting wins, and only when they are
all empty is there anything to wait on, which is one blocking read across all of them. The
price is one extra round trip per claim while a high queue is empty and a low one is not, paid
by workers that asked for more than one queue.

**Strict at the moment of claiming, which is the only moment it can be.** A batch already
claimed is already leased to this worker and still runs; priority decides what is read next,
not what is already held. On Redis that is a prefetch batch of slack; on Postgres, which claims
one row at a time, there is none.

This page used to say `--queues high,low` merely preferred the first within one read, which was
true and is what a single `XREADGROUP` over two streams does — Redis answers it with up to
`COUNT` from *each*, so a batch held low work that ran ahead of high work arriving while it
drained.

`--hard-max-rss` is the exception, and it is off by default. Set it and a child that reaches it
mid-task is killed rather than allowed to keep growing — the task is retried, and if it cannot
fit it is dead-lettered instead of retried forever. It cannot make an oversized task succeed;
it decides who loses when one task threatens the box. It must sit above `--max-rss`, or the
graceful ceiling could never fire.

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

**Nothing here grows without a bound.** Results and progress carry the TTL the task asked for
and expire on their own; the live stream loses an entry when it is acked; reservations, buckets
and cancellations run on their own clocks. Dead letters were the exception — one payload and
one full traceback per row, kept forever — so `--max-dead` caps them at 1,000 a queue and
drops the oldest past that.

That is a count, and a count is the proxy the tables above spend their time arguing against: it
bounds bytes only if you know what a row weighs. Redis Streams offer `MAXLEN` and `MINID` and
nothing by size, so a count is what the datastore gives. Measured, a failure costs about **2 KB
plus its payload** — mostly traceback — so a thousand is a couple of megabytes a queue with
small payloads, and ten thousand of a one-megabyte payload would be ten gigabytes. Raise it
knowing what yours weigh.

The default is small because of which way it fails. Too small loses old failures, which is
visible and documented. Too large runs the broker out of memory and takes the healthy work down
with it, which is the failure this whole project exists to prevent. `--max-dead 0` keeps every
failure, and is right only if something else is emptying them.

On Redis the trim is approximate — it drops whole nodes and charges almost nothing for it — so
the cap is a bound rather than an exact count.

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


---

[← Back to the index](./)
