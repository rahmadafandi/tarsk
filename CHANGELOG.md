# Changelog

## Unreleased

### Fixed

- **A worker told to stop now stops.** SIGTERM used to end the worker only once the
  queue was empty: it kept claiming for as long as there was a backlog, and under an
  orchestrator that meant being killed mid-task when the grace period ran out.
- **Children no longer die on the signal meant for the supervisor.** ctrl-C in a
  terminal and systemd's default stop both signal the whole process group, which cut
  running tasks off instead of draining them. Children now ignore SIGINT, and SIGTERM
  until the supervisor has told them to drain.
- **A handler that raises `TimeoutError` is a handler that raised.** It was read as the
  task's own deadline: the wrong message, no traceback, and for a sync handler a killed
  worker that took every task sharing it down as `ChildDied`.
- **Waiting for a rate limit or a `max_concurrency` slot no longer spends retries.**
  Each hand-back bumped the attempt, so a job that queued behind its cap could reach its
  first real run already past `retries`, and `ctx.attempt` counted waits.
- **`ctx.task_id` is the id `send()` returned.** It was the supervisor's dispatch
  counter, so a trace started in `before_send` had nothing to join on in the handler.
- `send({1: "a"})` — a dict keyed by integers — packed at the caller and then failed to
  unpack in the worker on every attempt.
- `Depends(scope="worker")` resolved once per concurrent first caller rather than once
  per process: a hundred slots opened a hundred pools.
- Reusing one `task.options(...)` for several sends gave them all the same id.
- **Redis: delayed jobs from two producers could overwrite each other.** The delayed
  set's member was `<pid>-<counter>`, and the counter started at zero in every process —
  two producers that are both pid 1 in their containers collided.
- **Brokers reconnect.** A Redis failover, a Postgres restart or a closed AMQP channel
  left a worker that was up and failing every command, and a producer that raised on
  every send until its process was restarted. The call that discovers the drop still
  fails; the next one reconnects.
- **A chain no longer loses its tail to a failed enqueue.** If queueing the next step
  failed, the finished step was acked anyway. It is now left to be redelivered. A cron
  run whose enqueue fails is retried rather than skipped fleet-wide.
- A send whose enqueue failed kept its dedup reservation, so the caller's retry was
  answered with the id of a job that was never queued.
- **`cancel` finds a job by its id.** On Redis and Postgres a cancel naming the wrong
  queue — including the default `"default"` for a job on any other queue — reported
  success and changed nothing. `queue` is still accepted and no longer decides anything.
- **Replaying a dead letter replays the same job.** On Redis and Postgres the replay
  dropped the rest of its chain, its `meta` and its expiry, and on Redis its id.
  Entries buried before this release replay as they did before.
- **Postgres: send dedup, `max_concurrency` and `rate_limit` are atomic.** The first send
  after a dedup window expired did not reserve it; the slot count and the token bucket
  were read and written in separate steps that two workers could interleave.
- **Postgres: connecting no longer deadlocks with a busy worker.** Schema setup ran on
  every connection and took its locks in an order that could collide with a claim. It
  now runs only when the schema has changed.
- **Console: an action whose form body arrived after its headers** was read as an empty
  form — and an empty id means "all", so replaying one dead letter could replay or
  purge every one in `default`. The body is now read to its `Content-Length`, and an
  action that names no queue is refused.
- `tarsk jobs --state …` filtered after `--limit`, so it could print nothing while
  matching jobs existed.
- ctrl-C on `tarsk worker` ended a clean drain with a `KeyboardInterrupt` traceback.
- **Cron: `7` is Sunday.** `0 0 * * 7` was refused, and a schedule that cannot be parsed
  was dropped without a word. It is now reported once on the worker's stderr.
- **Console without a token answers only as `127.0.0.1`, `localhost` or `[::1]`.** The
  loopback bind was its only protection, and DNS rebinding walks round a bind.
- Console: cancelling a delayed job now lasts until the job is due, not a fixed day.
- **Redis: the lease sweep looks past the first 64 pending entries.** A stranded job
  behind that many long-running ones was never examined while the queue stayed busy.
- `--hard-max-rss` kills at the limit instead of first waiting for a replacement child
  to finish starting.
- Creating a producer no longer holds the GIL while it connects, and a producer made
  before a fork is not shared with the child.

### Changed

- Broker failures are printed to stderr with their cause, at most once every five
  seconds, instead of only moving the `broker_errors` counter. Postgres errors say what
  the server said rather than `db error`.
- `set_progress` no longer holds up a child's other frames while it is written.
- `Task.send`, `send_async`, `send_in`, `send_at` and `si` carry the handler's signature,
  so a type checker sees a wrong argument at the call site.
- Redis: cancellations are stored under one key, `tarsk:revoked`. The per-queue keys
  0.2.1 wrote are still read.
- Postgres: adds `tarsk_schema`, three columns on `tarsk_dead`, one on `tarsk_buckets`
  and the function `tarsk_acquire_slot`. Applied automatically on first connect.
- The release workflow runs the full CI before it builds anything to publish.

## 0.2.1

### Fixed

- `before_send` middleware now sees and can add to `ctx.meta`. It was
  constructed empty and never read back, so the documented way to attach a
  trace id to a job did not reach the job.

## 0.2.0

### Breaking: one distribution per broker

`pip install tarsk` has shipped Redis, Postgres and AMQP since 0.1. It no longer ships
any of them. There are four distributions now, all the same Rust and the same `tarsk`
package, differing only in which broker is compiled in:

| install | brokers |
|---|---|
| `pip install tarsk` | `memory://` |
| `pip install tarsk-redis` | `memory://`, `redis://`, `rediss://` |
| `pip install tarsk-postgres` | `memory://`, `postgres://`, `postgresql://` |
| `pip install tarsk-amqp` | `memory://`, `amqp://`, `amqps://` |

**If your broker URL is not `memory://`, upgrading to 0.2.0 without changing the package
will break your workers.** They will start and then refuse the URL. The fix is one line:

```bash
pip uninstall tarsk && pip install tarsk-redis     # or -postgres, or -amqp
```

Nothing in your code changes — same `import tarsk`, same API, same CLI, same wire
format. A 0.1.3 worker and a 0.2.0 worker on the same queue understand each other.

Why: a wheel with four broker drivers in it is 3.3 MB of Rust to run one of them, and
nobody runs four brokers at once. This is the `opencv-python` / `opencv-python-headless`
arrangement, with the same consequence — **install exactly one.** All four provide
`tarsk._core`, so pip will happily let the second overwrite the first and leave you
running a binary that is not the one you asked for.

### Added

- `tarsk` refuses to import when it finds more than one `tarsk*` distribution installed,
  naming what it found and what to uninstall. Without it, the wrong binary is silent.
- `tarsk._core.backends()` lists the brokers the installed wheel actually has.
- `TARSK_REDIS_URL` and `TARSK_PG_URL` point the broker tests at a server you already
  run, as `TARSK_AMQP_URL` already did.

### Changed

- A URL for a backend this build lacks now names the package to install
  (`redis:// is not available in this build — install tarsk-redis …`) rather than a cargo
  feature to add, and lists what the build does have. It also prints only the scheme,
  not the URL, which was putting broker passwords in logs.
