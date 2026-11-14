//! Where jobs come from.
//!
//! Four implementations — in-memory, Redis Streams, Postgres, AMQP — which is
//! what earns the abstraction. Spec §6 forbids introducing one before the
//! second broker exists, and it now does.
//!
//! This file is the part they share: the job and receipt types, and `Broker`,
//! which dispatches to whichever backend the URL named. Each backend is a
//! module beside it, compiled in by its cargo feature.
//!
//! Enum dispatch rather than `dyn Broker`: async fns in traits are not
//! object-safe without boxing, and four variants do not need a vtable.
//!
//! Every backend is at-least-once (spec §4.5). A lease is a promise that no
//! one else will run this job for `lease` seconds, not that it ran only once.

// Some of these are used only by the backend modules, which take them from
// here with `use super::*` rather than each repeating the list.
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(any(feature = "postgres", feature = "amqp"))]
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

#[cfg(feature = "amqp")]
mod amqp;
mod memory;
#[cfg(feature = "postgres")]
mod postgres;
// Not `redis`: a module by that name would shadow the crate inside this one.
#[cfg(feature = "redis")]
mod redis_streams;

#[cfg(feature = "amqp")]
pub use amqp::{AmqpBroker, AmqpRecord};
pub use memory::MemoryBroker;
#[cfg(feature = "postgres")]
pub use postgres::PgBroker;
#[cfg(feature = "redis")]
pub use redis_streams::RedisBroker;

pub type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// An error with its causes, on one line.
///
/// tokio-postgres displays every server-side failure as the two words
/// "db error" and keeps what the server actually said in `source()`. Printed
/// without the chain, that is all an operator ever saw.
pub fn describe(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(inner) = source {
        let text = inner.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = inner.source();
    }
    out
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Every backend's URL schemes, listed whether or not that backend was
/// compiled in: a scheme this build cannot serve is a different problem from a
/// scheme nothing serves, and the error has to tell them apart.
const REDIS_SCHEMES: &[&str] = &["redis://", "rediss://"];
const POSTGRES_SCHEMES: &[&str] = &["postgres://", "postgresql://"];
const AMQP_SCHEMES: &[&str] = &["amqp://", "amqps://"];

fn scheme_is(url: &str, schemes: &[&str]) -> bool {
    schemes.iter().any(|s| url.starts_with(s))
}

/// Backends this build can actually connect to.
///
/// Public, and exposed to Python as `tarsk._core.backends()`: each PyPI
/// distribution compiles a different set, and a wheel that quietly lacks the
/// backend you installed it for is the one failure this split can introduce.
/// Asking the binary beats reading the build log.
pub fn backends() -> Vec<&'static str> {
    let mut names = vec!["memory"];
    for (name, on) in [
        ("redis", cfg!(feature = "redis")),
        ("postgres", cfg!(feature = "postgres")),
        ("amqp", cfg!(feature = "amqp")),
    ] {
        if on {
            names.push(name);
        }
    }
    names
}

fn compiled_backends() -> String {
    backends().join(", ")
}

/// Everything up to and including `://`, or the whole string if there is no
/// scheme. A broker URL carries a password, and this ends up in logs.
fn scheme_of(url: &str) -> &str {
    match url.find("://") {
        Some(at) => &url[..at + 3],
        None => url,
    }
}

/// Why no compiled-in backend would take this URL.
///
/// "unsupported broker url" was a lie once the backends became optional:
/// `redis://` is supported, it just was not built. Since 0.2.0 the fix is a
/// different PyPI package rather than a cargo feature, so the message names
/// the package — the user of a wheel has no Cargo.toml to edit.
fn unsupported(url: &str) -> String {
    let gated = [
        ("redis", REDIS_SCHEMES, cfg!(feature = "redis")),
        ("postgres", POSTGRES_SCHEMES, cfg!(feature = "postgres")),
        ("amqp", AMQP_SCHEMES, cfg!(feature = "amqp")),
    ]
    .into_iter()
    .find(|(_, schemes, on)| !on && scheme_is(url, schemes));
    match gated {
        Some((name, schemes, _)) => format!(
            "{} is not available in this build — install tarsk-{name} \
             (pip install tarsk-{name}). This build has: {}",
            schemes[0],
            compiled_backends()
        ),
        None => format!(
            "unsupported broker url: {} (this build has: {})",
            scheme_of(url),
            compiled_backends()
        ),
    }
}

/// Whether this build has a backend for `url` at all.
///
/// Separate from connecting so a caller can tell the two failures apart: a
/// URL no backend here will take is a mistake in the URL or the install, and
/// stays wrong however often it is retried; a backend that could not be
/// reached may be there in a second.
pub fn supports(url: &str) -> Result<(), String> {
    let served = url == "memory://"
        || url.is_empty()
        || (cfg!(feature = "redis") && scheme_is(url, REDIS_SCHEMES))
        || (cfg!(feature = "postgres") && scheme_is(url, POSTGRES_SCHEMES))
        || (cfg!(feature = "amqp") && scheme_is(url, AMQP_SCHEMES));
    if served {
        Ok(())
    } else {
        Err(unsupported(url))
    }
}

/// Queue key. Shared rather than Redis-only: the AMQP broker names its
/// queues with it too, so gating it with the Redis driver takes AMQP down.
#[cfg(any(feature = "redis", feature = "amqp"))]
fn stream_key(queue: &str) -> String {
    format!("tarsk:{queue}")
}

/// Result key, shared with AMQP for the same reason as `stream_key`.
#[cfg(any(feature = "redis", feature = "amqp"))]
fn result_key(id: &str) -> String {
    format!("tarsk:result:{id}")
}

#[derive(Clone, Debug)]
pub struct NewJob {
    /// Chosen by the producer so `send()` can hand it back without a round
    /// trip, and so the supervisor knows where to file the result.
    pub id: String,
    pub queue: String,
    pub name: String,
    pub payload: Vec<u8>,
    pub timeout_ms: u32,
    /// Opaque msgpack the caller attached, carried to the handler and readable
    /// from a listing. Never parsed here: the supervisor has no business
    /// knowing what a trace id looks like.
    pub meta: Vec<u8>,
    /// Milliseconds this job may wait after becoming runnable before it is
    /// dropped instead of run. Zero defers to the task's registered `expires`,
    /// which is itself zero by default. Per job rather than per task because
    /// staleness is a property of the request: the nightly rebuild can wait an
    /// hour, the same task fired from a page load cannot.
    pub expires_ms: u64,
    /// msgpack list of the steps still to run after this one, each
    /// `[id, name, payload, timeout_ms, queue, feed]`. Empty for a lone job.
    ///
    /// Carried on the job rather than resolved by the supervisor, because the
    /// supervisor cannot import the user's code to look a callable up (spec
    /// §4.1). A chain is data the whole way through.
    pub chain: Vec<u8>,
}

/// What has to be handed back to settle a delivery.
#[derive(Clone, Debug)]
// The AMQP receipt carries the record to republish and so dwarfs the others.
// Only visible as a lint when the mid-sized Redis and Postgres variants are
// gated out; boxing it to quiet a build nobody ships is not worth the churn.
#[allow(clippy::large_enum_variant)]
pub enum Receipt {
    Memory {
        id: u64,
    },
    /// AMQP consumes destructively: once a delivery is acked the server has
    /// nothing left to requeue, so the receipt itself carries the record a
    /// retry or a dead-letter would republish. Redis and Postgres keep the
    /// record server-side and their receipts are just keys.
    #[cfg(feature = "amqp")]
    Amqp {
        queue: String,
        acker: lapin::Acker,
        record: AmqpRecord,
    },
    #[cfg(feature = "redis")]
    Redis {
        queue: String,
        id: String,
    },
    /// `run_lease` is the guard: a worker that lost its lease and comes back to
    /// ack finds the counter moved on and its ack lands on nothing. Without it
    /// a slow worker can settle a job somebody else has already re-run.
    #[cfg(feature = "postgres")]
    Postgres {
        row: i64,
        run_lease: i64,
    },
}

#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: String,
    /// The steps queued to run after this one. See `NewJob::chain`.
    pub chain: Vec<u8>,
    /// Whatever the caller attached. See `NewJob::meta`.
    pub meta: Vec<u8>,
    pub name: String,
    pub payload: Vec<u8>,
    pub attempt: u32,
    pub receipt: Receipt,
    /// When this became runnable, in milliseconds since the epoch — enqueue
    /// time for an ordinary job, promotion time for a delayed one. Expiry is
    /// measured from here rather than from enqueue, or `send_in(3600)` with a
    /// five-minute expiry would be dead before it was ever due.
    ///
    /// Free on Redis: a stream id is `<millis>-<seq>`, and a delayed job only
    /// enters the stream when the sweep promotes it. Zero means unknown, which
    /// no expiry check treats as expired.
    pub ready_at_ms: u64,
    /// What this particular send asked for. See `NewJob::expires_ms`. Zero
    /// means the sender did not say, and the task's registration decides.
    pub expires_ms: u64,
}

/// How much work is sitting in one queue, as an operator needs to see it.
///
/// The question "how far behind are we" had no answer here: the metrics
/// counted tasks finished and never tasks waiting, which reports throughput
/// while saying nothing about backlog.
pub struct Depth {
    pub queue: String,
    /// Claimable right now.
    pub ready: u64,
    /// Handed to a worker and not yet settled.
    pub in_flight: u64,
    /// Enqueued for later and not yet due.
    pub delayed: u64,
    pub dead: u64,
}

/// One job in a queue, as someone looking for a stuck one needs to see it.
///
/// The depth counters answer "how far behind are we". This answers the
/// question that follows it, which is "behind on what".
pub struct Listed {
    pub id: String,
    pub queue: String,
    pub name: String,
    /// "ready", "running" or "delayed".
    pub state: &'static str,
    pub attempt: u32,
    /// Milliseconds in this state, or negative for how long until a delayed
    /// job is due. One field because a listing reads better with one column.
    pub age_ms: i64,
    /// Which worker holds it, empty unless it is running. Answers the question
    /// that follows "what is stuck": stuck *where*.
    pub worker: String,
    /// Whatever the sender attached, still msgpack — decoded by the caller,
    /// which is the only side that knows what it means.
    pub meta: Vec<u8>,
}

/// One parked failure, as a human needs to see it.
pub struct Dead {
    pub id: String,
    pub name: String,
    pub error: String,
    pub traceback: String,
    /// Milliseconds since the epoch, or 0 where the store does not say.
    pub died_at_ms: u64,
}

// Already boxed where it mattered, see the Amqp variant. The lint only fires
// in feature sets where the middle variants are compiled out.
#[allow(clippy::large_enum_variant)]
pub enum Broker {
    Memory(MemoryBroker),
    #[cfg(feature = "redis")]
    Redis(RedisBroker),
    #[cfg(feature = "postgres")]
    Postgres(PgBroker),
    // Boxed for size only: lapin's connection state makes this variant dwarf
    // the others, and one Broker exists per process — the indirection costs
    // nothing anyone can measure.
    #[cfg(feature = "amqp")]
    Amqp(Box<AmqpBroker>),
}

/// With a backend compiled out, the arguments only that backend reads go
/// unused. Allowed here rather than underscored per method, because which
/// argument is dead depends on the feature set and the full build still gets
/// the lint.
#[cfg_attr(
    not(all(feature = "redis", feature = "postgres", feature = "amqp")),
    allow(unused_variables)
)]
impl Broker {
    /// `memory://`, `redis://…`, `postgres://…`, `amqp://…`
    ///
    /// Early returns rather than an if/else-if chain: an else-if branch cannot
    /// carry a `#[cfg]`, and each scheme here is one.
    pub async fn connect(url: &str, queues: Vec<String>) -> Res<Broker> {
        if url == "memory://" || url.is_empty() {
            return Ok(Broker::Memory(MemoryBroker::default()));
        }
        #[cfg(feature = "redis")]
        if scheme_is(url, REDIS_SCHEMES) {
            return Ok(Broker::Redis(RedisBroker::connect(url, queues).await?));
        }
        #[cfg(feature = "postgres")]
        if scheme_is(url, POSTGRES_SCHEMES) {
            return Ok(Broker::Postgres(PgBroker::connect(url, queues).await?));
        }
        #[cfg(feature = "amqp")]
        if scheme_is(url, AMQP_SCHEMES) {
            return Ok(Broker::Amqp(Box::new(
                AmqpBroker::connect(url, queues).await?,
            )));
        }
        Err(unsupported(url).into())
    }

    /// True when running out of jobs means the run is over, rather than idle.
    pub fn drains_when_empty(&self) -> bool {
        matches!(self, Broker::Memory(_))
    }

    /// Enqueue, optionally not before `delay` has passed.
    pub async fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        match self {
            Broker::Memory(b) => b.push(job, delay),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.push(job, delay).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.push(job, delay).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.push(job, delay).await,
        }
    }

    /// Enqueue many jobs for immediate delivery, as one unit where the backend
    /// has such a thing.
    ///
    /// Redis and Postgres take them all or none. AMQP has no multi-message
    /// transaction worth the name with confirms on, so there they are
    /// published in turn and a failure part-way leaves the first ones queued.
    pub async fn push_many(&self, jobs: Vec<NewJob>) -> Res<()> {
        match self {
            Broker::Memory(b) => {
                for job in jobs {
                    b.push(job, Duration::ZERO)?;
                }
                Ok(())
            }
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.push_many(jobs).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.push_many(jobs).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => {
                for job in jobs {
                    b.push(job, Duration::ZERO).await?;
                }
                Ok(())
            }
        }
    }

    /// Take one job. The lease runs for the job's own timeout plus `grace`
    /// (spec §4.5) — a global lease would strand a 5-second task for as long
    /// as the slowest task in the system. Waits up to `block` for work.
    pub async fn claim(&self, grace: Duration, block: Duration) -> Res<Option<Delivery>> {
        match self {
            Broker::Memory(b) => Ok(b.claim()),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.claim(block).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.claim(grace, block).await,
            // `grace` is the lease slack, and AMQP needs none: an unacked
            // delivery returns the instant its connection dies.
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.claim(block).await,
        }
    }

    /// Settle a delivery for good.
    pub async fn ack(&self, receipt: &Receipt) -> Res<()> {
        match (self, receipt) {
            (Broker::Memory(b), Receipt::Memory { id }) => b.settle(*id),
            #[cfg(feature = "redis")]
            (Broker::Redis(b), r) => b.ack(r).await,
            #[cfg(feature = "postgres")]
            (Broker::Postgres(b), r) => b.ack(r).await,
            #[cfg(feature = "amqp")]
            (Broker::Amqp(_), Receipt::Amqp { acker, .. }) => {
                acker
                    .ack(lapin::options::BasicAckOptions::default())
                    .await?;
                Ok(())
            }
            #[cfg(any(feature = "redis", feature = "postgres", feature = "amqp"))]
            _ => Err("receipt does not belong to this broker".into()),
        }
    }

    /// Hand the job back for redelivery after `delay`. The supervisor decides
    /// whether there should be a redelivery at all; this layer only schedules.
    pub async fn retry(&self, receipt: &Receipt, delay: Duration) -> Res<()> {
        match (self, receipt) {
            (Broker::Memory(b), Receipt::Memory { id }) => b.requeue(*id, delay),
            #[cfg(feature = "redis")]
            (Broker::Redis(b), r) => b.requeue(r, delay).await,
            #[cfg(feature = "postgres")]
            (Broker::Postgres(b), r) => b.requeue(r, delay).await,
            #[cfg(feature = "amqp")]
            (Broker::Amqp(b), r @ Receipt::Amqp { .. }) => b.retry(r, delay).await,
            #[cfg(any(feature = "redis", feature = "postgres", feature = "amqp"))]
            _ => Err("receipt does not belong to this broker".into()),
        }
    }

    /// Hand the job back because it may not start yet — over its rate limit or
    /// its concurrency cap — rather than because it failed.
    ///
    /// The difference from `retry` is the attempt count. Every backend bumps
    /// it on redelivery, which is right for a failure and wrong for a wait: a
    /// job that queued behind a cap would arrive at its first real run already
    /// out of retries. `attempt` is what the delivery being handed back
    /// carried.
    pub async fn defer(&self, receipt: &Receipt, delay: Duration, attempt: u32) -> Res<()> {
        match (self, receipt) {
            (Broker::Memory(b), Receipt::Memory { id }) => b.defer(*id, delay),
            #[cfg(feature = "redis")]
            (Broker::Redis(b), r) => {
                b.requeue_as(r, delay, Some(attempt.saturating_sub(1)))
                    .await
            }
            #[cfg(feature = "postgres")]
            (Broker::Postgres(b), r) => b.requeue_as(r, delay, 1).await,
            #[cfg(feature = "amqp")]
            (Broker::Amqp(b), r @ Receipt::Amqp { .. }) => b.retry_as(r, delay, false).await,
            #[cfg(any(feature = "redis", feature = "postgres", feature = "amqp"))]
            _ => Err("receipt does not belong to this broker".into()),
        }
    }

    /// Take one of `task`'s concurrency slots, or report that none is free.
    ///
    /// The slot carries its own expiry rather than being a plain counter: a
    /// worker that dies holding one would otherwise throttle that task forever,
    /// and the failure would look like a task that mysteriously stopped
    /// running. `lease_ms` should be the job's lease, so a lost slot expires
    /// exactly when the job it was holding does.
    pub async fn acquire_slot(
        &self,
        task: &str,
        job_id: &str,
        max: u32,
        lease_ms: u64,
    ) -> Res<bool> {
        match self {
            Broker::Memory(b) => Ok(b.local.acquire_slot(task, job_id, max, lease_ms)),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.acquire_slot(task, job_id, max, lease_ms).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.acquire_slot(task, job_id, max, lease_ms).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => Ok(b.local.acquire_slot(task, job_id, max, lease_ms)),
        }
    }

    /// Give the slot back. Idempotent, so settling twice cannot free two.
    pub async fn release_slot(&self, task: &str, job_id: &str) -> Res<()> {
        match self {
            Broker::Memory(b) => {
                b.local.release_slot(task, job_id);
                Ok(())
            }
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.release_slot(task, job_id).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.release_slot(task, job_id).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => {
                b.local.release_slot(task, job_id);
                Ok(())
            }
        }
    }

    /// Take one token for `task`, or say how long to wait in milliseconds.
    ///
    /// The counter lives in the broker because a per-worker limit is not a
    /// limit: three workers each allowing ten a second is thirty a second at
    /// the API you were protecting. This is the reason to pay a round trip
    /// here, and only tasks that asked for a limit pay it.
    pub async fn take_token(&self, task: &str, per_sec: f64, burst: u32) -> Res<u64> {
        match self {
            Broker::Memory(b) => Ok(b.local.take_token(task, per_sec, burst)),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.take_token(task, per_sec, burst).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.take_token(task, per_sec, burst).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => Ok(b.local.take_token(task, per_sec, burst)),
        }
    }

    /// Mark a job id as cancelled for `ttl_ms`, so it is never dispatched.
    ///
    /// The TTL has to outlive the job it is cancelling: a delayed task due next
    /// week needs a cancellation that is still there next week. It is the
    /// caller's number because only the caller knows how far ahead it queued.
    ///
    /// `queue` is recorded where the store has a column for it and decides
    /// nothing: cancellations are matched by id alone.
    pub async fn revoke(&self, queue: &str, id: &str, ttl_ms: u64) -> Res<()> {
        // Ten years is as good as forever, and unlike forever it can be added
        // to a clock. `ttl=float("inf")` used to wrap round into the past on
        // Redis, where the cancellation was pruned by the next read, and to
        // panic outright on the memory broker.
        const LONGEST_MS: u64 = 10 * 365 * 86_400_000;
        let ttl_ms = ttl_ms.min(LONGEST_MS);
        match self {
            Broker::Memory(b) => b.revoke(id, ttl_ms),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.revoke(id, ttl_ms).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.revoke(queue, id, ttl_ms).await,
            // Not faked with a local set: a cancel the producer records and no
            // worker can see would report success and change nothing.
            #[cfg(feature = "amqp")]
            Broker::Amqp(_) => Err(
                "cancellation needs state every worker can read, and AMQP has no \
                 shared store to hold it — use the Redis or Postgres broker if you \
                 need to cancel queued work"
                    .into(),
            ),
        }
    }

    /// Stop every worker taking from `queue`, or let them start again.
    ///
    /// For when the thing a queue's tasks call is down: the jobs stay queued
    /// instead of burning their retries against it, and nothing else the
    /// workers serve is held up. What is already running finishes. Refused on
    /// AMQP for the reason cancellation is: there is no store every worker
    /// can read.
    pub async fn set_paused(&self, queue: &str, paused: bool) -> Res<()> {
        match self {
            Broker::Memory(b) => {
                let mut held = b.held.lock().unwrap();
                if paused {
                    held.insert(queue.to_string());
                } else {
                    held.remove(queue);
                }
                Ok(())
            }
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.set_paused(queue, paused).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.set_paused(queue, paused).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(_) => Err(
                "pausing a queue needs state every worker can read, and AMQP has no \
                 shared store to hold it — use the Redis or Postgres broker, or stop \
                 the workers that serve that queue"
                    .into(),
            ),
        }
    }

    /// The queues paused just now, as the store has them.
    pub async fn paused(&self) -> Res<Vec<String>> {
        match self {
            Broker::Memory(b) => Ok(b.held.lock().unwrap().iter().cloned().collect()),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.paused().await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.paused().await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(_) => Ok(Vec::new()),
        }
    }

    /// Bring this worker's idea of what is paused up to date with the store.
    /// Called on the timer that pulls cancellations.
    pub async fn refresh_paused(&self) -> Res<()> {
        match self {
            #[cfg(feature = "redis")]
            Broker::Redis(b) => {
                let now = b.paused().await?.into_iter().collect();
                *b.held.lock().unwrap() = now;
            }
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => {
                let now = b.paused().await?.into_iter().collect();
                *b.held.lock().unwrap() = now;
            }
            _ => {}
        }
        Ok(())
    }

    /// Every id cancelled and not yet expired, on any queue.
    ///
    /// Read as a set on a timer rather than asked per job: a lookup at every
    /// dispatch would put a broker round trip on the path of every task, to
    /// answer "no" for almost all of them.
    pub async fn revoked_all(&self) -> Res<Vec<String>> {
        match self {
            Broker::Memory(b) => Ok(b.revoked()),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.revoked_all().await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.revoked_all().await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(_) => Ok(Vec::new()),
        }
    }

    /// Backlog across the queues this broker was opened on — what a worker
    /// reports about its own queues.
    pub async fn depth(&self) -> Res<Vec<Depth>> {
        match self {
            Broker::Memory(_) => Ok(Vec::new()),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.depth(&b.queues).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.depth(&b.queues).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.depth(&b.queues).await,
        }
    }

    /// Backlog for named queues, for a caller that opened no queues of its own
    /// — `tarsk status` is not a worker and subscribes to nothing.
    pub async fn depth_of(&self, queues: &[String]) -> Res<Vec<Depth>> {
        match self {
            Broker::Memory(_) => Ok(Vec::new()),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.depth(queues).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.depth(queues).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.depth(queues).await,
        }
    }

    /// The queues this broker was opened on, for a caller that needs to name
    /// them and was not told which they are.
    pub fn queue_names(&self) -> Vec<String> {
        match self {
            Broker::Memory(_) => Vec::new(),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.queues.clone(),
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.queues.clone(),
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.queues.clone(),
        }
    }

    /// The individual jobs in named queues, oldest first.
    pub async fn jobs(&self, queues: &[String], limit: usize) -> Res<Vec<Listed>> {
        let mut rows = match self {
            Broker::Memory(_) => Vec::new(),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.jobs(queues, limit).await?,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.jobs(queues, limit).await?,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.jobs(queues, limit).await?,
        };
        // The Redis side reads the stream and the delayed set separately, so
        // the bound has to be applied again to the two of them together — a
        // caller asking for fifty should not be handed fifty-one.
        rows.truncate(limit);
        Ok(rows)
    }

    /// What is parked in the dead letters, newest last.
    ///
    /// The store existed from the first broker commit and nothing could read
    /// it, which made it a place work went to be forgotten rather than found.
    pub async fn dead_list(&self, queue: &str, limit: usize) -> Res<Vec<Dead>> {
        match self {
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.dead_list(queue, limit).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.dead_list(queue, limit).await,
            Broker::Memory(_) => Ok(Vec::new()),
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.dead_list(queue, limit).await,
        }
    }

    /// Put dead letters back on the live queue. Empty `ids` means all of them.
    ///
    /// Returns how many moved. A replayed job starts its retries over: the
    /// reason it died has usually been deployed away by the time anyone runs
    /// this, and carrying the old attempt count would spend the fix's first
    /// attempt on the last failure's budget.
    pub async fn dead_replay(&self, queue: &str, ids: &[String]) -> Res<usize> {
        match self {
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.dead_replay(queue, ids).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.dead_replay(queue, ids).await,
            Broker::Memory(_) => Ok(0),
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.dead_drain(queue, ids, true).await,
        }
    }

    /// Drop dead letters. Empty `ids` means all of them.
    pub async fn dead_purge(&self, queue: &str, ids: &[String]) -> Res<usize> {
        match self {
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.dead_purge(queue, ids).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.dead_purge(queue, ids).await,
            Broker::Memory(_) => Ok(0),
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.dead_drain(queue, ids, false).await,
        }
    }

    /// Retries are exhausted: park the job somewhere a human can find it.
    /// `keep` is how many failures a queue's dead letters may hold; zero keeps
    /// everything.
    ///
    /// A count, which is the proxy this project spends a benchmark suite
    /// arguing against — it bounds bytes only if you know what a row weighs.
    /// Redis Streams offer MAXLEN and MINID and nothing by size, so a count is
    /// what the datastore gives. Measured at about 2KB a row with a small
    /// payload, most of it traceback, so the default of a thousand is a couple
    /// of megabytes a queue and someone shipping megabyte payloads should say
    /// so with the flag. Every other store here expires on its own — results and
    /// progress by TTL, the live stream by acking, the buckets and reservations
    /// by their own clocks — and this one grew forever, carrying a payload and
    /// a full traceback per row. For a queue whose whole claim is bounded
    /// memory, the store of last resort filling Redis is a poor way to be
    /// proved wrong.
    pub async fn dead_letter(
        &self,
        receipt: &Receipt,
        error: &str,
        traceback: &str,
        keep: u64,
    ) -> Res<()> {
        match (self, receipt) {
            (Broker::Memory(b), Receipt::Memory { id }) => b.settle(*id),
            #[cfg(feature = "redis")]
            (Broker::Redis(b), r) => b.dead_letter(r, error, traceback, keep).await,
            #[cfg(feature = "postgres")]
            (Broker::Postgres(b), r) => b.dead_letter(r, error, traceback, keep).await,
            #[cfg(feature = "amqp")]
            (Broker::Amqp(b), r @ Receipt::Amqp { .. }) => {
                b.dead_letter(r, error, traceback, keep).await
            }
            #[cfg(any(feature = "redis", feature = "postgres", feature = "amqp"))]
            _ => Err("receipt does not belong to this broker".into()),
        }
    }

    /// Fetch at most one message per child per read.
    ///
    /// Sixty-four per read measures the same as four, so this is not chosen for
    /// speed — it is chosen because a claimed message holds a lease whether or
    /// not a child is free to run it, and a buffer deeper than the children can
    /// drain is just leases ageing in memory.
    ///
    /// An earlier version of this comment said sixty-four was slower than one.
    /// It was, while the driver held a mutex round its Redis connection: a large
    /// batch parsed in one burst blocked every other command. That mutex is
    /// gone, and the effect went with it.
    pub fn set_prefetch_cap(&self, children: usize) {
        #[cfg(feature = "redis")]
        if let Broker::Redis(b) = self {
            b.prefetch_cap
                .store((children as u64).max(1), Ordering::Relaxed);
        }
    }

    /// Tell the broker the shortest timeout any registered task can have.
    ///
    /// The Redis sweep needs a floor for XPENDING's single IDLE filter, and
    /// learning it from jobs already claimed makes it depend on what this
    /// process happens to have run. The registry knows it up front.
    pub fn observe_min_timeout(&self, timeout_ms: u64) {
        #[cfg(feature = "redis")]
        if let Broker::Redis(b) = self {
            b.min_timeout_ms.fetch_min(timeout_ms, Ordering::Relaxed);
        }
    }

    /// Win the right to fire `name` for the minute it was last due, fleet-wide.
    ///
    /// True for exactly one caller per due minute, and only if that minute is
    /// later than the last one anybody fired for. Without the election every
    /// worker fires every schedule, which is the failure mode of bolting cron
    /// onto a worker pool.
    ///
    /// "Later than the last one fired" rather than "this minute, once" is what
    /// lets a missed run be caught up: `due` may be in the past, and it still
    /// wins if nothing has fired since. `now` matters only the first time a
    /// schedule is seen at all, when there is no last run to compare with:
    /// the record then starts just behind the present, so deploying a nightly
    /// job at noon does not run it at noon.
    pub async fn claim_run(&self, name: &str, due: i64, now: i64) -> Res<bool> {
        match self {
            Broker::Memory(b) => Ok(b.local.claim_run(name, due, now)),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.claim_run(name, due, now).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.claim_run(name, due, now).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => Ok(b.local.claim_run(name, due, now)),
        }
    }

    /// Record that member `index` of a chord has finished with `result`, and
    /// if that completes it, hand back every member's result in order.
    ///
    /// `Some` is returned to exactly one member: the one whose arrival made
    /// the set whole. It is returned to that member again if it is delivered
    /// again, because a callback that could not be queued the first time has
    /// to be queued the second — and to no other, so the callback is queued
    /// once however the members race.
    ///
    /// What is recorded expires after a day. Refused on AMQP: counting
    /// members needs somewhere every worker can write.
    pub async fn chord_done(
        &self,
        chord: &str,
        index: u64,
        total: u64,
        result: &[u8],
    ) -> Res<Option<Vec<Vec<u8>>>> {
        match self {
            Broker::Memory(b) => Ok(b.local.chord_done(chord, index, total, result)),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.chord_done(chord, index, total, result).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.chord_done(chord, index, total, result).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(_) => Err(
                "a chord needs somewhere every worker can record a finished member, \
                 and AMQP has no shared store — use the Redis or Postgres broker"
                    .into(),
            ),
        }
    }

    /// Reserve `key` for `job_id` for `ttl_ms`, or report who holds it.
    ///
    /// `None` means this caller won and should send. `Some(id)` means an
    /// identical send is already covered by that job, and its id is returned so
    /// the caller can wait on the same answer rather than being handed one for
    /// a job that was never queued.
    pub async fn claim_dedup(&self, key: &str, job_id: &str, ttl_ms: u64) -> Res<Option<String>> {
        match self {
            Broker::Memory(_) => Ok(None),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.claim_dedup(key, job_id, ttl_ms).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.claim_dedup(key, job_id, ttl_ms).await,
            // None means "not a duplicate, go ahead": with no shared store two
            // producers cannot see each other's sends, so dedup on AMQP only
            // holds within one producer process. The docs say so.
            #[cfg(feature = "amqp")]
            Broker::Amqp(_) => Ok(None),
        }
    }

    /// How many times this broker has had to open its connection again.
    pub fn reconnects(&self) -> u64 {
        match self {
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.reconnects.load(Ordering::Relaxed),
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.reconnects.load(Ordering::Relaxed),
            _ => 0,
        }
    }

    /// Give an idle connection the chance to find out it has been closed,
    /// before the caller sends anything it would mind losing. Only Redis needs
    /// asking; see `RedisBroker::refresh`.
    pub async fn refresh(&self) {
        #[cfg(feature = "redis")]
        if let Broker::Redis(b) = self {
            b.refresh().await;
        }
    }

    /// Hand back what this worker fetched ahead and will now never start.
    ///
    /// Only Redis reads ahead. A worker that stops claiming still owns the
    /// entries it prefetched, and left alone they sit out their whole lease —
    /// minutes, at the default timeout — before anyone else may run them.
    pub async fn release_unstarted(&self) {
        #[cfg(feature = "redis")]
        if let Broker::Redis(b) = self {
            b.release_unstarted().await;
        }
    }

    /// Give a reservation back, if `job_id` still holds it.
    ///
    /// For a send whose push failed after `claim_dedup` succeeded. Left in
    /// place, the key would answer the caller's retry with the id of a job
    /// that was never queued, for the whole window.
    pub async fn release_dedup(&self, key: &str, job_id: &str) -> Res<()> {
        match self {
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.release_dedup(key, job_id).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.release_dedup(key, job_id).await,
            _ => Ok(()),
        }
    }

    /// File a finished task's answer under its id, to expire after `ttl`.
    /// Only called for tasks that asked for it: writing every result to a
    /// broker nobody reads from is most of why Celery feels heavy (spec §2).
    pub async fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        match self {
            Broker::Memory(b) => b.store_result(id, blob, ttl),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.store_result(id, blob, ttl).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.store_result(id, blob, ttl).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.store_result(id, blob, ttl).await,
        }
    }

    pub async fn get_result(&self, id: &str) -> Res<Option<Vec<u8>>> {
        match self {
            Broker::Memory(b) => Ok(b.get_result(id)),
            #[cfg(feature = "redis")]
            Broker::Redis(b) => b.get_result(id).await,
            #[cfg(feature = "postgres")]
            Broker::Postgres(b) => b.get_result(id).await,
            #[cfg(feature = "amqp")]
            Broker::Amqp(b) => b.get_result(id).await,
        }
    }

    /// Return jobs whose lease has expired. Postgres does this inside `claim`,
    /// so only Redis needs a sweep.
    pub async fn reclaim_expired(&self, grace: Duration) -> Res<usize> {
        #[cfg(feature = "redis")]
        if let Broker::Redis(b) = self {
            let promoted = b.promote_due().await?;
            return Ok(b.reclaim_expired(grace).await? + promoted);
        }
        // On AMQP the server does this itself: an unacked delivery requeues
        // the moment its connection dies, and TTL queues promote on their own.
        // Memory and Postgres reclaim inside claim.
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_error_is_described_with_its_causes() {
        #[derive(Debug)]
        struct Outer(std::io::Error);
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "db error")
            }
        }
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        // The shape tokio-postgres hands back: two words on top, the server's
        // own message underneath.
        let err = Outer(std::io::Error::other("deadlock detected"));
        assert_eq!(describe(&err), "db error: deadlock detected");
        // A cause the message already carries is not said twice.
        let plain = std::io::Error::other("connection closed");
        assert_eq!(describe(&plain), "connection closed");
    }

    #[tokio::test]
    async fn a_deferral_gives_its_attempt_back() {
        let broker = Broker::connect("memory://", Vec::new()).await.unwrap();
        let job = NewJob {
            id: "j".into(),
            queue: "default".into(),
            name: "t".into(),
            payload: Vec::new(),
            timeout_ms: 1_000,
            meta: Vec::new(),
            expires_ms: 0,
            chain: Vec::new(),
        };
        broker.push(job, Duration::ZERO).await.unwrap();
        let wait = Duration::ZERO;
        let first = broker.claim(wait, wait).await.unwrap().unwrap();
        assert_eq!(first.attempt, 1);
        // Waiting for a slot, three times over, is still the first attempt.
        let mut seen = first;
        for _ in 0..3 {
            broker
                .defer(&seen.receipt, wait, seen.attempt)
                .await
                .unwrap();
            seen = broker.claim(wait, wait).await.unwrap().unwrap();
            assert_eq!(seen.attempt, 1, "a deferral was charged as an attempt");
        }
        // A failure is not a wait, and is still counted.
        broker.retry(&seen.receipt, wait).await.unwrap();
        let after = broker.claim(wait, wait).await.unwrap().unwrap();
        assert_eq!(after.attempt, 2);
    }

    #[tokio::test]
    async fn a_chord_completes_once_in_member_order() {
        let broker = Broker::connect("memory://", Vec::new()).await.unwrap();
        // Members finish out of order; nothing is handed back until all have.
        assert_eq!(broker.chord_done("c", 2, 3, b"two").await.unwrap(), None);
        assert_eq!(broker.chord_done("c", 0, 3, b"zero").await.unwrap(), None);
        // A member delivered twice is one member.
        assert_eq!(broker.chord_done("c", 0, 3, b"again").await.unwrap(), None);
        let whole = broker.chord_done("c", 1, 3, b"one").await.unwrap();
        let expected: Vec<Vec<u8>> = vec![b"zero".to_vec(), b"one".to_vec(), b"two".to_vec()];
        assert_eq!(whole.as_ref(), Some(&expected));
        // The one that completed it hears so again if it is redelivered —
        // its callback may not have been queued the first time — and the
        // others never do.
        assert_eq!(
            broker.chord_done("c", 1, 3, b"one").await.unwrap(),
            Some(expected)
        );
        assert_eq!(broker.chord_done("c", 2, 3, b"two").await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_paused_queue_is_passed_over_and_not_lost() {
        let broker = Broker::connect("memory://", Vec::new()).await.unwrap();
        let wait = Duration::ZERO;
        for (queue, name) in [("slow", "first"), ("fast", "second"), ("slow", "third")] {
            let job = NewJob {
                id: name.into(),
                queue: queue.into(),
                name: name.into(),
                payload: Vec::new(),
                timeout_ms: 1_000,
                meta: Vec::new(),
                expires_ms: 0,
                chain: Vec::new(),
            };
            broker.push(job, wait).await.unwrap();
        }
        broker.set_paused("slow", true).await.unwrap();
        assert_eq!(broker.paused().await.unwrap(), vec!["slow".to_string()]);
        // The job behind the paused ones is reachable; they are not.
        let got = broker.claim(wait, wait).await.unwrap().unwrap();
        assert_eq!(got.name, "second");
        assert!(broker.claim(wait, wait).await.unwrap().is_none());
        // And they are still there, in order, when the queue is let go.
        broker.set_paused("slow", false).await.unwrap();
        let mut rest = Vec::new();
        while let Some(job) = broker.claim(wait, wait).await.unwrap() {
            rest.push(job.name);
        }
        assert_eq!(rest, vec!["first", "third"]);
    }

    #[tokio::test]
    async fn a_cancellation_can_be_asked_to_last_forever() {
        let broker = Broker::connect("memory://", Vec::new()).await.unwrap();
        broker.revoke("default", "j", u64::MAX).await.unwrap();
        assert_eq!(broker.revoked_all().await.unwrap(), vec!["j".to_string()]);
    }

    #[tokio::test]
    async fn a_run_is_claimed_once_and_only_forwards() {
        // Irrefutable when only the memory broker is compiled in.
        #[allow(irrefutable_let_patterns)]
        let Broker::Memory(broker) = Broker::connect("memory://", Vec::new()).await.unwrap() else {
            unreachable!()
        };
        let local = &broker.local;
        // Due now, first time seen: fires, once.
        assert!(local.claim_run("nightly", 100, 100));
        assert!(!local.claim_run("nightly", 100, 100), "one winner per run");
        // A later run that came due while nobody was looking is still claimed
        // when someone looks, and an older one never is.
        assert!(local.claim_run("nightly", 160, 200));
        assert!(!local.claim_run("nightly", 130, 200), "ran backwards");
        // New here, and last due an hour ago: that hour was never ours to fire.
        assert!(!local.claim_run("weekly", 140, 200));
        assert!(local.claim_run("weekly", 201, 201));
        // One record a schedule, however long it runs.
        for minute in 202..600 {
            local.claim_run("nightly", minute, minute);
        }
        assert_eq!(local.runs.lock().unwrap().len(), 2);
    }

    #[test]
    fn a_gated_out_backend_is_not_reported_as_an_unsupported_url() {
        let msg = unsupported("redis://localhost/0");
        assert!(
            msg.contains("memory"),
            "should name what is compiled in: {msg}"
        );
        if cfg!(feature = "redis") {
            assert!(msg.starts_with("unsupported broker url"), "{msg}");
        } else {
            // The package, not the cargo feature: whoever sees this installed
            // a wheel and has no Cargo.toml to add a feature to.
            assert!(msg.starts_with("redis:// is not available"), "{msg}");
            assert!(msg.contains("tarsk-redis"), "{msg}");
            assert!(!msg.contains("tarsk-core"), "{msg}");
        }
    }

    #[test]
    fn a_scheme_no_backend_serves_is_still_unsupported() {
        let msg = unsupported("kafka://user:hunter2@broker:9092");
        assert!(msg.starts_with("unsupported broker url"), "{msg}");
        // The URL is echoed into logs, so only the scheme is.
        assert!(!msg.contains("hunter2"), "{msg}");
    }
}
