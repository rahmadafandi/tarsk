//! Where jobs come from.
//!
//! Three implementations — in-memory, Redis Streams, Postgres — which is what
//! earns the abstraction. Spec §6 forbids introducing one before the second
//! broker exists, and it now does.
//!
//! Enum dispatch rather than `dyn Broker`: async fns in traits are not
//! object-safe without boxing, and three variants do not need a vtable.
//!
//! Every backend is at-least-once (spec §4.5). A lease is a promise that no
//! one else will run this job for `lease` seconds, not that it ran only once.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use tokio_postgres::NoTls;

pub type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Group and consumer names are fixed: one logical worker pool per broker.
const REDIS_GROUP: &str = "tarsk";
/// Fallback ceiling on a batched read, replaced by the worker's child count.
///
/// Measured on 5,000 no-ops across four children: one message per read gives a
/// median of 0.96s, four gives 0.65s, sixty-four gives 1.18s. Fetching enough
/// to keep every child fed helps; fetching more than that means parsing a burst
/// on a single-threaded runtime while the children it was meant to feed wait.
const DEFAULT_PREFETCH_CAP: u64 = 4;

#[derive(Clone, Debug)]
pub struct NewJob {
    /// Chosen by the producer so `send()` can hand it back without a round
    /// trip, and so the supervisor knows where to file the result.
    pub id: String,
    pub queue: String,
    pub name: String,
    pub payload: Vec<u8>,
    pub timeout_ms: u32,
}

/// What has to be handed back to settle a delivery.
#[derive(Clone, Debug)]
pub enum Receipt {
    Memory {
        id: u64,
    },
    Redis {
        queue: String,
        id: String,
    },
    /// `run_lease` is the guard: a worker that lost its lease and comes back to
    /// ack finds the counter moved on and its ack lands on nothing. Without it
    /// a slow worker can settle a job somebody else has already re-run.
    Postgres {
        row: i64,
        run_lease: i64,
    },
}

#[derive(Clone, Debug)]
pub struct Delivery {
    pub id: String,
    pub name: String,
    pub payload: Vec<u8>,
    pub attempt: u32,
    pub receipt: Receipt,
}

pub enum Broker {
    Memory(MemoryBroker),
    Redis(RedisBroker),
    Postgres(PgBroker),
}

impl Broker {
    /// `memory://`, `redis://…`, `postgres://…`
    pub async fn connect(url: &str, queues: Vec<String>) -> Res<Broker> {
        if url == "memory://" || url.is_empty() {
            Ok(Broker::Memory(MemoryBroker::default()))
        } else if url.starts_with("redis://") || url.starts_with("rediss://") {
            Ok(Broker::Redis(RedisBroker::connect(url, queues).await?))
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Ok(Broker::Postgres(PgBroker::connect(url, queues).await?))
        } else {
            Err(format!("unsupported broker url: {url}").into())
        }
    }

    /// True when running out of jobs means the run is over, rather than idle.
    pub fn drains_when_empty(&self) -> bool {
        matches!(self, Broker::Memory(_))
    }

    /// Enqueue, optionally not before `delay` has passed.
    pub async fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        match self {
            Broker::Memory(b) => b.push(job, delay),
            Broker::Redis(b) => b.push(job, delay).await,
            Broker::Postgres(b) => b.push(job, delay).await,
        }
    }

    /// Take one job. The lease runs for the job's own timeout plus `grace`
    /// (spec §4.5) — a global lease would strand a 5-second task for as long
    /// as the slowest task in the system. Waits up to `block` for work.
    pub async fn claim(&self, grace: Duration, block: Duration) -> Res<Option<Delivery>> {
        match self {
            Broker::Memory(b) => Ok(b.claim()),
            Broker::Redis(b) => b.claim(block).await,
            Broker::Postgres(b) => b.claim(grace, block).await,
        }
    }

    /// Settle a delivery for good.
    pub async fn ack(&self, receipt: &Receipt) -> Res<()> {
        match (self, receipt) {
            (Broker::Memory(b), Receipt::Memory { id }) => b.settle(*id),
            (Broker::Redis(b), r) => b.ack(r).await,
            (Broker::Postgres(b), r) => b.ack(r).await,
            _ => Err("receipt does not belong to this broker".into()),
        }
    }

    /// Hand the job back for redelivery after `delay`. The supervisor decides
    /// whether there should be a redelivery at all; this layer only schedules.
    pub async fn retry(&self, receipt: &Receipt, delay: Duration) -> Res<()> {
        match (self, receipt) {
            (Broker::Memory(b), Receipt::Memory { id }) => b.requeue(*id, delay),
            (Broker::Redis(b), r) => b.requeue(r, delay).await,
            (Broker::Postgres(b), r) => b.requeue(r, delay).await,
            _ => Err("receipt does not belong to this broker".into()),
        }
    }

    /// Retries are exhausted: park the job somewhere a human can find it.
    pub async fn dead_letter(&self, receipt: &Receipt, error: &str, traceback: &str) -> Res<()> {
        match (self, receipt) {
            (Broker::Memory(b), Receipt::Memory { id }) => b.settle(*id),
            (Broker::Redis(b), r) => b.dead_letter(r, error, traceback).await,
            (Broker::Postgres(b), r) => b.dead_letter(r, error, traceback).await,
            _ => Err("receipt does not belong to this broker".into()),
        }
    }

    /// Fetch at most one message per child per read.
    ///
    /// More than that is not free: the extra entries are parsed in one burst on
    /// a single-threaded runtime, and the children the batch exists to feed wait
    /// through it. Measured, sixty-four per read is slower than one.
    pub fn set_prefetch_cap(&self, children: usize) {
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
        if let Broker::Redis(b) = self {
            b.min_timeout_ms.fetch_min(timeout_ms, Ordering::Relaxed);
        }
    }

    /// Win the right to fire `name` for `minute`, fleet-wide. Returns false if
    /// another worker already has it — without this every worker fires every
    /// schedule, which is the failure mode of bolting cron onto a worker pool.
    pub async fn claim_tick(&self, name: &str, minute: i64) -> Res<bool> {
        match self {
            Broker::Memory(b) => Ok(b.claim_tick(name, minute)),
            Broker::Redis(b) => b.claim_tick(name, minute).await,
            Broker::Postgres(b) => b.claim_tick(name, minute).await,
        }
    }

    /// File a finished task's answer under its id, to expire after `ttl`.
    /// Only called for tasks that asked for it: writing every result to a
    /// broker nobody reads from is most of why Celery feels heavy (spec §2).
    pub async fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        match self {
            Broker::Memory(b) => b.store_result(id, blob, ttl),
            Broker::Redis(b) => b.store_result(id, blob, ttl).await,
            Broker::Postgres(b) => b.store_result(id, blob, ttl).await,
        }
    }

    pub async fn get_result(&self, id: &str) -> Res<Option<Vec<u8>>> {
        match self {
            Broker::Memory(b) => Ok(b.get_result(id)),
            Broker::Redis(b) => b.get_result(id).await,
            Broker::Postgres(b) => b.get_result(id).await,
        }
    }

    /// Return jobs whose lease has expired. Postgres does this inside `claim`,
    /// so only Redis needs a sweep.
    pub async fn reclaim_expired(&self, grace: Duration) -> Res<usize> {
        match self {
            Broker::Redis(b) => {
                let promoted = b.promote_due().await?;
                Ok(b.reclaim_expired(grace).await? + promoted)
            }
            _ => Ok(0),
        }
    }
}

// ------------------------------------------------------------------ memory

/// Backs the batch API and the test suite. Nothing durable: a crash loses the
/// queue, which is the honest trade for having no broker to run.
#[derive(Default)]
pub struct MemoryBroker {
    ready: Mutex<VecDeque<(u64, Option<std::time::Instant>)>>,
    jobs: Mutex<HashMap<u64, (NewJob, u32)>>,
    next_id: AtomicU64,
    results: Mutex<HashMap<String, (Vec<u8>, std::time::Instant)>>,
    ticks: Mutex<std::collections::HashSet<(String, i64)>>,
}

impl MemoryBroker {
    fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        self.jobs.lock().unwrap().insert(id, (job, 0));
        let due = (!delay.is_zero()).then(|| std::time::Instant::now() + delay);
        self.ready.lock().unwrap().push_back((id, due));
        Ok(())
    }

    fn claim(&self) -> Option<Delivery> {
        let id = {
            let mut ready = self.ready.lock().unwrap();
            let now = std::time::Instant::now();
            let position = ready
                .iter()
                .position(|(_, due)| due.is_none_or(|d| d <= now))?;
            ready.remove(position)?.0
        };
        let mut jobs = self.jobs.lock().unwrap();
        let (job, attempt) = jobs.get_mut(&id)?;
        *attempt += 1;
        Some(Delivery {
            name: job.name.clone(),
            payload: job.payload.clone(),
            attempt: *attempt,
            receipt: Receipt::Memory { id },
            id: job.id.clone(),
        })
    }

    fn claim_tick(&self, name: &str, minute: i64) -> bool {
        self.ticks
            .lock()
            .unwrap()
            .insert((name.to_string(), minute))
    }

    fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        self.results
            .lock()
            .unwrap()
            .insert(id.to_string(), (blob, std::time::Instant::now() + ttl));
        Ok(())
    }

    fn get_result(&self, id: &str) -> Option<Vec<u8>> {
        let mut results = self.results.lock().unwrap();
        match results.get(id) {
            Some((_, expiry)) if *expiry <= std::time::Instant::now() => {
                results.remove(id);
                None
            }
            Some((blob, _)) => Some(blob.clone()),
            None => None,
        }
    }

    fn settle(&self, id: u64) -> Res<()> {
        self.jobs.lock().unwrap().remove(&id);
        Ok(())
    }

    fn requeue(&self, id: u64, delay: Duration) -> Res<()> {
        if self.jobs.lock().unwrap().contains_key(&id) {
            let due = (!delay.is_zero()).then(|| std::time::Instant::now() + delay);
            self.ready.lock().unwrap().push_back((id, due));
        }
        Ok(())
    }
}

// ------------------------------------------------------------------- redis

pub struct RedisBroker {
    /// Cloned per command rather than locked. A MultiplexedConnection is built
    /// to be used concurrently — putting a mutex round it made every command
    /// from every child queue behind one lock on a single-threaded runtime,
    /// which is a bottleneck this driver invented for itself.
    conn: redis::aio::MultiplexedConnection,
    consumer: String,
    queues: Vec<String>,
    /// Shortest timeout registered anywhere, the floor for the pending sweep.
    min_timeout_ms: AtomicU64,
    /// Slack added to every job's own timeout before its lease is considered
    /// dead. Learned from the sweep, which is the only caller that knows it.
    grace_ms: AtomicU64,
    /// Makes each delayed job's key unique; two identical jobs must not
    /// collide into one sorted-set member.
    next_delayed: AtomicU64,
    /// Entries this consumer owns and has not handed out yet: both the ones a
    /// sweep reclaimed and the ones a batched read fetched ahead.
    ///
    /// They cannot be re-read with `XREADGROUP … 0`, because that also returns
    /// the entries this worker is running right now. Holding them here instead
    /// keeps reclaim to a single atomic command; if the process dies with some
    /// still queued, their idle time simply starts growing again and the next
    /// sweep — anyone's — picks them up.
    ///
    /// Each carries when it was claimed and the deadline it must be dispatched
    /// by, because a message waiting its turn is a message holding a lease.
    claimed: Mutex<VecDeque<(Delivery, std::time::Instant, u64)>>,
    /// How many messages to fetch per read. Grows while batches are consumed in
    /// time and halves whenever one is not: long tasks empty a batch slowly, and
    /// a message that outwaits its own lease has to be dropped rather than run.
    prefetch: AtomicU64,
    /// Upper bound for the above, set to the number of children this worker runs.
    prefetch_cap: AtomicU64,
}

fn stream_key(queue: &str) -> String {
    format!("tarsk:{queue}")
}

fn delayed_key(queue: &str) -> String {
    format!("tarsk:{queue}:delayed")
}

/// Move due jobs from the delayed set into the stream.
///
/// This is the one place a Lua script earns its keep. Everywhere else the
/// driver either holds a single command or has every field already in hand;
/// here it must read what is due, decide, and write — and a crash between the
/// XADD and the ZREM would leave the job scheduled *and* queued.
///
/// KEYS[1] delayed zset · KEYS[2] stream · ARGV[1] now in ms · ARGV[2] batch size
///
/// ponytail: the per-job hash key is derived inside the script rather than
/// declared in KEYS, which Redis Cluster forbids. Single-node only until
/// someone needs otherwise; a hash tag on both keys is the fix.
const PROMOTE_DUE: &str = r"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0
for _, id in ipairs(due) do
  local fields = redis.call('HGETALL', KEYS[1] .. ':' .. id)
  if #fields > 0 then
    redis.call('XADD', KEYS[2], '*', unpack(fields))
    moved = moved + 1
  end
  redis.call('DEL', KEYS[1] .. ':' .. id)
  redis.call('ZREM', KEYS[1], id)
end
return moved
";

fn entry_fields(entry: &redis::streams::StreamId) -> (String, Vec<u8>, u64) {
    (
        entry.get("n").unwrap_or_default(),
        entry.get("p").unwrap_or_default(),
        entry.get("t").unwrap_or(0),
    )
}

fn entry_id(entry: &redis::streams::StreamId) -> String {
    entry.get("i").unwrap_or_default()
}

fn result_key(id: &str) -> String {
    format!("tarsk:result:{id}")
}

impl RedisBroker {
    async fn connect(url: &str, queues: Vec<String>) -> Res<RedisBroker> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        for queue in &queues {
            // MKSTREAM so producers and consumers can start in either order;
            // from 0 so messages published before the group existed are seen.
            let created: Result<String, _> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream_key(queue))
                .arg(REDIS_GROUP)
                .arg("0")
                .arg("MKSTREAM")
                .query_async(&mut conn)
                .await;
            if let Err(err) = created {
                if !err.to_string().contains("BUSYGROUP") {
                    return Err(err.into());
                }
            }
        }
        Ok(RedisBroker {
            conn,
            consumer: format!("tarsk-{}", std::process::id()),
            queues,
            min_timeout_ms: AtomicU64::new(u64::MAX),
            grace_ms: AtomicU64::new(0),
            next_delayed: AtomicU64::new(0),
            claimed: Mutex::new(VecDeque::new()),
            prefetch: AtomicU64::new(1),
            prefetch_cap: AtomicU64::new(DEFAULT_PREFETCH_CAP),
        })
    }

    async fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        let mut conn = self.conn.clone();
        if delay.is_zero() {
            let _: String = redis::cmd("XADD")
                .arg(stream_key(&job.queue))
                .arg("*")
                .arg("n")
                .arg(&job.name)
                .arg("p")
                .arg(&job.payload)
                .arg("t")
                .arg(job.timeout_ms)
                .arg("i")
                .arg(&job.id)
                .query_async(&mut conn)
                .await?;
            return Ok(());
        }
        // Streams have no notion of "later", so a delayed job waits in a sorted
        // set with its fields beside it and the sweep promotes it when due.
        let key = delayed_key(&job.queue);
        let id = format!(
            "{}-{}",
            std::process::id(),
            self.next_delayed.fetch_add(1, Ordering::SeqCst)
        );
        let due = now_ms() + delay.as_millis() as u64;
        // MULTI, so a job is never scheduled without its payload or the reverse.
        let _: redis::Value = redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(format!("{key}:{id}"))
            .arg("n")
            .arg(&job.name)
            .arg("p")
            .arg(&job.payload)
            .arg("t")
            .arg(job.timeout_ms)
            .arg("i")
            .arg(&job.id)
            .cmd("ZADD")
            .arg(&key)
            .arg(due)
            .arg(&id)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn claim_tick(&self, name: &str, minute: i64) -> Res<bool> {
        let mut conn = self.conn.clone();
        // NX is the whole election: one SET wins, the rest see the key. The TTL
        // only has to outlive the minute it guards.
        let won: Option<String> = redis::cmd("SET")
            .arg(format!("tarsk:cron:{name}:{minute}"))
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(120)
            .query_async(&mut conn)
            .await?;
        Ok(won.is_some())
    }

    async fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        let mut conn = self.conn.clone();
        // PX rather than a sweeper: an expiry the server enforces cannot be
        // forgotten by a worker that died holding the job of forgetting it.
        let _: redis::Value = redis::cmd("SET")
            .arg(result_key(id))
            .arg(blob)
            .arg("PX")
            .arg(ttl.as_millis().max(1) as u64)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn get_result(&self, id: &str) -> Res<Option<Vec<u8>>> {
        let mut conn = self.conn.clone();
        let blob: Option<Vec<u8>> = redis::cmd("GET")
            .arg(result_key(id))
            .query_async(&mut conn)
            .await?;
        Ok(blob)
    }

    /// Hand due jobs to the stream. Runs on the sweep that already exists.
    async fn promote_due(&self) -> Res<usize> {
        let mut moved = 0;
        for queue in &self.queues {
            let mut conn = self.conn.clone();
            let promoted: usize = redis::Script::new(PROMOTE_DUE)
                .key(delayed_key(queue))
                .key(stream_key(queue))
                .arg(now_ms())
                .arg(128)
                .invoke_async(&mut conn)
                .await?;
            moved += promoted;
        }
        Ok(moved)
    }

    async fn claim(&self, block: Duration) -> Res<Option<Delivery>> {
        // Anything already owned goes out first — but only while its lease has
        // time left. Past that the sweep may have handed it to someone else,
        // and running it here would be a duplicate we chose to create.
        loop {
            let head = self.claimed.lock().unwrap().pop_front();
            match head {
                Some((delivery, claimed_at, deadline_ms)) => {
                    if claimed_at.elapsed().as_millis() as u64 >= deadline_ms {
                        let _ =
                            self.prefetch
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                                    Some((n / 2).max(1))
                                });
                        continue; // let it expire and return to whoever is free
                    }
                    return Ok(Some(delivery));
                }
                None => break,
            }
        }
        let batch = self.prefetch.load(Ordering::Relaxed).max(1);
        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(REDIS_GROUP)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(batch)
            .arg("BLOCK")
            .arg(block.as_millis().max(1) as u64)
            .arg("STREAMS");
        for queue in &self.queues {
            cmd.arg(stream_key(queue));
        }
        for _ in &self.queues {
            cmd.arg(">"); // never-delivered only; our own backlog is in `recovered`
        }
        let reply: Option<redis::streams::StreamReadReply> = cmd.query_async(&mut conn).await?;
        let Some(reply) = reply else { return Ok(None) };
        // Everything the read returned is now in this consumer's PEL, so all of
        // it has to be accounted for: hand one out and hold the rest. Taking
        // one and forgetting the others would leave them owned but unrun until
        // a sweep noticed.
        let mut first = None;
        let now = std::time::Instant::now();
        for key in reply.keys {
            let queue = key
                .key
                .strip_prefix("tarsk:")
                .unwrap_or(&key.key)
                .to_string();
            for entry in key.ids {
                let (name, payload, timeout_ms) = entry_fields(&entry);
                if timeout_ms > 0 {
                    self.min_timeout_ms.fetch_min(timeout_ms, Ordering::Relaxed);
                }
                let delivery = Delivery {
                    id: entry_id(&entry),
                    name,
                    payload,
                    attempt: entry.delivered_count.unwrap_or(1) as u32,
                    receipt: Receipt::Redis {
                        queue: queue.clone(),
                        id: entry.id,
                    },
                };
                if first.is_none() {
                    first = Some(delivery);
                } else {
                    self.claimed
                        .lock()
                        .unwrap()
                        .push_back((delivery, now, timeout_ms.max(1)));
                }
            }
        }
        if first.is_some() {
            // A read that returned work earns a larger one next time; the
            // discard path above walks it back down when batches go stale.
            let _ = self
                .prefetch
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some((n * 2).min(self.prefetch_cap.load(Ordering::Relaxed).max(1)))
                });
        }
        Ok(first)
    }

    async fn ack(&self, receipt: &Receipt) -> Res<()> {
        let Receipt::Redis { queue, id } = receipt else {
            return Err("not a redis receipt".into());
        };
        let key = stream_key(queue);
        let mut conn = self.conn.clone();
        // XACK clears the pending entry; XDEL stops the stream growing forever.
        // Pipelined so a crash between them cannot leave the entry both
        // unacknowledged and deleted.
        let _: (i64, i64) = redis::pipe()
            .atomic()
            .cmd("XACK")
            .arg(&key)
            .arg(REDIS_GROUP)
            .arg(id)
            .cmd("XDEL")
            .arg(&key)
            .arg(id)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Hand the job back without duplicating it.
    ///
    /// The entry stays exactly where it is; only its idle clock moves, set past
    /// the job's own deadline so the next sweep treats it as overdue. Re-adding
    /// a copy and acking the original would be two writes with a window between
    /// them, and a crash in that window leaves the job in the stream twice.
    async fn requeue(&self, receipt: &Receipt, delay: Duration) -> Res<()> {
        let Receipt::Redis { queue, id } = receipt else {
            return Err("not a redis receipt".into());
        };
        let key = stream_key(queue);
        let mut conn = self.conn.clone();
        let entries: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(&key)
            .arg(id)
            .arg(id)
            .query_async(&mut conn)
            .await?;
        let deadline = entries.ids.first().map(|e| entry_fields(e).2).unwrap_or(0)
            + self.grace_ms.load(Ordering::Relaxed);

        // Backoff without a delay queue: the sweep fires once idle passes the
        // deadline, so winding the idle clock back by `delay` is the same thing
        // as scheduling. Streams cannot express a negative idle, which caps the
        // backoff at one deadline.
        //
        // ponytail: ceiling is timeout + grace, ~5.5 min at the default cap. A
        // sorted set of due times promoted by a Lua script lifts it, and that
        // is the first place a script would actually earn its keep here.
        let delay_ms = delay.as_millis() as u64;
        let idle = deadline.saturating_sub(delay_ms) + 1;
        let _: redis::Value = redis::cmd("XCLAIM")
            .arg(&key)
            .arg(REDIS_GROUP)
            .arg(&self.consumer)
            .arg(0)
            .arg(id)
            .arg("IDLE")
            .arg(idle)
            .arg("JUSTID") // no delivery-count bump: the reclaim will do that
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Move the entry to `tarsk:{queue}:dead` and drop it from the live stream.
    ///
    /// MULTI is enough: every field is already in hand from the delivery, so
    /// there is nothing to read and decide between the writes. That is the line
    /// where a Lua script would become necessary rather than decorative.
    async fn dead_letter(&self, receipt: &Receipt, error: &str, traceback: &str) -> Res<()> {
        let Receipt::Redis { queue, id } = receipt else {
            return Err("not a redis receipt".into());
        };
        let key = stream_key(queue);
        let grave = format!("{key}:dead");
        let mut conn = self.conn.clone();
        let entries: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(&key)
            .arg(id)
            .arg(id)
            .query_async(&mut conn)
            .await?;
        let (name, payload, timeout_ms) = entries
            .ids
            .first()
            .map(entry_fields)
            .unwrap_or_else(|| (String::new(), Vec::new(), 0));
        let _: redis::Value = redis::pipe()
            .atomic()
            .cmd("XADD")
            .arg(&grave)
            .arg("*")
            .arg("n")
            .arg(name)
            .arg("p")
            .arg(payload)
            .arg("t")
            .arg(timeout_ms)
            .arg("e")
            .arg(error)
            .arg("tb")
            .arg(traceback)
            .cmd("XACK")
            .arg(&key)
            .arg(REDIS_GROUP)
            .arg(id)
            .cmd("XDEL")
            .arg(&key)
            .arg(id)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    /// Take ownership of pending entries whose own timeout has elapsed (§4.5).
    ///
    /// XPENDING's IDLE filter is a single number while each job carries its own
    /// timeout, so the floor is the shortest registered one and every candidate
    /// is then checked against the deadline it actually has. XCLAIM with that
    /// deadline as min-idle-time is the atomic gate between supervisors: Redis
    /// runs one at a time, the winner resets the entry's idle clock, and the
    /// loser gets an empty reply. Nothing is created or deleted, so there is no
    /// window in which the job exists twice.
    async fn reclaim_expired(&self, grace: Duration) -> Res<usize> {
        let grace_ms = grace.as_millis() as u64;
        self.grace_ms.store(grace_ms, Ordering::Relaxed);
        let floor = self
            .min_timeout_ms
            .load(Ordering::Relaxed)
            .saturating_add(grace_ms);
        if self.min_timeout_ms.load(Ordering::Relaxed) == u64::MAX {
            return Ok(0); // nothing claimed yet, so nothing can be overdue
        }
        let mut reclaimed = 0;
        for queue in &self.queues {
            let key = stream_key(queue);
            let mut conn = self.conn.clone();
            let pending: redis::streams::StreamPendingCountReply = redis::cmd("XPENDING")
                .arg(&key)
                .arg(REDIS_GROUP)
                .arg("IDLE")
                .arg(floor)
                .arg("-")
                .arg("+")
                .arg(64)
                .query_async(&mut conn)
                .await?;
            for candidate in pending.ids {
                let entries: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
                    .arg(&key)
                    .arg(&candidate.id)
                    .arg(&candidate.id)
                    .query_async(&mut conn)
                    .await?;
                let Some(entry) = entries.ids.first() else {
                    continue;
                };
                let (name, payload, timeout_ms) = entry_fields(entry);
                let deadline = timeout_ms + grace_ms;
                if (candidate.last_delivered_ms as u64) <= deadline {
                    continue; // still inside its own lease
                }
                let claimed: redis::streams::StreamClaimReply = redis::cmd("XCLAIM")
                    .arg(&key)
                    .arg(REDIS_GROUP)
                    .arg(&self.consumer)
                    .arg(deadline)
                    .arg(&candidate.id)
                    .query_async(&mut conn)
                    .await?;
                if claimed.ids.is_empty() {
                    continue; // another supervisor got there first
                }
                self.claimed.lock().unwrap().push_back((
                    Delivery {
                        id: entry_id(entry),
                        name,
                        payload,
                        attempt: candidate.times_delivered as u32 + 1,
                        receipt: Receipt::Redis {
                            queue: queue.clone(),
                            id: candidate.id,
                        },
                    },
                    std::time::Instant::now(),
                    timeout_ms.max(1),
                ));
                reclaimed += 1;
            }
        }
        Ok(reclaimed)
    }
}

// ---------------------------------------------------------------- postgres

/// One table, because the 5-minute `max_timeout` cap (spec §9) means a lease
/// never needs renewing. `awa` splits ready / deferred / lease / tombstone
/// tables to support heartbeats and mutable attempt state; none of that is
/// reachable when a lease cannot outlive a known ceiling.
const PG_SCHEMA: &str = "
create table if not exists tarsk_jobs (
    id          bigserial primary key,
    job_id      text        not null,
    queue       text        not null,
    name        text        not null,
    payload     bytea       not null,
    timeout_ms  integer     not null,
    attempt     integer     not null default 0,
    run_lease   bigint      not null default 0,
    lease_until timestamptz,
    created_at  timestamptz not null default now()
);
create index if not exists tarsk_jobs_ready on tarsk_jobs (queue, id) where lease_until is null;
create index if not exists tarsk_jobs_leased on tarsk_jobs (lease_until) where lease_until is not null;

create table if not exists tarsk_cron (
    name    text   not null,
    minute  bigint not null,
    primary key (name, minute)
);

create table if not exists tarsk_results (
    id         text        primary key,
    blob       bytea       not null,
    expires_at timestamptz not null
);
create index if not exists tarsk_results_expiry on tarsk_results (expires_at);

create table if not exists tarsk_dead (
    id          bigint      primary key,
    queue       text        not null,
    name        text        not null,
    payload     bytea       not null,
    attempt     integer     not null,
    error       text        not null,
    traceback   text        not null,
    died_at     timestamptz not null default now()
);
";

/// Claiming and reclaiming are the same statement: a lease that has run out is
/// indistinguishable from one that was never taken, so expiry needs no sweep.
const PG_CLAIM: &str = "
update tarsk_jobs set
    lease_until = now() + make_interval(secs => timeout_ms / 1000.0 + $2::double precision),
    attempt     = attempt + 1,
    run_lease   = run_lease + 1
where id = (
    select id from tarsk_jobs
    where queue = any($1) and (lease_until is null or lease_until < now())
    order by id
    for update skip locked
    limit 1
)
returning id, name, payload, attempt, run_lease, job_id
";

pub struct PgBroker {
    client: tokio_postgres::Client,
    queues: Vec<String>,
    stores: AtomicU64,
}

impl PgBroker {
    async fn connect(url: &str, queues: Vec<String>) -> Res<PgBroker> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
        // The connection future drives the socket; dropping it closes the link.
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.batch_execute(PG_SCHEMA).await?;
        Ok(PgBroker {
            client,
            queues,
            stores: AtomicU64::new(0),
        })
    }

    /// A delay needs no schedule table: the lease column is already a
    /// visibility timer, so a row leased into the future is simply not claimed
    /// until then.
    async fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        self.client
            .execute(
                "insert into tarsk_jobs (job_id, queue, name, payload, timeout_ms, lease_until)
                 values ($6, $1, $2, $3, $4, case when $5::double precision > 0
                     then now() + make_interval(secs => $5::double precision) else null end)",
                &[
                    &job.queue,
                    &job.name,
                    &job.payload,
                    &(job.timeout_ms as i32),
                    &delay.as_secs_f64(),
                    &job.id,
                ],
            )
            .await?;
        Ok(())
    }

    async fn claim(&self, grace: Duration, block: Duration) -> Res<Option<Delivery>> {
        // ponytail: polls. LISTEN/NOTIFY is the upgrade and would cut idle
        // pickup latency to the round trip; it costs a dedicated connection to
        // hold the notification stream, which is not worth it until someone
        // measures the wait.
        let deadline = tokio::time::Instant::now() + block;
        loop {
            let rows = self
                .client
                .query(PG_CLAIM, &[&self.queues, &grace.as_secs_f64()])
                .await?;
            if let Some(row) = rows.first() {
                let run_lease: i64 = row.get(4);
                return Ok(Some(Delivery {
                    id: row.get(5),
                    name: row.get(1),
                    payload: row.get(2),
                    attempt: row.get::<_, i32>(3) as u32,
                    receipt: Receipt::Postgres {
                        row: row.get(0),
                        run_lease,
                    },
                }));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(25).min(block)).await;
        }
    }

    async fn ack(&self, receipt: &Receipt) -> Res<()> {
        let Receipt::Postgres { row, run_lease } = receipt else {
            return Err("not a postgres receipt".into());
        };
        // The run_lease guard: nothing happens if this job was reclaimed and
        // re-run while we were away.
        self.client
            .execute(
                "delete from tarsk_jobs where id = $1 and run_lease = $2",
                &[row, run_lease],
            )
            .await?;
        Ok(())
    }

    /// The lease column doubles as the visibility timer: a row leased into the
    /// future is invisible to `claim` until then, so backoff needs no separate
    /// schedule. Zero delay clears it and the row is claimable immediately.
    async fn requeue(&self, receipt: &Receipt, delay: Duration) -> Res<()> {
        let Receipt::Postgres { row, run_lease } = receipt else {
            return Err("not a postgres receipt".into());
        };
        self.client
            .execute(
                "update tarsk_jobs set lease_until = case when $3::double precision > 0
                     then now() + make_interval(secs => $3::double precision) else null end
                 where id = $1 and run_lease = $2",
                &[row, run_lease, &delay.as_secs_f64()],
            )
            .await?;
        Ok(())
    }

    async fn claim_tick(&self, name: &str, minute: i64) -> Res<bool> {
        // The primary key is the election: exactly one insert can succeed.
        let inserted = self
            .client
            .execute(
                "insert into tarsk_cron (name, minute) values ($1, $2) on conflict do nothing",
                &[&name, &minute],
            )
            .await?;
        if inserted == 1 {
            self.client
                .execute(
                    "delete from tarsk_cron where minute < $1",
                    &[&(minute - 120)],
                )
                .await?;
        }
        Ok(inserted == 1)
    }

    async fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        self.client
            .execute(
                "insert into tarsk_results (id, blob, expires_at)
                 values ($1, $2, now() + make_interval(secs => $3::double precision))
                 on conflict (id) do update set blob = excluded.blob,
                                                expires_at = excluded.expires_at",
                &[&id, &blob, &ttl.as_secs_f64()],
            )
            .await?;
        // Postgres has no TTL of its own, so expiry has to be swept. Every
        // 256th write keeps it off the hot path without needing a scheduler.
        if self
            .stores
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(256)
        {
            self.client
                .execute("delete from tarsk_results where expires_at < now()", &[])
                .await?;
        }
        Ok(())
    }

    async fn get_result(&self, id: &str) -> Res<Option<Vec<u8>>> {
        let rows = self
            .client
            .query(
                "select blob from tarsk_results where id = $1 and expires_at > now()",
                &[&id],
            )
            .await?;
        Ok(rows.first().map(|row| row.get(0)))
    }

    /// One statement, so the row cannot be in both tables or neither.
    async fn dead_letter(&self, receipt: &Receipt, error: &str, traceback: &str) -> Res<()> {
        let Receipt::Postgres { row, run_lease } = receipt else {
            return Err("not a postgres receipt".into());
        };
        self.client
            .execute(
                "with gone as (
                     delete from tarsk_jobs where id = $1 and run_lease = $2
                     returning id, queue, name, payload, attempt
                 )
                 insert into tarsk_dead (id, queue, name, payload, attempt, error, traceback)
                 select id, queue, name, payload, attempt, $3, $4 from gone
                 on conflict (id) do nothing",
                &[row, run_lease, &error, &traceback],
            )
            .await?;
        Ok(())
    }
}
