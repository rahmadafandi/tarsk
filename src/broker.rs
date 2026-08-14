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

pub fn now_ms() -> u64 {
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
    /// The steps queued to run after this one. See `NewJob::chain`.
    pub chain: Vec<u8>,
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

/// One parked failure, as a human needs to see it.
pub struct Dead {
    pub id: String,
    pub name: String,
    pub error: String,
    pub traceback: String,
    /// Milliseconds since the epoch, or 0 where the store does not say.
    pub died_at_ms: u64,
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
            Broker::Memory(_) => Ok(true),
            Broker::Redis(b) => b.acquire_slot(task, job_id, max, lease_ms).await,
            Broker::Postgres(b) => b.acquire_slot(task, job_id, max, lease_ms).await,
        }
    }

    /// Give the slot back. Idempotent, so settling twice cannot free two.
    pub async fn release_slot(&self, task: &str, job_id: &str) -> Res<()> {
        match self {
            Broker::Memory(_) => Ok(()),
            Broker::Redis(b) => b.release_slot(task, job_id).await,
            Broker::Postgres(b) => b.release_slot(task, job_id).await,
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
            Broker::Memory(b) => Ok(b.take_token(task, per_sec, burst)),
            Broker::Redis(b) => b.take_token(task, per_sec, burst).await,
            Broker::Postgres(b) => b.take_token(task, per_sec, burst).await,
        }
    }

    /// Mark a job id as cancelled for `ttl_ms`, so it is never dispatched.
    ///
    /// The TTL has to outlive the job it is cancelling: a delayed task due next
    /// week needs a cancellation that is still there next week. It is the
    /// caller's number because only the caller knows how far ahead it queued.
    pub async fn revoke(&self, queue: &str, id: &str, ttl_ms: u64) -> Res<()> {
        match self {
            Broker::Memory(b) => b.revoke(id, ttl_ms),
            Broker::Redis(b) => b.revoke(queue, id, ttl_ms).await,
            Broker::Postgres(b) => b.revoke(queue, id, ttl_ms).await,
        }
    }

    /// Every id cancelled and not yet expired, across the queues this broker
    /// was opened on — which it knows and the supervisor does not.
    ///
    /// Read as a set on a timer rather than asked per job: a lookup at every
    /// dispatch would put a broker round trip on the path of every task, to
    /// answer "no" for almost all of them.
    pub async fn revoked_all(&self) -> Res<Vec<String>> {
        match self {
            Broker::Memory(b) => Ok(b.revoked()),
            Broker::Redis(b) => {
                let mut out = Vec::new();
                for queue in &b.queues {
                    out.extend(b.revoked(queue).await?);
                }
                Ok(out)
            }
            Broker::Postgres(b) => b.revoked_all().await,
        }
    }

    /// Backlog across the queues this broker was opened on — what a worker
    /// reports about its own queues.
    pub async fn depth(&self) -> Res<Vec<Depth>> {
        match self {
            Broker::Memory(_) => Ok(Vec::new()),
            Broker::Redis(b) => b.depth(&b.queues).await,
            Broker::Postgres(b) => b.depth(&b.queues).await,
        }
    }

    /// Backlog for named queues, for a caller that opened no queues of its own
    /// — `tarsk status` is not a worker and subscribes to nothing.
    pub async fn depth_of(&self, queues: &[String]) -> Res<Vec<Depth>> {
        match self {
            Broker::Memory(_) => Ok(Vec::new()),
            Broker::Redis(b) => b.depth(queues).await,
            Broker::Postgres(b) => b.depth(queues).await,
        }
    }

    /// What is parked in the dead letters, newest last.
    ///
    /// The store existed from the first broker commit and nothing could read
    /// it, which made it a place work went to be forgotten rather than found.
    pub async fn dead_list(&self, queue: &str, limit: usize) -> Res<Vec<Dead>> {
        match self {
            Broker::Redis(b) => b.dead_list(queue, limit).await,
            Broker::Postgres(b) => b.dead_list(queue, limit).await,
            Broker::Memory(_) => Ok(Vec::new()),
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
            Broker::Redis(b) => b.dead_replay(queue, ids).await,
            Broker::Postgres(b) => b.dead_replay(queue, ids).await,
            Broker::Memory(_) => Ok(0),
        }
    }

    /// Drop dead letters. Empty `ids` means all of them.
    pub async fn dead_purge(&self, queue: &str, ids: &[String]) -> Res<usize> {
        match self {
            Broker::Redis(b) => b.dead_purge(queue, ids).await,
            Broker::Postgres(b) => b.dead_purge(queue, ids).await,
            Broker::Memory(_) => Ok(0),
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

    /// Reserve `key` for `job_id` for `ttl_ms`, or report who holds it.
    ///
    /// `None` means this caller won and should send. `Some(id)` means an
    /// identical send is already covered by that job, and its id is returned so
    /// the caller can wait on the same answer rather than being handed one for
    /// a job that was never queued.
    pub async fn claim_dedup(&self, key: &str, job_id: &str, ttl_ms: u64) -> Res<Option<String>> {
        match self {
            Broker::Memory(_) => Ok(None),
            Broker::Redis(b) => b.claim_dedup(key, job_id, ttl_ms).await,
            Broker::Postgres(b) => b.claim_dedup(key, job_id, ttl_ms).await,
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
    revoked: Mutex<HashMap<String, std::time::Instant>>,
    buckets: Mutex<HashMap<String, (f64, std::time::Instant)>>,
}

impl MemoryBroker {
    fn take_token(&self, task: &str, per_sec: f64, burst: u32) -> u64 {
        let now = std::time::Instant::now();
        let mut buckets = self.buckets.lock().unwrap();
        let (tokens, seen) = buckets
            .entry(task.to_string())
            .or_insert((burst as f64, now));
        *tokens = (*tokens + seen.elapsed().as_secs_f64() * per_sec).min(burst as f64);
        *seen = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            0
        } else {
            (((1.0 - *tokens) / per_sec) * 1000.0).ceil() as u64
        }
    }

    fn revoke(&self, id: &str, ttl_ms: u64) -> Res<()> {
        let until = std::time::Instant::now() + Duration::from_millis(ttl_ms);
        self.revoked.lock().unwrap().insert(id.to_string(), until);
        Ok(())
    }

    fn revoked(&self) -> Vec<String> {
        let now = std::time::Instant::now();
        let mut held = self.revoked.lock().unwrap();
        held.retain(|_, until| *until > now);
        held.keys().cloned().collect()
    }

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
            ready_at_ms: 0,
            chain: job.chain.clone(),
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
    /// Only for `XREADGROUP … BLOCK`, and never shared with anything else.
    ///
    /// A blocking command on a multiplexed connection holds up every command
    /// multiplexed onto it, because there is one socket and the server answers
    /// in order. With children waiting for work, that put the reclaim sweep,
    /// the acks and the rate-limit script behind a 250ms wait each time — the
    /// sweep was timing out and requeued jobs were never coming back.
    blocking: redis::aio::MultiplexedConnection,
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

/// The millisecond in a Redis stream id, which is `<millis>-<seq>`.
fn stream_id_ms(id: &str) -> u64 {
    id.split('-')
        .next()
        .and_then(|m| m.parse().ok())
        .unwrap_or(0)
}

fn dedup_key(key: &str) -> String {
    format!("tarsk:dedup:{key}")
}

fn slots_key(task: &str) -> String {
    format!("tarsk:slots:{task}")
}

fn revoked_key(queue: &str) -> String {
    format!("{}:revoked", stream_key(queue))
}

fn result_key(id: &str) -> String {
    format!("tarsk:result:{id}")
}

impl RedisBroker {
    async fn connect(url: &str, queues: Vec<String>) -> Res<RedisBroker> {
        let client = redis::Client::open(url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let blocking = client.get_multiplexed_async_connection().await?;
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
            blocking,
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
                .arg("c")
                .arg(&job.chain)
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
        let mut conn = self.blocking.clone();
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
                    ready_at_ms: stream_id_ms(&entry.id),
                    chain: entry.get("c").unwrap_or_default(),
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

    async fn depth(&self, queues: &[String]) -> Res<Vec<Depth>> {
        let mut out = Vec::new();
        for queue in queues {
            let key = stream_key(queue);
            let mut conn = self.conn.clone();
            // XLEN counts what is still in the stream, and an entry only leaves
            // on ack — so it is ready plus in-flight, and the pending count
            // separates them.
            let (len, dead, delayed): (u64, u64, u64) = redis::pipe()
                .cmd("XLEN")
                .arg(&key)
                .cmd("XLEN")
                .arg(format!("{key}:dead"))
                .cmd("ZCARD")
                .arg(delayed_key(queue))
                .query_async(&mut conn)
                .await?;
            // NOGROUP when nothing has ever been sent to this queue, which is a
            // real state to report as zeros rather than an error to raise at
            // someone who typed `tarsk status`.
            let pending: Option<redis::streams::StreamPendingReply> = redis::cmd("XPENDING")
                .arg(&key)
                .arg(REDIS_GROUP)
                .query_async(&mut conn)
                .await
                .ok();
            let in_flight = match pending {
                Some(redis::streams::StreamPendingReply::Data(d)) => d.count as u64,
                _ => 0,
            };
            out.push(Depth {
                queue: queue.clone(),
                ready: len.saturating_sub(in_flight),
                in_flight,
                delayed,
                dead,
            });
        }
        Ok(out)
    }

    async fn claim_dedup(&self, key: &str, job_id: &str, ttl_ms: u64) -> Res<Option<String>> {
        let mut conn = self.conn.clone();
        // SET NX is the whole mechanism: whoever writes first owns the window,
        // and the loser reads back the winner rather than guessing.
        let won: Option<String> = redis::cmd("SET")
            .arg(dedup_key(key))
            .arg(job_id)
            .arg("NX")
            .arg("PX")
            .arg(ttl_ms.max(1))
            .query_async(&mut conn)
            .await?;
        if won.is_some() {
            return Ok(None);
        }
        let holder: Option<String> = redis::cmd("GET")
            .arg(dedup_key(key))
            .query_async(&mut conn)
            .await?;
        // Empty means the window lapsed between the two commands, so nobody
        // holds it and this send should go ahead.
        Ok(holder)
    }

    async fn acquire_slot(&self, task: &str, job_id: &str, max: u32, lease_ms: u64) -> Res<bool> {
        // Prune, count and add cannot be three commands: two workers would both
        // see the last slot free. A sorted set scored by expiry gives the prune
        // for free, which a counter would need a separate reaper for.
        let script = redis::Script::new(
            r"local now, until_ms, max = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
              redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now)
              if redis.call('ZCARD', KEYS[1]) >= max
                 and redis.call('ZSCORE', KEYS[1], ARGV[4]) == false then
                return 0
              end
              redis.call('ZADD', KEYS[1], until_ms, ARGV[4])
              redis.call('PEXPIRE', KEYS[1], until_ms - now + 60000)
              return 1",
        );
        let mut conn = self.conn.clone();
        let now = now_ms();
        let taken: i64 = script
            .key(slots_key(task))
            .arg(now)
            .arg(now + lease_ms.max(1))
            .arg(max)
            .arg(job_id)
            .invoke_async(&mut conn)
            .await?;
        Ok(taken == 1)
    }

    async fn release_slot(&self, task: &str, job_id: &str) -> Res<()> {
        let mut conn = self.conn.clone();
        let _: i64 = redis::cmd("ZREM")
            .arg(slots_key(task))
            .arg(job_id)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn take_token(&self, task: &str, per_sec: f64, burst: u32) -> Res<u64> {
        // Read-refill-write has to be one step or two workers both see the last
        // token. Lua is the only thing Redis offers that is; MULTI would not
        // help, since the decision depends on what the read returned.
        let script = redis::Script::new(
            r"local tokens = tonumber(redis.call('HGET', KEYS[1], 't'))
              local seen   = tonumber(redis.call('HGET', KEYS[1], 's'))
              local rate, burst, now = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
              if tokens == nil then tokens = burst; seen = now end
              tokens = math.min(burst, tokens + math.max(0, now - seen) * rate / 1000)
              local wait = 0
              if tokens >= 1 then
                tokens = tokens - 1
              else
                wait = math.ceil((1 - tokens) * 1000 / rate)
              end
              redis.call('HSET', KEYS[1], 't', tokens, 's', now)
              -- Long enough to refill from empty; a bucket nobody touches for
              -- that long is indistinguishable from a fresh one.
              redis.call('PEXPIRE', KEYS[1], math.ceil(burst * 1000 / rate) + 1000)
              return wait",
        );
        let mut conn = self.conn.clone();
        let wait: i64 = script
            .key(format!("tarsk:rate:{task}"))
            .arg(per_sec)
            .arg(burst)
            .arg(now_ms())
            .invoke_async(&mut conn)
            .await?;
        Ok(wait.max(0) as u64)
    }

    async fn revoke(&self, queue: &str, id: &str, ttl_ms: u64) -> Res<()> {
        let mut conn = self.conn.clone();
        let _: i64 = redis::cmd("ZADD")
            .arg(revoked_key(queue))
            .arg(now_ms() + ttl_ms)
            .arg(id)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    async fn revoked(&self, queue: &str) -> Res<Vec<String>> {
        let key = revoked_key(queue);
        let mut conn = self.conn.clone();
        // Prune and read in one round trip. A sorted set scored by expiry gives
        // per-member TTL, which a plain SET cannot.
        let (_, live): (i64, Vec<String>) = redis::pipe()
            .cmd("ZREMRANGEBYSCORE")
            .arg(&key)
            .arg(0)
            .arg(now_ms())
            .cmd("ZRANGE")
            .arg(&key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;
        Ok(live)
    }

    async fn dead_list(&self, queue: &str, limit: usize) -> Res<Vec<Dead>> {
        let grave = format!("{}:dead", stream_key(queue));
        let mut conn = self.conn.clone();
        let entries: redis::streams::StreamRangeReply = redis::cmd("XREVRANGE")
            .arg(&grave)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(limit)
            .query_async(&mut conn)
            .await?;
        let mut out: Vec<Dead> = entries
            .ids
            .iter()
            .map(|e| Dead {
                // A stream id is `<millis>-<seq>`, so when it died is already
                // in the key and does not need storing twice.
                died_at_ms: e
                    .id
                    .split('-')
                    .next()
                    .and_then(|m| m.parse().ok())
                    .unwrap_or(0),
                id: e.id.clone(),
                name: e.get("n").unwrap_or_default(),
                error: e.get("e").unwrap_or_default(),
                traceback: e.get("tb").unwrap_or_default(),
            })
            .collect();
        out.reverse(); // XREVRANGE reads newest first; print oldest first
        Ok(out)
    }

    async fn dead_replay(&self, queue: &str, ids: &[String]) -> Res<usize> {
        let key = stream_key(queue);
        let grave = format!("{key}:dead");
        let mut conn = self.conn.clone();
        let entries: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(&grave)
            .arg("-")
            .arg("+")
            .query_async(&mut conn)
            .await?;
        let mut moved = 0;
        for entry in &entries.ids {
            if !ids.is_empty() && !ids.contains(&entry.id) {
                continue;
            }
            let (name, payload, timeout_ms) = entry_fields(entry);
            let job_id: String = entry.get("i").unwrap_or_else(|| entry.id.clone());
            // XADD then XDEL in one MULTI: a crash between them would either
            // lose the job or leave it in both places, and both are worse than
            // this being one round trip slower.
            let _: redis::Value = redis::pipe()
                .atomic()
                .cmd("XADD")
                .arg(&key)
                .arg("*")
                .arg("n")
                .arg(&name)
                .arg("p")
                .arg(&payload)
                .arg("t")
                .arg(timeout_ms)
                .arg("i")
                .arg(&job_id)
                .cmd("XDEL")
                .arg(&grave)
                .arg(&entry.id)
                .query_async(&mut conn)
                .await?;
            moved += 1;
        }
        Ok(moved)
    }

    async fn dead_purge(&self, queue: &str, ids: &[String]) -> Res<usize> {
        let grave = format!("{}:dead", stream_key(queue));
        let mut conn = self.conn.clone();
        if ids.is_empty() {
            let n: i64 = redis::cmd("XLEN")
                .arg(&grave)
                .query_async(&mut conn)
                .await?;
            let _: i64 = redis::cmd("DEL").arg(&grave).query_async(&mut conn).await?;
            return Ok(n as usize);
        }
        let n: i64 = redis::cmd("XDEL")
            .arg(&grave)
            .arg(ids)
            .query_async(&mut conn)
            .await?;
        Ok(n as usize)
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
                        ready_at_ms: stream_id_ms(&candidate.id),
                        chain: entry.get("c").unwrap_or_default(),
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
    chain       bytea,
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
-- job_id and timeout_ms were not kept until the dead letters could be read
-- back. Nothing noticed, because nothing read them: a store you can only write
-- to cannot tell you it is missing a column. Added separately from the CREATE
-- so a table made by an earlier version gains them too.
create table if not exists tarsk_dedup (
    key        text        primary key,
    job_id     text        not null,
    expires_at timestamptz not null
);
create index if not exists tarsk_dedup_expiry on tarsk_dedup (expires_at);
create table if not exists tarsk_slots (
    task       text        not null,
    job_id     text        not null,
    expires_at timestamptz not null,
    primary key (task, job_id)
);
create index if not exists tarsk_slots_expiry on tarsk_slots (expires_at);
create table if not exists tarsk_buckets (
    task   text             primary key,
    tokens double precision not null,
    seen   timestamptz      not null
);
create table if not exists tarsk_revoked (
    id         text        primary key,
    queue      text        not null,
    expires_at timestamptz not null
);
create index if not exists tarsk_revoked_expiry on tarsk_revoked (expires_at);
alter table tarsk_jobs add column if not exists chain      bytea;
alter table tarsk_dead add column if not exists job_id     text;
alter table tarsk_dead add column if not exists timeout_ms integer not null default 0;
";

/// Claiming and reclaiming are the same statement: a lease that has run out is
/// indistinguishable from one that was never taken, so expiry needs no sweep.
const PG_CLAIM: &str = "
with picked as (
    select id,
           -- The moment this became runnable, captured before the update
           -- overwrites it: lease_until is when a delayed job came due or a
           -- lost lease lapsed, and null means it was runnable when written.
           coalesce(lease_until, created_at) as ready_at
      from tarsk_jobs
     where queue = any($1) and (lease_until is null or lease_until < now())
     order by id
       for update skip locked
     limit 1
)
update tarsk_jobs set
    lease_until = now() + make_interval(secs => timeout_ms / 1000.0 + $2::double precision),
    attempt     = attempt + 1,
    run_lease   = run_lease + 1
from picked
where tarsk_jobs.id = picked.id
returning tarsk_jobs.id, name, payload, attempt, run_lease, job_id, picked.ready_at, chain
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
                "insert into tarsk_jobs
                     (job_id, queue, name, payload, timeout_ms, lease_until, chain)
                 values ($6, $1, $2, $3, $4, case when $5::double precision > 0
                     then now() + make_interval(secs => $5::double precision) else null end, $7)",
                &[
                    &job.queue,
                    &job.name,
                    &job.payload,
                    &(job.timeout_ms as i32),
                    &delay.as_secs_f64(),
                    &job.id,
                    &job.chain,
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
                let ready: std::time::SystemTime = row.get(6);
                return Ok(Some(Delivery {
                    id: row.get(5),
                    name: row.get(1),
                    payload: row.get(2),
                    attempt: row.get::<_, i32>(3) as u32,
                    ready_at_ms: ready
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    chain: row.get::<_, Option<Vec<u8>>>(7).unwrap_or_default(),
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
    async fn depth(&self, queues: &[String]) -> Res<Vec<Depth>> {
        // run_lease separates the two kinds of future lease_until: a delayed
        // job has never been claimed, an in-flight one has. Without it the
        // backlog and the work in progress look identical.
        let rows = self
            .client
            .query(
                "select queue,
                        count(*) filter (where lease_until is null or lease_until < now()),
                        count(*) filter (where lease_until >= now() and run_lease > 0),
                        count(*) filter (where lease_until >= now() and run_lease = 0)
                   from tarsk_jobs where queue = any($1) group by queue",
                &[&queues],
            )
            .await?;
        let dead = self
            .client
            .query(
                "select queue, count(*) from tarsk_dead where queue = any($1) group by queue",
                &[&queues],
            )
            .await?;
        let mut out = Vec::new();
        for queue in queues {
            let row = rows.iter().find(|r| r.get::<_, String>(0) == *queue);
            let buried = dead
                .iter()
                .find(|r| r.get::<_, String>(0) == *queue)
                .map(|r| r.get::<_, i64>(1))
                .unwrap_or(0);
            out.push(Depth {
                queue: queue.clone(),
                ready: row.map(|r| r.get::<_, i64>(1)).unwrap_or(0).max(0) as u64,
                in_flight: row.map(|r| r.get::<_, i64>(2)).unwrap_or(0).max(0) as u64,
                delayed: row.map(|r| r.get::<_, i64>(3)).unwrap_or(0).max(0) as u64,
                dead: buried.max(0) as u64,
            });
        }
        Ok(out)
    }

    async fn claim_dedup(&self, key: &str, job_id: &str, ttl_ms: u64) -> Res<Option<String>> {
        let rows = self
            .client
            .query(
                "with fresh as (delete from tarsk_dedup where expires_at <= now())
                 insert into tarsk_dedup (key, job_id, expires_at)
                 values ($1, $2, now() + make_interval(secs => $3))
                 on conflict (key) do nothing
                 returning job_id",
                &[&key, &job_id, &(ttl_ms.max(1) as f64 / 1000.0)],
            )
            .await?;
        if !rows.is_empty() {
            return Ok(None); // inserted, so this caller owns the window
        }
        let held = self
            .client
            .query(
                "select job_id from tarsk_dedup where key = $1 and expires_at > now()",
                &[&key],
            )
            .await?;
        Ok(held.first().map(|r| r.get(0)))
    }

    async fn acquire_slot(&self, task: &str, job_id: &str, max: u32, lease_ms: u64) -> Res<bool> {
        // One statement again: the delete of expired holders, the count and the
        // insert have to be indivisible or two workers both find room.
        // `on conflict do update` makes a redelivery renew its own slot rather
        // than be refused by it.
        let row = self
            .client
            .query_one(
                "with gone as (delete from tarsk_slots where expires_at <= now())
                 insert into tarsk_slots (task, job_id, expires_at)
                 select $1, $2, now() + make_interval(secs => $4)
                  where (select count(*) from tarsk_slots
                          where task = $1 and expires_at > now()
                            and job_id <> $2) < $3
                 on conflict (task, job_id) do update set expires_at = excluded.expires_at
                 returning true",
                &[
                    &task,
                    &job_id,
                    &(max as i64),
                    &(lease_ms.max(1) as f64 / 1000.0),
                ],
            )
            .await;
        match row {
            Ok(_) => Ok(true),
            // No row inserted means the where-clause found no room. tokio-postgres
            // reports that as "unexpected number of rows", which is the answer
            // rather than a fault.
            Err(e) if e.to_string().contains("unexpected number of rows") => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn release_slot(&self, task: &str, job_id: &str) -> Res<()> {
        self.client
            .execute(
                "delete from tarsk_slots where task = $1 and job_id = $2",
                &[&task, &job_id],
            )
            .await?;
        Ok(())
    }

    async fn take_token(&self, task: &str, per_sec: f64, burst: u32) -> Res<u64> {
        // One statement, so a refill and a take cannot be split by another
        // worker. `refill` reads the row inside the same statement that
        // rewrites it, and reports the level *before* the take, which is the
        // only number that says whether there was a token to take.
        let row = self
            .client
            .query_one(
                // No `$2::float8`: a cast makes Postgres infer the parameter
                // as unknown and coerce at runtime, so the driver expects text
                // and serializing an f64 fails. Inference from context is what
                // types these — b.tokens is float8, and extract() returns
                // numeric, so that one needs the cast on the column instead.
                "with refill as (
                     select least($2,
                                  coalesce(b.tokens, $2)
                                  + extract(epoch from now() - coalesce(b.seen, now()))::float8
                                    * $3
                            ) as level
                       from (select 1) one
                       left join tarsk_buckets b on b.task = $1
                 ), taken as (
                     insert into tarsk_buckets (task, tokens, seen)
                     -- Cast every parameter: in a SELECT list Postgres has
                     -- nothing to infer $1's type from and refuses the whole
                     -- statement, which this failed silently on until the
                     -- limiter was measured rather than assumed.
                     select $1::text,
                            case when level >= 1 then level - 1 else level end,
                            now()
                       from refill
                     on conflict (task) do update
                        set tokens = excluded.tokens, seen = excluded.seen
                     returning 1
                 )
                 select level from refill, taken",
                &[&task, &(burst as f64), &per_sec],
            )
            .await?;
        let level: f64 = row.get(0);
        if level >= 1.0 {
            Ok(0)
        } else {
            Ok(((1.0 - level) / per_sec * 1000.0).ceil() as u64)
        }
    }

    async fn revoke(&self, queue: &str, id: &str, ttl_ms: u64) -> Res<()> {
        self.client
            .execute(
                "insert into tarsk_revoked (id, queue, expires_at)
                 values ($1, $2, now() + make_interval(secs => $3))
                 on conflict (id) do update set expires_at = excluded.expires_at",
                &[&id, &queue, &(ttl_ms as f64 / 1000.0)],
            )
            .await?;
        Ok(())
    }

    async fn revoked_all(&self) -> Res<Vec<String>> {
        self.client
            .execute("delete from tarsk_revoked where expires_at <= now()", &[])
            .await?;
        let rows = self
            .client
            .query(
                "select id from tarsk_revoked where queue = any($1)",
                &[&self.queues],
            )
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    async fn dead_list(&self, queue: &str, limit: usize) -> Res<Vec<Dead>> {
        let rows = self
            .client
            .query(
                "select id, name, error, traceback,
                        (extract(epoch from died_at) * 1000)::bigint as died_ms
                   from tarsk_dead where queue = $1
                  order by died_at desc limit $2",
                &[&queue, &(limit as i64)],
            )
            .await?;
        let mut out: Vec<Dead> = rows
            .iter()
            .map(|r| Dead {
                id: r.get::<_, i64>(0).to_string(),
                name: r.get(1),
                error: r.get(2),
                traceback: r.get(3),
                died_at_ms: r.get::<_, i64>(4).max(0) as u64,
            })
            .collect();
        out.reverse();
        Ok(out)
    }

    async fn dead_replay(&self, queue: &str, ids: &[String]) -> Res<usize> {
        // Back onto the live table with the lease cleared, inside one statement
        // so nothing can observe a job in both places.
        let picked: Vec<i64> = ids.iter().filter_map(|i| i.parse().ok()).collect();
        // A fresh row rather than the old id: bigserial hands out the next one,
        // and reusing a primary key that a result or a log still refers to
        // would quietly merge two runs of the same job into one identity.
        let sql = "with gone as (
                       delete from tarsk_dead
                        where queue = $1 and ($2::bigint[] = '{}' or id = any($2))
                    returning job_id, queue, name, payload, timeout_ms)
                   insert into tarsk_jobs
                       (job_id, queue, name, payload, timeout_ms, attempt, lease_until)
                   select coalesce(job_id, ''), queue, name, payload, timeout_ms, 0, null
                     from gone";
        let n = self.client.execute(sql, &[&queue, &picked]).await?;
        Ok(n as usize)
    }

    async fn dead_purge(&self, queue: &str, ids: &[String]) -> Res<usize> {
        let picked: Vec<i64> = ids.iter().filter_map(|i| i.parse().ok()).collect();
        let n = self
            .client
            .execute(
                "delete from tarsk_dead
                  where queue = $1 and ($2::bigint[] = '{}' or id = any($2))",
                &[&queue, &picked],
            )
            .await?;
        Ok(n as usize)
    }

    async fn dead_letter(&self, receipt: &Receipt, error: &str, traceback: &str) -> Res<()> {
        let Receipt::Postgres { row, run_lease } = receipt else {
            return Err("not a postgres receipt".into());
        };
        self.client
            .execute(
                "with gone as (
                     delete from tarsk_jobs where id = $1 and run_lease = $2
                     returning id, job_id, queue, name, payload, timeout_ms, attempt
                 )
                 insert into tarsk_dead
                     (id, job_id, queue, name, payload, timeout_ms, attempt, error, traceback)
                 select id, job_id, queue, name, payload, timeout_ms, attempt, $3, $4 from gone
                 on conflict (id) do nothing",
                &[row, run_lease, &error, &traceback],
            )
            .await?;
        Ok(())
    }
}
