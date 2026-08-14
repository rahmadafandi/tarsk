//! tarsk supervisor core (spec §4.1, §4.2, §4.4).
//!
//! Owns child processes, the IPC socket, RSS monitoring, and overlap
//! replacement. Never imports or executes user Python: the task registry
//! arrives over the wire in `Register`, so this process stays a constant-RSS
//! Rust process no matter what the user's app module drags in.
//!
//! Not here yet: broker (step 3), retry state machine (step 4), metrics
//! (step 5). Jobs arrive as an in-memory list from the caller.

mod broker;
mod cron;
mod metrics;
mod transport;

use broker::{Broker, Delivery, NewJob, Receipt};
use metrics::Metrics;

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use rmpv::Value;
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, Notify};

const MAX_FRAME: usize = 32 * 1024 * 1024;
/// Mirrors tarsk._child.EXIT_STARTUP_FAILED.
const EXIT_STARTUP_FAILED: i32 = 78;

// ---------------------------------------------------------------- framing

async fn read_frame(reader: &mut transport::Reader) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let size = u32::from_be_bytes(header) as usize;
    if size > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds MAX_FRAME",
        ));
    }
    let mut body = vec![0u8; size];
    reader.read_exact(&mut body).await?;
    Ok(Some(body))
}

async fn write_frame(writer: &mut transport::Writer, value: &Value) -> io::Result<()> {
    let mut body = Vec::new();
    rmpv::encode::write_value(&mut body, value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await
}

fn decode(body: &[u8]) -> Option<Vec<Value>> {
    match rmpv::decode::read_value(&mut &body[..]).ok()? {
        Value::Array(items) if !items.is_empty() => Some(items),
        _ => None,
    }
}

// ------------------------------------------------------------------ state

struct Job {
    task_id: u64,
    name: String,
    payload: Vec<u8>,
    /// Attempt number as the broker counts it, not as we count it.
    attempt: u32,
    /// The producer's id for this job, and the key its result is filed under.
    id: String,
    /// What settles this delivery with whichever broker produced it.
    receipt: Receipt,
    /// Set when the Dispatch frame goes out, so the histogram measures what a
    /// caller would call the task's duration rather than the handler's.
    dispatched: Instant,
    /// When the broker says this became runnable. Expiry is measured from here.
    ready_at_ms: u64,
    /// Steps to run after this one, msgpack, empty for a lone job.
    chain: Vec<u8>,
}

/// What the handler asked for, on top of having failed.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
enum Directive {
    /// Apply the task's own retry policy.
    #[default]
    Policy,
    /// Skip the remaining attempts: this will not start working.
    Reject,
    /// Hand it back after this many milliseconds, still charged an attempt.
    RetryAfter(u32),
}

impl Directive {
    fn read(items: &[Value]) -> Directive {
        match items.get(4).and_then(|v| v.as_str()) {
            Some("reject") => Directive::Reject,
            Some("retry") => {
                Directive::RetryAfter(items.get(5).and_then(|v| v.as_u64()).unwrap_or(0) as u32)
            }
            _ => Directive::Policy,
        }
    }
}

#[derive(Clone)]
struct Outcome {
    ok: bool,
    result: Vec<u8>,
    error_type: String,
    traceback: String,
    directive: Directive,
}

impl Outcome {
    fn nack(error_type: &str, traceback: String) -> Self {
        Outcome {
            ok: false,
            result: Vec::new(),
            error_type: error_type.into(),
            traceback,
            directive: Directive::Policy,
        }
    }
}

struct Cfg {
    app_spec: String,
    python: String,
    socket: String,
    max_rss: u64,
    max_tasks: u64,
    max_lifetime: Option<Duration>,
    poll: Duration,
    drain_timeout: Duration,
    term_grace: Duration,
    connect_timeout: Duration,
    spawn_cap: u64,
    /// Start the replacement once the trigger is this many spawn-durations away.
    warm_multiple: u32,
    /// Retire a pre-warmed child that was never needed after this long.
    spare_idle: Duration,
    /// host:port for the Prometheus endpoint, when one is wanted.
    metrics_addr: Option<String>,
    /// Kill a child that reaches this, mid-task, rather than let it keep
    /// growing. Zero disables it, which is the default: the soft ceiling never
    /// loses work, and this one trades a task to protect the box.
    hard_max_rss: u64,
    /// Slack added to a job's own timeout before its lease counts as dead.
    lease_grace: Duration,
    /// How long a claim may wait for work before returning empty-handed.
    claim_block: Duration,
    /// Tasks a single child may have in flight at once.
    ///
    /// One is the default and the reason the ceiling is precise: a child with
    /// nothing running cannot grow, so RSS read at a dispatch decision bounds
    /// overshoot to a single task's peak. Raising this trades that precision
    /// for concurrency, and is worth it only when handlers wait rather than
    /// allocate — see the io table in bench/README.md.
    slots: usize,
}

#[derive(Default)]
struct Counters {
    rejected: AtomicU64,
    spawns: AtomicU64,
    recycles: AtomicU64,
    prewarmed: AtomicU64,
    wasted_spares: AtomicU64,
    crashes: AtomicU64,
    kills: AtomicU64,
    hard_killed: AtomicU64,
    retried: AtomicU64,
    rate_limited: AtomicU64,
    expired: AtomicU64,
    chained: AtomicU64,
    at_capacity: AtomicU64,
    /// Largest child RSS this supervisor ever read, in bytes.
    ///
    /// A counter rather than a gauge because it only ever climbs, and it
    /// answers the question a failed ceiling asks first: was the limit never
    /// crossed, or crossed and not acted on? Those want different fixes, and
    /// on a platform the author cannot run, the difference has to arrive in
    /// the failure message.
    child_rss_peak: AtomicU64,
    dead_lettered: AtomicU64,
    cron_fired: AtomicU64,
    broker_errors: AtomicU64,
}

/// Hands an accepted, registered connection to the slot that spawned it.
/// None means the child was rejected and the slot should stop waiting.
type Handoff = oneshot::Sender<Option<(transport::Reader, transport::Writer)>>;

struct Shared {
    broker: Broker,
    /// Only populated in batch mode. A worker draining a real broker runs for
    /// weeks; keeping every outcome in a map would be a leak with a schedule.
    results: Mutex<HashMap<u64, Outcome>>,
    total: usize,
    next_task_id: AtomicU64,
    work: Notify,
    done: AtomicBool,
    registry: Mutex<Option<(u64, usize)>>, // (hash, task count) from the first child
    /// Retry policy per task, learned from Register. The supervisor cannot read
    /// the user's decorators (spec §4.1), so this is the only way it knows.
    specs: Mutex<HashMap<String, Spec>>,
    conns: Mutex<HashMap<u64, Handoff>>,
    exits: Mutex<Vec<i32>>,
    /// Set when the run cannot proceed at all, as opposed to a task failing.
    fatal: Mutex<Option<String>>,
    next_child_id: AtomicU64,
    /// Job ids cancelled by a caller, refreshed from the broker on a timer.
    ///
    /// Checked where the ceiling is checked — at the dispatch decision — so a
    /// cancellation costs nothing per task and takes effect within one refresh.
    revoked: Mutex<std::collections::HashSet<String>>,
    /// Observed cost of bringing a child up, in milliseconds. Pre-warming needs
    /// to know this: how early to start is a question about wall-clock, and a
    /// percentage of a limit answers a different question.
    spawn_ms: AtomicU64,
    counters: Counters,
    metrics: Metrics,
    /// Which trigger fired, per recycle — the leaky-handler demo needs to show
    /// max_rss firing, not just that *something* recycled.
    reasons: Mutex<HashMap<&'static str, u64>>,
}

impl Shared {
    fn spawn_estimate(&self) -> Duration {
        Duration::from_millis(self.spawn_ms.load(Ordering::Relaxed))
    }

    fn batch(&self) -> bool {
        self.broker.drains_when_empty()
    }

    /// Abandon the run with a diagnosis. Marking it done unblocks every parked
    /// slot, so the supervisor unwinds instead of retrying its way to a cap.
    fn fail(&self, message: String) {
        let mut fatal = self.fatal.lock().unwrap();
        if fatal.is_none() {
            *fatal = Some(message);
        }
        drop(fatal);
        self.done.store(true, Ordering::SeqCst);
        self.work.notify_waiters();
    }

    fn record(&self, task_id: u64, outcome: Outcome) {
        if !self.batch() {
            return;
        }
        let mut results = self.results.lock().unwrap();
        results.entry(task_id).or_insert(outcome); // at-least-once: first wins
        let settled = results.len();
        drop(results);
        if settled >= self.total {
            self.done.store(true, Ordering::SeqCst);
        }
        self.work.notify_waiters();
    }

    /// The child answered. Success settles; failure enters the retry machine.
    /// File the answer, if this task asked for one to be kept.
    async fn keep_result(&self, job: &Job, outcome: &Outcome) {
        if job.id.is_empty() {
            return;
        }
        let ttl = self
            .specs
            .lock()
            .unwrap()
            .get(&job.name)
            .map(|s| s.result_ttl_ms)
            .unwrap_or(0);
        if ttl == 0 {
            return;
        }
        // Failures are stored too. Without them a caller waiting on a task that
        // raised has nothing to wait for and learns about it by timing out.
        let envelope = Value::Array(vec![
            Value::Boolean(outcome.ok),
            Value::Binary(outcome.result.clone()),
            Value::from(outcome.error_type.as_str()),
            Value::from(outcome.traceback.as_str()),
        ]);
        let mut blob = Vec::new();
        if rmpv::encode::write_value(&mut blob, &envelope).is_err() {
            return;
        }
        if self
            .broker
            .store_result(&job.id, blob, Duration::from_millis(ttl))
            .await
            .is_err()
        {
            self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn settle(&self, job: Job, outcome: Outcome) {
        if outcome.ok {
            self.free_slot(&job).await;
            self.metrics
                .observe(&job.name, true, job.dispatched.elapsed());
            self.keep_result(&job, &outcome).await;
            // Before the ack, so a crash between the two redelivers this step
            // rather than losing the rest of the chain. That can run a step
            // twice, which is the at-least-once this queue already promises.
            self.advance_chain(&job, &outcome).await;
            if self.broker.ack(&job.receipt).await.is_err() {
                self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
            }
            self.record(job.task_id, outcome);
            return;
        }
        self.metrics
            .observe(&job.name, false, job.dispatched.elapsed());
        self.fail_job(job, outcome).await;
    }

    /// Hand back a concurrency slot, if this task holds any.
    async fn free_slot(&self, job: &Job) {
        let capped = self
            .specs
            .lock()
            .unwrap()
            .get(&job.name)
            .is_some_and(|s| s.max_concurrency > 0);
        if capped {
            let _ = self.broker.release_slot(&job.name, &job.id).await;
        }
    }

    /// Queue the next step of a chain, if this job was carrying one.
    ///
    /// Every step's id was chosen by the client before any of them ran, so a
    /// caller can hold a handle to the last one's result while the first is
    /// still being written. The supervisor only moves data: it never has to
    /// know what any of these names mean (spec §4.1).
    async fn advance_chain(&self, job: &Job, outcome: &Outcome) {
        if job.chain.is_empty() {
            return;
        }
        let Ok(Value::Array(steps)) = rmpv::decode::read_value(&mut job.chain.as_slice()) else {
            return;
        };
        let Some((next, rest)) = steps.split_first() else {
            return;
        };
        let Value::Array(f) = next else { return };
        if f.len() < 6 {
            return;
        }
        let (id, name, payload, timeout_ms, queue, feed) = (
            f[0].as_str().unwrap_or_default().to_string(),
            f[1].as_str().unwrap_or_default().to_string(),
            f[2].as_slice().unwrap_or_default().to_vec(),
            f[3].as_u64().unwrap_or(0) as u32,
            f[4].as_str().unwrap_or("default").to_string(),
            f[5].as_bool().unwrap_or(true),
        );
        let payload = if feed {
            prepend_result(&payload, &outcome.result)
        } else {
            payload
        };
        let mut tail = Vec::new();
        if !rest.is_empty() {
            let _ = rmpv::encode::write_value(&mut tail, &Value::Array(rest.to_vec()));
        }
        let next_job = NewJob {
            id,
            queue,
            name,
            payload,
            timeout_ms,
            chain: tail,
        };
        if self.broker.push(next_job, Duration::ZERO).await.is_err() {
            self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.chained.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A child died holding this job (spec §4.4) — same policy as any other
    /// failure. A crash is not a special kind of failure, it is just one the
    /// handler never got to report.
    async fn give_back(&self, job: Job) {
        let message = format!("{} did not survive its child", job.name);
        self.fail_job(job, Outcome::nack("ChildDied", message))
            .await;
    }

    /// Publish how far a running task has got, under the same expiry as its
    /// result. A progress record that outlives interest in the result is a leak
    /// that reports on itself.
    async fn keep_progress(&self, job: &Job, blob: Vec<u8>) {
        if job.id.is_empty() {
            return;
        }
        let ttl = self
            .specs
            .lock()
            .unwrap()
            .get(&job.name)
            .map(|s| s.result_ttl_ms)
            .unwrap_or(0);
        if ttl == 0 {
            return;
        }
        if self
            .broker
            .store_result(
                &format!("progress:{}", job.id),
                blob,
                Duration::from_millis(ttl),
            )
            .await
            .is_err()
        {
            self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Retry with backoff while attempts remain, then dead-letter.
    ///
    /// `attempt` is what the broker counted, not what this process remembers,
    /// so a job that has been passed between workers still runs out of retries.
    async fn fail_job(&self, job: Job, outcome: Outcome) {
        // Every way out of "running" that is not a success arrives here —
        // including a child that died holding the job and a hard-ceiling kill,
        // neither of which goes through settle. A slot left behind throttles
        // the task until its lease expires, which reads as a task that
        // mysteriously stopped running.
        self.free_slot(&job).await;
        let spec = self.specs.lock().unwrap().get(&job.name).cloned();
        let retries = spec.as_ref().map(|s| s.retries).unwrap_or(0);
        // A rejection skips whatever attempts are left: the handler is saying
        // more of them would reach the same answer more slowly.
        let rejected = outcome.directive == Directive::Reject;
        if !rejected && job.attempt <= retries {
            let delay = match outcome.directive {
                Directive::RetryAfter(ms) => Duration::from_millis(u64::from(ms)),
                _ => backoff_for(spec.as_ref(), job.attempt, &job.id),
            };
            self.counters.retried.fetch_add(1, Ordering::Relaxed);
            if self.broker.retry(&job.receipt, delay).await.is_err() {
                self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
            }
            self.work.notify_waiters();
            return;
        }
        self.counters.dead_lettered.fetch_add(1, Ordering::Relaxed);
        self.keep_result(&job, &outcome).await;
        if self
            .broker
            .dead_letter(&job.receipt, &outcome.error_type, &outcome.traceback)
            .await
            .is_err()
        {
            self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.record(job.task_id, outcome);
    }

    /// Take the next job, or None once there is nothing left to wait for.
    ///
    /// Batch mode never reports "no work" while a job is unaccounted for — a
    /// crash redelivery can land after the queue drains, and a child that left
    /// on an early Drain is not there to run it. Against a real broker there is
    /// no such thing as finished: only a shutdown ends the wait.
    /// One claim attempt. Never parks, so it is safe to call from a frame loop
    /// that still owes itself the reading of an Ack.
    async fn try_job(&self, lease: Duration) -> Option<Job> {
        match self.broker.claim(lease, Duration::from_millis(0)).await {
            Ok(Some(delivery)) => Some(self.adopt(delivery)),
            Ok(None) => None,
            Err(_) => {
                self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    async fn next_job(&self, lease: Duration, block: Duration) -> Option<Job> {
        loop {
            match self.broker.claim(lease, block).await {
                Ok(Some(delivery)) => return Some(self.adopt(delivery)),
                Ok(None) => {}
                Err(_) => {
                    self.counters.broker_errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            if self.done.load(Ordering::SeqCst) {
                return None;
            }
            if self.batch() {
                // Nothing blocks in memory, so wait for a redelivery or for the
                // last outstanding result to land.
                let notified = self.work.notified();
                if self.done.load(Ordering::SeqCst) {
                    return None;
                }
                tokio::select! {
                    _ = notified => {}
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                }
            }
        }
    }

    fn adopt(&self, delivery: Delivery) -> Job {
        // In batch mode the memory broker's own id is the job's identity and
        // survives redelivery, so results stay keyed to the job rather than to
        // the attempt. Real brokers record nothing, so a counter is enough.
        let task_id = match &delivery.receipt {
            Receipt::Memory { id } => *id,
            _ => self.next_task_id.fetch_add(1, Ordering::SeqCst),
        };
        Job {
            task_id,
            id: delivery.id,
            name: delivery.name,
            payload: delivery.payload,
            attempt: delivery.attempt,
            receipt: delivery.receipt,
            dispatched: Instant::now(),
            ready_at_ms: delivery.ready_at_ms,
            chain: delivery.chain,
        }
    }
}

// ------------------------------------------------------------- child slots

struct ChildHandle {
    pid: u32,
    proc: tokio::process::Child,
    frames: mpsc::Receiver<Vec<u8>>,
    writer: transport::Writer,
    started: Instant,
    tasks_done: u64,
    inflight: HashMap<u64, Job>,
}

#[derive(Clone)]
struct Spec {
    timeout_ms: u64,
    retries: u32,
    backoff: String,
    /// Zero means the answer is thrown away, which is the default. A result
    /// store with no expiry is a leak that moved from the worker to the broker.
    result_ttl_ms: u64,
    queue: String,
    /// Five-field cron, empty when the task is only ever sent by hand.
    cron: String,
    /// Tokens per second and bucket depth. Zero rate means no limit, and no
    /// broker round trip: the cost is paid only by tasks that asked for one.
    rate_per_sec: f64,
    rate_burst: u32,
    /// Milliseconds a job may wait after becoming runnable. Zero is never.
    expires_ms: u64,
    /// How many of this task may be in progress at once. Zero is unlimited.
    max_concurrency: u32,
}

/// Wait before the next attempt. Exponential from one second, capped so a
/// backoff cannot outrun what a broker can express (see the Redis driver).
///
/// Jittered into the top half of the window, keyed off the job's own id. A
/// dependency that came back after a blip would otherwise be hit by every
/// retry it caused, at the same instant, in step — the outage's own echo.
fn backoff_for(spec: Option<&Spec>, attempt: u32, seed: &str) -> Duration {
    let base = Duration::from_secs(1);
    let full = match spec.map(|s| s.backoff.as_str()) {
        Some("none") => return Duration::ZERO,
        Some("fixed") => base,
        _ => base * 2u32.pow(attempt.saturating_sub(1).min(6)),
    };
    // FNV-1a over the id: no rand crate, and deterministic per job, which is
    // all that spreading requires.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in seed.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let fraction = (hash >> 11) as f64 / (1u64 << 53) as f64; // [0, 1)
    full.mul_f64(0.5 + fraction * 0.5)
}

/// A scheduled task takes no arguments: msgpack for `([], {})`, the same shape
/// the producer sends.
/// Put the previous step's result in front of the next step's arguments.
///
/// A payload is `[args, kwargs]`. Splicing rather than replacing is what lets
/// `parse.s("utf-8")` receive both what it was given and what came before it.
fn prepend_result(payload: &[u8], result: &[u8]) -> Vec<u8> {
    let mut cursor = payload;
    let Ok(Value::Array(parts)) = rmpv::decode::read_value(&mut cursor) else {
        return payload.to_vec();
    };
    if parts.len() != 2 {
        return payload.to_vec();
    }
    let Value::Array(args) = &parts[0] else {
        return payload.to_vec();
    };
    let feed = rmpv::decode::read_value(&mut &result[..]).unwrap_or(Value::Nil);
    let mut merged = vec![feed];
    merged.extend(args.iter().cloned());
    let mut out = Vec::new();
    let _ = rmpv::encode::write_value(
        &mut out,
        &Value::Array(vec![Value::Array(merged), parts[1].clone()]),
    );
    out
}

/// Ask a child to stop, politely, where the platform has a way to.
///
/// The polite step is SIGTERM, and Windows has no equivalent — it can only
/// terminate. That is less of a loss than it sounds: the supervisor has already
/// sent a Drain frame and waited, so this is the second escalation rather than
/// the first, and the third is the same on both.
fn ask_to_stop(pid: u32) {
    #[cfg(unix)]
    if pid != 0 {
        unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    }
    #[cfg(windows)]
    let _ = pid;
}

/// End a child immediately, for the hard ceiling.
fn stop_now(child: &mut ChildHandle) {
    #[cfg(unix)]
    if child.pid != 0 {
        unsafe { libc::kill(child.pid as i32, libc::SIGKILL) };
    }
    #[cfg(windows)]
    {
        // start_kill rather than kill().await: this is called from a path that
        // reaps immediately afterwards and will collect the status there.
        let _ = child.proc.start_kill();
    }
}

fn no_arguments() -> Vec<u8> {
    let mut out = Vec::new();
    let empty = Value::Array(vec![Value::Array(Vec::new()), Value::Map(Vec::new())]);
    let _ = rmpv::encode::write_value(&mut out, &empty);
    out
}

/// One definition of every counter, shared by the Python return value and the
/// Prometheus text so the two can never drift.
fn counter_list(counters: &Counters) -> Vec<(&'static str, u64)> {
    let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
    vec![
        ("children_rejected", load(&counters.rejected)),
        ("children_spawned", load(&counters.spawns)),
        ("children_recycled", load(&counters.recycles)),
        ("children_recycled_prewarmed", load(&counters.prewarmed)),
        ("spares_wasted", load(&counters.wasted_spares)),
        ("children_crashed", load(&counters.crashes)),
        ("children_killed", load(&counters.kills)),
        ("children_hard_killed", load(&counters.hard_killed)),
        ("task_retries", load(&counters.retried)),
        ("tasks_rate_limited", load(&counters.rate_limited)),
        ("tasks_expired", load(&counters.expired)),
        ("chain_steps_queued", load(&counters.chained)),
        ("tasks_at_capacity", load(&counters.at_capacity)),
        ("child_rss_peak", load(&counters.child_rss_peak)),
        ("tasks_dead_lettered", load(&counters.dead_lettered)),
        ("cron_fired", load(&counters.cron_fired)),
        ("broker_errors", load(&counters.broker_errors)),
    ]
}

enum Exit {
    Finished, // we sent Drain because the work is done
    Recycle(&'static str),
    /// Past the hard ceiling with work in flight. The only case where tarsk
    /// kills a running task, and it is opt-in for exactly that reason.
    OverHardLimit,
    Died,
}

async fn accept_loop(listener: transport::Listener, shared: Arc<Shared>) {
    loop {
        let Ok((reader, writer)) = listener.accept().await else {
            return;
        };
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut reader = reader;
            let Ok(Some(body)) = read_frame(&mut reader).await else {
                return;
            };
            let Some(items) = decode(&body) else { return };
            if items[0].as_str() != Some("Register") || items.len() != 4 {
                return;
            }
            let (Some(child_id), Some(hash)) = (items[1].as_u64(), items[2].as_u64()) else {
                return;
            };
            let Value::Array(rows) = &items[3] else {
                return;
            };
            let count = rows.len();
            let specs: HashMap<String, Spec> = rows
                .iter()
                .filter_map(|row| match row {
                    Value::Array(fields) if fields.len() >= 5 => Some((
                        fields[0].as_str()?.to_string(),
                        Spec {
                            timeout_ms: fields[1].as_u64().unwrap_or(0),
                            retries: fields[2].as_u64().unwrap_or(0) as u32,
                            backoff: fields[3].as_str().unwrap_or("exp").to_string(),
                            queue: fields[4].as_str().unwrap_or("default").to_string(),
                            result_ttl_ms: fields.get(5).and_then(|v| v.as_u64()).unwrap_or(0),
                            cron: fields
                                .get(6)
                                .and_then(|v| v.as_str())
                                .unwrap_or("")
                                .to_string(),
                            rate_per_sec: fields.get(7).and_then(|v| v.as_f64()).unwrap_or(0.0),
                            rate_burst: fields.get(8).and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                            expires_ms: fields.get(9).and_then(|v| v.as_u64()).unwrap_or(0),
                            max_concurrency: fields.get(10).and_then(|v| v.as_u64()).unwrap_or(0)
                                as u32,
                        },
                    )),
                    _ => None,
                })
                .collect();

            let mut registry = shared.registry.lock().unwrap();
            let accepted = match *registry {
                None => {
                    *registry = Some((hash, count));
                    if let Some(shortest) = specs
                        .values()
                        .map(|s| s.timeout_ms)
                        .filter(|ms| *ms > 0)
                        .min()
                    {
                        shared.broker.observe_min_timeout(shortest);
                    }
                    *shared.specs.lock().unwrap() = specs;
                    true
                }
                // Stale child from a mid-rollout code change (spec §4.2).
                Some((known, _)) => known == hash,
            };
            drop(registry);

            let waiting = shared.conns.lock().unwrap().remove(&child_id);
            if !accepted {
                shared.counters.rejected.fetch_add(1, Ordering::Relaxed);
                if let Some(tx) = waiting {
                    let _ = tx.send(None);
                }
                return;
            }
            if let Some(tx) = waiting {
                let _ = tx.send(Some((reader, writer)));
            }
        });
    }
}

async fn spawn_child(shared: &Arc<Shared>, cfg: &Cfg) -> Option<ChildHandle> {
    if shared.counters.spawns.fetch_add(1, Ordering::SeqCst) >= cfg.spawn_cap {
        return None; // children are dying on startup; stop feeding the fire
    }
    let launched = Instant::now();
    let child_id = shared.next_child_id.fetch_add(1, Ordering::SeqCst);
    let (tx, rx) = oneshot::channel();
    shared.conns.lock().unwrap().insert(child_id, tx);

    let mut proc = tokio::process::Command::new(&cfg.python)
        .arg("-m")
        .arg("tarsk._child")
        .arg(&cfg.socket)
        .arg(&cfg.app_spec)
        .arg(child_id.to_string())
        .arg(cfg.slots.to_string())
        .kill_on_drop(true)
        .spawn()
        .ok()?;
    let pid = proc.id().unwrap_or(0);

    // Watch the process as well as the socket. A child that dies on the way up
    // — a bad app module, a start hook that raised — would otherwise cost the
    // full connect timeout before anyone noticed, per attempt.
    let halves = tokio::select! {
        connected = tokio::time::timeout(cfg.connect_timeout, rx) => match connected {
            Ok(Ok(Some(halves))) => Some(halves),
            _ => None,
        },
        status = proc.wait() => {
            let code = status.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
            shared.exits.lock().unwrap().push(code);
            if code == EXIT_STARTUP_FAILED {
                shared.fail(format!(
                    "a worker's on_start hook raised; its traceback is on the \
                     worker's stderr (exit {code})"
                ));
            }
            None
        }
    };
    let Some(halves) = halves else {
        shared.conns.lock().unwrap().remove(&child_id);
        let _ = proc.kill().await;
        return None;
    };
    let (mut reader, writer) = halves;

    // Frames arrive through a channel so the serve loop can select! on them:
    // read_exact is not cancel-safe, mpsc::Receiver::recv is.
    let (frame_tx, frames) = mpsc::channel(8);
    tokio::spawn(async move {
        while let Ok(Some(body)) = read_frame(&mut reader).await {
            if frame_tx.send(body).await.is_err() {
                return;
            }
        }
    });

    // A child whose imports alone clear the ceiling can never run a task: it
    // would register, be recycled on its first Ready, and hand the same problem
    // to its replacement forever. That is a misconfiguration, not a leak, and it
    // deserves a diagnosis rather than a spawn loop.
    if cfg.max_rss > 0 && pid != 0 {
        let mut sys = System::new();
        let key = Pid::from_u32(pid);
        sys.refresh_processes(ProcessesToUpdate::Some(&[key]), false);
        if let Some(baseline) = sys.process(key).map(|p| p.memory()) {
            if baseline >= cfg.max_rss {
                shared.fail(format!(
                    "child baseline is {}MB but max_rss is {}MB: importing {} already \
                     clears the ceiling before any task runs. Raise the ceiling above the \
                     interpreter plus your task modules.",
                    baseline / (1024 * 1024),
                    cfg.max_rss / (1024 * 1024),
                    cfg.app_spec,
                ));
                return None;
            }
        }
    }

    shared.spawn_ms.store(
        launched.elapsed().as_millis().max(1) as u64,
        Ordering::Relaxed,
    );
    Some(ChildHandle {
        pid,
        proc,
        frames,
        writer,
        started: Instant::now(),
        tasks_done: 0,
        inflight: HashMap::new(),
    })
}

async fn drain_owned(shared: Arc<Shared>, child: ChildHandle, cfg: Arc<Cfg>) {
    drain(&shared, child, &cfg).await;
}

/// How close this child is to the nearest of its recycle triggers.
struct Pressure {
    trigger: Option<&'static str>,
    /// Fraction of the closest limit already consumed; 0.0 when unlimited.
    ratio: f64,
    /// Whatever the RSS read cost us, handed on rather than taken twice.
    rss: Option<u64>,
}

fn pressure(sys: &mut System, pid: u32, tasks_done: u64, started: Instant, cfg: &Cfg) -> Pressure {
    let mut worst = (0.0f64, "");
    let mut rss = None;
    if cfg.max_tasks > 0 {
        worst = (tasks_done as f64 / cfg.max_tasks as f64, "max_tasks");
    }
    if let Some(limit) = cfg.max_lifetime {
        let ratio = started.elapsed().as_secs_f64() / limit.as_secs_f64();
        if ratio > worst.0 {
            worst = (ratio, "max_lifetime");
        }
    }
    if pid != 0 {
        // RSS is read by the parent (spec §4.4) — a thrashing child is the
        // least reliable reporter of its own state.
        let pid = Pid::from_u32(pid);
        sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), false);
        if let Some(proc) = sys.process(pid) {
            let bytes = proc.memory();
            rss = Some(bytes);
            if cfg.max_rss > 0 {
                let ratio = bytes as f64 / cfg.max_rss as f64;
                if ratio > worst.0 {
                    worst = (ratio, "max_rss");
                }
            }
        }
    }
    Pressure {
        trigger: (worst.0 >= 1.0).then_some(worst.1),
        ratio: worst.0,
        rss,
    }
}

/// Should the replacement start now?
///
/// Project when the trigger will fire from how far this child has already
/// travelled towards it, and start the replacement once that is only a few
/// spawns away. A fixed percentage cannot do this job: 10% of a child that
/// lives 100ms is nowhere near an interpreter startup, while 10% of one that
/// lives an hour leaves a spare interpreter idling for six minutes.
fn should_warm(pressure: &Pressure, elapsed: Duration, spawn: Duration, cfg: &Cfg) -> bool {
    if pressure.ratio <= 0.0 {
        return false;
    }
    let remaining = elapsed.as_secs_f64() * (1.0 - pressure.ratio) / pressure.ratio;
    remaining <= spawn.as_secs_f64() * cfg.warm_multiple as f64
}

/// What filling this child's free slots achieved.
enum Fill {
    Sent,
    Empty,
    Dead,
    /// The run is shutting down and nothing is outstanding.
    Done,
}

/// What `serve` handed back: why it stopped, and a replacement if one was
/// started early enough to be ready.
struct Served {
    exit: Exit,
    spare: Option<ChildHandle>,
}

/// Handle one child until it needs replacing, dies, or the work runs out.
async fn serve(shared: &Arc<Shared>, child: &mut ChildHandle, cfg: &Arc<Cfg>) -> Served {
    let ChildHandle {
        pid,
        frames,
        writer,
        started,
        tasks_done,
        inflight,
        ..
    } = child;
    let mut sys = System::new();
    let mut ticker = tokio::time::interval(cfg.poll);
    // Carries Option so a failed spawn reports back instead of leaving a
    // waiter hanging on a channel that will never produce.
    let (spare_tx, mut spare_rx) = mpsc::channel::<Option<ChildHandle>>(1);
    let mut spare: Option<ChildHandle> = None;
    let mut spare_at: Option<Instant> = None;
    let mut arming = false;
    // Slots this child has advertised with a Ready and we have not filled.
    let mut free: usize = 0;

    // Read the pressure, start the replacement if the trigger is close, and
    // report the trigger if it has already fired.
    macro_rules! check {
        () => {{
            let reading = pressure(&mut sys, *pid, *tasks_done, *started, cfg);
            if let Some(bytes) = reading.rss {
                shared.metrics.set_child_rss(*pid, bytes);
                shared
                    .counters
                    .child_rss_peak
                    .fetch_max(bytes, Ordering::Relaxed);
            }
            if spare.is_none()
                && !arming
                && reading.trigger.is_none()
                && should_warm(&reading, started.elapsed(), shared.spawn_estimate(), cfg)
            {
                arming = true;
                let (shared, cfg, tx) = (shared.clone(), cfg.clone(), spare_tx.clone());
                tokio::spawn(async move {
                    let _ = tx.send(spawn_child(&shared, &cfg).await).await;
                });
            }
            reading
        }};
    }

    // Parking inside the frame loop is only safe while this child has nothing
    // outstanding: with more than one slot, an Ack that would end the batch may
    // be sitting unread in the very channel the park is keeping us out of.
    // Empty child, park (that is the original behaviour and the shutdown path);
    // busy child, take what is there and leave the slot free otherwise.
    macro_rules! fill {
        () => {{
            let mut outcome = Fill::Empty;
            while free > 0 {
                let job = if inflight.is_empty() {
                    match shared.next_job(cfg.lease_grace, cfg.claim_block).await {
                        Some(job) => job,
                        None => {
                            outcome = Fill::Done;
                            break;
                        }
                    }
                } else {
                    match shared.try_job(cfg.lease_grace).await {
                        Some(job) => job,
                        None => break,
                    }
                };
                // Too long in the queue to be worth running. Checked here
                // rather than at enqueue, because the question is how long it
                // waited, and that is only answerable at the moment something
                // would have started it — including on a retry, since stale
                // work is still stale the second time.
                let expires_ms = shared
                    .specs
                    .lock()
                    .unwrap()
                    .get(&job.name)
                    .map(|s| s.expires_ms)
                    .unwrap_or(0);
                if expires_ms > 0 && job.ready_at_ms > 0 {
                    let waited = broker::now_ms().saturating_sub(job.ready_at_ms);
                    if waited > expires_ms {
                        shared.counters.expired.fetch_add(1, Ordering::Relaxed);
                        shared
                            .settle(
                                job,
                                Outcome::nack(
                                    "Expired",
                                    format!(
                                        "waited {}ms for a worker, past its expires of {}ms",
                                        waited, expires_ms
                                    ),
                                ),
                            )
                            .await;
                        continue;
                    }
                }
                // Over its rate: hand it back with the wait the bucket asked
                // for, rather than hold this slot idle. The delay is what stops
                // it being re-claimed immediately, and requeue is the same path
                // a retry takes.
                let limit = shared
                    .specs
                    .lock()
                    .unwrap()
                    .get(&job.name)
                    .map(|s| (s.rate_per_sec, s.rate_burst))
                    .filter(|(rate, _)| *rate > 0.0);
                if let Some((rate, burst)) = limit {
                    match shared.broker.take_token(&job.name, rate, burst).await {
                        Ok(0) => {}
                        Ok(wait_ms) => {
                            shared.counters.rate_limited.fetch_add(1, Ordering::Relaxed);
                            let _ = shared
                                .broker
                                .retry(&job.receipt, Duration::from_millis(wait_ms))
                                .await;
                            continue;
                        }
                        Err(_) => {
                            // Fail closed. A limit exists because exceeding it
                            // hurts something outside this process — a quota, a
                            // database, someone else's API. Dispatching when the
                            // bucket cannot be read trades a delay the caller
                            // asked for against a breach they did not.
                            shared
                                .counters
                                .broker_errors
                                .fetch_add(1, Ordering::Relaxed);
                            let _ = shared
                                .broker
                                .retry(&job.receipt, Duration::from_millis(1000))
                                .await;
                            continue;
                        }
                    }
                }
                // Too many of this task already in progress. Checked after the
                // rate limit so a job refused for one reason is not also
                // holding a slot for the other.
                let (cap, lease_ms) = shared
                    .specs
                    .lock()
                    .unwrap()
                    .get(&job.name)
                    .map(|s| (s.max_concurrency, s.timeout_ms))
                    .unwrap_or((0, 0));
                if cap > 0 {
                    let lease = lease_ms + cfg.lease_grace.as_millis() as u64;
                    match shared
                        .broker
                        .acquire_slot(&job.name, &job.id, cap, lease)
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            shared.counters.at_capacity.fetch_add(1, Ordering::Relaxed);
                            let _ = shared
                                .broker
                                .retry(&job.receipt, Duration::from_millis(250))
                                .await;
                            continue;
                        }
                        Err(_) => {
                            // Fail closed, for the same reason the rate limit
                            // does: the cap is protecting something outside.
                            shared
                                .counters
                                .broker_errors
                                .fetch_add(1, Ordering::Relaxed);
                            let _ = shared
                                .broker
                                .retry(&job.receipt, Duration::from_millis(1000))
                                .await;
                            continue;
                        }
                    }
                }
                if shared.revoked.lock().unwrap().contains(&job.id) {
                    // Settled, not run. The delivery still has to be acked or
                    // the lease would expire and hand it back for another go.
                    shared
                        .settle(
                            job,
                            Outcome::nack("Cancelled", "cancelled before it started".into()),
                        )
                        .await;
                    continue;
                }
                let dispatch = Value::Array(vec![
                    Value::from("Dispatch"),
                    Value::from(job.task_id),
                    Value::from(job.name.as_str()),
                    Value::Binary(job.payload.clone()),
                ]);
                inflight.insert(job.task_id, job);
                free -= 1;
                if write_frame(writer, &dispatch).await.is_err() {
                    outcome = Fill::Dead;
                    break;
                }
                outcome = Fill::Sent;
            }
            outcome
        }};
    }

    loop {
        tokio::select! {
            Some(replacement) = spare_rx.recv() => {
                arming = false;
                if replacement.is_some() {
                    spare = replacement;
                    spare_at = Some(Instant::now());
                }
            }
            frame = frames.recv() => {
                let Some(body) = frame else { return Served { exit: Exit::Died, spare } };
                let Some(items) = decode(&body) else { return Served { exit: Exit::Died, spare } };
                match items[0].as_str() {
                    Some("Ready") => {
                        // Check the ceiling at the dispatch decision, not just on
                        // the timer: between two ticks a child can run many short
                        // tasks, and every one of them is unbudgeted growth.
                        //
                        // With one slot the child is idle here by definition, so
                        // overshoot is capped at one task's peak — the floor for
                        // any design that refuses to kill running work. With more
                        // slots it is capped at the peak of whatever is still in
                        // flight, which is the price of the concurrency and the
                        // reason slots defaults to one.
                        if let Some(reason) = check!().trigger {
                            if spare.is_none() && arming {
                                // A replacement is already starting. Waiting out
                                // the rest of its startup beats launching a second
                                // one from scratch — which costs a full spawn *and*
                                // makes both slower by competing for the machine.
                                spare = spare_rx.recv().await.flatten();
                            }
                            return Served { exit: Exit::Recycle(reason), spare };
                        }
                        free += 1;
                        match fill!() {
                            Fill::Sent | Fill::Empty => {}
                            Fill::Dead => return Served { exit: Exit::Died, spare },
                            Fill::Done => {
                                let _ = write_frame(writer, &Value::Array(vec![Value::from("Drain")])).await;
                                return Served { exit: Exit::Finished, spare };
                            }
                        }
                    }
                    Some("Ack") => {
                        let (Some(task_id), Some(result)) = (items[1].as_u64(), items[2].as_slice()) else {
                            return Served { exit: Exit::Died, spare };
                        };
                        if let Some(job) = inflight.remove(&task_id) {
                            let done = Outcome { ok: true, result: result.to_vec(), error_type: String::new(), traceback: String::new(), directive: Directive::Policy };
                            shared.settle(job, done).await;
                        }
                        *tasks_done += 1;
                    }
                    Some("Nack") => {
                        let (Some(task_id), Some(kind), Some(tb)) =
                            (items[1].as_u64(), items[2].as_str(), items[3].as_str()) else {
                            return Served { exit: Exit::Died, spare };
                        };
                        let directive = Directive::read(&items);
                        if let Some(job) = inflight.remove(&task_id) {
                            let mut outcome = Outcome::nack(kind, tb.to_string());
                            outcome.directive = directive;
                            shared.settle(job, outcome).await;
                        }
                        *tasks_done += 1;
                    }
                    Some("Progress") => {
                        // The child holds no broker connection by design, so
                        // the only way its progress reaches one is through here.
                        if let (Some(task_id), Some(blob)) =
                            (items[1].as_u64(), items[2].as_slice())
                        {
                            if let Some(job) = inflight.get(&task_id) {
                                shared.keep_progress(job, blob.to_vec()).await;
                            }
                        }
                    }
                    _ => return Served { exit: Exit::Died, spare },
                }
            }
            _ = ticker.tick() => {
                // The trigger levelled off below its limit and the replacement
                // we started is now just an idle interpreter. Let it go rather
                // than hold a whole process against a recycle that may be hours
                // away — the estimate will re-arm it when pressure returns.
                if let (Some(idle), true) = (spare_at, spare.is_some()) {
                    if idle.elapsed() > cfg.spare_idle {
                        let stale = spare.take().expect("checked");
                        spare_at = None;
                        shared.counters.wasted_spares.fetch_add(1, Ordering::Relaxed);
                        tokio::spawn(drain_owned(shared.clone(), stale, cfg.clone()));
                    }
                }
                if free > 0 && !inflight.is_empty() {
                    if let Fill::Dead = fill!() {
                        return Served { exit: Exit::Died, spare };
                    }
                }
                let reading = check!();
                // The hard ceiling is the one that does not wait for a good
                // moment. Draining an idle child gets there just as well and
                // costs nothing, so only work in flight is worth killing over.
                if cfg.hard_max_rss > 0 && reading.rss.is_some_and(|b| b >= cfg.hard_max_rss) {
                    if spare.is_none() && arming {
                        spare = spare_rx.recv().await.flatten();
                    }
                    let exit = if !inflight.is_empty() {
                        Exit::OverHardLimit
                    } else {
                        Exit::Recycle("hard_max_rss")
                    };
                    return Served { exit, spare };
                }
                if let Some(reason) = reading.trigger {
                    if spare.is_none() && arming {
                        spare = spare_rx.recv().await.flatten();
                    }
                    return Served { exit: Exit::Recycle(reason), spare };
                }
            }
        }
    }
}

/// Drain, never kill outright (spec §4.4): stop dispatching, let in-flight
/// work finish under a deadline, then escalate.
async fn drain(shared: &Arc<Shared>, mut child: ChildHandle, cfg: &Cfg) {
    let _ = write_frame(&mut child.writer, &Value::Array(vec![Value::from("Drain")])).await;

    let deadline = tokio::time::Instant::now() + cfg.drain_timeout;
    loop {
        tokio::select! {
            frame = child.frames.recv() => {
                let Some(body) = frame else { break };
                let Some(items) = decode(&body) else { break };
                match items[0].as_str() {
                    Some("Ack") => {
                        if let (Some(task_id), Some(result)) = (items[1].as_u64(), items[2].as_slice()) {
                            if let Some(job) = child.inflight.remove(&task_id) {
                                let done = Outcome { ok: true, result: result.to_vec(), error_type: String::new(), traceback: String::new(), directive: Directive::Policy };
                                shared.settle(job, done).await;
                            }
                        }
                    }
                    Some("Nack") => {
                        if let (Some(task_id), Some(kind), Some(tb)) = (items[1].as_u64(), items[2].as_str(), items[3].as_str()) {
                            let directive = Directive::read(&items);
                            if let Some(job) = child.inflight.remove(&task_id) {
                                let mut outcome = Outcome::nack(kind, tb.to_string());
                                outcome.directive = directive;
                                shared.settle(job, outcome).await;
                            }
                        }
                    }
                    _ => {} // a Ready from a child we are retiring is nothing to answer
                }
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }
    reap(shared, child, cfg).await;
}

async fn reap(shared: &Arc<Shared>, mut child: ChildHandle, cfg: &Cfg) {
    // Before any of the early returns below: a retired child that stays in the
    // gauge makes tarsk_child_rss_bytes_max monotone, which reads as a leak the
    // ceiling failed to stop — the exact opposite of what happened.
    shared.metrics.forget_child(child.pid);
    if let Ok(Ok(status)) = tokio::time::timeout(cfg.term_grace, child.proc.wait()).await {
        shared
            .exits
            .lock()
            .unwrap()
            .push(status.code().unwrap_or(-1));
        for (_, job) in std::mem::take(&mut child.inflight) {
            shared.give_back(job).await;
        }
        return;
    }
    // SIGTERM, grace, SIGKILL.
    shared.counters.kills.fetch_add(1, Ordering::Relaxed);
    ask_to_stop(child.pid);
    if tokio::time::timeout(cfg.term_grace, child.proc.wait())
        .await
        .is_err()
    {
        let _ = child.proc.kill().await;
    }
    if let Ok(status) = child.proc.wait().await {
        shared
            .exits
            .lock()
            .unwrap()
            .push(status.code().unwrap_or(-1));
    }
    for (_, job) in std::mem::take(&mut child.inflight) {
        shared.give_back(job).await;
    }
}

async fn slot(shared: Arc<Shared>, cfg: Arc<Cfg>) {
    let Some(mut current) = spawn_child(&shared, &cfg).await else {
        return;
    };
    loop {
        let Served { exit, spare } = serve(&shared, &mut current, &cfg).await;
        match exit {
            Exit::Finished => {
                reap(&shared, current, &cfg).await;
                if let Some(unused) = spare {
                    drain(&shared, unused, &cfg).await;
                }
                return;
            }
            Exit::OverHardLimit => {
                // Take the jobs before reaping, so the crash path does not also
                // claim them: this failure has a name worth putting in the DLQ.
                // With more than one slot the kill takes every task the child
                // held, not only the one that grew — they were sharing a
                // process, which is what a slot count buys and costs.
                let killed = std::mem::take(&mut current.inflight);
                shared.counters.hard_killed.fetch_add(1, Ordering::Relaxed);
                *shared
                    .reasons
                    .lock()
                    .unwrap()
                    .entry("hard_max_rss")
                    .or_insert(0) += 1;
                stop_now(&mut current);
                reap(&shared, current, &cfg).await;
                for (_, job) in killed {
                    let message = format!(
                        "{} was killed at the hard memory ceiling of {} MB",
                        job.name,
                        cfg.hard_max_rss / (1024 * 1024)
                    );
                    shared
                        .fail_job(job, Outcome::nack("HardMemoryLimit", message))
                        .await;
                }
                if shared.done.load(Ordering::SeqCst) {
                    if let Some(unused) = spare {
                        drain(&shared, unused, &cfg).await;
                    }
                    return;
                }
                current = match spare {
                    Some(next) => next,
                    None => match spawn_child(&shared, &cfg).await {
                        Some(next) => next,
                        None => return,
                    },
                };
            }
            Exit::Died => {
                shared.counters.crashes.fetch_add(1, Ordering::Relaxed);
                reap(&shared, current, &cfg).await;
                if shared.done.load(Ordering::SeqCst) {
                    if let Some(unused) = spare {
                        drain(&shared, unused, &cfg).await;
                    }
                    return;
                }
                current = match spare {
                    Some(next) => next,
                    None => match spawn_child(&shared, &cfg).await {
                        Some(next) => next,
                        None => return,
                    },
                };
            }
            Exit::Recycle(reason) => {
                shared.counters.recycles.fetch_add(1, Ordering::Relaxed);
                *shared.reasons.lock().unwrap().entry(reason).or_insert(0) += 1;
                // Overlap replacement (spec §4.4). If the replacement was warmed
                // in advance it is already registered, so the slot goes from old
                // child to new one with nothing in between. Falling back to a
                // spawn here means paying an interpreter startup with the slot
                // empty — correct, but the stall the pre-warm exists to avoid.
                let next = match spare {
                    Some(next) => {
                        shared.counters.prewarmed.fetch_add(1, Ordering::Relaxed);
                        Some(next)
                    }
                    None => spawn_child(&shared, &cfg).await,
                };
                match next {
                    Some(next) => {
                        // Retiring the old child is not on the critical path: it
                        // still has to finish its in-flight task and exit, and
                        // making the slot wait for that is the same empty slot
                        // the pre-warm just removed. Its Acks are still recorded
                        // — drain() completes them from wherever it runs.
                        tokio::spawn(drain_owned(shared.clone(), current, cfg.clone()));
                        current = next;
                    }
                    None => {
                        drain(&shared, current, &cfg).await;
                        return;
                    }
                }
            }
        }
    }
}

async fn supervise(
    broker: Broker,
    total: usize,
    children: usize,
    cfg: Cfg,
) -> io::Result<(
    Vec<(u64, Outcome)>,
    HashMap<String, u64>,
    Vec<i32>,
    Option<String>,
)> {
    broker.set_prefetch_cap(children);
    let batch = broker.drains_when_empty();
    let shared = Arc::new(Shared {
        broker,
        results: Mutex::new(HashMap::new()),
        total,
        next_task_id: AtomicU64::new(0),
        work: Notify::new(),
        done: AtomicBool::new(batch && total == 0),
        registry: Mutex::new(None),
        specs: Mutex::new(HashMap::new()),
        conns: Mutex::new(HashMap::new()),
        exits: Mutex::new(Vec::new()),
        fatal: Mutex::new(None),
        next_child_id: AtomicU64::new(0),
        revoked: Mutex::new(std::collections::HashSet::new()),
        spawn_ms: AtomicU64::new(250), // replaced by the first real measurement
        counters: Counters::default(),
        metrics: Metrics::default(),
        reasons: Mutex::new(HashMap::new()),
    });
    if batch && total == 0 {
        return Ok((Vec::new(), HashMap::new(), Vec::new(), None));
    }

    // Both platforms' details live in transport: a 0600 socket inside a 0700
    // directory here, an ACL'd named pipe there.
    let listener = transport::Listener::bind(&cfg.socket)?;
    let cfg = Arc::new(cfg);
    let acceptor = tokio::spawn(accept_loop(listener, shared.clone()));

    // A worker against a real broker has no natural end, so a signal is the
    // only thing that stops it — and it has to stop the way a recycle does,
    // draining children rather than dropping their work.
    let signals = (!batch).then(|| {
        let shared = shared.clone();
        tokio::spawn(async move {
            // Ctrl-C on both; SIGTERM as well where it exists, because that is
            // what an orchestrator sends when it stops a pod.
            #[cfg(unix)]
            {
                let mut term =
                    match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    {
                        Ok(term) => term,
                        Err(_) => return,
                    };
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            #[cfg(windows)]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            shared.done.store(true, Ordering::SeqCst);
            shared.work.notify_waiters();
        })
    });

    // Cancellations are pulled as a set on a timer, not asked about per job.
    // A second of latency on a cancel is nothing next to a broker round trip
    // on the dispatch path of every task, almost all of which are not
    // cancelled. Batch mode skips it: there is no one to cancel from.
    let revoker = (!batch).then(|| {
        let shared = shared.clone();
        tokio::spawn(async move {
            while !shared.done.load(Ordering::SeqCst) {
                match shared.broker.depth().await {
                    Ok(rows) => shared.metrics.set_depth(
                        rows.into_iter()
                            .map(|d| (d.queue, [d.ready, d.in_flight, d.delayed, d.dead]))
                            .collect(),
                    ),
                    Err(_) => {
                        shared
                            .counters
                            .broker_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                match shared.broker.revoked_all().await {
                    Ok(ids) => *shared.revoked.lock().unwrap() = ids.into_iter().collect(),
                    Err(_) => {
                        shared
                            .counters
                            .broker_errors
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    });

    // Only Redis needs a sweep: Postgres reclaims an expired lease inside the
    // same statement that claims a fresh job.
    let sweeper = (!batch).then(|| {
        let shared = shared.clone();
        let grace = cfg.lease_grace;
        tokio::spawn(async move {
            while !shared.done.load(Ordering::SeqCst) {
                if shared.broker.reclaim_expired(grace).await.is_err() {
                    shared
                        .counters
                        .broker_errors
                        .fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    });

    let exporter = cfg.metrics_addr.clone().map(|addr| {
        let shared = shared.clone();
        tokio::spawn(metrics::serve(addr, move || {
            let mut sys = System::new();
            let me = Pid::from_u32(std::process::id());
            sys.refresh_processes(ProcessesToUpdate::Some(&[me]), false);
            let own = sys.process(me).map(|p| p.memory()).unwrap_or(0);
            metrics::render(
                &shared.metrics,
                &counter_list(&shared.counters),
                &shared.reasons.lock().unwrap(),
                own,
            )
        }))
    });

    // Cron lives with the supervisor because that is the only process that has
    // the registry and no user code in it.
    let scheduler = (!batch).then(|| {
        let shared = shared.clone();
        tokio::spawn(async move {
            let mut fired: Option<i64> = None;
            while !shared.done.load(Ordering::SeqCst) {
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(0);
                let minute = now / 60;
                if fired != Some(minute) {
                    fired = Some(minute);
                    let due: Vec<(String, Spec)> = shared
                        .specs
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(_, spec)| !spec.cron.is_empty())
                        .filter_map(|(name, spec)| {
                            cron::parse(&spec.cron)
                                .ok()
                                .filter(|schedule| schedule.matches(minute * 60))
                                .map(|_| (name.clone(), spec.clone()))
                        })
                        .collect();
                    for (name, spec) in due {
                        match shared.broker.claim_tick(&name, minute).await {
                            Ok(true) => {}
                            Ok(false) => continue, // another worker has this minute
                            Err(_) => {
                                shared
                                    .counters
                                    .broker_errors
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        }
                        let job = NewJob {
                            id: format!("cron-{name}-{minute}"),
                            queue: spec.queue.clone(),
                            name: name.clone(),
                            payload: no_arguments(),
                            timeout_ms: spec.timeout_ms as u32,
                            chain: Vec::new(),
                        };
                        if shared.broker.push(job, Duration::ZERO).await.is_err() {
                            shared
                                .counters
                                .broker_errors
                                .fetch_add(1, Ordering::Relaxed);
                        } else {
                            shared.counters.cron_fired.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    });

    let slots: Vec<_> = (0..children)
        .map(|_| tokio::spawn(slot(shared.clone(), cfg.clone())))
        .collect();
    for handle in slots {
        let _ = handle.await;
    }
    acceptor.abort();
    if let Some(handle) = signals {
        handle.abort();
    }
    if let Some(handle) = sweeper {
        handle.abort();
    }
    if let Some(handle) = revoker {
        handle.abort();
    }
    if let Some(handle) = exporter {
        handle.abort();
    }
    if let Some(handle) = scheduler {
        handle.abort();
    }

    // A slot can give up (spawn cap, socket failure) with jobs outstanding.
    // Say so rather than hanging or reporting a clean run.
    if batch && !shared.done.load(Ordering::SeqCst) {
        let settled = shared.results.lock().unwrap().len();
        for task_id in 0..total as u64 {
            if shared.results.lock().unwrap().contains_key(&task_id) {
                continue;
            }
            shared.record(
                task_id,
                Outcome::nack("NoWorker", "every child slot gave up".into()),
            );
        }
        let _ = settled;
    }

    let registry = shared.registry.lock().unwrap();
    let mut stats: HashMap<String, u64> = counter_list(&shared.counters)
        .into_iter()
        .map(|(name, value)| (name.to_string(), value))
        .collect();
    stats.insert(
        "registry_hash".to_string(),
        registry.map(|r| r.0).unwrap_or(0),
    );
    stats.insert(
        "registry_len".to_string(),
        registry.map(|r| r.1 as u64).unwrap_or(0),
    );
    for (reason, count) in shared.reasons.lock().unwrap().iter() {
        stats.insert(format!("recycle_{reason}"), *count);
    }
    drop(registry);

    let mut outcomes: Vec<(u64, Outcome)> = shared
        .results
        .lock()
        .unwrap()
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    outcomes.sort_by_key(|(task_id, _)| *task_id);
    let exits = shared.exits.lock().unwrap().clone();
    let fatal = shared.fatal.lock().unwrap().clone();
    Ok((outcomes, stats, exits, fatal))
}

// ----------------------------------------------------------- python facing

fn io_err(err: Box<dyn std::error::Error + Send + Sync>) -> io::Error {
    io::Error::other(err.to_string())
}

/// Scratch directory for the child socket. Torn down with the run.
fn socket_dir() -> io::Result<std::path::PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("tarsk-{}-{}", std::process::id(), stamp));
    std::fs::create_dir_all(&dir)?;
    // 0700, or anyone with an account on the box can connect to the dispatch
    // socket, register as a child, and be handed other people's task payloads —
    // and have its Acks believed. A default umask leaves the directory
    // traversable, which is enough.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(dir)
}

#[allow(clippy::too_many_arguments)]
fn build_cfg(
    app_spec: String,
    python: String,
    socket: String,
    children: usize,
    max_rss: u64,
    max_tasks: u64,
    max_lifetime: f64,
    lease_grace: f64,
    metrics_addr: Option<String>,
    hard_max_rss: u64,
    spawn_slack: u64,
    slots: usize,
) -> Cfg {
    Cfg {
        app_spec,
        python,
        socket,
        max_rss,
        max_tasks,
        max_lifetime: (max_lifetime > 0.0).then(|| Duration::from_secs_f64(max_lifetime)),
        poll: Duration::from_millis(100),
        drain_timeout: Duration::from_secs(30),
        term_grace: Duration::from_secs(5),
        connect_timeout: Duration::from_secs(30),
        spawn_cap: 8 * children as u64 + spawn_slack + 8,
        warm_multiple: 3,
        spare_idle: Duration::from_secs(30),
        // Each job leases for its own timeout; this is only the slack on top.
        lease_grace: Duration::from_secs_f64(lease_grace.max(0.0)),
        metrics_addr,
        hard_max_rss,
        claim_block: Duration::from_millis(250),
        slots: slots.max(1),
    }
}

fn to_python(py: Python<'_>, outcomes: Vec<(u64, Outcome)>) -> PyResult<Vec<Py<PyAny>>> {
    outcomes
        .into_iter()
        .map(|(task_id, o)| {
            (
                task_id,
                o.ok,
                PyBytes::new(py, &o.result),
                o.error_type,
                o.traceback,
            )
                .into_pyobject(py)
                .map(|t| t.into_any().unbind())
        })
        .collect()
}

/// Outcomes, counters, and the exit code of every child that finished.
type BatchResult = (Vec<Py<PyAny>>, HashMap<String, u64>, Vec<i32>);

/// Batch mode: run a fixed list of jobs to completion over the memory broker.
/// The tests and the benchmark harness live here; production goes through
/// `work`, which never returns on its own.
#[pyfunction]
#[pyo3(signature = (app_spec, jobs, python, children=2, slots=1, max_rss=0, max_tasks=0,
                    max_lifetime=0.0, hard_max_rss=0))]
#[allow(clippy::too_many_arguments)]
fn run(
    py: Python<'_>,
    app_spec: String,
    jobs: Vec<(String, Vec<u8>)>,
    python: String,
    children: usize,
    slots: usize,
    max_rss: u64,
    max_tasks: u64,
    max_lifetime: f64,
    hard_max_rss: u64,
) -> PyResult<BatchResult> {
    let dir = socket_dir()?;
    let total = jobs.len();
    let cfg = build_cfg(
        app_spec,
        python,
        transport::connect_path(&dir),
        children,
        max_rss,
        max_tasks,
        max_lifetime,
        30.0,
        None,
        hard_max_rss,
        total as u64,
        slots,
    );

    let outcome = py.detach(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .and_then(|rt| {
                rt.block_on(async move {
                    let broker = Broker::connect("memory://", Vec::new())
                        .await
                        .map_err(io_err)?;
                    for (name, payload) in jobs {
                        broker
                            .push(
                                NewJob {
                                    id: String::new(), // batch mode keeps no results
                                    queue: "default".into(),
                                    name,
                                    payload,
                                    timeout_ms: 0,
                                    chain: Vec::new(),
                                },
                                Duration::ZERO,
                            )
                            .await
                            .map_err(io_err)?;
                    }
                    supervise(broker, total, children, cfg).await
                })
            })
    });
    let _ = std::fs::remove_dir_all(&dir);
    let (outcomes, stats, exits, fatal) = outcome?;
    if let Some(message) = fatal {
        return Err(PyValueError::new_err(message));
    }
    Ok((to_python(py, outcomes)?, stats, exits))
}

/// Worker mode: consume `queues` from a real broker until SIGINT or SIGTERM.
#[pyfunction]
#[pyo3(signature = (app_spec, broker_url, queues, python, children=2, slots=1, max_rss=0, max_tasks=0,
                    max_lifetime=0.0, lease_grace=30.0, metrics_addr=None, hard_max_rss=0))]
#[allow(clippy::too_many_arguments)]
fn work(
    py: Python<'_>,
    app_spec: String,
    broker_url: String,
    queues: Vec<String>,
    python: String,
    children: usize,
    slots: usize,
    max_rss: u64,
    max_tasks: u64,
    max_lifetime: f64,
    lease_grace: f64,
    metrics_addr: Option<String>,
    hard_max_rss: u64,
) -> PyResult<HashMap<String, u64>> {
    let dir = socket_dir()?;
    let cfg = build_cfg(
        app_spec,
        python,
        transport::connect_path(&dir),
        children,
        max_rss,
        max_tasks,
        max_lifetime,
        lease_grace,
        metrics_addr,
        hard_max_rss,
        u64::MAX / 2, // no batch to bound the spawn cap against
        slots,
    );

    let outcome = py.detach(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .and_then(|rt| {
                rt.block_on(async move {
                    let broker = Broker::connect(&broker_url, queues).await.map_err(io_err)?;
                    supervise(broker, 0, children, cfg).await
                })
            })
    });
    let _ = std::fs::remove_dir_all(&dir);
    let (_, stats, _, fatal) = outcome?;
    if let Some(message) = fatal {
        return Err(PyValueError::new_err(message));
    }
    Ok(stats)
}

/// One parked failure crossing into Python: id, name, error, traceback, and
/// when it died in milliseconds since the epoch.
type DeadRow = (String, String, String, String, u64);

/// One queue's backlog crossing into Python: name, ready, in flight, delayed, dead.
type DepthRow = (String, u64, u64, u64, u64);

/// Producer handle. Holds its own runtime and connection so enqueueing from a
/// web request is one round trip, not a reconnect.
#[pyclass]
struct Producer {
    runtime: tokio::runtime::Runtime,
    broker: Broker,
}

#[pymethods]
impl Producer {
    #[new]
    fn new(broker_url: String) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let broker = runtime
            .block_on(Broker::connect(&broker_url, Vec::new()))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Producer { runtime, broker })
    }

    /// `delay` in seconds; zero enqueues immediately.
    #[pyo3(signature = (id, queue, name, payload, timeout_ms, delay=0.0, chain=Vec::new(),
                        dedup_key=String::new(), dedup_ttl_ms=0))]
    #[allow(clippy::too_many_arguments)] // a pyo3 entry point, not a call site
    fn send(
        &self,
        py: Python<'_>,
        id: String,
        queue: String,
        name: String,
        payload: Vec<u8>,
        timeout_ms: u32,
        delay: f64,
        chain: Vec<u8>,
        dedup_key: String,
        dedup_ttl_ms: u64,
    ) -> PyResult<Option<String>> {
        let job = NewJob {
            id: id.clone(),
            queue,
            name,
            payload,
            timeout_ms,
            chain,
        };
        let delay = Duration::from_secs_f64(delay.max(0.0));
        py.detach(|| {
            self.runtime.block_on(async {
                if !dedup_key.is_empty() {
                    // Reserve before pushing. The other order would let two
                    // callers both queue and only then discover one of them
                    // should not have.
                    if let Some(held) = self
                        .broker
                        .claim_dedup(&dedup_key, &id, dedup_ttl_ms)
                        .await?
                    {
                        return Ok(Some(held));
                    }
                }
                self.broker.push(job, delay).await?;
                Ok(None)
            })
        })
        .map_err(|e: Box<dyn std::error::Error + Send + Sync>| PyValueError::new_err(e.to_string()))
    }

    /// The stored envelope for `id`, or None while it is unfinished, was never
    /// kept, or has expired — three states the caller has to tell apart from
    /// context, because the broker cannot.
    fn result(&self, py: Python<'_>, id: String) -> PyResult<Option<Py<PyAny>>> {
        let found = py
            .detach(|| self.runtime.block_on(self.broker.get_result(&id)))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        match found {
            Some(blob) => Ok(Some(PyBytes::new(py, &blob).into_any().unbind())),
            None => Ok(None),
        }
    }

    /// Cancel a job so it is never dispatched.
    ///
    /// Takes effect within a second — the supervisor pulls cancellations on a
    /// timer rather than asking per job. A job already running is not
    /// interrupted; see `App.cancel` for why.
    #[pyo3(signature = (id, queue="default", ttl=86400.0))]
    fn cancel(&self, py: Python<'_>, id: &str, queue: &str, ttl: f64) -> PyResult<()> {
        py.detach(|| {
            self.runtime.block_on(
                self.broker
                    .revoke(queue, id, (ttl.max(0.0) * 1000.0) as u64),
            )
        })
        .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Backlog per queue: (queue, ready, in flight, delayed, dead).
    #[pyo3(signature = (queues))]
    fn depth(&self, py: Python<'_>, queues: Vec<String>) -> PyResult<Vec<DepthRow>> {
        let rows = py
            .detach(|| self.runtime.block_on(self.broker.depth_of(&queues)))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|d| (d.queue, d.ready, d.in_flight, d.delayed, d.dead))
            .collect())
    }

    /// Parked failures, oldest first.
    #[pyo3(signature = (queue="default", limit=50))]
    fn dead_list(&self, py: Python<'_>, queue: &str, limit: usize) -> PyResult<Vec<DeadRow>> {
        let found = py
            .detach(|| self.runtime.block_on(self.broker.dead_list(queue, limit)))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(found
            .into_iter()
            .map(|d| (d.id, d.name, d.error, d.traceback, d.died_at_ms))
            .collect())
    }

    /// Put dead letters back on the queue. Empty `ids` means all of them.
    #[pyo3(signature = (queue="default", ids=Vec::new()))]
    fn dead_replay(&self, py: Python<'_>, queue: &str, ids: Vec<String>) -> PyResult<usize> {
        py.detach(|| self.runtime.block_on(self.broker.dead_replay(queue, &ids)))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    /// Drop dead letters. Empty `ids` means all of them.
    #[pyo3(signature = (queue="default", ids=Vec::new()))]
    fn dead_purge(&self, py: Python<'_>, queue: &str, ids: Vec<String>) -> PyResult<usize> {
        py.detach(|| self.runtime.block_on(self.broker.dead_purge(queue, &ids)))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }
}

/// The resident set of one process, as the supervisor reads it.
///
/// Exposed because the ceiling is only as good as this number, and when it is
/// wrong — as it was on Windows, reporting 4MB for a child holding hundreds —
/// there is no way to tell a broken reading from a broken trigger without it.
#[pyfunction]
fn rss_of(pid: u32) -> u64 {
    let mut sys = System::new();
    let key = Pid::from_u32(pid);
    sys.refresh_processes(ProcessesToUpdate::Some(&[key]), false);
    sys.process(key).map(|p| p.memory()).unwrap_or(0)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(work, m)?)?;
    m.add_function(wrap_pyfunction!(rss_of, m)?)?;
    m.add_class::<Producer>()
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn socket_dir_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = socket_dir().unwrap();
        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        std::fs::remove_dir_all(&dir).ok();
        // Group- or world-reachable means another account on the box can
        // connect to the dispatch socket, register as a child, and be handed
        // other people's task payloads.
        assert_eq!(
            mode, 0o700,
            "socket dir must not be reachable by other users"
        );
    }
}
