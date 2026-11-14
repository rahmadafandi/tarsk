//! Redis Streams.

use super::*;

/// Group and consumer names are fixed: one logical worker pool per broker.
const REDIS_GROUP: &str = "tarsk";
/// Fallback ceiling on a batched read, replaced by the worker's child count.
///
/// Measured on 5,000 no-ops across four children: one message per read gives a
/// median of 0.96s, four gives 0.65s, sixty-four gives 1.18s. Fetching enough
/// to keep every child fed helps; fetching more than that means parsing a burst
/// on a single-threaded runtime while the children it was meant to feed wait.
const DEFAULT_PREFETCH_CAP: u64 = 4;

/// Pending entries the lease sweep reads per round trip.
const PENDING_PAGE: usize = 64;

pub struct RedisBroker {
    /// Cloned per command rather than locked. The connection is multiplexed and
    /// built to be used concurrently — putting a mutex round it made every command
    /// from every child queue behind one lock on a single-threaded runtime,
    /// which is a bottleneck this driver invented for itself.
    conn: redis::aio::ConnectionManager,
    /// When a caller last asked `refresh` whether the connection was alive, in
    /// milliseconds. See there.
    last_used_ms: AtomicU64,
    /// Only for `XREADGROUP … BLOCK`, and never shared with anything else.
    ///
    /// A blocking command on a multiplexed connection holds up every command
    /// multiplexed onto it, because there is one socket and the server answers
    /// in order. With children waiting for work, that put the reclaim sweep,
    /// the acks and the rate-limit script behind a 250ms wait each time — the
    /// sweep was timing out and requeued jobs were never coming back.
    blocking: redis::aio::ConnectionManager,
    consumer: String,
    pub(super) queues: Vec<String>,
    /// Shortest timeout registered anywhere, the floor for the pending sweep.
    pub(super) min_timeout_ms: AtomicU64,
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
    pub(super) prefetch_cap: AtomicU64,
}

fn delayed_key(queue: &str) -> String {
    format!("tarsk:{queue}:delayed")
}

/// The server's clock, in milliseconds, as the first lines of a script.
///
/// Every deadline this driver keeps in Redis — when a delayed job is due, when
/// a slot or a cancellation lapses, how full a bucket is — is compared by more
/// than one machine. Stamped with each machine's own clock, two hosts a few
/// seconds apart disagreed about all of them: a rate limit refilled early for
/// one and late for the other. Redis has one clock, and scripts can read it.
///
/// `replicate_commands` is what lets a script write after reading the time on
/// Redis 5 and 6; from 7 on it is the default and the call does nothing.
const LUA_NOW: &str = r"
if redis.replicate_commands then redis.replicate_commands() end
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
";

/// Move due jobs from the delayed set into the stream.
///
/// This is the one place a Lua script earns its keep. Everywhere else the
/// driver either holds a single command or has every field already in hand;
/// here it must read what is due, decide, and write — and a crash between the
/// XADD and the ZREM would leave the job scheduled *and* queued.
///
/// KEYS[1] delayed zset · KEYS[2] stream · ARGV[1] batch size. Follows `LUA_NOW`.
///
/// ponytail: the per-job hash key is derived inside the script rather than
/// declared in KEYS, which Redis Cluster forbids. Single-node only until
/// someone needs otherwise; a hash tag on both keys is the fix.
const PROMOTE_DUE: &str = r"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
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

/// Absent on jobs written before this field existed, which reads as zero and
/// means the registration decides — the behaviour those jobs were queued with.
fn entry_expires(entry: &redis::streams::StreamId) -> u64 {
    entry.get("x").unwrap_or(0)
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

/// Where cancellations live: one set for every queue, because an id is unique
/// without one. `revoked_key` is where 0.2 wrote them, still
/// read so a cancel issued before an upgrade survives it.
const REVOKED: &str = "tarsk:revoked";

impl RedisBroker {
    pub(super) async fn connect(url: &str, queues: Vec<String>) -> Res<RedisBroker> {
        let client = redis::Client::open(url)?;
        // Managers rather than bare connections: they reconnect after a
        // failover or a restart, where a multiplexed connection stays closed
        // and leaves a worker that is up, healthy to its orchestrator, and
        // failing every command.
        let mut conn = client.get_connection_manager().await?;
        // Every serve loop parks its `XREADGROUP … BLOCK` here, and one socket
        // answers them one at a time — with each wait rounded up to the
        // server's next `hz` sweep (~100ms at the default hz 10), N children
        // stack up to ~N × 300ms before the last reply lands. The driver's
        // default response timeout is 500ms, so two idle children were enough
        // to manufacture a steady "timed out" every ~300ms: broker_errors
        // climbed ~3/s on a healthy worker doing nothing, burying real errors.
        // The commands themselves are fine; only the deadline was wrong.
        let blocking = client
            .get_connection_manager_with_config(
                redis::aio::ConnectionManagerConfig::new()
                    .set_response_timeout(Some(std::time::Duration::from_secs(30))),
            )
            .await?;
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
            last_used_ms: AtomicU64::new(now_ms()),
            min_timeout_ms: AtomicU64::new(u64::MAX),
            grace_ms: AtomicU64::new(0),
            // Seeded from the clock, not zero: the member is `<pid>-<n>`, and
            // two producers that are both pid 1 in their containers would each
            // count from the same place and overwrite each other's jobs.
            next_delayed: AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0),
            ),
            claimed: Mutex::new(VecDeque::new()),
            prefetch: AtomicU64::new(1),
            prefetch_cap: AtomicU64::new(DEFAULT_PREFETCH_CAP),
        })
    }

    pub(super) async fn push_many(&self, jobs: Vec<NewJob>) -> Res<()> {
        // MULTI: every XADD or none, and one round trip for the lot.
        let mut pipe = redis::pipe();
        pipe.atomic();
        for job in &jobs {
            pipe.cmd("XADD")
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
                .arg("m")
                .arg(&job.meta)
                .arg("x")
                .arg(job.expires_ms)
                .ignore();
        }
        let mut conn = self.conn.clone();
        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Find out that the connection has gone before something that matters is
    /// sent on it.
    ///
    /// The manager reconnects when a command fails, which makes the command
    /// that fails the price of finding out — and for a producer that had been
    /// idle, that command was a job being sent. So after a second of quiet the
    /// first thing sent is a PING instead: if the server hung up meanwhile it
    /// is the PING that fails, the manager starts over, and the real command
    /// waits for the new connection. Nothing is lost that was worth keeping.
    ///
    /// Only an idle connection is probed. A busy one learns the same thing
    /// from its own traffic, and should not pay a round trip per call for it.
    pub(super) async fn refresh(&self) {
        let now = now_ms();
        let idle = now.saturating_sub(self.last_used_ms.swap(now, Ordering::Relaxed));
        if idle >= 1_000 {
            let mut conn = self.conn.clone();
            let _: Result<String, _> = redis::cmd("PING").query_async(&mut conn).await;
        }
    }

    pub(super) async fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
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
                .arg("m")
                .arg(&job.meta)
                .arg("x")
                .arg(job.expires_ms)
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
        // One script, so a job is never scheduled without its payload or the
        // reverse, and so "due" is measured on the clock that will promote it.
        let script = redis::Script::new(&format!(
            "{LUA_NOW}
             redis.call('HSET', KEYS[2], unpack(ARGV, 2))
             redis.call('ZADD', KEYS[1], now + tonumber(ARGV[1]), KEYS[3])
             return 1"
        ));
        let _: i64 = script
            .key(&key)
            .key(format!("{key}:{id}"))
            .key(&id)
            .arg(delay.as_millis() as u64)
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
            .arg("m")
            .arg(&job.meta)
            .arg("x")
            .arg(job.expires_ms)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    pub(super) async fn claim_run(&self, name: &str, due: i64, now: i64) -> Res<bool> {
        // Read, compare and write in one step, or two workers both see the
        // old value and both fire. One key a schedule, holding the last minute
        // anyone fired it for. See `Broker::claim_run` for the first-sight rule.
        let script = redis::Script::new(
            r"local last = tonumber(redis.call('GET', KEYS[1]))
              local due, now = tonumber(ARGV[1]), tonumber(ARGV[2])
              if not last then last = now - 1 end
              if due > last then
                redis.call('SET', KEYS[1], due)
                return 1
              end
              redis.call('SET', KEYS[1], last)
              return 0",
        );
        let mut conn = self.conn.clone();
        let won: i64 = script
            .key(format!("tarsk:cron:last:{name}"))
            .arg(due)
            .arg(now)
            .invoke_async(&mut conn)
            .await?;
        Ok(won == 1)
    }

    pub(super) async fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
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

    pub(super) async fn get_result(&self, id: &str) -> Res<Option<Vec<u8>>> {
        let mut conn = self.conn.clone();
        let blob: Option<Vec<u8>> = redis::cmd("GET")
            .arg(result_key(id))
            .query_async(&mut conn)
            .await?;
        Ok(blob)
    }

    /// Hand due jobs to the stream. Runs on the sweep that already exists.
    pub(super) async fn promote_due(&self) -> Res<usize> {
        let mut moved = 0;
        for queue in &self.queues {
            let mut conn = self.conn.clone();
            let promoted: usize = redis::Script::new(&format!("{LUA_NOW}{PROMOTE_DUE}"))
                .key(delayed_key(queue))
                .key(stream_key(queue))
                .arg(128)
                .invoke_async(&mut conn)
                .await?;
            moved += promoted;
        }
        Ok(moved)
    }

    /// One `XREADGROUP`, over the streams given.
    ///
    /// `block` picks the connection as well as the command: a blocking read
    /// holds up everything multiplexed onto its socket, so it gets the one
    /// reserved for it and a probe shares the ordinary one.
    async fn read_streams(
        &self,
        queues: &[String],
        batch: u64,
        block: Option<Duration>,
    ) -> Res<Option<redis::streams::StreamReadReply>> {
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(REDIS_GROUP)
            .arg(&self.consumer)
            .arg("COUNT")
            .arg(batch);
        if let Some(wait) = block {
            cmd.arg("BLOCK").arg(wait.as_millis().max(1) as u64);
        }
        cmd.arg("STREAMS");
        for queue in queues {
            cmd.arg(stream_key(queue));
        }
        for _ in queues {
            cmd.arg(">"); // never-delivered only; our own backlog is in `claimed`
        }
        let mut conn = if block.is_some() {
            self.blocking.clone()
        } else {
            self.conn.clone()
        };
        Ok(cmd.query_async(&mut conn).await?)
    }

    /// Read a batch, highest-priority queue first.
    ///
    /// `--queues high,low` used to be one read across both streams, and Redis
    /// answers that with up to `COUNT` from *each*. The batch then held low
    /// work that ran before high work that arrived while it drained, which is
    /// a preference and not a priority.
    ///
    /// So: probe each queue in turn without blocking and take the first that
    /// has anything. Only when they are all empty is there something to wait
    /// on, and that wait covers every queue at once so any of them wakes us.
    ///
    /// The cost is one extra round trip per claim while a high queue is empty
    /// and a low one is not — paid on a connection that is not the blocking
    /// one, and only by workers that asked for more than one queue.
    ///
    /// Strict at the moment of reading, which is the only moment it can be.
    /// A batch already claimed is already leased to this worker and is still
    /// run; priority decides what is read next, not what is already held.
    async fn read_in_order(
        &self,
        batch: u64,
        block: Duration,
    ) -> Res<Option<redis::streams::StreamReadReply>> {
        if self.queues.len() > 1 {
            for queue in &self.queues {
                let reply = self
                    .read_streams(std::slice::from_ref(queue), batch, None)
                    .await?;
                if let Some(reply) = reply {
                    if reply.keys.iter().any(|k| !k.ids.is_empty()) {
                        return Ok(Some(reply));
                    }
                }
            }
        }
        // A zero block means "do not wait", and the only way to say that to
        // Redis is to omit BLOCK entirely. Sending BLOCK 1 instead parks the
        // consumer until the server's next timeout sweep, which runs at `hz`
        // (default 10) — so "wait one millisecond" actually waits ~100ms.
        // Measured: BLOCK 1 on an empty stream answers in 100.8ms, BLOCK 100
        // in 201ms, no BLOCK in 0.1ms. A worker with N slots pays that stall
        // once per Ready, serially — 100 slots put ~10 seconds between a chain
        // step's Ack and the dispatch of the next step, which read as a hang.
        self.read_streams(&self.queues, batch, (!block.is_zero()).then_some(block))
            .await
    }

    pub(super) async fn claim(&self, block: Duration) -> Res<Option<Delivery>> {
        // Anything already owned goes out first — but only while its lease has
        // time left. Past that the sweep may have handed it to someone else,
        // and running it here would be a duplicate we chose to create.
        loop {
            let head = self.claimed.lock().unwrap().pop_front();
            match head {
                Some((delivery, claimed_at, deadline_ms)) => {
                    if claimed_at.elapsed().as_millis() as u64 >= deadline_ms {
                        // ponytail: `fetch_update` was renamed `try_update` in a Rust newer than
                        // the one mise.toml pins. Allowed rather than renamed so both the pinned
                        // toolchain and CI's stable build; switch when the pin moves past the rename.
                        #[allow(deprecated)]
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
        let Some(reply) = self.read_in_order(batch, block).await? else {
            return Ok(None);
        };
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
                    expires_ms: entry_expires(&entry),
                    chain: entry.get("c").unwrap_or_default(),
                    meta: entry.get("m").unwrap_or_default(),
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
            // See the note on the other `fetch_update` above.
            #[allow(deprecated)]
            let _ = self
                .prefetch
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                    Some((n * 2).min(self.prefetch_cap.load(Ordering::Relaxed).max(1)))
                });
        }
        Ok(first)
    }

    pub(super) async fn ack(&self, receipt: &Receipt) -> Res<()> {
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
    pub(super) async fn requeue(&self, receipt: &Receipt, delay: Duration) -> Res<()> {
        self.requeue_as(receipt, delay, None).await
    }

    /// `delivered`, when given, rewrites the entry's delivery count. A deferral
    /// sets it one back so the reclaim's own bump lands on the number the job
    /// already had. See `Broker::defer`.
    pub(super) async fn requeue_as(
        &self,
        receipt: &Receipt,
        delay: Duration,
        delivered: Option<u32>,
    ) -> Res<()> {
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
        let mut claim = redis::cmd("XCLAIM");
        claim
            .arg(&key)
            .arg(REDIS_GROUP)
            .arg(&self.consumer)
            .arg(0)
            .arg(id)
            .arg("IDLE")
            .arg(idle);
        if let Some(count) = delivered {
            claim.arg("RETRYCOUNT").arg(count);
        }
        claim.arg("JUSTID"); // no delivery-count bump: the reclaim will do that
        let _: redis::Value = claim.query_async(&mut conn).await?;
        Ok(())
    }

    pub(super) async fn release_unstarted(&self) {
        let held: Vec<Delivery> = self
            .claimed
            .lock()
            .unwrap()
            .drain(..)
            .map(|(delivery, _, _)| delivery)
            .collect();
        for delivery in held {
            // Best effort: one that cannot be handed back is still reclaimed
            // when its lease runs out, which is what happened to all of them
            // before this existed.
            let _ = self
                .requeue_as(
                    &delivery.receipt,
                    Duration::ZERO,
                    Some(delivery.attempt.saturating_sub(1)),
                )
                .await;
        }
    }

    pub(super) async fn jobs(&self, queues: &[String], limit: usize) -> Res<Vec<Listed>> {
        let mut out = Vec::new();
        let now = now_ms();
        for queue in queues {
            let key = stream_key(queue);
            let mut conn = self.conn.clone();

            // Which entries are claimed, and how long ago. An entry in the
            // stream is ready unless it is in the group's pending list; there
            // is no other flag to read.
            let pending: Option<redis::streams::StreamPendingCountReply> = redis::cmd("XPENDING")
                .arg(&key)
                .arg(REDIS_GROUP)
                .arg("-")
                .arg("+")
                .arg(limit)
                .query_async(&mut conn)
                .await
                .ok();
            let claimed: HashMap<String, (i64, u32, String)> = pending
                .map(|p| {
                    p.ids
                        .into_iter()
                        .map(|e| {
                            (
                                e.id,
                                (
                                    e.last_delivered_ms as i64,
                                    e.times_delivered as u32,
                                    e.consumer,
                                ),
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();

            let entries: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
                .arg(&key)
                .arg("-")
                .arg("+")
                .arg("COUNT")
                .arg(limit)
                .query_async(&mut conn)
                .await?;
            for entry in entries.ids {
                let (name, _payload, _timeout) = entry_fields(&entry);
                let meta: Vec<u8> = entry.get("m").unwrap_or_default();
                let (state, age_ms, attempt, worker) = match claimed.get(&entry.id) {
                    Some((idle, delivered, who)) => ("running", *idle, *delivered, who.clone()),
                    None => (
                        "ready",
                        now.saturating_sub(stream_id_ms(&entry.id)) as i64,
                        0,
                        String::new(),
                    ),
                };
                out.push(Listed {
                    id: entry_id(&entry),
                    queue: queue.clone(),
                    name,
                    state,
                    attempt,
                    age_ms,
                    worker,
                    meta,
                });
            }

            // Delayed jobs are not in the stream at all: they wait in a sorted
            // set scored by when they come due, with their fields beside them.
            let due: Vec<(String, u64)> = redis::cmd("ZRANGE")
                .arg(delayed_key(queue))
                .arg(0)
                .arg(limit as i64 - 1)
                .arg("WITHSCORES")
                .query_async(&mut conn)
                .await
                .unwrap_or_default();
            for (member, at) in due {
                // HMGET, not HGETALL: the payload field beside these is msgpack,
                // and asking for the whole hash as strings fails on the first
                // byte that is not UTF-8 — silently, leaving every row blank.
                let (name, job_id): (Option<String>, Option<String>) = redis::cmd("HMGET")
                    .arg(format!("{}:{member}", delayed_key(queue)))
                    .arg("n")
                    .arg("i")
                    .query_async(&mut conn)
                    .await
                    .unwrap_or((None, None));
                out.push(Listed {
                    id: job_id.unwrap_or(member),
                    queue: queue.clone(),
                    name: name.unwrap_or_default(),
                    state: "delayed",
                    attempt: 0,
                    age_ms: now as i64 - at as i64,
                    worker: String::new(),
                    meta: Vec::new(),
                });
            }
        }
        Ok(out)
    }

    pub(super) async fn depth(&self, queues: &[String]) -> Res<Vec<Depth>> {
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

    pub(super) async fn claim_dedup(
        &self,
        key: &str,
        job_id: &str,
        ttl_ms: u64,
    ) -> Res<Option<String>> {
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

    pub(super) async fn release_dedup(&self, key: &str, job_id: &str) -> Res<()> {
        // Only if it is still ours: the window may have lapsed and been taken
        // by another send while the push that failed was in flight.
        let script = redis::Script::new(
            r"if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
              end
              return 0",
        );
        let mut conn = self.conn.clone();
        let _: i64 = script
            .key(dedup_key(key))
            .arg(job_id)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    pub(super) async fn acquire_slot(
        &self,
        task: &str,
        job_id: &str,
        max: u32,
        lease_ms: u64,
    ) -> Res<bool> {
        // Prune, count and add cannot be three commands: two workers would both
        // see the last slot free. A sorted set scored by expiry gives the prune
        // for free, which a counter would need a separate reaper for.
        let script = redis::Script::new(&format!(
            "{LUA_NOW}
              local until_ms, max = now + tonumber(ARGV[1]), tonumber(ARGV[2])
              redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now)
              if redis.call('ZCARD', KEYS[1]) >= max
                 and redis.call('ZSCORE', KEYS[1], ARGV[3]) == false then
                return 0
              end
              redis.call('ZADD', KEYS[1], until_ms, ARGV[3])
              redis.call('PEXPIRE', KEYS[1], until_ms - now + 60000)
              return 1"
        ));
        let mut conn = self.conn.clone();
        let taken: i64 = script
            .key(slots_key(task))
            .arg(lease_ms.max(1))
            .arg(max)
            .arg(job_id)
            .invoke_async(&mut conn)
            .await?;
        Ok(taken == 1)
    }

    pub(super) async fn release_slot(&self, task: &str, job_id: &str) -> Res<()> {
        let mut conn = self.conn.clone();
        let _: i64 = redis::cmd("ZREM")
            .arg(slots_key(task))
            .arg(job_id)
            .query_async(&mut conn)
            .await?;
        Ok(())
    }

    pub(super) async fn take_token(&self, task: &str, per_sec: f64, burst: u32) -> Res<u64> {
        // Read-refill-write has to be one step or two workers both see the last
        // token. Lua is the only thing Redis offers that is; MULTI would not
        // help, since the decision depends on what the read returned.
        let script = redis::Script::new(&format!(
            "{LUA_NOW}
              local tokens = tonumber(redis.call('HGET', KEYS[1], 't'))
              local seen   = tonumber(redis.call('HGET', KEYS[1], 's'))
              local rate, burst = tonumber(ARGV[1]), tonumber(ARGV[2])
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
              return wait"
        ));
        let mut conn = self.conn.clone();
        let wait: i64 = script
            .key(format!("tarsk:rate:{task}"))
            .arg(per_sec)
            .arg(burst)
            .invoke_async(&mut conn)
            .await?;
        Ok(wait.max(0) as u64)
    }

    pub(super) async fn revoke(&self, id: &str, ttl_ms: u64) -> Res<()> {
        let script = redis::Script::new(&format!(
            "{LUA_NOW}
             return redis.call('ZADD', KEYS[1], now + tonumber(ARGV[1]), ARGV[2])"
        ));
        let mut conn = self.conn.clone();
        let _: i64 = script
            .key(REVOKED)
            .arg(ttl_ms)
            .arg(id)
            .invoke_async(&mut conn)
            .await?;
        Ok(())
    }

    pub(super) async fn revoked_all(&self) -> Res<Vec<String>> {
        let mut out = self.revoked(REVOKED).await?;
        for queue in &self.queues {
            out.extend(self.revoked(&revoked_key(queue)).await?);
        }
        Ok(out)
    }

    async fn revoked(&self, key: &str) -> Res<Vec<String>> {
        // Prune and read in one round trip. A sorted set scored by expiry gives
        // per-member TTL, which a plain SET cannot.
        let script = redis::Script::new(&format!(
            "{LUA_NOW}
             redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now)
             return redis.call('ZRANGE', KEYS[1], 0, -1)"
        ));
        let mut conn = self.conn.clone();
        let live: Vec<String> = script.key(key).invoke_async(&mut conn).await?;
        Ok(live)
    }

    pub(super) async fn dead_list(&self, queue: &str, limit: usize) -> Res<Vec<Dead>> {
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

    pub(super) async fn dead_replay(&self, queue: &str, ids: &[String]) -> Res<usize> {
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
            // Entries buried before these fields were kept have none of them;
            // the dead id stands in for the job's, as it always did.
            let job_id: String = entry.get("i").unwrap_or_else(|| entry.id.clone());
            let chain: Vec<u8> = entry.get("c").unwrap_or_default();
            let meta: Vec<u8> = entry.get("m").unwrap_or_default();
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
                .arg("c")
                .arg(&chain)
                .arg("m")
                .arg(&meta)
                .arg("x")
                .arg(entry_expires(entry))
                .cmd("XDEL")
                .arg(&grave)
                .arg(&entry.id)
                .query_async(&mut conn)
                .await?;
            moved += 1;
        }
        Ok(moved)
    }

    pub(super) async fn dead_purge(&self, queue: &str, ids: &[String]) -> Res<usize> {
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
    pub(super) async fn dead_letter(
        &self,
        receipt: &Receipt,
        error: &str,
        traceback: &str,
        keep: u64,
    ) -> Res<()> {
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
        let entry = entries.ids.first();
        let (name, payload, timeout_ms) = entry
            .map(entry_fields)
            .unwrap_or_else(|| (String::new(), Vec::new(), 0));
        // Kept so a replay is the same job: its id is where the caller is
        // waiting for a result, and its chain is the work still owed after it.
        let job_id = entry.map(entry_id).unwrap_or_default();
        let chain: Vec<u8> = entry.and_then(|e| e.get("c")).unwrap_or_default();
        let meta: Vec<u8> = entry.and_then(|e| e.get("m")).unwrap_or_default();
        let expires_ms = entry.map(entry_expires).unwrap_or(0);
        let mut trim: Vec<String> = Vec::new();
        if keep > 0 {
            // Approximate trimming, which Redis does at whole-node granularity
            // and charges almost nothing for. An exact cap would cost more than
            // the few extra entries it saves.
            trim = vec!["MAXLEN".into(), "~".into(), keep.to_string()];
        }
        let mut add = redis::cmd("XADD");
        add.arg(&grave);
        for part in &trim {
            add.arg(part);
        }
        let _: redis::Value = redis::pipe()
            .atomic()
            .add_command(add)
            .arg("*")
            .arg("n")
            .arg(name)
            .arg("p")
            .arg(payload)
            .arg("t")
            .arg(timeout_ms)
            .arg("i")
            .arg(job_id)
            .arg("c")
            .arg(chain)
            .arg("m")
            .arg(meta)
            .arg("x")
            .arg(expires_ms)
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
    pub(super) async fn reclaim_expired(&self, grace: Duration) -> Res<usize> {
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
            // In pages, not one read. The filter is the *shortest* timeout,
            // so the first 64 entries past it can all be long jobs still inside
            // their own lease — and an entry stranded behind them was never
            // looked at, for as long as the queue stayed that busy.
            //
            // ponytail: stops after 16 pages, 1024 entries a sweep per queue.
            // Each candidate costs an XRANGE, and the sweep runs every second;
            // carry the cursor between sweeps if a queue holds more in flight.
            let mut start = "-".to_string();
            for _ in 0..16 {
                let pending: redis::streams::StreamPendingCountReply = redis::cmd("XPENDING")
                    .arg(&key)
                    .arg(REDIS_GROUP)
                    .arg("IDLE")
                    .arg(floor)
                    .arg(&start)
                    .arg("+")
                    .arg(PENDING_PAGE)
                    .query_async(&mut conn)
                    .await?;
                let full = pending.ids.len() == PENDING_PAGE;
                if let Some(last) = pending.ids.last() {
                    start = format!("({}", last.id); // exclusive: the next page
                }
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
                            expires_ms: entry_expires(entry),
                            chain: entry.get("c").unwrap_or_default(),
                            meta: entry.get("m").unwrap_or_default(),
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
                if !full {
                    break;
                }
            }
        }
        Ok(reclaimed)
    }
}
