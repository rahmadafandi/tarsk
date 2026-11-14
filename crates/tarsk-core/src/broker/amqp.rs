//! RabbitMQ, and anything else speaking AMQP 0.9.1.

use super::memory::LocalCoordination;
use super::*;

/// Everything a claimed AMQP delivery needs to be retried or dead-lettered.
///
/// Carried inside the receipt because AMQP consumes destructively: after an
/// ack the server holds nothing to requeue, so a retry is a fresh publish of
/// this record with the attempt bumped.
#[derive(Clone, Debug)]
pub struct AmqpRecord {
    pub id: String,
    pub name: String,
    pub payload: Vec<u8>,
    pub timeout_ms: u32,
    pub chain: Vec<u8>,
    pub meta: Vec<u8>,
    pub expires_ms: u64,
    /// Explicit retries only. A crash redelivery reuses the server's copy of
    /// the message, whose headers this process never got to bump — so on AMQP
    /// `retries` counts handler failures, not worker deaths. Said in the docs.
    pub attempt: u32,
}

/// RabbitMQ and anything else speaking AMQP 0.9.1.
///
/// What maps cleanly: durable queues, publisher confirms, ack/nack, crash
/// redelivery (an unacked delivery returns the instant the connection dies —
/// no lease clock needed, the connection *is* the lease), delays via per-TTL
/// queues that dead-letter back, and a capped dead-letter queue.
///
/// What AMQP has no primitive for is shared state: no counters, no sets, no
/// compare-and-swap. Rate limits, concurrency caps and the cron election are
/// held in this process instead — correct with one worker, per-worker across
/// many, which is exactly the shape Celery has always had on RabbitMQ.
/// Cancellation and send-dedup are refused outright rather than faked.
pub struct AmqpBroker {
    /// Replaced when the connection or its channel closes. A channel-level
    /// error closes the channel for good, and before this every call after
    /// one failed for the rest of the process's life.
    link: tokio::sync::Mutex<Arc<AmqpLink>>,
    url: String,
    pub(super) queues: Vec<String>,
    /// Queues declared this run, so steady-state publishes skip the declare.
    /// Only queues without `x-expires` are cached: a delay queue deletes
    /// itself when idle, and publishing to a deleted queue through the default
    /// exchange drops the message without a sound.
    declared: Mutex<std::collections::HashSet<String>>,
    // Per-process stand-ins for state AMQP cannot hold.
    pub(super) local: LocalCoordination,
}

struct AmqpLink {
    conn: lapin::Connection,
    channel: lapin::Channel,
}

fn dead_key(queue: &str) -> String {
    format!("{}:dead", stream_key(queue))
}

fn amqp_header_str(headers: &lapin::types::FieldTable, key: &str) -> String {
    match headers.inner().get(key) {
        Some(lapin::types::AMQPValue::LongString(v)) => {
            String::from_utf8_lossy(v.as_bytes()).into_owned()
        }
        _ => String::new(),
    }
}

fn amqp_header_u64(headers: &lapin::types::FieldTable, key: &str) -> u64 {
    match headers.inner().get(key) {
        Some(lapin::types::AMQPValue::LongLongInt(v)) => (*v).max(0) as u64,
        _ => 0,
    }
}

fn amqp_header_bytes(headers: &lapin::types::FieldTable, key: &str) -> Vec<u8> {
    match headers.inner().get(key) {
        Some(lapin::types::AMQPValue::ByteArray(v)) => v.as_slice().to_vec(),
        _ => Vec::new(),
    }
}

/// Past this many bytes of chain, meta and failure text, a message carries
/// them in its body instead of its headers.
///
/// AMQP sends a message's headers as one frame, and a frame has a ceiling the
/// server sets — 131,072 bytes on RabbitMQ unless someone raised it. A chain
/// whose steps carry real payloads, or the traceback of a RecursionError, goes
/// straight through that, and the server answers by closing the connection: the
/// send fails, or worse, a job that could not be dead-lettered is redelivered
/// for ever. Half the ceiling, to leave room for everything else in the frame.
const INLINE_LIMIT: usize = 64 * 1024;

/// Marks a body that is an envelope rather than the bare payload.
const ENVELOPE: &str = "tarsk-body";

/// `[payload, chain, meta, [[key, value], …]]`, msgpack. The pairs are the
/// headers that would have been text: what failed, and its traceback.
fn seal(record: &AmqpRecord, extra: &[(&str, &str)]) -> Vec<u8> {
    use rmpv::Value;
    let pairs = extra
        .iter()
        .map(|(key, value)| Value::Array(vec![Value::from(*key), Value::from(*value)]))
        .collect();
    let envelope = Value::Array(vec![
        Value::Binary(record.payload.clone()),
        Value::Binary(record.chain.clone()),
        Value::Binary(record.meta.clone()),
        Value::Array(pairs),
    ]);
    let mut out = Vec::new();
    // Writing to a Vec cannot fail.
    let _ = rmpv::encode::write_value(&mut out, &envelope);
    out
}

/// What an envelope held: payload, chain, meta, and the text pairs.
type Opened = (Vec<u8>, Vec<u8>, Vec<u8>, Vec<(String, String)>);

fn open(data: &[u8]) -> Option<Opened> {
    use rmpv::Value;
    let Ok(Value::Array(parts)) = rmpv::decode::read_value(&mut &data[..]) else {
        return None;
    };
    let bytes = |at: usize| parts.get(at)?.as_slice().map(<[u8]>::to_vec);
    let pairs = match parts.get(3) {
        Some(Value::Array(pairs)) => pairs
            .iter()
            .filter_map(|pair| {
                let pair = pair.as_array()?;
                Some((
                    pair.first()?.as_str()?.to_string(),
                    pair.get(1)?.as_str()?.to_string(),
                ))
            })
            .collect(),
        _ => Vec::new(),
    };
    Some((bytes(0)?, bytes(1)?, bytes(2)?, pairs))
}

/// The record, read back off the headers `encode` wrote — and off the body,
/// when the message was too large to keep everything in its headers.
///
/// The second half of the result is the text an envelope carried, which the
/// dead-letter listing reads; for a message that fitted, it is in the headers
/// and this is empty.
fn amqp_read(
    headers: &lapin::types::FieldTable,
    data: &[u8],
) -> (AmqpRecord, Vec<(String, String)>) {
    let sealed = amqp_header_u64(headers, ENVELOPE) == 2;
    let (payload, chain, meta, pairs) = sealed.then(|| open(data)).flatten().unwrap_or_else(|| {
        (
            data.to_vec(),
            amqp_header_bytes(headers, "tarsk-chain"),
            amqp_header_bytes(headers, "tarsk-meta"),
            Vec::new(),
        )
    });
    let record = AmqpRecord {
        id: amqp_header_str(headers, "tarsk-id"),
        name: amqp_header_str(headers, "tarsk-name"),
        payload,
        timeout_ms: amqp_header_u64(headers, "tarsk-timeout-ms") as u32,
        chain,
        meta,
        expires_ms: amqp_header_u64(headers, "tarsk-expires-ms"),
        attempt: amqp_header_u64(headers, "tarsk-attempt") as u32,
    };
    (record, pairs)
}

fn amqp_record_from(headers: &lapin::types::FieldTable, data: &[u8]) -> AmqpRecord {
    amqp_read(headers, data).0
}

impl AmqpBroker {
    async fn open(url: &str) -> Res<AmqpLink> {
        let conn = lapin::Connection::connect(url, lapin::ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;
        // Publisher confirms, so push returning Ok means the server has the
        // message — the durability push promises everywhere else.
        channel
            .confirm_select(lapin::options::ConfirmSelectOptions::default())
            .await?;
        Ok(AmqpLink { conn, channel })
    }

    /// The live connection and channel, reopened first if either has closed.
    ///
    /// Deliveries claimed on the old channel cannot be acked on the new one;
    /// the server requeued them when it closed, which is the at-least-once
    /// this queue already promises.
    async fn link(&self) -> Res<Arc<AmqpLink>> {
        let mut link = self.link.lock().await;
        if !link.conn.status().connected() || !link.channel.status().connected() {
            *link = Arc::new(Self::open(&self.url).await?);
        }
        Ok(link.clone())
    }

    pub(super) async fn connect(url: &str, queues: Vec<String>) -> Res<AmqpBroker> {
        let broker = AmqpBroker {
            link: tokio::sync::Mutex::new(Arc::new(Self::open(url).await?)),
            url: url.to_string(),
            queues,
            declared: Mutex::new(std::collections::HashSet::new()),
            local: LocalCoordination::default(),
        };
        for queue in broker.queues.clone() {
            broker.declare_plain(&stream_key(&queue)).await?;
        }
        Ok(broker)
    }

    /// Declare a durable queue with no arguments; cache it, since it cannot
    /// expire out from under the cache.
    async fn declare_plain(&self, name: &str) -> Res<u32> {
        let q = self
            .link()
            .await?
            .channel
            .queue_declare(
                name.into(),
                lapin::options::QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await?;
        self.declared.lock().unwrap().insert(name.to_string());
        Ok(q.message_count())
    }

    /// The wait queue for one exact delay: fixed per-queue TTL, dead-lettering
    /// back to the real queue when it fires.
    ///
    /// One queue per distinct delay value rather than per message, because a
    /// queue only expires its *head* — mixed TTLs in one queue leave a 5s
    /// delay stuck behind an hour. `x-expires` deletes an idle wait queue, so
    /// the set stays bounded by the delays actually in use; the declare is
    /// repeated every publish for the same reason it is not cached.
    async fn delay_queue(&self, queue: &str, ms: u64) -> Res<String> {
        let name = format!("{}:delay:{}", stream_key(queue), ms);
        let mut args = lapin::types::FieldTable::default();
        args.insert(
            "x-message-ttl".into(),
            lapin::types::AMQPValue::LongLongInt(ms as i64),
        );
        args.insert(
            "x-dead-letter-exchange".into(),
            lapin::types::AMQPValue::LongString("".into()),
        );
        args.insert(
            "x-dead-letter-routing-key".into(),
            lapin::types::AMQPValue::LongString(stream_key(queue).into()),
        );
        args.insert(
            "x-expires".into(),
            lapin::types::AMQPValue::LongLongInt(ms as i64 + 60_000),
        );
        self.link()
            .await?
            .channel
            .queue_declare(
                name.as_str().into(),
                lapin::options::QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await?;
        Ok(name)
    }

    /// The headers, and the body if it has to be an envelope.
    ///
    /// A message that fits is written exactly as 0.2 wrote it, so a worker
    /// that has not been upgraded reads it. Only one that could not have been
    /// published at all before is written the new way.
    fn encode(
        record: &AmqpRecord,
        ready_at_ms: u64,
        extra: &[(&str, &str)],
    ) -> (lapin::BasicProperties, Option<Vec<u8>>) {
        use lapin::types::AMQPValue;
        let mut headers = lapin::types::FieldTable::default();
        headers.insert(
            "tarsk-id".into(),
            AMQPValue::LongString(record.id.as_str().into()),
        );
        headers.insert(
            "tarsk-name".into(),
            AMQPValue::LongString(record.name.as_str().into()),
        );
        headers.insert(
            "tarsk-timeout-ms".into(),
            AMQPValue::LongLongInt(record.timeout_ms as i64),
        );
        headers.insert(
            "tarsk-expires-ms".into(),
            AMQPValue::LongLongInt(record.expires_ms as i64),
        );
        headers.insert(
            "tarsk-attempt".into(),
            AMQPValue::LongLongInt(record.attempt as i64),
        );
        headers.insert(
            "tarsk-ready-at".into(),
            AMQPValue::LongLongInt(ready_at_ms as i64),
        );
        let bulk = record.chain.len()
            + record.meta.len()
            + extra.iter().map(|(_, value)| value.len()).sum::<usize>();
        let body = (bulk > INLINE_LIMIT).then(|| seal(record, extra));
        if body.is_some() {
            headers.insert(ENVELOPE.into(), AMQPValue::LongLongInt(2));
        } else {
            if !record.chain.is_empty() {
                headers.insert(
                    "tarsk-chain".into(),
                    AMQPValue::ByteArray(record.chain.clone().into()),
                );
            }
            if !record.meta.is_empty() {
                headers.insert(
                    "tarsk-meta".into(),
                    AMQPValue::ByteArray(record.meta.clone().into()),
                );
            }
            for (key, value) in extra {
                headers.insert((*key).into(), AMQPValue::LongString((*value).into()));
            }
        }
        let properties = lapin::BasicProperties::default()
            .with_delivery_mode(2)
            .with_headers(headers);
        (properties, body)
    }

    async fn publish_record(
        &self,
        target: &str,
        record: &AmqpRecord,
        ready_at_ms: u64,
        extra: &[(&str, &str)],
    ) -> Res<()> {
        let (properties, envelope) = Self::encode(record, ready_at_ms, extra);
        self.link()
            .await?
            .channel
            .basic_publish(
                "".into(),
                target.into(),
                lapin::options::BasicPublishOptions::default(),
                envelope.as_deref().unwrap_or(&record.payload),
                properties,
            )
            .await?
            .await?;
        Ok(())
    }

    pub(super) async fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        let queue = job.queue.clone();
        let record = AmqpRecord {
            id: job.id,
            name: job.name,
            payload: job.payload,
            timeout_ms: job.timeout_ms,
            chain: job.chain,
            meta: job.meta,
            expires_ms: job.expires_ms,
            attempt: 0,
        };
        let ready_at = now_ms() + delay.as_millis() as u64;
        // The queue has to exist first: a publish through the default exchange
        // to a missing queue is dropped without a sound — no error, no return,
        // just a message that never was. Redis creates its stream on first
        // XADD; AMQP does not, and a producer connects knowing no queue names.
        let main = stream_key(&queue);
        if !self.declared.lock().unwrap().contains(&main) {
            self.declare_plain(&main).await?;
        }
        let target = if delay.is_zero() {
            main
        } else {
            self.delay_queue(&queue, delay.as_millis() as u64).await?
        };
        self.publish_record(&target, &record, ready_at, &[]).await
    }

    fn delivery(&self, queue: &str, msg: lapin::message::BasicGetMessage) -> Delivery {
        let empty = lapin::types::FieldTable::default();
        let headers = msg.properties.headers().as_ref().unwrap_or(&empty);
        let record = amqp_record_from(headers, &msg.data);
        let ready_at_ms = amqp_header_u64(headers, "tarsk-ready-at");
        // ponytail: the payload is held twice per in-flight job — once here
        // for dispatch, once in the receipt for a retry's republish. Halving
        // that means Bytes/Arc through Delivery and Job, a type change across
        // both crates' hot path, and nothing has shown payloads big enough to
        // pay for it. If yours are, this line is where the second copy is born.
        Delivery {
            id: record.id.clone(),
            name: record.name.clone(),
            payload: record.payload.clone(),
            attempt: record.attempt + 1,
            ready_at_ms,
            expires_ms: record.expires_ms,
            chain: record.chain.clone(),
            meta: record.meta.clone(),
            receipt: Receipt::Amqp {
                queue: queue.to_string(),
                acker: msg.acker.clone(),
                record,
            },
        }
    }

    /// One pass over the queues in priority order; sleep and repeat until
    /// `block` runs out. Polling, like the Postgres broker — AMQP's push-based
    /// consumers cannot give strict priority across queues, and a claim that
    /// buffers pushed messages is holding work other workers could be doing.
    pub(super) async fn claim(&self, block: Duration) -> Res<Option<Delivery>> {
        let deadline = std::time::Instant::now() + block;
        loop {
            for queue in &self.queues {
                let got = self
                    .link()
                    .await?
                    .channel
                    .basic_get(
                        stream_key(queue).as_str().into(),
                        lapin::options::BasicGetOptions { no_ack: false },
                    )
                    .await?;
                if let Some(msg) = got {
                    return Ok(Some(self.delivery(queue, msg)));
                }
            }
            if std::time::Instant::now() >= deadline {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub(super) async fn retry(&self, receipt: &Receipt, delay: Duration) -> Res<()> {
        self.retry_as(receipt, delay, true).await
    }

    /// `failed` is whether this hand-back spends an attempt. See `Broker::defer`.
    pub(super) async fn retry_as(
        &self,
        receipt: &Receipt,
        delay: Duration,
        failed: bool,
    ) -> Res<()> {
        let Receipt::Amqp {
            queue,
            acker,
            record,
        } = receipt
        else {
            return Err("not an amqp receipt".into());
        };
        let mut again = record.clone();
        again.attempt += u32::from(failed);
        let ready_at = now_ms() + delay.as_millis() as u64;
        let target = if delay.is_zero() {
            stream_key(queue)
        } else {
            self.delay_queue(queue, delay.as_millis() as u64).await?
        };
        // Publish before ack: a crash between the two duplicates the job,
        // which is the at-least-once this queue promises. The other order
        // loses it.
        self.publish_record(&target, &again, ready_at, &[]).await?;
        acker
            .ack(lapin::options::BasicAckOptions::default())
            .await?;
        Ok(())
    }

    pub(super) async fn dead_letter(
        &self,
        receipt: &Receipt,
        error: &str,
        traceback: &str,
        keep: u64,
    ) -> Res<()> {
        let Receipt::Amqp {
            queue,
            acker,
            record,
        } = receipt
        else {
            return Err("not an amqp receipt".into());
        };
        let dead = dead_key(queue);
        if !self.declared.lock().unwrap().contains(&dead) {
            self.declare_plain(&dead).await?;
        }
        let died = now_ms().to_string();
        self.publish_record(
            &dead,
            record,
            now_ms(),
            &[
                ("tarsk-error", error),
                ("tarsk-traceback", traceback),
                ("tarsk-died-at", &died),
            ],
        )
        .await?;
        acker
            .ack(lapin::options::BasicAckOptions::default())
            .await?;
        if keep > 0 {
            // The cap, enforced the only way AMQP offers after the fact:
            // count, then eat the oldest. Redis trims with MAXLEN and
            // Postgres deletes past the newest N; this is the same promise
            // paid for one get at a time.
            let mut count = self.declare_plain(&dead).await? as u64;
            while count > keep {
                let Some(oldest) = self
                    .link()
                    .await?
                    .channel
                    .basic_get(
                        dead.as_str().into(),
                        lapin::options::BasicGetOptions { no_ack: false },
                    )
                    .await?
                else {
                    break;
                };
                oldest
                    .acker
                    .ack(lapin::options::BasicAckOptions::default())
                    .await?;
                count -= 1;
            }
        }
        Ok(())
    }

    /// Peek up to `limit` messages: get, record, and put every one back.
    async fn peek(
        &self,
        name: &str,
        limit: usize,
    ) -> Res<Vec<(AmqpRecord, u64, lapin::types::FieldTable, lapin::Acker)>> {
        if !self.declared.lock().unwrap().contains(name) {
            self.declare_plain(name).await?;
        }
        let mut seen = Vec::new();
        for _ in 0..limit {
            let Some(msg) = self
                .link()
                .await?
                .channel
                .basic_get(
                    name.into(),
                    lapin::options::BasicGetOptions { no_ack: false },
                )
                .await?
            else {
                break;
            };
            let empty = lapin::types::FieldTable::default();
            let mut headers = msg.properties.headers().as_ref().unwrap_or(&empty).clone();
            let (record, pairs) = amqp_read(&headers, &msg.data);
            // Put back where the listings look for it. In memory a header has
            // no ceiling; it was only ever the wire that did.
            for (key, value) in pairs {
                headers.insert(
                    key.as_str().into(),
                    lapin::types::AMQPValue::LongString(value.into()),
                );
            }
            let ready = amqp_header_u64(&headers, "tarsk-ready-at");
            seen.push((record, ready, headers, msg.acker.clone()));
        }
        Ok(seen)
    }

    async fn requeue_all(
        &self,
        seen: Vec<(AmqpRecord, u64, lapin::types::FieldTable, lapin::Acker)>,
    ) -> Res<()> {
        for (_, _, _, acker) in seen {
            acker
                .nack(lapin::options::BasicNackOptions {
                    requeue: true,
                    ..Default::default()
                })
                .await?;
        }
        Ok(())
    }

    pub(super) async fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        let name = result_key(id);
        let ttl_ms = ttl.as_millis().max(1) as i64;
        let mut args = lapin::types::FieldTable::default();
        args.insert(
            "x-expires".into(),
            lapin::types::AMQPValue::LongLongInt(ttl_ms),
        );
        args.insert(
            "x-message-ttl".into(),
            lapin::types::AMQPValue::LongLongInt(ttl_ms),
        );
        // A result queue holds exactly one blob, and the server itself makes
        // that true: at length one, publishing drops the head — so a fresh
        // progress report replaces the old one with no purge round trip. A
        // task reporting progress in a loop used to pay declare + purge +
        // publish every time; this is declare + publish, and the declare
        // cannot be cached away because x-expires deletes idle queues out
        // from under any cache.
        args.insert(
            "x-max-length".into(),
            lapin::types::AMQPValue::LongLongInt(1),
        );
        args.insert(
            "x-overflow".into(),
            lapin::types::AMQPValue::LongString("drop-head".into()),
        );
        self.link()
            .await?
            .channel
            .queue_declare(
                name.as_str().into(),
                lapin::options::QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                args,
            )
            .await?;
        self.link()
            .await?
            .channel
            .basic_publish(
                "".into(),
                name.as_str().into(),
                lapin::options::BasicPublishOptions::default(),
                &blob,
                lapin::BasicProperties::default().with_delivery_mode(2),
            )
            .await?
            .await?;
        Ok(())
    }

    pub(super) async fn get_result(&self, id: &str) -> Res<Option<Vec<u8>>> {
        // A passive declare on a missing queue closes the channel, so this
        // uses a throwaway one and reads a closed channel as "no result yet".
        let channel = match self.link().await?.conn.create_channel().await {
            Ok(channel) => channel,
            Err(_) => return Ok(None),
        };
        let name = result_key(id);
        let exists = channel
            .queue_declare(
                name.as_str().into(),
                lapin::options::QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await;
        if exists.is_err() {
            return Ok(None);
        }
        let Some(msg) = channel
            .basic_get(
                name.as_str().into(),
                lapin::options::BasicGetOptions { no_ack: false },
            )
            .await?
        else {
            return Ok(None);
        };
        let blob = msg.data.clone();
        msg.acker
            .nack(lapin::options::BasicNackOptions {
                requeue: true,
                ..Default::default()
            })
            .await?;
        Ok(Some(blob))
    }

    /// Ready and dead are what a declare reports; in-flight and delayed are
    /// zeros, honestly. Unacked counts live in the management API this driver
    /// does not speak, and delay queues are named by value, so neither is
    /// countable from AMQP proper. The docs say so next to the status table.
    pub(super) async fn depth(&self, wanted: &[String]) -> Res<Vec<Depth>> {
        let mut rows = Vec::new();
        for queue in wanted {
            let ready = self.declare_plain(&stream_key(queue)).await? as u64;
            let dead = self.declare_plain(&dead_key(queue)).await? as u64;
            rows.push(Depth {
                queue: queue.clone(),
                ready,
                in_flight: 0,
                delayed: 0,
                dead,
            });
        }
        Ok(rows)
    }

    pub(super) async fn jobs(&self, wanted: &[String], limit: usize) -> Res<Vec<Listed>> {
        let mut rows = Vec::new();
        for queue in wanted {
            let seen = self
                .peek(&stream_key(queue), limit.saturating_sub(rows.len()))
                .await?;
            let now = now_ms();
            for (record, ready, _, _) in &seen {
                rows.push(Listed {
                    id: record.id.clone(),
                    queue: queue.clone(),
                    name: record.name.clone(),
                    state: "ready",
                    attempt: record.attempt + 1,
                    age_ms: now.saturating_sub(*ready) as i64,
                    worker: String::new(),
                    meta: record.meta.clone(),
                });
            }
            self.requeue_all(seen).await?;
            if rows.len() >= limit {
                break;
            }
        }
        Ok(rows)
    }

    pub(super) async fn dead_list(&self, queue: &str, limit: usize) -> Res<Vec<Dead>> {
        let seen = self.peek(&dead_key(queue), limit).await?;
        let rows = seen
            .iter()
            .map(|(record, _, headers, _)| Dead {
                id: record.id.clone(),
                name: record.name.clone(),
                error: amqp_header_str(headers, "tarsk-error"),
                traceback: amqp_header_str(headers, "tarsk-traceback"),
                died_at_ms: amqp_header_str(headers, "tarsk-died-at")
                    .parse()
                    .unwrap_or(0),
            })
            .collect();
        self.requeue_all(seen).await?;
        Ok(rows)
    }

    /// Drain the dead queue once, replaying or purging what matches and
    /// putting the rest back. Bounded by what one pass sees, which for a
    /// capped dead queue is the whole of it.
    pub(super) async fn dead_drain(&self, queue: &str, ids: &[String], replay: bool) -> Res<usize> {
        let seen = self.peek(&dead_key(queue), 10_000).await?;
        let mut hit = 0;
        let mut keep = Vec::new();
        for entry in seen {
            let wanted = ids.is_empty() || ids.contains(&entry.0.id);
            if !wanted {
                keep.push(entry);
                continue;
            }
            let (record, _, _, acker) = entry;
            if replay {
                let main = stream_key(queue);
                if !self.declared.lock().unwrap().contains(&main) {
                    self.declare_plain(&main).await?;
                }
                let fresh = AmqpRecord {
                    attempt: 0,
                    ..record
                };
                self.publish_record(&main, &fresh, now_ms(), &[]).await?;
            }
            acker
                .ack(lapin::options::BasicAckOptions::default())
                .await?;
            hit += 1;
        }
        self.requeue_all(keep).await?;
        Ok(hit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(chain: usize, meta: usize) -> AmqpRecord {
        AmqpRecord {
            id: "id".into(),
            name: "task".into(),
            payload: b"payload".to_vec(),
            timeout_ms: 1_000,
            chain: vec![7; chain],
            meta: vec![9; meta],
            expires_ms: 5,
            attempt: 2,
        }
    }

    fn round_trip(
        record: &AmqpRecord,
        extra: &[(&str, &str)],
    ) -> (bool, AmqpRecord, Vec<(String, String)>) {
        let (properties, body) = AmqpBroker::encode(record, 0, extra);
        let headers = properties.headers().clone().unwrap();
        let sealed = body.is_some();
        let (read, pairs) = amqp_read(&headers, body.as_deref().unwrap_or(&record.payload));
        (sealed, read, pairs)
    }

    #[test]
    fn a_message_that_fits_is_written_as_it_always_was() {
        // Headers only, bare payload: the form a 0.2 worker reads.
        let small = record(100, 100);
        let (sealed, read, pairs) = round_trip(&small, &[("tarsk-error", "boom")]);
        assert!(!sealed);
        assert!(pairs.is_empty(), "small text stays in the headers");
        assert_eq!(
            (read.payload, read.chain.len(), read.meta.len()),
            (b"payload".to_vec(), 100, 100)
        );
    }

    #[test]
    fn a_message_too_large_for_its_headers_goes_in_the_body() {
        // Each of these alone is past RabbitMQ's 131,072-byte frame.
        let traceback = "x".repeat(300_000);
        for (big, extra) in [
            (record(200_000, 0), vec![]),
            (record(0, 200_000), vec![]),
            (
                record(10, 10),
                vec![
                    ("tarsk-error", "RecursionError"),
                    ("tarsk-traceback", traceback.as_str()),
                ],
            ),
        ] {
            let (properties, body) = AmqpBroker::encode(&big, 0, &extra);
            let headers = properties.headers().clone().unwrap();
            assert!(body.is_some(), "a 200KB header would have been refused");
            let mut wire = Vec::new();
            for (key, value) in headers.inner() {
                wire.push(key.as_str().len() + format!("{value:?}").len());
            }
            assert!(
                wire.iter().sum::<usize>() < 4_096,
                "the headers are still large: {wire:?}"
            );

            let (sealed, read, pairs) = round_trip(&big, &extra);
            assert!(sealed);
            assert_eq!(read.payload, big.payload);
            assert_eq!(read.chain, big.chain);
            assert_eq!(read.meta, big.meta);
            assert_eq!(
                (read.id.as_str(), read.attempt, read.expires_ms),
                ("id", 2, 5)
            );
            let expected: Vec<(String, String)> = extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            assert_eq!(pairs, expected);
        }
    }
}
