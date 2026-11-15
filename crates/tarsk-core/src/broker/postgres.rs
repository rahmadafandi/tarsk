//! Postgres.

use super::*;

/// One table, because the 5-minute `max_timeout` cap (spec §9) means a lease
/// never needs renewing. `awa` splits ready / deferred / lease / tombstone
/// tables to support heartbeats and mutable attempt state; none of that is
/// reachable when a lease cannot outlive a known ceiling.
const PG_SCHEMA: &str = "
-- This whole batch is one implicit transaction, so the lock makes workers that
-- start together set the schema up one at a time instead of racing each other
-- into duplicate-object errors.
select pg_advisory_xact_lock(7401001);
create table if not exists tarsk_schema (stamp bigint primary key);
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
-- Before the indexes, not after. `alter table` takes the strongest lock there
-- is and `create index` a weaker one that still blocks writers; taken in that
-- order inside this one transaction, a worker mid-claim and a process just
-- connecting each ended up waiting on the other, and Postgres killed one.
alter table tarsk_jobs add column if not exists chain      bytea;
alter table tarsk_jobs add column if not exists claimed_by text;
alter table tarsk_jobs add column if not exists meta       bytea;
alter table tarsk_jobs add column if not exists expires_ms bigint not null default 0;
create index if not exists tarsk_jobs_ready on tarsk_jobs (queue, id) where lease_until is null;
create index if not exists tarsk_jobs_leased on tarsk_jobs (lease_until) where lease_until is not null;
-- A queue is a table that is nearly all churn: every job is an insert, an
-- update and a delete, and the rows left behind are what a claim wades
-- through. The default waits for a fifth of the table to be dead, which on a
-- table that is mostly empty is either at once or, with a backlog, far too
-- late. A fixed count keeps what is left behind bounded whatever the backlog.
alter table tarsk_jobs set (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 2000,
                            autovacuum_analyze_scale_factor = 0, autovacuum_analyze_threshold = 5000);

-- The last minute each schedule was fired for. One row a schedule: `tarsk_cron`,
-- which 0.2 used, held a row per schedule per minute and could only answer
-- whether one exact minute had been fired, which is not enough to catch a
-- missed one up.
create table if not exists tarsk_cron_runs (
    name    text   primary key,
    minute  bigint not null
);

-- A row per finished member of a chord, by its position, and one at position
-- -1 naming the member that completed it.
create table if not exists tarsk_chords (
    id         text        not null,
    idx        integer     not null,
    result     bytea       not null,
    created_at timestamptz not null default now(),
    primary key (id, idx)
);
create index if not exists tarsk_chords_age on tarsk_chords (created_at);

create table if not exists tarsk_paused (
    queue text primary key
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
alter table tarsk_dead add column if not exists job_id     text;
alter table tarsk_dead add column if not exists timeout_ms integer not null default 0;
alter table tarsk_dead add column if not exists chain      bytea;
alter table tarsk_dead add column if not exists meta       bytea;
alter table tarsk_dead add column if not exists expires_ms bigint not null default 0;
alter table tarsk_buckets add column if not exists took boolean not null default false;
-- A function because the cap needs lock, then count, then insert, and a single
-- statement cannot do that: its snapshot is taken before it waits for the
-- lock, so the count would not see the insert it had just waited behind. Each
-- statement in a plpgsql body gets a fresh one.
create or replace function tarsk_acquire_slot(
    p_task text, p_job text, p_max bigint, p_secs double precision
) returns boolean language plpgsql as $tarsk$
begin
    perform pg_advisory_xact_lock(hashtext('tarsk_slots:' || p_task));
    delete from tarsk_slots where expires_at <= now();
    if (select count(*) from tarsk_slots
         where task = p_task and job_id <> p_job) >= p_max then
        return false;
    end if;
    insert into tarsk_slots (task, job_id, expires_at)
    values (p_task, p_job, now() + make_interval(secs => p_secs))
    on conflict (task, job_id) do update set expires_at = excluded.expires_at;
    return true;
end
$tarsk$;
";

/// FNV-1a of the schema text: changes whenever the schema does, with nobody
/// having to remember to bump a number.
fn schema_stamp() -> i64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in PG_SCHEMA.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as i64
}

/// Claiming and reclaiming are the same statement: a lease that has run out is
/// indistinguishable from one that was never taken, so expiry needs no sweep.
const PG_CLAIM: &str = "
with picked as (
    -- One queue at a time, in the order they were given, and inside each queue
    -- two probes that can each use an index: jobs never leased, and jobs whose
    -- lease has run out. The single query this replaces filtered on
    -- `lease_until is null or lease_until < now()` and sorted the result, so
    -- every claim read and sorted the whole backlog — 126ms a claim with
    -- 200,000 jobs waiting, against 0.3ms for this.
    --
    -- `ready_at` is the moment the job became runnable: lease_until is when a
    -- delayed job came due or a lost lease lapsed, and a job never leased was
    -- runnable when written.
    --
    -- Priority before age: `--queues high,low` is an ordering. $4 is how many
    -- the caller can start at once, so what is taken is what runs now, and
    -- no batch of the wrong queue is left waiting behind it.
    --
    -- Both probes have to stay cheap on a table that has been busy, which is
    -- where they were not. A claimed row leaves a dead entry at the low end
    -- of the ready index until a vacuum takes it, and a probe that starts at
    -- the bottom walks every one of them: with 10,000 jobs done since the
    -- last vacuum a claim read 214 pages to hand out one row. So the first
    -- probe starts from $5, a little below the highest id this worker has
    -- already claimed from that queue, and the second walks the lease index
    -- in lease order, where the entries of finished jobs sit in the future
    -- until long after a vacuum has had them.
    select c.id, c.ready_at, q.ord, c.fresh
      from unnest($1::text[], $5::bigint[]) with ordinality as q(queue, floor, ord)
     cross join lateral (
           select * from (select id, created_at as ready_at, true as fresh from tarsk_jobs
                           where queue = q.queue and lease_until is null and id > q.floor
                           order by id for update skip locked limit $4) fresh
           union all
           select * from (select id, lease_until as ready_at, false as fresh from tarsk_jobs
                           where queue = q.queue and lease_until < now()
                           order by lease_until for update skip locked limit $4) due
           ) c
     order by q.ord, c.id
     limit $4
)
update tarsk_jobs set
    lease_until = now() + make_interval(secs => timeout_ms / 1000.0 + $2::double precision),
    attempt     = attempt + 1,
    run_lease   = run_lease + 1,
    claimed_by  = $3
from picked, (select set_config('synchronous_commit', 'off', true)) relaxed
where tarsk_jobs.id = picked.id
returning tarsk_jobs.id, name, payload, attempt, run_lease, job_id, picked.ready_at,
          chain, meta, expires_ms, picked.ord, timeout_ms, picked.fresh, tarsk_jobs.queue
";

/// How far below the highest id it has claimed a worker still looks.
///
/// Ids are handed out in order and committed in whatever order the inserts
/// finish, so a row can appear just below one already claimed. This many ids
/// of slack covers that; anything older is found by the full look below.
const FLOOR_SLACK: i64 = 1024;

/// How often a claim looks from the very bottom regardless.
///
/// A row handed back with no delay keeps its old id and is below every floor.
/// So is anything the slack did not cover. This is how long such a row can
/// wait while newer ones are taken ahead of it.
const FULL_LOOK: Duration = Duration::from_secs(1);

/// Why a claim and an ack do not wait for the disk.
///
/// `synchronous_commit` off, for that statement alone: the server answers
/// once the change is in its own memory and writes it out within a fraction
/// of a second. If the server crashes inside that window the change is lost,
/// and for these two that means a job is delivered again — a claim forgotten
/// leaves the job unclaimed while it runs, an ack forgotten leaves it to be
/// redelivered when its lease lapses. That is the at-least-once this queue
/// already promises, bought for half of what a job costs here. Everything
/// that must not be lost — a job sent, a result, a dead letter — still waits.
const RELAXED: &str = "(select set_config('synchronous_commit', 'off', true)) is not null";

/// The half of a Postgres connection that reads the socket. Named because it
/// is long, and because the listener needs to hold one itself.
type PgConnection = tokio_postgres::Connection<
    tokio_postgres::Socket,
    <tokio_postgres_rustls::MakeRustlsConnect as tokio_postgres::tls::MakeTlsConnect<
        tokio_postgres::Socket,
    >>::Stream,
>;

/// A connection, and the statements it has already prepared.
///
/// Handed SQL as text, the driver prepares it on every call: a parse, a plan
/// and a round trip of its own before the one that does the work. Every claim,
/// ack and send was paying that — on a local server, more than the statement
/// itself cost. A statement is prepared once per connection and kept.
///
/// The text is the key. Everything asked here is one of a few dozen fixed
/// strings, so the cache is bounded by the source and needs no eviction.
struct Db {
    client: tokio_postgres::Client,
    prepared: Mutex<HashMap<String, tokio_postgres::Statement>>,
}

type Params<'a> = &'a [&'a (dyn tokio_postgres::types::ToSql + Sync)];

impl Db {
    fn new(client: tokio_postgres::Client) -> Self {
        Db {
            client,
            prepared: Mutex::new(HashMap::new()),
        }
    }

    fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    async fn statement(
        &self,
        sql: &str,
    ) -> Result<tokio_postgres::Statement, tokio_postgres::Error> {
        if let Some(known) = self.prepared.lock().unwrap().get(sql) {
            return Ok(known.clone());
        }
        let made = self.client.prepare(sql).await?;
        self.prepared
            .lock()
            .unwrap()
            .insert(sql.to_string(), made.clone());
        Ok(made)
    }

    /// A statement that failed is forgotten, so the next call prepares it
    /// afresh: a plan can be invalidated under a connection, by a migration
    /// another worker ran, and a kept one would then fail for good.
    fn forget<T>(
        &self,
        sql: &str,
        outcome: Result<T, tokio_postgres::Error>,
    ) -> Result<T, tokio_postgres::Error> {
        if outcome.is_err() {
            self.prepared.lock().unwrap().remove(sql);
        }
        outcome
    }

    async fn execute(&self, sql: &str, params: Params<'_>) -> Result<u64, tokio_postgres::Error> {
        let statement = self.statement(sql).await?;
        self.forget(sql, self.client.execute(&statement, params).await)
    }

    async fn query(
        &self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
        let statement = self.statement(sql).await?;
        self.forget(sql, self.client.query(&statement, params).await)
    }

    async fn query_one(
        &self,
        sql: &str,
        params: Params<'_>,
    ) -> Result<tokio_postgres::Row, tokio_postgres::Error> {
        let statement = self.statement(sql).await?;
        self.forget(sql, self.client.query_one(&statement, params).await)
    }
}

pub struct PgBroker {
    /// Replaced when the server goes away. A `Client` whose connection has
    /// ended fails every query from then on, and nothing short of a new one
    /// brings it back.
    link: tokio::sync::Mutex<Arc<Db>>,
    url: String,
    pub(super) reconnects: AtomicU64,
    pub(super) wake: Arc<Wake>,
    pub(super) queues: Vec<String>,
    /// Queues this worker has been told not to take from. A copy of what the
    /// store says, refreshed on the timer that pulls cancellations, so a claim
    /// does not pay a round trip to learn that nothing is paused.
    pub(super) held: Mutex<std::collections::HashSet<String>>,
    /// Claimed in the same read as a job already handed out, and waiting for
    /// the calls that follow: each with when it was claimed and how long its
    /// lease runs, so one that waited too long is left for the sweep.
    claimed: Mutex<VecDeque<(Delivery, std::time::Instant, u64)>>,
    /// The highest id claimed fresh from each queue, and when a claim last
    /// looked from the bottom. See `PG_CLAIM`.
    floors: Mutex<(HashMap<String, i64>, std::time::Instant)>,
    stores: AtomicU64,
    /// The same identity Redis gets for free from its consumer group. Postgres
    /// has no equivalent, so a claim writes it down.
    consumer: String,
}

impl PgBroker {
    async fn dial(url: &str) -> Res<(tokio_postgres::Client, PgConnection)> {
        // A TLS connector is always supplied, which is not the same as always
        // using TLS: tokio-postgres reads sslmode from the URL and defaults to
        // `prefer`, so a local server without TLS still connects and a managed
        // one gets an encrypted link. Before this, `sslmode=require` failed
        // with "error performing TLS handshake" — loudly, at least, rather than
        // quietly in the clear, but it meant no managed Postgres at all.
        // The system trust store, so `sslmode=verify-full` has roots to check
        // against. Individual certificates that fail to load are ignored the
        // way every other client does: one unparseable file in /etc/ssl should
        // not stop a connection the rest of the store can verify.
        let (tls, _ignored) = tokio_postgres_rustls::MakeRustlsConnect::with_native_certs()
            .map_err(|errors| format!("could not read the system certificate store: {errors:?}"))?;
        // tokio-postgres knows disable, prefer and require, and rejects the
        // whole string on verify-ca or verify-full — which is what a managed
        // provider's copy-paste connection string usually says. They are mapped
        // to require rather than refused, and that is not a downgrade: rustls
        // checks the chain and the hostname on every handshake, so what this
        // driver calls require is what libpq calls verify-full.
        let url = url
            .replace("sslmode=verify-full", "sslmode=require")
            .replace("sslmode=verify-ca", "sslmode=require");
        let (client, connection) = tokio_postgres::connect(&url, tls).await?;
        Ok((client, connection))
    }

    async fn open(url: &str) -> Res<tokio_postgres::Client> {
        let (client, connection) = Self::dial(url).await?;
        // The connection future drives the socket; dropping it closes the link.
        tokio::spawn(async move {
            let _ = connection.await;
        });
        // The DDL takes table locks every running worker has to wait behind,
        // so it runs when the schema text has changed and not on every
        // connection. A missing table is an error here, and means "never set
        // up" as surely as a missing row does.
        let stamp = schema_stamp();
        let current = client
            .query_opt("select 1 from tarsk_schema where stamp = $1", &[&stamp])
            .await;
        if !matches!(current, Ok(Some(_))) {
            client.batch_execute(PG_SCHEMA).await?;
            client
                .execute(
                    "insert into tarsk_schema (stamp) values ($1) on conflict do nothing",
                    &[&stamp],
                )
                .await?;
        }
        Ok(client)
    }

    pub(super) async fn connect(url: &str, queues: Vec<String>) -> Res<PgBroker> {
        Ok(PgBroker {
            consumer: format!("tarsk-{}", std::process::id()),
            link: tokio::sync::Mutex::new(Arc::new(Db::new(Self::open(url).await?))),
            url: url.to_string(),
            reconnects: AtomicU64::new(0),
            wake: Arc::default(),
            held: Mutex::new(std::collections::HashSet::new()),
            claimed: Mutex::new(VecDeque::new()),
            floors: Mutex::new((HashMap::new(), std::time::Instant::now())),
            queues,
            stores: AtomicU64::new(0),
        })
    }

    /// Start hearing NOTIFY, once, for as long as this broker lives.
    ///
    /// A connection of its own: a session that has said LISTEN receives
    /// notifications between its queries, and the one the queries go down is
    /// not read from unless a query is waiting. Reconnects by itself, and
    /// while it is down `wake.live` is false and waiters go back to polling.
    pub(super) fn listen(&self) {
        use std::future::Future;
        use std::task::Poll;
        if self.wake.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let (url, wake) = (self.url.clone(), self.wake.clone());
        tokio::spawn(async move {
            loop {
                if let Ok((client, mut connection)) = Self::dial(&url).await {
                    // LISTEN is a query like any other, and only makes
                    // progress while the connection is being read — which is
                    // this same loop. So both are driven from one poll.
                    let asked = client.batch_execute("listen tarsk_jobs; listen tarsk_results");
                    tokio::pin!(asked);
                    let mut listening = false;
                    loop {
                        let message = std::future::poll_fn(|cx| {
                            if !listening {
                                match asked.as_mut().poll(cx) {
                                    Poll::Ready(Ok(())) => {
                                        listening = true;
                                        wake.live.store(true, Ordering::Relaxed);
                                    }
                                    Poll::Ready(Err(_)) => return Poll::Ready(None),
                                    Poll::Pending => {}
                                }
                            }
                            connection.poll_message(cx)
                        })
                        .await;
                        match message {
                            Some(Ok(tokio_postgres::AsyncMessage::Notification(said))) => {
                                if said.channel() == "tarsk_jobs" {
                                    wake.jobs.notify_waiters();
                                } else {
                                    wake.results.notify_waiters();
                                }
                            }
                            Some(Ok(_)) => {}
                            Some(Err(_)) | None => break,
                        }
                    }
                }
                wake.live.store(false, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    /// The live client, reconnecting first if the last one died.
    ///
    /// Held behind a lock so a restart is met with one reconnect rather than
    /// one per waiting caller. While the server is still down this fails like
    /// any other query, and the next call tries again.
    async fn db(&self) -> Res<Arc<Db>> {
        let mut link = self.link.lock().await;
        if link.is_closed() {
            *link = Arc::new(Db::new(Self::open(&self.url).await?));
            self.reconnects.fetch_add(1, Ordering::Relaxed);
        }
        Ok(link.clone())
    }

    /// A delay needs no schedule table: the lease column is already a
    /// visibility timer, so a row leased into the future is simply not claimed
    /// until then.
    pub(super) async fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        self.db()
            .await?
            .execute(
                // The NOTIFY rides in the same statement, so it costs no round
                // trip and is delivered when the row is: a worker waiting for
                // work is told there is some, instead of asking forty times a
                // second whether there might be.
                "with queued as (
                     insert into tarsk_jobs
                         (job_id, queue, name, payload, timeout_ms, lease_until, chain, meta,
                          expires_ms)
                     values ($6, $1, $2, $3, $4, case when $5::double precision > 0
                         then now() + make_interval(secs => $5::double precision) else null end,
                         $7, $8, $9)
                     returning 1)
                 select pg_notify('tarsk_jobs', '') from queued",
                &[
                    &job.queue,
                    &job.name,
                    &job.payload,
                    &(job.timeout_ms as i32),
                    &delay.as_secs_f64(),
                    &job.id,
                    &job.chain,
                    &job.meta,
                    &(job.expires_ms as i64),
                ],
            )
            .await?;
        Ok(())
    }

    /// The queues this worker serves, less the paused ones, in their order.
    fn active(&self) -> Vec<String> {
        let held = self.held.lock().unwrap();
        self.queues
            .iter()
            .filter(|queue| !held.contains(*queue))
            .cloned()
            .collect()
    }

    pub(super) async fn set_paused(&self, queue: &str, paused: bool) -> Res<()> {
        let sql = if paused {
            "insert into tarsk_paused (queue) values ($1) on conflict do nothing"
        } else {
            "delete from tarsk_paused where queue = $1"
        };
        self.db().await?.execute(sql, &[&queue]).await?;
        Ok(())
    }

    pub(super) async fn paused(&self) -> Res<Vec<String>> {
        let rows = self
            .db()
            .await?
            .query("select queue from tarsk_paused", &[])
            .await?;
        Ok(rows.iter().map(|row| row.get(0)).collect())
    }

    pub(super) async fn push_many(&self, jobs: Vec<NewJob>) -> Res<()> {
        // One statement, so one transaction: all the rows or none. Columns
        // travel as arrays and are unzipped by the server, which keeps this a
        // single prepared statement whatever the batch size.
        let mut ids = Vec::with_capacity(jobs.len());
        let mut queues = Vec::with_capacity(jobs.len());
        let mut names = Vec::with_capacity(jobs.len());
        let mut payloads = Vec::with_capacity(jobs.len());
        let mut timeouts = Vec::with_capacity(jobs.len());
        let mut chains = Vec::with_capacity(jobs.len());
        let mut metas = Vec::with_capacity(jobs.len());
        let mut expires = Vec::with_capacity(jobs.len());
        for job in jobs {
            ids.push(job.id);
            queues.push(job.queue);
            names.push(job.name);
            payloads.push(job.payload);
            timeouts.push(job.timeout_ms as i32);
            chains.push(job.chain);
            metas.push(job.meta);
            expires.push(job.expires_ms as i64);
        }
        self.db()
            .await?
            .execute(
                "with queued as (
                     insert into tarsk_jobs
                         (job_id, queue, name, payload, timeout_ms, chain, meta, expires_ms)
                     select * from unnest($1::text[], $2::text[], $3::text[], $4::bytea[],
                                          $5::int[], $6::bytea[], $7::bytea[], $8::bigint[])
                     returning 1)
                 select pg_notify('tarsk_jobs', '') from (select count(*) from queued) once",
                &[
                    &ids, &queues, &names, &payloads, &timeouts, &chains, &metas, &expires,
                ],
            )
            .await?;
        Ok(())
    }

    pub(super) async fn claim(
        &self,
        grace: Duration,
        block: Duration,
        room: usize,
    ) -> Res<Option<Delivery>> {
        // What an earlier read took beyond the one job it handed back.
        loop {
            let head = self.claimed.lock().unwrap().pop_front();
            match head {
                Some((_, claimed_at, lease_ms))
                    if claimed_at.elapsed().as_millis() as u64 >= lease_ms =>
                {
                    continue; // its lease may have lapsed: the sweep's to redeliver
                }
                Some((delivery, _, _)) => return Ok(Some(delivery)),
                None => break,
            }
        }
        let room = room.clamp(1, 256) as i64;
        // ponytail: polls. LISTEN/NOTIFY is the upgrade and would cut idle
        // pickup latency to the round trip; it costs a dedicated connection to
        // hold the notification stream, which is not worth it until someone
        // measures the wait.
        self.listen();
        let deadline = tokio::time::Instant::now() + block;
        loop {
            // Armed before the query, so a job queued between the query
            // coming back empty and the wait beginning still ends the wait.
            let queued = self.wake.jobs.notified();
            tokio::pin!(queued);
            queued.as_mut().enable();
            let queues = self.active();
            if queues.is_empty() {
                // Everything this worker serves is paused.
                tokio::time::sleep(block).await;
                return Ok(None);
            }
            let rows = self
                .db()
                .await?
                .query(
                    PG_CLAIM,
                    &[
                        &queues,
                        &grace.as_secs_f64(),
                        &self.consumer,
                        &room,
                        &self.floors_for(&queues),
                    ],
                )
                .await?;
            // RETURNING promises no order, and the order is the priority.
            let mut rows = rows;
            rows.sort_by_key(|row| (row.get::<_, i64>(10), row.get::<_, i64>(0)));
            let now = std::time::Instant::now();
            let mut first = None;
            for row in &rows {
                let run_lease: i64 = row.get(4);
                let ready: std::time::SystemTime = row.get(6);
                let lease_ms = row.get::<_, i32>(11).max(1) as u64;
                if row.get::<_, bool>(12) {
                    let mut floors = self.floors.lock().unwrap();
                    let top = floors.0.entry(row.get(13)).or_insert(0);
                    *top = (*top).max(row.get::<_, i64>(0));
                }
                let delivery = Delivery {
                    id: row.get(5),
                    name: row.get(1),
                    payload: row.get(2),
                    attempt: row.get::<_, i32>(3) as u32,
                    ready_at_ms: ready
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                    chain: row.get::<_, Option<Vec<u8>>>(7).unwrap_or_default(),
                    meta: row.get::<_, Option<Vec<u8>>>(8).unwrap_or_default(),
                    expires_ms: row.get::<_, i64>(9).max(0) as u64,
                    receipt: Receipt::Postgres {
                        row: row.get(0),
                        run_lease,
                    },
                };
                if first.is_none() {
                    first = Some(delivery);
                } else {
                    self.claimed
                        .lock()
                        .unwrap()
                        .push_back((delivery, now, lease_ms));
                }
            }
            if first.is_some() {
                return Ok(first);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }
            // Told when work arrives, so there is nothing to poll for. What a
            // notification cannot announce is a job coming due — a delay
            // running out, a lease lapsing — and those are found when the
            // block ends and the caller asks again. With no listener, the
            // short poll this replaced.
            let listening = self.wake.live.load(Ordering::Relaxed);
            let nap = if listening {
                deadline - now
            } else {
                Duration::from_millis(25).min(block)
            };
            tokio::select! {
                _ = &mut queued => {}
                _ = tokio::time::sleep(nap) => {
                    if listening {
                        // The block is over and nothing was announced. The
                        // caller asks again at once, and that is the look for
                        // jobs that came due; a second one here is wasted.
                        return Ok(None);
                    }
                }
            }
        }
    }

    pub(super) async fn ack(&self, receipt: &Receipt) -> Res<()> {
        let Receipt::Postgres { row, run_lease } = receipt else {
            return Err("not a postgres receipt".into());
        };
        // The run_lease guard: nothing happens if this job was reclaimed and
        // re-run while we were away.
        self.db()
            .await?
            .execute(
                &format!("delete from tarsk_jobs where id = $1 and run_lease = $2 and {RELAXED}"),
                &[row, run_lease],
            )
            .await?;
        Ok(())
    }

    /// Where each queue's first probe starts: just below the highest id this
    /// worker has claimed from it, or the bottom when a full look is due.
    fn floors_for(&self, queues: &[String]) -> Vec<i64> {
        let mut floors = self.floors.lock().unwrap();
        if floors.1.elapsed() >= FULL_LOOK {
            floors.1 = std::time::Instant::now();
            return vec![0; queues.len()];
        }
        queues
            .iter()
            .map(|queue| {
                floors
                    .0
                    .get(queue)
                    .map_or(0, |top| (top - FLOOR_SLACK).max(0))
            })
            .collect()
    }

    /// `ack` for many rows, as one statement and one commit.
    pub(super) async fn ack_many(&self, receipts: &[&Receipt]) -> Res<()> {
        let mut rows = Vec::with_capacity(receipts.len());
        let mut leases = Vec::with_capacity(receipts.len());
        for receipt in receipts {
            let Receipt::Postgres { row, run_lease } = receipt else {
                return Err("not a postgres receipt".into());
            };
            rows.push(*row);
            leases.push(*run_lease);
        }
        self.db()
            .await?
            .execute(
                &format!(
                    "delete from tarsk_jobs job
                      using unnest($1::bigint[], $2::bigint[]) as done(id, run_lease)
                      where job.id = done.id and job.run_lease = done.run_lease and {RELAXED}"
                ),
                &[&rows, &leases],
            )
            .await?;
        Ok(())
    }

    /// Hand back what a read took and no call came for: unleased, and with
    /// the attempt the claim charged returned, since none was spent.
    pub(super) async fn release_unstarted(&self) {
        let held: Vec<Delivery> = self
            .claimed
            .lock()
            .unwrap()
            .drain(..)
            .map(|(delivery, _, _)| delivery)
            .collect();
        for delivery in held {
            let _ = self.requeue_as(&delivery.receipt, Duration::ZERO, 1).await;
        }
    }

    /// The lease column doubles as the visibility timer: a row leased into the
    /// future is invisible to `claim` until then, so backoff needs no separate
    /// schedule. Zero delay clears it and the row is claimable immediately.
    pub(super) async fn requeue(&self, receipt: &Receipt, delay: Duration) -> Res<()> {
        self.requeue_as(receipt, delay, 0).await
    }

    /// `refund` is how many attempts to give back: one for a deferral, which
    /// the claim counted and the job did not spend. See `Broker::defer`.
    pub(super) async fn requeue_as(
        &self,
        receipt: &Receipt,
        delay: Duration,
        refund: i32,
    ) -> Res<()> {
        let Receipt::Postgres { row, run_lease } = receipt else {
            return Err("not a postgres receipt".into());
        };
        self.db()
            .await?
            .execute(
                "update tarsk_jobs set lease_until = case when $3::double precision > 0
                     then now() + make_interval(secs => $3::double precision) else null end,
                     attempt = greatest(attempt - $4, 0)
                 where id = $1 and run_lease = $2",
                &[row, run_lease, &delay.as_secs_f64(), &refund],
            )
            .await?;
        Ok(())
    }

    pub(super) async fn chord_done(
        &self,
        chord: &str,
        index: u64,
        total: u64,
        result: &[u8],
    ) -> Res<Option<Vec<Vec<u8>>>> {
        let db = self.db().await?;
        let index = index as i32;
        db.execute(
            "insert into tarsk_chords (id, idx, result) values ($1, $2, $3)
             on conflict (id, idx) do nothing",
            &[&chord, &index, &result],
        )
        .await?;
        let members: i64 = db
            .query_one(
                "select count(*) from tarsk_chords where id = $1 and idx >= 0",
                &[&chord],
            )
            .await?
            .get(0);
        if (members as u64) < total {
            return Ok(None);
        }
        // The primary key is the election: one member writes the -1 row, and
        // the rest find somebody else's position in it.
        let marker = index.to_string().into_bytes();
        db.execute(
            "insert into tarsk_chords (id, idx, result) values ($1, -1, $2)
             on conflict (id, idx) do nothing",
            &[&chord, &marker],
        )
        .await?;
        let fired: Vec<u8> = db
            .query_one(
                "select result from tarsk_chords where id = $1 and idx = -1",
                &[&chord],
            )
            .await?
            .get(0);
        if fired != marker {
            return Ok(None);
        }
        let rows = db
            .query(
                "select result from tarsk_chords where id = $1 and idx >= 0 order by idx",
                &[&chord],
            )
            .await?;
        // Swept here, where a chord has just finished, rather than on a timer.
        db.execute(
            "delete from tarsk_chords where created_at < now() - interval '1 day'",
            &[],
        )
        .await?;
        Ok(Some(rows.iter().map(|row| row.get(0)).collect()))
    }

    pub(super) async fn claim_run(&self, name: &str, due: i64, now: i64) -> Res<bool> {
        let db = self.db().await?;
        // First sight: start the record just behind the present. See
        // `Broker::claim_run`.
        db.execute(
            "insert into tarsk_cron_runs (name, minute) values ($1, $2)
             on conflict (name) do nothing",
            &[&name, &(now - 1)],
        )
        .await?;
        // The row lock is the election: the second writer waits, then finds
        // the minute already moved and updates nothing.
        let moved = db
            .execute(
                "update tarsk_cron_runs set minute = $2 where name = $1 and minute < $2",
                &[&name, &due],
            )
            .await?;
        Ok(moved == 1)
    }

    pub(super) async fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        self.db()
            .await?
            .execute(
                "with stored as (
                     insert into tarsk_results (id, blob, expires_at)
                     values ($1, $2, now() + make_interval(secs => $3::double precision))
                     on conflict (id) do update set blob = excluded.blob,
                                                    expires_at = excluded.expires_at
                     returning 1)
                 select pg_notify('tarsk_results', '') from stored",
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
            self.db()
                .await?
                .execute("delete from tarsk_results where expires_at < now()", &[])
                .await?;
        }
        Ok(())
    }

    pub(super) async fn get_result(&self, id: &str) -> Res<Option<Vec<u8>>> {
        let rows = self
            .db()
            .await?
            .query(
                "select blob from tarsk_results where id = $1 and expires_at > now()",
                &[&id],
            )
            .await?;
        Ok(rows.first().map(|row| row.get(0)))
    }

    /// One statement, so the row cannot be in both tables or neither.
    pub(super) async fn jobs(&self, queues: &[String], limit: usize) -> Res<Vec<Listed>> {
        // The same three states the depth query counts, named here instead.
        // extract(epoch) is milliseconds since the row became relevant:
        // positive for time waited, negative for time still to wait.
        let rows = self
            .db()
            .await?
            .query(
                "select job_id, queue, name, attempt,
                        case when lease_until is null or lease_until < now() then 'ready'
                             when run_lease > 0 then 'running'
                             else 'delayed' end as state,
                        (extract(epoch from
                            now() - coalesce(lease_until, created_at)) * 1000)::bigint as age_ms,
                        coalesce(claimed_by, '') as worker, meta
                   from tarsk_jobs
                  where queue = any($1)
                  order by id
                  limit $2",
                &[&queues, &(limit as i64)],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| Listed {
                id: r.get(0),
                queue: r.get(1),
                name: r.get(2),
                attempt: r.get::<_, i32>(3).max(0) as u32,
                state: match r.get::<_, &str>(4) {
                    "running" => "running",
                    "delayed" => "delayed",
                    _ => "ready",
                },
                age_ms: r.get(5),
                worker: if r.get::<_, &str>(4) == "running" {
                    r.get(6)
                } else {
                    String::new()
                },
                meta: r.get::<_, Option<Vec<u8>>>(7).unwrap_or_default(),
            })
            .collect())
    }

    pub(super) async fn depth(&self, queues: &[String]) -> Res<Vec<Depth>> {
        // run_lease separates the two kinds of future lease_until: a delayed
        // job has never been claimed, an in-flight one has. Without it the
        // backlog and the work in progress look identical.
        let rows = self
            .db()
            .await?
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
            .db()
            .await?
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

    pub(super) async fn claim_dedup(
        &self,
        key: &str,
        job_id: &str,
        ttl_ms: u64,
    ) -> Res<Option<String>> {
        let rows = self
            .db()
            .await?
            .query(
                // The upsert is the reservation. `do update … where` reads the
                // row as it is now, under its lock, so an expired window is
                // taken over by exactly one caller and a live one is left
                // alone — which a separate delete of expired rows could not
                // promise, since the insert beside it never saw the delete.
                "insert into tarsk_dedup as d (key, job_id, expires_at)
                 values ($1, $2, now() + make_interval(secs => $3))
                 on conflict (key) do update
                    set job_id = excluded.job_id, expires_at = excluded.expires_at
                  where d.expires_at <= now()
                 returning job_id",
                &[&key, &job_id, &(ttl_ms.max(1) as f64 / 1000.0)],
            )
            .await?;
        if !rows.is_empty() {
            return Ok(None); // inserted, so this caller owns the window
        }
        let held = self
            .db()
            .await?
            .query(
                "select job_id from tarsk_dedup where key = $1 and expires_at > now()",
                &[&key],
            )
            .await?;
        Ok(held.first().map(|r| r.get(0)))
    }

    pub(super) async fn release_dedup(&self, key: &str, job_id: &str) -> Res<()> {
        self.db()
            .await?
            .execute(
                "delete from tarsk_dedup where key = $1 and job_id = $2",
                &[&key, &job_id],
            )
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
        // Lock, prune, count and insert, indivisibly — see the function in
        // PG_SCHEMA for why that is a function rather than one statement. A
        // redelivery renews its own slot rather than being refused by it.
        let row = self
            .db()
            .await?
            .query_one(
                "select tarsk_acquire_slot($1, $2, $3, $4)",
                &[
                    &task,
                    &job_id,
                    &(max as i64),
                    &(lease_ms.max(1) as f64 / 1000.0),
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    pub(super) async fn release_slot(&self, task: &str, job_id: &str) -> Res<()> {
        self.db()
            .await?
            .execute(
                "delete from tarsk_slots where task = $1 and job_id = $2",
                &[&task, &job_id],
            )
            .await?;
        Ok(())
    }

    pub(super) async fn take_token(&self, task: &str, per_sec: f64, burst: u32) -> Res<u64> {
        // The refill is computed inside `do update`, from the row as it is
        // under its lock. Reading the bucket first and writing back a number
        // worked out from that read let two workers both see the last token.
        // `took` says whether this call got one, which the level after the
        // take cannot: 0.4 left is either a refusal or the change from 1.4.
        let row = self
            .db()
            .await?
            .query_one(
                "insert into tarsk_buckets as b (task, tokens, seen, took)
                 values ($1, $2 - 1::float8, now(), true)
                 on conflict (task) do update set (tokens, took, seen) = (
                     select case when level >= 1 then level - 1 else level end,
                            level >= 1,
                            now()
                       from (select least($2,
                                b.tokens + greatest(0::float8,
                                    extract(epoch from now() - b.seen)::float8) * $3
                             ) as level) refill)
                 returning took, tokens",
                &[&task, &(burst as f64), &per_sec],
            )
            .await?;
        if row.get::<_, bool>(0) {
            Ok(0)
        } else {
            let level: f64 = row.get(1);
            Ok(((1.0 - level) / per_sec * 1000.0).ceil() as u64)
        }
    }

    pub(super) async fn revoke(&self, queue: &str, id: &str, ttl_ms: u64) -> Res<()> {
        self.db()
            .await?
            .execute(
                "insert into tarsk_revoked (id, queue, expires_at)
                 values ($1, $2, now() + make_interval(secs => $3))
                 on conflict (id) do update
                    set expires_at = excluded.expires_at, queue = excluded.queue",
                &[&id, &queue, &(ttl_ms as f64 / 1000.0)],
            )
            .await?;
        Ok(())
    }

    pub(super) async fn revoked_all(&self) -> Res<Vec<String>> {
        self.db()
            .await?
            .execute("delete from tarsk_revoked where expires_at <= now()", &[])
            .await?;
        // Every id, whatever queue the caller named: an id is unique on its
        // own, and filtering by queue made a cancel with the wrong one a
        // success that changed nothing.
        let rows = self
            .db()
            .await?
            .query("select id from tarsk_revoked", &[])
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }

    pub(super) async fn dead_list(&self, queue: &str, limit: usize) -> Res<Vec<Dead>> {
        let rows = self
            .db()
            .await?
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

    pub(super) async fn dead_replay(&self, queue: &str, ids: &[String]) -> Res<usize> {
        // Back onto the live table with the lease cleared, inside one statement
        // so nothing can observe a job in both places.
        let picked: Vec<i64> = ids.iter().filter_map(|i| i.parse().ok()).collect();
        // A fresh row rather than the old id: bigserial hands out the next one,
        // and reusing a primary key that a result or a log still refers to
        // would quietly merge two runs of the same job into one identity.
        let sql = "with gone as (
                       delete from tarsk_dead
                        where queue = $1 and ($2::bigint[] = '{}' or id = any($2))
                    returning job_id, queue, name, payload, timeout_ms, chain, meta,
                              expires_ms)
                   insert into tarsk_jobs
                       (job_id, queue, name, payload, timeout_ms, attempt, lease_until,
                        chain, meta, expires_ms)
                   select coalesce(job_id, ''), queue, name, payload, timeout_ms, 0, null,
                          chain, meta, expires_ms
                     from gone";
        let n = self.db().await?.execute(sql, &[&queue, &picked]).await?;
        Ok(n as usize)
    }

    pub(super) async fn dead_purge(&self, queue: &str, ids: &[String]) -> Res<usize> {
        let picked: Vec<i64> = ids.iter().filter_map(|i| i.parse().ok()).collect();
        let n = self
            .db()
            .await?
            .execute(
                "delete from tarsk_dead
                  where queue = $1 and ($2::bigint[] = '{}' or id = any($2))",
                &[&queue, &picked],
            )
            .await?;
        Ok(n as usize)
    }

    pub(super) async fn dead_letter(
        &self,
        receipt: &Receipt,
        error: &str,
        traceback: &str,
        keep: u64,
    ) -> Res<()> {
        let Receipt::Postgres { row, run_lease } = receipt else {
            return Err("not a postgres receipt".into());
        };
        self.db()
            .await?
            .execute(
                "with gone as (
                     delete from tarsk_jobs where id = $1 and run_lease = $2
                     returning id, job_id, queue, name, payload, timeout_ms, attempt,
                               chain, meta, expires_ms
                 )
                 insert into tarsk_dead
                     (id, job_id, queue, name, payload, timeout_ms, attempt, error, traceback,
                      chain, meta, expires_ms)
                 select id, job_id, queue, name, payload, timeout_ms, attempt, $3, $4,
                        chain, meta, expires_ms
                   from gone
                 on conflict (id) do nothing",
                &[row, run_lease, &error, &traceback],
            )
            .await?;
        if keep > 0 {
            // Trimmed on write like the Redis side, rather than on a timer:
            // the row that pushes a queue over its cap is the one that should
            // pay for the trim, and nothing else has to remember to run.
            self.db()
                .await?
                .execute(
                    "delete from tarsk_dead where id in (
                         select id from tarsk_dead
                          where queue = (select queue from tarsk_dead where id = $1)
                          order by died_at desc offset $2)",
                    &[row, &(keep as i64)],
                )
                .await?;
        }
        Ok(())
    }
}
