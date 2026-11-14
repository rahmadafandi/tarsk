//! The in-process broker, and the coordination state a broker without a shared
//! store keeps in-process.

use super::*;

/// What each member of a chord returned, by its position, and which member
/// completed it.
type ChordState = (HashMap<u64, Vec<u8>>, Option<u64>);

/// The coordination state a broker without a shared store keeps in-process:
/// rate-limit buckets, concurrency slots, and the once-per-minute cron tick.
///
/// Two brokers embed this. For `memory://` in-process *is* the whole world,
/// so these simply are the feature. For AMQP they are the per-worker
/// degradation the docs describe — correct alone, one copy per worker across
/// many — and one implementation means the bucket arithmetic cannot drift
/// between the two.
#[derive(Default)]
pub(super) struct LocalCoordination {
    /// The last minute each schedule was fired for.
    pub(super) runs: Mutex<HashMap<String, i64>>,
    /// chord id → (what each member returned, by its position; which member
    /// completed it). See `Broker::chord_done`.
    chords: Mutex<HashMap<String, ChordState>>,
    buckets: Mutex<HashMap<String, (f64, std::time::Instant)>>,
    slots: Mutex<HashMap<String, HashMap<String, std::time::Instant>>>,
}

impl LocalCoordination {
    pub(super) fn take_token(&self, task: &str, per_sec: f64, burst: u32) -> u64 {
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

    pub(super) fn acquire_slot(&self, task: &str, job_id: &str, cap: u32, lease_ms: u64) -> bool {
        let now = std::time::Instant::now();
        let mut slots = self.slots.lock().unwrap();
        let held = slots.entry(task.to_string()).or_default();
        held.retain(|_, deadline| *deadline > now);
        if held.len() >= cap as usize {
            return false;
        }
        held.insert(
            job_id.to_string(),
            now + Duration::from_millis(lease_ms.max(1)),
        );
        true
    }

    pub(super) fn release_slot(&self, task: &str, job_id: &str) {
        if let Some(held) = self.slots.lock().unwrap().get_mut(task) {
            held.remove(job_id);
        }
    }

    pub(super) fn chord_done(
        &self,
        chord: &str,
        index: u64,
        total: u64,
        result: &[u8],
    ) -> Option<Vec<Vec<u8>>> {
        let mut chords = self.chords.lock().unwrap();
        let (results, fired) = chords.entry(chord.to_string()).or_default();
        results.entry(index).or_insert_with(|| result.to_vec());
        if (results.len() as u64) < total {
            return None;
        }
        // Complete. The member that completed it is remembered, so that if it
        // is delivered again it is told the same thing — and nobody else is.
        if fired.is_some_and(|by| by != index) {
            return None;
        }
        *fired = Some(index);
        Some(
            (0..total)
                .map(|at| results.get(&at).cloned().unwrap_or_default())
                .collect(),
        )
    }

    pub(super) fn claim_run(&self, name: &str, due: i64, now: i64) -> bool {
        let mut runs = self.runs.lock().unwrap();
        // Never seen before: start the record just behind the present, so a
        // schedule that is new here does not fire for a minute it was never
        // registered for. See `Broker::claim_run`.
        let last = runs.entry(name.to_string()).or_insert(now - 1);
        if due > *last {
            *last = due;
            true
        } else {
            false
        }
    }
}

/// Backs the batch API and the test suite. Nothing durable: a crash loses the
/// queue, which is the honest trade for having no broker to run.
#[derive(Default)]
pub struct MemoryBroker {
    ready: Mutex<VecDeque<(u64, Option<std::time::Instant>)>>,
    /// job -> (record, attempts, the epoch-ms it became runnable). The third
    /// field is what expiry measures from; leaving it zero made `expires`
    /// silently unenforceable on this broker, since a zero ready time reads
    /// as "unknown" and no expiry check treats unknown as expired.
    pub(super) jobs: Mutex<HashMap<u64, (NewJob, u32, u64)>>,
    next_id: AtomicU64,
    results: Mutex<HashMap<String, (Vec<u8>, std::time::Instant)>>,
    pub(super) revoked: Mutex<HashMap<String, std::time::Instant>>,
    /// Queues nothing may be taken from. Here the store and the worker's copy
    /// of it are the same thing.
    pub(super) held: Mutex<std::collections::HashSet<String>>,
    /// Makes `push` fail, for the tests of what happens when an enqueue does.
    /// This broker cannot fail one on its own, and the real ones can.
    #[cfg(test)]
    pub(crate) refuse_pushes: std::sync::atomic::AtomicBool,
    pub(super) local: LocalCoordination,
    pub(super) wake: Arc<Wake>,
}

impl MemoryBroker {
    pub(super) fn revoke(&self, id: &str, ttl_ms: u64) -> Res<()> {
        let until = std::time::Instant::now() + Duration::from_millis(ttl_ms);
        self.revoked.lock().unwrap().insert(id.to_string(), until);
        Ok(())
    }

    pub(super) fn revoked(&self) -> Vec<String> {
        let now = std::time::Instant::now();
        let mut held = self.revoked.lock().unwrap();
        held.retain(|_, until| *until > now);
        held.keys().cloned().collect()
    }

    pub(super) fn push(&self, job: NewJob, delay: Duration) -> Res<()> {
        #[cfg(test)]
        if self.refuse_pushes.load(Ordering::SeqCst) {
            return Err("push refused".into());
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let ready_ms = now_ms() + delay.as_millis() as u64;
        self.jobs.lock().unwrap().insert(id, (job, 0, ready_ms));
        let due = (!delay.is_zero()).then(|| std::time::Instant::now() + delay);
        self.ready.lock().unwrap().push_back((id, due));
        Ok(())
    }

    pub(super) fn claim(&self) -> Option<Delivery> {
        let held = self.held.lock().unwrap().clone();
        let mut jobs = self.jobs.lock().unwrap();
        let id = {
            let mut ready = self.ready.lock().unwrap();
            let now = std::time::Instant::now();
            let position = ready.iter().position(|(id, due)| {
                due.is_none_or(|d| d <= now)
                    && (held.is_empty()
                        || jobs
                            .get(id)
                            .is_none_or(|(job, _, _)| !held.contains(&job.queue)))
            })?;
            ready.remove(position)?.0
        };
        let (job, attempt, ready_ms) = jobs.get_mut(&id)?;
        *attempt += 1;
        Some(Delivery {
            name: job.name.clone(),
            payload: job.payload.clone(),
            attempt: *attempt,
            receipt: Receipt::Memory { id },
            id: job.id.clone(),
            ready_at_ms: *ready_ms,
            expires_ms: job.expires_ms,
            chain: job.chain.clone(),
            meta: job.meta.clone(),
        })
    }

    pub(super) fn store_result(&self, id: &str, blob: Vec<u8>, ttl: Duration) -> Res<()> {
        self.results
            .lock()
            .unwrap()
            .insert(id.to_string(), (blob, std::time::Instant::now() + ttl));
        // In one process the store and the announcement are the same act.
        self.wake.live.store(true, Ordering::Relaxed);
        self.wake.results.notify_waiters();
        Ok(())
    }

    pub(super) fn get_result(&self, id: &str) -> Option<Vec<u8>> {
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

    pub(super) fn settle(&self, id: u64) -> Res<()> {
        self.jobs.lock().unwrap().remove(&id);
        Ok(())
    }

    pub(super) fn defer(&self, id: u64, delay: Duration) -> Res<()> {
        if let Some((_, attempt, _)) = self.jobs.lock().unwrap().get_mut(&id) {
            *attempt = attempt.saturating_sub(1);
        }
        self.requeue(id, delay)
    }

    pub(super) fn requeue(&self, id: u64, delay: Duration) -> Res<()> {
        if self.jobs.lock().unwrap().contains_key(&id) {
            let due = (!delay.is_zero()).then(|| std::time::Instant::now() + delay);
            self.ready.lock().unwrap().push_back((id, due));
        }
        Ok(())
    }
}
