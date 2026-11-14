//! Prometheus exposition, entirely inside the supervisor.
//!
//! "Zero Python overhead" (spec §2) is literal: every counter is incremented in
//! Rust on paths that already exist, and the scrape is answered by the tokio
//! runtime with the GIL released. A Python worker never learns it is being
//! observed.
//!
//! The HTTP is hand-written. A metrics endpoint is one path, one method, and a
//! response that closes the connection; a framework to express that would be
//! larger than the thing it expresses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// Seconds. Wide because task durations here span an IPC round trip to a
/// handler that may legitimately run for minutes (spec §5, §9).
const BUCKETS: [f64; 15] = [
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    300.0,
    f64::INFINITY,
];

#[derive(Default)]
pub struct Metrics {
    /// task name → [succeeded, failed]. Bounded by the registry, so the label
    /// cannot explode the way a per-argument label would.
    tasks: Mutex<HashMap<String, [u64; 2]>>,
    histogram: Mutex<[u64; BUCKETS.len()]>,
    duration_sum: Mutex<f64>,
    duration_count: AtomicU64,
    /// Last RSS reading per live child, from the supervision loop that was
    /// already taking it. Nothing is sampled for the sake of metrics alone.
    child_rss: Mutex<HashMap<u32, u64>>,
    /// Backlog per queue: (ready, in flight, delayed, dead). Polled on a timer
    /// rather than counted here, because the numbers live in the broker and
    /// this process is not the only one working the queue.
    depth: Mutex<Vec<(String, [u64; 4])>>,
    /// task name → (runs measured, bytes of child RSS they left behind, the
    /// most any one of them left). See `observe_growth`.
    growth: Mutex<HashMap<String, (u64, u64, u64)>>,
    /// task name → (runs, objects the interpreter was still holding after
    /// them, summed). See `observe_kept`.
    kept: Mutex<HashMap<String, (u64, i64)>>,
}

/// What one task has cost in child memory that did not come back.
pub struct Growth {
    pub task: String,
    pub runs: u64,
    pub total: u64,
    pub max: u64,
    /// Runs the child reported an object count for, and the sum of what each
    /// left behind. Zero runs where the child is too old to say.
    pub kept_runs: u64,
    pub kept: i64,
}

impl Metrics {
    pub fn observe(&self, task: &str, ok: bool, elapsed: Duration) {
        let seconds = elapsed.as_secs_f64();
        let mut histogram = self.histogram.lock().unwrap();
        for (slot, upper) in histogram.iter_mut().zip(BUCKETS) {
            if seconds <= upper {
                *slot += 1;
            }
        }
        drop(histogram);
        *self.duration_sum.lock().unwrap() += seconds;
        self.duration_count.fetch_add(1, Ordering::Relaxed);
        let mut tasks = self.tasks.lock().unwrap();
        tasks.entry(task.to_string()).or_default()[usize::from(!ok)] += 1;
    }

    /// Charge `bytes` of growth in one child to the tasks that finished in it
    /// since the last time anything was charged.
    ///
    /// "Which of my tasks leaks" is the question a memory ceiling raises and
    /// cannot answer: it says a child grew, not what grew it. The supervisor
    /// already reads each child's RSS at every dispatch decision, so the
    /// difference between two readings is known, and so is what ran between
    /// them.
    ///
    /// With one slot that is exact — one task ran, and the growth is its. With
    /// more, the tasks that finished share it evenly, and what the ones still
    /// running are holding is in there too: an estimate, good for ranking and
    /// not for accounting.
    pub fn observe_growth(&self, finished: &[String], bytes: u64) {
        if finished.is_empty() {
            return;
        }
        let each = bytes / finished.len() as u64;
        let mut growth = self.growth.lock().unwrap();
        for task in finished {
            let entry = growth.entry(task.clone()).or_default();
            entry.0 += 1;
            entry.1 += each;
            entry.2 = entry.2.max(each);
        }
    }

    /// Record how many objects one run of `task` left the interpreter holding.
    ///
    /// The bytes above say how much a child grew and are a poor witness to who
    /// grew it: an allocator hands the memory one task freed to the next task
    /// that asks, so a leak is paid for by whichever task allocates after it.
    /// Measured with a leaking task and a tidy one taking turns, the tidy one
    /// was charged 40 MB and the leak 16. Objects are counted where they are
    /// kept. A task that returns the interpreter to where it found it scores
    /// zero, give or take a cache, and one that does not is the leak.
    ///
    /// It is a count, not a size, and with more than one slot the tasks that
    /// overlap are in each other's numbers.
    pub fn observe_kept(&self, task: &str, blocks: i64) {
        let mut kept = self.kept.lock().unwrap();
        let entry = kept.entry(task.to_string()).or_default();
        entry.0 += 1;
        entry.1 += blocks;
    }

    /// Every task measured so far: the ones that keep the most objects first,
    /// then the ones charged the most bytes.
    pub fn growth(&self) -> Vec<Growth> {
        let bytes = self.growth.lock().unwrap();
        let kept = self.kept.lock().unwrap();
        let mut names: Vec<&String> = bytes.keys().chain(kept.keys()).collect();
        names.sort();
        names.dedup();
        let mut rows: Vec<Growth> = names
            .into_iter()
            .map(|task| {
                let (runs, total, max) = bytes.get(task).copied().unwrap_or_default();
                let (kept_runs, kept) = kept.get(task).copied().unwrap_or_default();
                Growth {
                    task: task.clone(),
                    runs,
                    total,
                    max,
                    kept_runs,
                    kept,
                }
            })
            .collect();
        rows.sort_by(|a, b| {
            b.kept
                .cmp(&a.kept)
                .then(b.total.cmp(&a.total))
                .then(a.task.cmp(&b.task))
        });
        rows
    }

    pub fn set_depth(&self, rows: Vec<(String, [u64; 4])>) {
        *self.depth.lock().unwrap() = rows;
    }

    pub fn set_child_rss(&self, pid: u32, bytes: u64) {
        self.child_rss.lock().unwrap().insert(pid, bytes);
    }

    pub fn forget_child(&self, pid: u32) {
        self.child_rss.lock().unwrap().remove(&pid);
    }
}

/// A label value, as the exposition format wants it: backslash, quote and
/// newline are the three characters it gives a meaning to. A queue or task
/// name carrying one would otherwise end its own label and corrupt the scrape.
fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Render the exposition text. `counters` and `reasons` are passed in rather
/// than held here so there is one definition of each number, the one the Python
/// API already returns.
pub fn render(
    metrics: &Metrics,
    counters: &[(&str, u64)],
    reasons: &HashMap<&'static str, u64>,
    supervisor_rss: u64,
) -> String {
    let mut out = String::with_capacity(4096);

    for (name, value) in counters {
        out.push_str(&format!("# TYPE tarsk_{name}_total counter\n"));
        out.push_str(&format!("tarsk_{name}_total {value}\n"));
    }

    let depth = metrics.depth.lock().unwrap();
    if !depth.is_empty() {
        out.push_str("# HELP tarsk_queue_jobs How much work is waiting, by queue and state.\n");
        out.push_str("# TYPE tarsk_queue_jobs gauge\n");
        for (queue, [ready, in_flight, delayed, dead]) in depth.iter() {
            for (state, value) in [
                ("ready", ready),
                ("in_flight", in_flight),
                ("delayed", delayed),
                ("dead", dead),
            ] {
                out.push_str(&format!(
                    "tarsk_queue_jobs{{queue=\"{}\",state=\"{state}\"}} {value}\n",
                    escape(queue)
                ));
            }
        }
    }
    drop(depth);

    out.push_str("# HELP tarsk_recycles_by_reason_total Which limit retired a child.\n");
    out.push_str("# TYPE tarsk_recycles_by_reason_total counter\n");
    for reason in ["max_rss", "max_tasks", "max_lifetime"] {
        let count = reasons.get(reason).copied().unwrap_or(0);
        out.push_str(&format!(
            "tarsk_recycles_by_reason_total{{reason=\"{reason}\"}} {count}\n"
        ));
    }

    out.push_str("# HELP tarsk_tasks_total Tasks that reached a terminal answer.\n");
    out.push_str("# TYPE tarsk_tasks_total counter\n");
    for (task, [ok, failed]) in metrics.tasks.lock().unwrap().iter() {
        let task = escape(task);
        out.push_str(&format!(
            "tarsk_tasks_total{{task=\"{task}\",outcome=\"ok\"}} {ok}\n"
        ));
        out.push_str(&format!(
            "tarsk_tasks_total{{task=\"{task}\",outcome=\"failed\"}} {failed}\n"
        ));
    }

    out.push_str(
        "# HELP tarsk_task_rss_growth_bytes_total Child RSS that did not come back after \
         this task ran, summed. Only measured with --max-rss or --hard-max-rss set.\n",
    );
    out.push_str("# TYPE tarsk_task_rss_growth_bytes_total counter\n");
    let growth = metrics.growth();
    for row in &growth {
        out.push_str(&format!(
            "tarsk_task_rss_growth_bytes_total{{task=\"{}\"}} {}\n",
            escape(&row.task),
            row.total
        ));
    }
    out.push_str("# TYPE tarsk_task_rss_growth_runs_total counter\n");
    for row in &growth {
        out.push_str(&format!(
            "tarsk_task_rss_growth_runs_total{{task=\"{}\"}} {}\n",
            escape(&row.task),
            row.runs
        ));
    }
    out.push_str("# TYPE tarsk_task_rss_growth_bytes_max gauge\n");
    for row in &growth {
        out.push_str(&format!(
            "tarsk_task_rss_growth_bytes_max{{task=\"{}\"}} {}\n",
            escape(&row.task),
            row.max
        ));
    }

    out.push_str(
        "# HELP tarsk_task_kept_objects Objects the interpreter was still holding after \
         this task's runs, summed. What climbs here is what leaks.\n",
    );
    out.push_str("# TYPE tarsk_task_kept_objects gauge\n");
    for row in growth.iter().filter(|row| row.kept_runs > 0) {
        out.push_str(&format!(
            "tarsk_task_kept_objects{{task=\"{}\"}} {}\n",
            escape(&row.task),
            row.kept
        ));
    }

    let rss = metrics.child_rss.lock().unwrap();
    let largest = rss.values().copied().max().unwrap_or(0);
    let total: u64 = rss.values().sum();
    let children = rss.len(); // live children, not the configured target
    drop(rss);
    out.push_str(
        "# HELP tarsk_child_rss_bytes_max Largest live child, the number the ceiling governs.\n",
    );
    out.push_str("# TYPE tarsk_child_rss_bytes_max gauge\n");
    out.push_str(&format!("tarsk_child_rss_bytes_max {largest}\n"));
    out.push_str("# TYPE tarsk_child_rss_bytes_sum gauge\n");
    out.push_str(&format!("tarsk_child_rss_bytes_sum {total}\n"));
    out.push_str(
        "# HELP tarsk_supervisor_rss_bytes The constant this project claims is constant.\n",
    );
    out.push_str("# TYPE tarsk_supervisor_rss_bytes gauge\n");
    out.push_str(&format!("tarsk_supervisor_rss_bytes {supervisor_rss}\n"));
    out.push_str("# TYPE tarsk_children gauge\n");
    out.push_str(&format!("tarsk_children {children}\n"));

    out.push_str(
        "# HELP tarsk_task_duration_seconds Dispatch to answer, as the supervisor sees it.\n",
    );
    out.push_str("# TYPE tarsk_task_duration_seconds histogram\n");
    let histogram = *metrics.histogram.lock().unwrap();
    for (count, upper) in histogram.iter().zip(BUCKETS) {
        let label = if upper.is_infinite() {
            "+Inf".to_string()
        } else {
            upper.to_string()
        };
        out.push_str(&format!(
            "tarsk_task_duration_seconds_bucket{{le=\"{label}\"}} {count}\n"
        ));
    }
    out.push_str(&format!(
        "tarsk_task_duration_seconds_sum {}\n",
        *metrics.duration_sum.lock().unwrap()
    ));
    out.push_str(&format!(
        "tarsk_task_duration_seconds_count {}\n",
        metrics.duration_count.load(Ordering::Relaxed)
    ));
    out
}

// The HTTP server that used to live here now serves the console as well, and
// moved to `console.rs` with it. This module went back to being what its name
// says: turning counters into text.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn growth_is_charged_to_what_finished_and_ranked_by_cost() {
        const MB: u64 = 1024 * 1024;
        let metrics = Metrics::default();
        // One slot: one task between two readings, and the growth is its.
        metrics.observe_growth(&["leaky".to_string()], 20 * MB);
        metrics.observe_growth(&["leaky".to_string()], 30 * MB);
        metrics.observe_growth(&["tidy".to_string()], 0);
        // Three finished between two readings: they share what was seen.
        let batch = ["tidy".to_string(), "tidy".to_string(), "leaky".to_string()];
        metrics.observe_growth(&batch, 3 * MB);
        // Nothing finished: nothing to charge, and nobody to charge it to.
        metrics.observe_growth(&[], 500 * MB);

        // The bytes above would rank these the same either way; who keeps
        // objects is what decides the order.
        metrics.observe_kept("tidy", 3);
        metrics.observe_kept("tidy", -3);
        metrics.observe_kept("leaky", 400);
        metrics.observe_kept("quiet", 0);

        let rows = metrics.growth();
        let seen: Vec<(&str, u64, u64, u64, i64)> = rows
            .iter()
            .map(|r| (r.task.as_str(), r.runs, r.total, r.max, r.kept))
            .collect();
        assert_eq!(
            seen,
            vec![
                ("leaky", 3, 51 * MB, 30 * MB, 400),
                ("tidy", 3, 2 * MB, MB, 0),
                ("quiet", 0, 0, 0, 0),
            ]
        );

        let text = render(&metrics, &[], &std::collections::HashMap::new(), 0);
        assert!(
            text.contains(&format!(
                "tarsk_task_rss_growth_bytes_total{{task=\"leaky\"}} {}",
                51 * MB
            )),
            "{text}"
        );
        assert!(
            text.contains("tarsk_task_kept_objects{task=\"leaky\"} 400")
                && text.contains("tarsk_task_rss_growth_runs_total{task=\"tidy\"} 3"),
            "{text}"
        );
    }

    #[test]
    fn a_label_cannot_break_out_of_its_quotes() {
        assert_eq!(escape("plain"), "plain");
        assert_eq!(escape(r#"a"b\c"#), r#"a\"b\\c"#);
        assert_eq!(escape("two\nlines"), "two\\nlines");

        let metrics = Metrics::default();
        metrics.set_depth(vec![("odd\"queue\n".to_string(), [1, 0, 0, 0])]);
        let text = render(&metrics, &[], &std::collections::HashMap::new(), 0);
        assert!(
            text.contains(r#"tarsk_queue_jobs{queue="odd\"queue\n",state="ready"} 1"#),
            "{text}"
        );
    }
}
