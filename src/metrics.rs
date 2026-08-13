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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

    pub fn set_child_rss(&self, pid: u32, bytes: u64) {
        self.child_rss.lock().unwrap().insert(pid, bytes);
    }

    pub fn forget_child(&self, pid: u32) {
        self.child_rss.lock().unwrap().remove(&pid);
    }
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
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

/// Answer scrapes on `addr` until the task is dropped.
pub async fn serve<F>(addr: String, snapshot: F)
where
    F: Fn() -> String + Send + Sync + 'static,
{
    let Ok(listener) = TcpListener::bind(&addr).await else {
        return;
    };
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        // Read and discard the request. Only one path is served, and a scraper
        // that asked for something else still gets numbers rather than a 404 it
        // would have to be configured around.
        let mut scratch = [0u8; 1024];
        let _ = stream.read(&mut scratch).await;
        let body = snapshot();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }
}
