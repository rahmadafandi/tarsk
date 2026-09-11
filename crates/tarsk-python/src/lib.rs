//! The Python binding: everything in tarsk that touches pyo3, and nothing else.
//!
//! The engine lives in `tarsk-core`, which knows nothing about Python. This
//! crate is the `_core` extension module the `tarsk` package imports.

use std::collections::HashMap;
use std::io;
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use sysinfo::System;

use tarsk_core::broker::{Broker, NewJob};
use tarsk_core::{
    build_cfg, console, io_err, new_shared, socket_dir, supervise, transport, Outcome,
};

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
/// One batch job as Python hands it over: name, packed args, result id,
/// queue, timeout ms, packed chain tail, packed meta, per-send expiry ms.
/// A tuple rather than a struct because pyo3 converts it for free and the
/// only caller is `Supervisor.run`, which builds it in one place.
type BatchJob = (String, Vec<u8>, String, String, u64, Vec<u8>, Vec<u8>, u64);

#[pyfunction]
#[pyo3(signature = (app_spec, jobs, python, children=2, slots=1, max_rss=0, max_tasks=0,
                    max_lifetime=0.0, hard_max_rss=0))]
#[allow(clippy::too_many_arguments)]
fn run(
    py: Python<'_>,
    app_spec: String,
    jobs: Vec<BatchJob>,
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
        0, // batch mode keeps no dead letters worth trimming
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
                    for (name, payload, id, queue, timeout_ms, chain, meta, expires_ms) in jobs {
                        broker
                            .push(
                                NewJob {
                                    id,
                                    queue,
                                    name,
                                    payload,
                                    timeout_ms: timeout_ms as u32,
                                    chain,
                                    meta,
                                    expires_ms,
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

/// Console only: serve the admin pages without running any children.
///
/// The console rides on the supervisor, which is right until every worker is
/// down — which is the moment you most want to look at the queue. This is the
/// same pages against the same broker, with nothing to supervise.
#[pyfunction]
#[pyo3(name = "console", signature = (broker_url, queues, addr))]
fn console_only(
    py: Python<'_>,
    broker_url: String,
    queues: Vec<String>,
    addr: String,
) -> PyResult<()> {
    py.detach(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async move {
            let broker = Broker::connect(&broker_url, queues).await.map_err(io_err)?;
            let shared = new_shared(broker, 0, false);
            // No counters worth reporting: nothing here has run a task. The
            // page's numbers all come from the broker, which is shared.
            let snapshot = || String::new();
            console::serve(addr, shared, snapshot).await;
            Ok::<(), io::Error>(())
        })
    })
    .map_err(|e: io::Error| PyValueError::new_err(e.to_string()))
}

/// Worker mode: consume `queues` from a real broker until SIGINT or SIGTERM.
#[pyfunction]
#[pyo3(signature = (app_spec, broker_url, queues, python, children=2, slots=1, max_rss=0, max_tasks=0,
                    max_lifetime=0.0, lease_grace=30.0, metrics_addr=None, hard_max_rss=0,
                    max_dead=1_000))]
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
    max_dead: u64,
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
        max_dead,
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

/// One job crossing into Python: id, queue, name, state, attempt, age, worker,
/// and whatever the sender attached, still packed.
type JobRow = (String, String, String, String, u32, i64, String, Py<PyAny>);

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
                        meta=Vec::new(), dedup_key=String::new(), dedup_ttl_ms=0,
                        expires_ms=0))]
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
        meta: Vec<u8>,
        dedup_key: String,
        dedup_ttl_ms: u64,
        expires_ms: u64,
    ) -> PyResult<Option<String>> {
        let job = NewJob {
            id: id.clone(),
            queue,
            name,
            payload,
            timeout_ms,
            chain,
            meta,
            expires_ms,
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

    /// The individual jobs waiting or running, oldest first.
    #[pyo3(signature = (queues, limit=50))]
    fn jobs(&self, py: Python<'_>, queues: Vec<String>, limit: usize) -> PyResult<Vec<JobRow>> {
        let rows = py
            .detach(|| self.runtime.block_on(self.broker.jobs(&queues, limit)))
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|j| {
                (
                    j.id,
                    j.queue,
                    j.name,
                    j.state.to_string(),
                    j.attempt,
                    j.age_ms,
                    j.worker,
                    PyBytes::new(py, &j.meta).into_any().unbind(),
                )
            })
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
    transport::child_rss_with(&mut sys, pid).unwrap_or(0)
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Pin the process-wide rustls crypto provider before any TLS connector
    // exists. With more than one provider in the dependency graph — lapin's
    // rustls stack can pull aws-lc-rs in beside ring — rustls refuses to
    // guess and panics at the first handshake instead. Choosing here, once,
    // makes every broker's TLS deterministic regardless of what the graph
    // grows next.
    let _ = rustls::crypto::ring::default_provider().install_default();
    m.add_function(wrap_pyfunction!(run, m)?)?;
    m.add_function(wrap_pyfunction!(work, m)?)?;
    m.add_function(wrap_pyfunction!(rss_of, m)?)?;
    m.add_function(wrap_pyfunction!(console_only, m)?)?;
    m.add_class::<Producer>()
}
