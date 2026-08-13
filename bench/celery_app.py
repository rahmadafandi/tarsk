"""Celery app under test — same handlers, result backend off (as tarsk's is)."""

import os

from celery import Celery

from bench import handlers

# BENCH_REDIS is always set by the harness; the fallback is for importing this
# module by hand, so it points at the conventional port rather than one nothing
# is listening on.
app = Celery("bench", broker=os.environ.get("BENCH_REDIS", "redis://localhost:6379/0"))
app.conf.update(
    task_ignore_result=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    worker_send_task_events=False,
    # Celery's answer to losing work on a kill: ack after the task, not on
    # receipt. Redelivery then costs a re-run of whatever was in flight.
    task_acks_late=os.environ.get("BENCH_ACKS_LATE") == "1",
)

app.task(name="leak")(handlers.leak)
app.task(name="noop")(handlers.noop)
app.task(name="work5")(handlers.work5)
app.task(name="work50")(handlers.work50)
