"""Celery app for the `imports` scenario. Same shape as footprint_app."""

import os

with open(os.environ["BENCH_IMPORT_LOG"], "a", buffering=1) as _fh:
    _fh.write(f"{os.getpid()}\n")

if os.environ.get("BENCH_HEAVY"):
    import celery  # noqa: F401
    import redis  # noqa: F401
    import taskiq  # noqa: F401

from celery import Celery

app = Celery("footprint", broker=os.environ.get("BENCH_REDIS", "redis://localhost:6399/0"))
app.conf.update(task_ignore_result=True, broker_connection_retry_on_startup=True)


@app.task(name="trivial")
def trivial(x):
    return x
