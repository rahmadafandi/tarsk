"""taskiq app for the `imports` scenario. Same shape as footprint_app."""

import os

with open(os.environ["BENCH_IMPORT_LOG"], "a", buffering=1) as _fh:
    _fh.write(f"{os.getpid()}\n")

if os.environ.get("BENCH_HEAVY"):
    import celery  # noqa: F401
    import redis  # noqa: F401
    import taskiq  # noqa: F401

from taskiq_redis import ListQueueBroker

broker = ListQueueBroker(url=os.environ.get("BENCH_REDIS", "redis://localhost:6399/0"))


@broker.task(task_name="trivial")
def trivial(x):
    return x
