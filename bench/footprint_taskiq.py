"""taskiq app for the `imports` scenario. Same shape as footprint_app."""

import os

with open(os.environ["BENCH_IMPORT_LOG"], "a", buffering=1) as _fh:
    _fh.write(f"{os.getpid()}\n")

if os.environ.get("BENCH_HEAVY"):
    import celery  # noqa: F401  — stands in for a real dependency tree
    import redis  # noqa: F401
    import taskiq  # noqa: F401

from taskiq_redis import ListQueueBroker

# redis-py 8.x sets a 5s default socket timeout even for a plain URL, and
# taskiq_redis's listen() catches only ConnectionError while redis-py raises
# TimeoutError (not a subclass) on an idle BRPOP. The worker then dies and is
# respawned every few seconds. Disabling the read timeout is the configuration
# that makes a blocking pop work as intended — the same courtesy this harness
# extends to Celery's tuned settings.
broker = ListQueueBroker(
    url=os.environ.get("BENCH_REDIS", "redis://localhost:6399/0"),
    socket_timeout=None,
)


@broker.task(task_name="trivial")
def trivial(x):
    return x
