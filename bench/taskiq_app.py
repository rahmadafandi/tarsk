"""taskiq app under test — same handlers, Redis list broker."""

import os

from taskiq_redis import ListQueueBroker

from bench import handlers

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

leak = broker.task(task_name="leak")(handlers.leak)
noop = broker.task(task_name="noop")(handlers.noop)
work5 = broker.task(task_name="work5")(handlers.work5)
work50 = broker.task(task_name="work50")(handlers.work50)
