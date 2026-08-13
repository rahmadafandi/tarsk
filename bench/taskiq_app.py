"""taskiq app under test — same handlers, Redis list broker."""

import os

from taskiq_redis import ListQueueBroker

from bench import handlers

broker = ListQueueBroker(url=os.environ.get("BENCH_REDIS", "redis://localhost:6399/0"))

leak = broker.task(task_name="leak")(handlers.leak)
noop = broker.task(task_name="noop")(handlers.noop)
work5 = broker.task(task_name="work5")(handlers.work5)
work50 = broker.task(task_name="work50")(handlers.work50)
