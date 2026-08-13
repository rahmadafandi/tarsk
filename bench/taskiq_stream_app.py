"""taskiq on its Streams broker — the configuration that acks.

The default ListQueueBroker is BRPOP with no acknowledgement: at-most-once, and
a killed worker loses whatever it held. tarsk's Redis driver is Streams with a
consumer group, which is three commands per task instead of one and survives a
kill. Comparing throughput or task loss across those two is comparing different
promises, so this module exists to make one row of the table honest.
"""

import os

from taskiq_redis import RedisStreamBroker

from bench import handlers

# Same read-timeout story as the list broker; see bench/taskiq_app.py.
broker = RedisStreamBroker(
    url=os.environ.get("BENCH_REDIS", "redis://localhost:6379/0"),
    socket_timeout=None,
)

leak = broker.task(task_name="leak")(handlers.leak)
noop = broker.task(task_name="noop")(handlers.noop)
work5 = broker.task(task_name="work5")(handlers.work5)
work50 = broker.task(task_name="work50")(handlers.work50)
io100 = broker.task(task_name="io100")(handlers.io100)
