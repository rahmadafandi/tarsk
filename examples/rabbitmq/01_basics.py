"""The same queue on RabbitMQ — with its trade printed on the label.

    Terminal 1:  tarsk worker --app examples.rabbitmq.01_basics:app \
                     --broker amqp://guest:guest@localhost:5672/%2f
    Terminal 2:  python examples/rabbitmq/01_basics.py

The task API is identical: every example in ../redis runs here unchanged once
TARSK_BROKER points at an amqp:// URL. What this file documents is the trade,
because AMQP is a message transport and not a store, and that cuts both ways.

What it does *better* than a lease can: an unacked delivery returns the
instant the worker's connection dies. There is no timeout to wait out — the
connection is the lease.

What degrades to per-worker, which is the shape Celery has always had on
RabbitMQ: rate limits, concurrency caps, and the cron election. Correct with
one worker; N workers each enforce their own copy.

What is refused rather than faked: cancelling queued work and send
deduplication. Both need state every worker can read, and AMQP has no shared
store to hold it — `app.cancel()` raises and says so. Use Redis or Postgres
if you need them.

One more habit worth keeping: `retries` here counts handler failures. A crash
redelivery reuses the server's copy of the message, whose attempt counter the
dead worker never got to bump.
"""

import os

from tarsk import App

BROKER = os.environ.get("TARSK_BROKER", "amqp://guest:guest@localhost:5672/%2f")
app = App(broker=BROKER)


@app.task(name="mq.greet")
def greet(name: str) -> str:
    print(f"    [worker] greeting {name}")
    return f"hello {name}"


@app.task(name="mq.add", result_ttl=60)
def add(a: int, b: int) -> int:
    print(f"    [worker] {a} + {b}")
    return a + b


def main() -> None:
    greet.send("rabbitmq")
    task_id = add.send(2, 3)
    print(f"  sent add as {task_id}, waiting up to 30s")
    print("  answer:", app.result(task_id).get(timeout=30))


if __name__ == "__main__":
    main()
