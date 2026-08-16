"""What happens when a handler fails, and the three ways to fail on purpose.

    Terminal 1:  tarsk worker --app examples.redis.03_reliability:app --broker redis://localhost:6379/0
    Terminal 2:  python examples/redis/03_reliability.py
    Then:        tarsk dead --broker redis://localhost:6379/0

Handlers must be idempotent. Delivery is at-least-once, so a worker killed
mid-task will run that task again — that is a contract, not a footnote.
"""

import os
import random

from tarsk import App, Reject, Retry

BROKER = os.environ.get("TARSK_BROKER", "redis://localhost:6379/0")
app = App(broker=BROKER)

# Every task is given an explicit `name=`. Without one the name is derived from
# where the function was defined, so this file produces `__main__.greet` when
# you run it and `examples.redis.01_basics.greet` when the worker imports it — the
# same function under two names, and the worker rejects what the producer sent.
# An explicit name is the fix and is worth doing in real code for the same
# reason: it survives the module being moved or renamed.


@app.task(name="reliability.flaky", retries=3, backoff="exp")
def flaky(n: int) -> str:
    """Raising is the ordinary path: it costs an attempt and backs off."""
    if random.random() < 0.6:
        raise ConnectionError("upstream said no")
    return f"succeeded on {n}"


@app.task(name="reliability.wait_for_upstream", retries=5)
def wait_for_upstream(n: int) -> str:
    """Retry asks for a specific delay, and still costs an attempt.

    A retry that costs nothing is an infinite loop with extra steps, and the
    thing being waited on is usually the thing least able to absorb one.
    """
    raise Retry("rate limited by upstream", delay=2.0)


@app.task(name="reliability.unparseable", retries=5)
def unparseable(payload: str) -> str:
    """Reject skips the remaining attempts.

    Retrying a payload that cannot be parsed spends the budget to reach the
    same answer, and only delays the moment somebody looks at it.
    """
    raise Reject(f"cannot parse {payload!r}, and trying again will not help")


@app.task(name="reliability.tidy_up", timeout=2, soft_timeout=1)
async def tidy_up(n: int) -> str:
    """Asked to stop at 1s, stopped at 2s.

    The handler sees asyncio.CancelledError — asyncio has no way to raise a
    type of our choosing into a running coroutine — and ctx.soft_expired tells
    a passed deadline from a worker shutting down.
    """
    import asyncio

    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        print("    [worker] asked to stop, saving partial work")
        return "partial work kept"
    return "unreachable"


def main() -> None:
    for n in range(3):
        flaky.send(n)
    wait_for_upstream.send(0)
    unparseable.send("{not json")
    tidy_up.send(0)
    print("  sent. Watch the worker retry, back off, reject and be asked to stop.")
    print("  Anything that exhausts its attempts lands in the dead letters:")
    print(f"      tarsk dead --broker {BROKER}")


if __name__ == "__main__":
    main()
