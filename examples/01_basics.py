"""Send a task, run a worker, read the answer back.

    Terminal 1:  tarsk worker --app examples.01_basics:app --broker redis://localhost:6379/0
    Terminal 2:  python examples/01_basics.py

The worker is a separate process on purpose. It imports this module to find the
handlers and nothing else — no broker driver, no scheduler — which is why the
process running your code is small enough to put a ceiling on.
"""

import os

from tarsk import App

BROKER = os.environ.get("TARSK_BROKER", "redis://localhost:6379/0")
app = App(broker=BROKER)

# Every task is given an explicit `name=`. Without one the name is derived from
# where the function was defined, so this file produces `__main__.greet` when
# you run it and `examples.01_basics.greet` when the worker imports it — the
# same function under two names, and the worker rejects what the producer sent.
# An explicit name is the fix and is worth doing in real code for the same
# reason: it survives the module being moved or renamed.


@app.task(name="basics.greet")
def greet(name: str) -> str:
    """Nothing is stored. send() hands back an id and the answer is dropped."""
    print(f"    [worker] greeting {name}")
    return f"hello {name}"


# result_ttl is what makes an answer readable, and it is required rather than
# defaulted: a result store with no expiry is a leak that moved from the worker
# to the broker.
@app.task(name="basics.add", result_ttl=60)
def add(a: int, b: int) -> int:
    print(f"    [worker] {a} + {b}")
    return a + b


def main() -> None:
    greet.send("world")
    print("  sent greet, not waiting for it — nothing was stored to wait for")

    task_id = add.send(2, 3)
    print(f"  sent add as {task_id}, waiting up to 30s")
    print("  answer:", app.result(task_id).get(timeout=30))


if __name__ == "__main__":
    main()
