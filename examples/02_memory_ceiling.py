"""The thing this project exists for: a leaky handler that never wins.

    Terminal 1:  tarsk worker --app examples.02_memory_ceiling:app \
                     --broker redis://localhost:6379/0 --max-rss 200MB --slots 1
    Terminal 2:  python examples/02_memory_ceiling.py

Watch the worker's log. It retires a child and starts a replacement before the
ceiling is crossed, and no task is lost doing it — the replacement is already
warm when the old one goes.

`--slots 1` is not the default. The default is 100, which is right for handlers
that wait on other services and wrong for handlers that allocate; the supervisor
budgets slots against what a task costs, so the ceiling holds either way, but
one slot is what makes it exact.
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

# Module-level and never cleared, which is what a leak looks like from the
# outside: the handler is correct, the process still grows.
_kept: list[bytearray] = []


@app.task(name="ceiling.leak")
def leak(n: int, megabytes: int = 20) -> int:
    block = bytearray(megabytes * 1024 * 1024)
    block[::4096] = b"\x01" * len(block[::4096])  # touch every page so it is resident
    _kept.append(block)
    print(f"    [worker] task {n}: holding {len(_kept) * megabytes} MB")
    return n


def main() -> None:
    for n in range(40):
        leak.send(n)
    print("  sent 40 tasks that each retain 20 MB — 800 MB if nothing intervenes")
    print("  the worker should stay under 200 MB and complete all 40")


if __name__ == "__main__":
    main()
