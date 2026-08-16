"""The in-process broker: a whole run in one command, no server anywhere.

    python examples/memory/01_batch.py

`memory://` is the batch broker the test suite and the benchmarks use. There
is no separate worker to start — `Supervisor.run` spawns children, drains the
queue, and returns every outcome. It lives and dies with this process, which
is the entire difference between it and Redis or Postgres: everything that is
about *a job* works here; anything about *more than one process* (a second
worker, the CLI, cancellation from outside) cannot.

Tasks still run in child processes that import only this module, so what runs
is what production would run — only the queue is in memory.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from tarsk import App, chain

app = App()  # no broker URL: batch mode never dials out


@app.task(name="mem.add")
def add(a: int, b: int) -> int:
    return a + b


@app.task(name="mem.one_lane", max_concurrency=1)
def one_lane(n: int) -> int:
    """The broker holds this to one at a time, however many slots are free."""
    time.sleep(0.3)
    return n


@app.task(name="mem.slow")
def slow() -> str:
    time.sleep(1.2)
    return "done"


@app.task(name="mem.perishable", expires=0.5)
def perishable() -> str:
    """Half a second on the shelf; anything later is dropped, not run."""
    return "fresh"


def main() -> None:
    from tarsk._supervisor import Supervisor

    # Plain jobs are (name, args, kwargs) triples; results come back keyed by
    # submission order.
    sup = Supervisor("examples.memory.01_batch:app", children=2, slots=4)
    results = sup.run([("mem.add", (n, n), {}) for n in range(3)])
    print("  three adds:", [results[i] for i in range(3)])

    # A Chain is submitted whole. The run only ends once the tail has settled,
    # and the tail is fed the head's result — 1+2 becomes add(3, 10).
    results = sup.run([chain(add.s(1, 2), add.s(10))])
    print("  chain outcomes:", sorted(v for kind, v in results.values() if kind == "ack"))

    # max_concurrency holds even here: three 0.3s tasks with slots to spare
    # still take ~0.9s, because the cap lives in the broker, not the worker.
    started = time.time()
    sup.run([("mem.one_lane", (n,), {}) for n in range(3)])
    print(f"  three capped tasks: {time.time() - started:.2f}s wall (serialised)")

    # And expiry: with one slot, `perishable` waits 1.2s behind `slow` —
    # past its 0.5s shelf life, so it comes back Expired instead of running.
    sup = Supervisor("examples.memory.01_batch:app", children=1, slots=1)
    results = sup.run([("mem.slow", (), {}), ("mem.perishable", (), {})])
    kind, detail = results[1]
    print(f"  stale job: {kind} ({detail[0] if kind == 'nack' else detail})")


if __name__ == "__main__":
    main()
