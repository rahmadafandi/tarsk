#!/usr/bin/env python3
"""Run one real task through an installed tarsk distribution.

    python packaging/smoke.py                          # tarsk, memory broker
    TARSK_SMOKE_BACKEND=redis TARSK_SMOKE_URL=redis://… python packaging/smoke.py

Run it from anywhere but the repository root, so `import tarsk` finds the
installed wheel rather than the source tree next to it — that is the whole
point: this checks what the wheel actually contains, which is a different
question from what the build log said it asked for.

Three things, in order of how quietly they would otherwise fail: the backends
compiled into the binary, the message a backend this distribution does not
ship gives back, and a task sent over the broker it does ship coming home.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# Children are separate interpreters that import the app module by name.
os.environ["PYTHONPATH"] = os.pathsep.join(
    [str(Path(__file__).resolve().parent), os.environ.get("PYTHONPATH", "")]
)

from tarsk import App  # noqa: E402
from tarsk._core import Producer, backends  # noqa: E402

BACKEND = os.environ.get("TARSK_SMOKE_BACKEND", "memory")
URL = os.environ.get("TARSK_SMOKE_URL", "memory://")
DISTRIBUTION = "tarsk" if BACKEND == "memory" else f"tarsk-{BACKEND}"

app = App(broker=URL, default_timeout=30, max_timeout=30)


@app.task(name="smoke_add", result_ttl=120)
def smoke_add(left, right):
    return left + right


def run_worker() -> None:
    from tarsk._supervisor import Supervisor

    Supervisor("smoke:app", children=1, slots=1).work(URL, ["default"], lease_grace=5)


def main() -> None:
    expected = ["memory"] + ([BACKEND] if BACKEND != "memory" else [])
    assert backends() == expected, f"{DISTRIBUTION} has {backends()}, wanted {expected}"

    absent = next(b for b in ("redis", "postgres", "amqp") if b != BACKEND)
    try:
        Producer(broker_url=f"{absent}://user:secret@127.0.0.1:1/0")
    except ValueError as exc:
        gated = str(exc)
        assert f"install tarsk-{absent}" in gated, gated
        assert "secret" not in gated, f"the error printed the password: {gated}"
    else:
        raise AssertionError(f"{absent}:// connected in a build without it")

    if BACKEND == "memory":
        from tarsk._supervisor import Supervisor

        done = Supervisor("smoke:app", children=1).run([("smoke_add", (2, 3), {})])
        assert list(done.values()) == [("ack", 5)], done
    else:
        worker = subprocess.Popen([sys.executable, __file__, "worker"])
        try:
            ids = [smoke_add.send(n, 1) for n in range(3)]
            got = [app.result(job).get(timeout=90) for job in ids]
            assert got == [1, 2, 3], got
        finally:
            worker.terminate()
            worker.wait(timeout=30)

    print(f"{DISTRIBUTION} {BACKEND}: backends={backends()} task=ok")
    print(f"  gated: {gated}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        run_worker()
    else:
        main()
