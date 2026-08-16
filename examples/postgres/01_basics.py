"""The same queue on Postgres: one dependency fewer if you already run one.

    Terminal 1:  tarsk worker --app examples.postgres.01_basics:app \
                     --broker postgres://tarsk@localhost:5432/tarsk
    Terminal 2:  python examples/postgres/01_basics.py

The task API is identical across brokers — every example in ../redis runs here
unchanged once TARSK_BROKER points at Postgres. This file exists for what
actually differs:

- **Schema**: the worker creates its own tables (`tarsk_jobs`, `tarsk_dead`,
  `tarsk_results`, ...) on first connect. Nothing to migrate by hand.
- **Claiming**: `FOR UPDATE SKIP LOCKED` plus a lease column — one statement
  claims a job and reclaims expired leases, so there is no separate sweep.
- **TLS**: `sslmode=require`, `verify-ca` and `verify-full` all verify the
  certificate chain and hostname, because rustls does that on every handshake.
  A managed instance's connection string works as pasted.
"""

import os

from tarsk import App

BROKER = os.environ.get("TARSK_BROKER", "postgres://tarsk@localhost:5432/tarsk")
app = App(broker=BROKER)

# Explicit names, as everywhere: without one the name derives from where the
# function was defined, and this file registers a different name when run
# directly than when the worker imports it.


@app.task(name="pg.greet")
def greet(name: str) -> str:
    print(f"    [worker] greeting {name}")
    return f"hello {name}"


@app.task(name="pg.add", result_ttl=60)
def add(a: int, b: int) -> int:
    print(f"    [worker] {a} + {b}")
    return a + b


def main() -> None:
    greet.send("postgres")
    task_id = add.send(2, 3)
    print(f"  sent add as {task_id}, waiting up to 30s")
    print("  answer:", app.result(task_id).get(timeout=30))


if __name__ == "__main__":
    main()
