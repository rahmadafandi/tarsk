"""Chains, groups, schedules, and the flags that decide when work runs.

    Terminal 1:  tarsk worker --app examples.04_workflows:app \
                     --broker redis://localhost:6379/0 --queues high,low,default
    Terminal 2:  python examples/04_workflows.py

The order in `--queues` is a priority, not a preference: nothing from `low` is
claimed while `high` has work. A worker only sees the queues it is given, so
`default` is listed too — the tasks below that name no queue land there, and a
worker told only `high,low` would sit idle beside them.
"""

import os

from tarsk import App, chain, group

BROKER = os.environ.get("TARSK_BROKER", "redis://localhost:6379/0")
app = App(broker=BROKER)

# Every task is given an explicit `name=`. Without one the name is derived from
# where the function was defined, so this file produces `__main__.greet` when
# you run it and `examples.01_basics.greet` when the worker imports it — the
# same function under two names, and the worker rejects what the producer sent.
# An explicit name is the fix and is worth doing in real code for the same
# reason: it survives the module being moved or renamed.


@app.task(name="workflows.double", result_ttl=60)
def double(n: int) -> int:
    return n * 2


@app.task(name="workflows.plus_ten", result_ttl=60)
def plus_ten(n: int) -> int:
    """A chain step is handed the previous step's result as its first argument."""
    return n + 10


@app.task(name="workflows.urgent", queue="high")
def urgent(what: str) -> str:
    print(f"    [worker] URGENT {what}")
    return what


@app.task(name="workflows.whenever", queue="low")
def whenever(what: str) -> str:
    print(f"    [worker] whenever {what}")
    return what


@app.task(name="workflows.polite", rate_limit="5/s")
def polite(n: int) -> int:
    """Five a second across every worker, because the bucket is in the broker.

    Celery's rate_limit is per worker, so four workers get four times the
    allowance. This one is shared.
    """
    return n


@app.task(name="workflows.only_two_at_once", max_concurrency=2)
def only_two_at_once(n: int) -> int:
    """A cap on how many run at once, not on how often they start."""
    return n


@app.task(name="workflows.every_five_minutes", cron="*/5 * * * *")
def every_five_minutes() -> None:
    """UTC, and elected through the broker so N workers fire it once."""
    print("    [worker] tick")


def main() -> None:
    # Sequential: each step's result becomes the next step's first argument,
    # and send() hands back the id of the final step's answer.
    pipeline = chain(double.s(5), plus_ten.s())
    print("  chain(double(5) -> plus_ten):", app.result(pipeline.send()).get(timeout=30))

    # Parallel: one id per member, and results collected as they land.
    fan = group(double.s(n) for n in (1, 2, 3))
    ids = fan.send()
    print(f"  group of {len(ids)} doubles:", sorted(h.get(timeout=30) for h in fan.results(app)))

    # Queued low first and given a head start; the worker still runs high first.
    for n in range(5):
        whenever.send(f"low{n}")
    for n in range(3):
        urgent.send(f"high{n}")
    # Priority decides what is *claimed next*, and an idle worker with free
    # slots has usually claimed and run the lows before the highs were even
    # sent. To watch it visibly reorder, queue a backlog larger than the
    # worker's capacity, or start the worker after the sends.
    print("  queued 5 low then 3 high — high wins whenever both are waiting")

    # Later, rather than now.
    double.send_in(5, 21)
    print("  double(21) queued for 5 seconds from now")

    # This send is worth doing now and not in an hour.
    urgent.options(expires=30).send("only useful for the next 30s")
    print("  a send with its own expiry, which the registration cannot override")


if __name__ == "__main__":
    main()
