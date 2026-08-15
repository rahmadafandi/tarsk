"""Task module for the broker integration tests."""

import os
import time
from pathlib import Path

from tarsk import App, Context, Depends

app = App(broker=os.environ.get("TARSK_BROKER"), default_timeout=30, max_timeout=30)


def _log(tag):
    with open(os.environ["TARSK_LOG"], "a", buffering=1) as fh:
        fh.write(f"{tag}\t{os.getpid()}\n")


@app.task(name="note")
def note(tag):
    _log(tag)
    return tag


@app.task(name="crash_once", retries=1)
def crash_once(tag):
    """Kill the child the first time, so the supervisor has to hand the job back."""
    marker = Path(os.environ["TARSK_LOG"] + ".crashed")
    if not marker.exists():
        marker.touch()
        os._exit(1)
    _log(tag)
    return tag


@app.task(name="slow", timeout=8, retries=1)
def slow(tag):
    """In flight when the worker is killed, but short enough to finish on the
    retry — a sleep longer than its own timeout would just time out forever."""
    time.sleep(6)
    _log(tag)
    return tag


@app.task(name="medium")
def medium(tag):
    """Runs long enough to still be in flight when the shutdown signal lands."""
    time.sleep(6)
    _log(tag)
    return tag


@app.task(name="echoes", result_ttl=60)
def echoes(value):
    """Hands back exactly what it was given, so the codec is what is measured."""
    _log(f"echoes-{type(value).__name__}")
    return value


@app.task(name="labelled", result_ttl=60)
def labelled(tag, ctx=Depends(Context)):
    """Reads what the sender attached rather than what it was called with."""
    _log(f"labelled-{ctx.meta.get('trace', 'none')}")
    return ctx.meta


@app.task(name="once", unique=30)
def once(tag):
    _log(f"once-{tag}")
    return tag


@app.task(name="unhurried", result_ttl=60)
def unhurried(tag):
    """Long enough that awaiting it is a real wait, short enough for a test.

    The async check needs something to wait *for*: if the answer lands before
    the event loop has come round twice, a responsive loop and a blocked one
    look the same.
    """
    time.sleep(1.0)
    _log(tag)
    return tag


@app.task(name="capped", max_concurrency=2, timeout=30)
def capped(tag):
    """Logs its own start and end, so overlap is measurable rather than assumed."""
    _log(f"start-{tag}")
    time.sleep(0.6)
    _log(f"end-{tag}")
    return tag


@app.task(name="double", result_ttl=60)
def double(n):
    _log(f"double-{n}")
    return n * 2


@app.task(name="add_to", result_ttl=60)
def add_to(previous, n):
    """First argument is what the step before returned."""
    _log(f"add_to-{previous}+{n}")
    return previous + n


@app.task(name="shout", result_ttl=60)
def shout(word):
    """Immutable in a chain: it must not be handed the previous result."""
    _log(f"shout-{word}")
    return word.upper()


@app.task(name="perishable", expires=2)
def perishable(tag):
    """Worth doing now, not worth doing later."""
    _log(tag)
    return tag


@app.task(name="durable")
def durable(tag):
    """No registered expiry, so only what a send asks for can drop it."""
    _log(tag)
    return tag


@app.task(name="carries_meta")
def carries_meta(ctx=Depends(Context)):
    """Reports what the sender attached, which a delayed send used to lose."""
    _log(f"meta-{ctx.meta.get('trace', 'MISSING')}")
    return ctx.meta


@app.task(name="metered", rate_limit="5/s")
def metered(tag):
    """Nothing slow: whatever paces this is the limiter, not the handler."""
    _log(tag)
    return tag


@app.task(name="always_fails", retries=1, backoff="none")
def always_fails(tag):
    _log(tag)
    raise RuntimeError(f"{tag} never works")


@app.task(name="answers", result_ttl=60)
def answers(a, b):
    """Opts into a stored result; everything else here throws its answer away."""
    return {"sum": a + b, "pid": os.getpid()}


@app.task(name="explodes", result_ttl=60)
def explodes():
    raise ValueError("this one was always going to")


@app.task(name="every_minute", cron="* * * * *")
def every_minute():
    _log("tick")
    return "tick"


@app.task(name="reports", result_ttl=60)
def reports(ctx=Depends(Context)):
    """Publishes progress from a sync handler, i.e. from a worker thread."""
    for step in range(1, 4):
        ctx.set_progress({"step": step, "of": 3})
        time.sleep(0.2)
    return "finished"
