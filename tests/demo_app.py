"""Task module for the step-1 protocol check — one task per path worth proving."""

import asyncio
import os
import time
from pathlib import Path

from tarsk import App, Context, Depends, Reject, Retry

app = App(default_timeout=5, max_timeout=5)


@app.task(name="add")
async def add(a, b):
    return a + b


@app.task(name="boom")
async def boom():
    raise ValueError("kaboom")


@app.task(name="waits", timeout=4)
async def waits():
    """Awaits without allocating — the shape a slot count exists for."""
    await asyncio.sleep(0.2)
    return os.getpid()


@app.task(name="blocks", timeout=4)
def blocks():
    """The same wait in a sync handler: it holds a thread, not a coroutine."""
    time.sleep(0.2)
    return os.getpid()


@app.task(name="naps", timeout=0.3)
async def naps():
    await asyncio.sleep(10)


@app.task(name="slow_sync", timeout=0.3)
def slow_sync():
    time.sleep(10)
    return "unreachable"


@app.task(name="one_lane", max_concurrency=1)
def one_lane(n):
    """Held to one at a time by the broker, however many slots exist."""
    time.sleep(0.3)
    return n


@app.task(name="slow_lane")
def slow_lane():
    time.sleep(1.2)
    return "done"


@app.task(name="milk", expires=0.5)
def milk():
    """Half a second on the shelf; anything later is not worth running."""
    return "fresh"


@app.task(name="hard_crash", retries=1)
def hard_crash():
    os._exit(1)


@app.task(name="flaky", retries=2, backoff="none")
def flaky(marker):
    """Fails twice, then works — the case retries exist for."""
    path = Path(marker)
    attempts = int(path.read_text()) if path.exists() else 0
    path.write_text(str(attempts + 1))
    if attempts < 2:
        raise RuntimeError(f"attempt {attempts + 1} of flaky fails")
    return attempts + 1


@app.task(name="records_attempts", retries=2, backoff="none")
def records_attempts(marker, ctx=Depends(Context)):
    """Writes ctx.attempt on every run, so a test can read the sequence.

    Fails every time: the point is the three numbers, not the outcome.
    """
    with open(marker, "a", buffering=1) as fh:
        fh.write(f"{ctx.attempt}\n")
    raise RuntimeError("never works, by design")


@app.task(name="glutton", retries=1)
def glutton(megabytes):
    """Allocates past any sane ceiling and then lingers, so the hard limit has
    to catch it while it is still running rather than at a task boundary."""
    hoard = bytearray(megabytes * 1024 * 1024)
    time.sleep(5)
    return len(hoard)


# Lifecycle hooks: a file per child, so a test can count how often they ran.
HOOK_LOG = Path(os.environ.get("TARSK_HOOK_LOG", os.devnull))


@app.on_start
def opened():
    with open(HOOK_LOG, "a", buffering=1) as fh:
        fh.write(f"start {os.getpid()}\n")


@app.on_stop
def closed():
    with open(HOOK_LOG, "a", buffering=1) as fh:
        fh.write(f"stop {os.getpid()}\n")


# --- middleware and injection ------------------------------------------
TRACE = Path(os.environ.get("TARSK_TRACE_LOG", os.devnull))


def _note(line):
    with open(TRACE, "a", buffering=1) as fh:
        fh.write(line + "\n")


class Recorder:
    """An onion layer: it sees the call open and close, including failures."""

    def __init__(self, tag):
        self.tag = tag

    async def execute(self, ctx, call):
        _note(f"{self.tag}>{ctx.name}")
        try:
            return await call()
        except Exception as exc:
            _note(f"{self.tag}!{type(exc).__name__}")
            raise
        finally:
            _note(f"{self.tag}<{ctx.name}")


class SyncRecorder:
    """A plain `with`-shaped layer: no async anywhere in it."""

    def __init__(self, tag):
        self.tag = tag

    def execute(self, ctx, call):
        _note(f"{self.tag}>{ctx.name}")
        try:
            return call()
        finally:
            _note(f"{self.tag}<{ctx.name}")


app.middleware(Recorder("outer"))
app.middleware(SyncRecorder("sync"))
app.middleware(Recorder("inner"))

_opened = []


def a_pool():
    _opened.append(1)
    return {"conn": len(_opened)}


@app.task(name="uses_pool")
def uses_pool(x, db=Depends(a_pool)):
    return [x, db["conn"], len(_opened)]


@app.task(name="wrapped_boom")
def wrapped_boom(db=Depends(a_pool)):
    raise KeyError("through the onion")


@app.task(name="hopeless", retries=5)
def hopeless():
    """Rejecting must beat the retry budget, not wait it out."""
    raise Reject("this payload will never parse")


@app.task(name="asks_again", retries=3)
def asks_again(marker):
    path = Path(marker)
    tries = int(path.read_text()) if path.exists() else 0
    path.write_text(str(tries + 1))
    if tries == 0:
        raise Retry("upstream is cold", delay=0)
    return tries + 1


@app.task(name="own_timeout")
def own_timeout():
    """Raises the builtin a socket or `Future.result()` would, well inside its
    own deadline — which is not this task timing out."""
    raise TimeoutError("the upstream socket timed out")


@app.task(name="mirror")
def mirror(value):
    return value


@app.task(name="lane_attempt", max_concurrency=1, retries=3)
def lane_attempt(ctx=Depends(Context)):
    """Reports the attempt it was run as, after queueing behind its own cap."""
    time.sleep(0.4)
    return ctx.attempt


_dialled = []


async def a_slow_pool():
    await asyncio.sleep(0.05)  # long enough for every slot to arrive first
    _dialled.append(1)
    return len(_dialled)


@app.task(name="shares_pool")
async def shares_pool(pool=Depends(a_slow_pool)):
    await asyncio.sleep(0.1)
    return pool


@app.task(name="balloons", timeout=5)
def balloons(megabytes, marker=""):
    """Grows a step at a time rather than all at once, so it is over the soft
    ceiling for a while before it reaches the hard one — which is how memory
    actually runs away, and the case a single allocation never exercises."""
    hoard = []
    for held in range(10, megabytes + 1, 10):
        hoard.append(bytearray(10 * 1024 * 1024))
        if marker:
            # Appended, a line a step: this task is killed mid-run by design,
            # and a file rewritten each time can be caught truncated and empty.
            with open(marker, "a") as progress:
                progress.write(f"{held}\n")
        time.sleep(0.1)
    time.sleep(1.5)
    return len(hoard)


def _provided(line):
    path = os.environ.get("TARSK_PROVIDER_LOG")
    if path:
        with open(path, "a", buffering=1) as fh:
            fh.write(line + "\n")


def a_session():
    """Opens something per call and closes it after: everything past the
    `yield` is what the handler would otherwise have had to remember."""
    _provided("session-open")
    yield "session"
    _provided("session-closed")


async def a_client():
    _provided("client-open")
    yield "client"
    _provided("client-closed")


@app.task(name="uses_a_session")
def uses_a_session(fail, s=Depends(a_session, scope="task")):
    _provided(f"handler-{s}")
    if fail:
        raise RuntimeError("after opening it")
    return s


@app.task(name="uses_a_client")
async def uses_a_client(c=Depends(a_client)):
    _provided(f"handler-{c}")
    return c


@app.task(name="square")
def square(n):
    time.sleep(0.05 * (5 - n))  # the later ones finish first
    return n * n


@app.task(name="total")
def total(squares, label="sum"):
    return [label, squares, sum(squares)]
