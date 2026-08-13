"""Task module for the step-1 protocol check — one task per path worth proving."""

import asyncio
import os
import time
from pathlib import Path

from tarsk import App

app = App(default_timeout=5, max_timeout=5)


@app.task(name="add")
async def add(a, b):
    return a + b


@app.task(name="boom")
async def boom():
    raise ValueError("kaboom")


@app.task(name="naps", timeout=0.3)
async def naps():
    await asyncio.sleep(10)


@app.task(name="slow_sync", timeout=0.3)
def slow_sync():
    time.sleep(10)
    return "unreachable"


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


@app.task(name="glutton", retries=1)
def glutton(megabytes):
    """Allocates past any sane ceiling and then lingers, so the hard limit has
    to catch it while it is still running rather than at a task boundary."""
    hoard = bytearray(megabytes * 1024 * 1024)
    time.sleep(5)
    return len(hoard)


# Lifecycle hooks: a file per child, so a test can count how often they ran.
HOOK_LOG = Path(os.environ.get("TARSK_HOOK_LOG", "/dev/null"))


@app.on_child_start
def opened():
    with open(HOOK_LOG, "a", buffering=1) as fh:
        fh.write(f"start {os.getpid()}\n")


@app.on_child_stop
def closed():
    with open(HOOK_LOG, "a", buffering=1) as fh:
        fh.write(f"stop {os.getpid()}\n")
