"""A deliberately leaky handler — the shape the whole project exists for."""

import os
import resource
import sys

from tarsk import App

app = App(default_timeout=30, max_timeout=30)

_leaked: list[bytearray] = []


def _peak_rss() -> int:
    """Own high-water RSS in bytes. ru_maxrss is KB on Linux, bytes on macOS."""
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


@app.task(name="leak")
def leak(megabytes):
    _leaked.append(bytearray(megabytes * 1024 * 1024))  # zero-filled, so genuinely resident
    return [os.getpid(), _peak_rss()]
