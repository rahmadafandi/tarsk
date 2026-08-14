"""A deliberately leaky handler — the shape the whole project exists for."""

import os
import sys

from tarsk import App

try:
    import resource  # POSIX only
except ImportError:  # Windows
    resource = None

app = App(default_timeout=30, max_timeout=30)

_leaked: list[bytearray] = []


def _peak_rss() -> int:
    """Own high-water RSS in bytes, or 0 where the platform will not say.

    ru_maxrss is kilobytes on Linux and bytes on macOS; Windows has no
    `resource` module at all. Returning zero there is honest — this figure is
    the handler reporting on itself, and the ceiling is enforced from the
    supervisor, which reads RSS through sysinfo and works on all three.
    """
    if resource is None:
        return 0
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


@app.task(name="leak")
def leak(megabytes):
    _leaked.append(bytearray(megabytes * 1024 * 1024))  # zero-filled, so genuinely resident
    return [os.getpid(), _peak_rss()]
