"""Handler bodies shared by every framework under test.

Byte-identical work in all three workers, so any difference in the numbers
comes from the runtime rather than from the task. Completion is recorded by
appending one line to a file: no result backend, no extra broker traffic, and
the same cost everywhere.
"""

from __future__ import annotations

import os
import resource
import sys
import time

_LOG = os.environ.get("BENCH_LOG")
_handle = None
_retained: list[bytearray] = []


def peak_rss() -> int:
    """Own high-water RSS in bytes (ru_maxrss is KB on Linux, bytes on macOS)."""
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def _log():
    global _handle
    if _handle is None and _LOG:
        # Line-buffered O_APPEND: writes this short are atomic, so several
        # worker processes can share one file without interleaving.
        _handle = open(_LOG, "a", buffering=1)
    return _handle


def record(task_id: int, started: float) -> None:
    handle = _log()
    if handle:
        handle.write(f"{task_id}\t{os.getpid()}\t{peak_rss()}\t{started:.6f}\t{time.time():.6f}\n")


def leak(task_id: int, megabytes: int) -> int:
    """Retain memory forever — the failure mode the whole project is about.

    `megabytes = -1` means "payload-dependent": a deterministic spread of
    2–80MB, the shape a task-count recycling proxy cannot see. Deterministic so
    every runtime under test gets the identical sequence.
    """
    started = time.time()
    if megabytes < 0:
        megabytes = 2 + ((task_id * 2654435761) >> 8) % 79
    _retained.append(bytearray(megabytes * 1024 * 1024))
    record(task_id, started)
    return os.getpid()


def noop(task_id: int) -> int:
    started = time.time()
    record(task_id, started)
    return os.getpid()


def work5(task_id: int) -> int:
    started = time.time()
    time.sleep(0.005)
    record(task_id, started)
    return os.getpid()


def work50(task_id: int) -> int:
    """A realistic handler: 50ms, the low end of what spec §2 assumes."""
    started = time.time()
    time.sleep(0.05)
    record(task_id, started)
    return os.getpid()
