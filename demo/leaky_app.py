"""A worker that leaks on purpose, the way real ones leak by accident.

Memory follows the payload — the shape no task-count recycling proxy can see
(bench/README.md) — and every task appends to a list nothing ever clears.
"""

import os
from pathlib import Path

from tarsk import App

app = App(broker=os.environ.get("TARSK_BROKER"), default_timeout=60, max_timeout=60)

_leaked: list[bytearray] = []
_log = open(os.environ["DEMO_LOG"], "a", buffering=1) if os.environ.get("DEMO_LOG") else None


@app.task(name="ingest", retries=2)
def ingest(tag: int, kilobytes: int) -> int:
    _leaked.append(bytearray(kilobytes * 1024))
    if _log:
        _log.write(f"{tag}\n")
    return os.getpid()
