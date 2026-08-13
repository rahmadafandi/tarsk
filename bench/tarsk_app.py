"""tarsk app under test."""

import os

from bench import handlers
from tarsk import App

app = App(
    broker=os.environ.get("BENCH_REDIS", "redis://localhost:6379/0"),
    default_timeout=120,
    max_timeout=120,
)

for _name, _fn in (("leak", handlers.leak), ("noop", handlers.noop),
                   ("work5", handlers.work5), ("work50", handlers.work50)):
    app.task(name=_name)(_fn)
