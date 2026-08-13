"""tarsk app for the `imports` scenario.

Deliberately minimal: the question is where *your* imports land, so the module
carries nothing but the toggle and one task.
"""

import os

with open(os.environ["BENCH_IMPORT_LOG"], "a", buffering=1) as _fh:
    _fh.write(f"{os.getpid()}\n")

if os.environ.get("BENCH_HEAVY"):
    import celery  # noqa: F401  — stands in for a real dependency tree
    import redis  # noqa: F401
    import taskiq  # noqa: F401

from tarsk import App

app = App(default_timeout=30, max_timeout=30)


@app.task(name="trivial")
def trivial(x):
    return x
