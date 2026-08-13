"""tarsk app under test."""

from bench import handlers
from tarsk import App

app = App(default_timeout=120, max_timeout=120)

for _name, _fn in (("leak", handlers.leak), ("noop", handlers.noop),
                   ("work5", handlers.work5), ("work50", handlers.work50)):
    app.task(name=_name)(_fn)
