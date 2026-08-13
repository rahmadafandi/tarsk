"""tarsk — memory-bounded task queue for Python.

Import invariant (spec §4.1): children import this module, so it must never pull
in the broker driver, scheduler, or supervisor. Those live in tarsk._supervisor
and belong to the parent process only.
"""

from __future__ import annotations

import functools
import hashlib
import importlib
import inspect
import secrets
import time
from dataclasses import dataclass

__all__ = ["App", "AsyncResult", "Task", "TaskSpec", "load_app"]

DEFAULT_TIMEOUT = 300.0  # seconds — spec §9, doubles as the hard cap


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """What the supervisor needs to run the retry/lease machinery for a task."""

    name: str
    timeout_ms: int
    retries: int
    backoff: str
    queue: str
    result_ttl_ms: int = 0
    cron: str = ""

    def as_row(self) -> list:
        """Positional form that crosses the wire (spec §4.2)."""
        return [
            self.name,
            self.timeout_ms,
            self.retries,
            self.backoff,
            self.queue,
            self.result_ttl_ms,
            self.cron,
        ]


class Task:
    def __init__(self, fn, spec: TaskSpec, app: "App"):
        self.fn = fn
        self.spec = spec
        self.app = app
        self.is_async = inspect.iscoroutinefunction(fn)
        functools.update_wrapper(self, fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def send(self, *args, **kwargs) -> str:
        """Enqueue this task and return its id.

        An id, not a future (spec §4.3). Nothing is written anywhere unless the
        task set `result_ttl`; the id is just how you would ask later.
        """
        return self._enqueue(0.0, args, kwargs)

    def send_in(self, delay: float, /, *args, **kwargs) -> str:
        """Enqueue, to run no earlier than `delay` seconds from now.

        `delay` is positional on purpose. The spec sketched
        `send_at(doc_id="abc", delay=60)`, which quietly breaks the day someone
        writes a task that takes an argument called `delay`.
        """
        if delay < 0:
            raise ValueError("delay must not be negative")
        return self._enqueue(delay, args, kwargs)

    def _enqueue(self, delay: float, args: tuple, kwargs: dict) -> str:
        from . import _proto  # lazy — see App.producer

        # Minted here so send() can answer without a round trip, and so the id
        # is known before the job exists anywhere.
        task_id = secrets.token_hex(8)
        payload = _proto.pack_args(args, kwargs)
        self.app.producer().send(
            task_id, self.spec.queue, self.spec.name, payload, self.spec.timeout_ms, delay
        )
        return task_id

    def __repr__(self) -> str:
        return f"<Task {self.spec.name}>"


class App:
    def __init__(
        self,
        broker: str | None = None,
        default_timeout: float = DEFAULT_TIMEOUT,
        max_timeout: float = DEFAULT_TIMEOUT,
    ):
        if max_timeout <= 0:
            raise ValueError("max_timeout must be positive")
        if default_timeout > max_timeout:
            raise ValueError(
                f"default_timeout={default_timeout}s exceeds max_timeout={max_timeout}s"
            )
        self.broker = broker
        self.default_timeout = default_timeout
        self.max_timeout = max_timeout
        self.registry: dict[str, Task] = {}
        self._producer = None

    def task(
        self,
        *,
        retries: int = 0,
        backoff: str = "exp",
        timeout: float | None = None,
        queue: str = "default",
        name: str | None = None,
        result_ttl: float = 0.0,
        cron: str = "",
    ):
        def decorate(fn):
            t = self.default_timeout if timeout is None else timeout
            if t <= 0:
                raise ValueError(f"{fn.__qualname__}: timeout must be positive")
            # Startup error, not a warning (spec §4.3): the cap is what bounds
            # lease TTL, and a bounded lease TTL is why there is no heartbeat.
            if t > self.max_timeout:
                raise ValueError(
                    f"{fn.__qualname__}: timeout={t}s exceeds max_timeout={self.max_timeout}s"
                )
            task_name = name or f"{fn.__module__}.{fn.__qualname__}"
            if task_name in self.registry:
                raise ValueError(f"duplicate task name {task_name!r}")
            if result_ttl < 0:
                raise ValueError(f"{fn.__qualname__}: result_ttl must not be negative")
            if cron and (args := inspect.signature(fn).parameters):
                required = [
                    p for p in args.values()
                    if p.default is p.empty and p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
                ]
                if required:
                    raise ValueError(
                        f"{fn.__qualname__}: a cron task is called with no arguments, but "
                        f"{[p.name for p in required]} have no default"
                    )
            spec = TaskSpec(
                task_name, int(t * 1000), retries, backoff, queue,
                int(result_ttl * 1000), cron,
            )
            task = Task(fn, spec, self)
            self.registry[task_name] = task
            return task

        return decorate

    def producer(self):
        """Connection to the broker for enqueueing.

        Imported lazily and cached: children import this module (spec §4.1) and
        must not pull in the extension module or a broker client to do it.
        """
        if self._producer is None:
            if not self.broker:
                raise RuntimeError("App(broker=...) is required to send tasks")
            from ._core import Producer

            self._producer = Producer(self.broker)
        return self._producer

    def result(self, task_id: str) -> "AsyncResult":
        """Handle for reading back a task that set `result_ttl`."""
        return AsyncResult(self, task_id)

    def registry_rows(self) -> list[list]:
        return [t.spec.as_row() for t in sorted(self.registry.values(), key=lambda t: t.spec.name)]

    def registry_hash(self) -> int:
        """Stable u64 over the registry — lets the supervisor reject stale children.

        repr() of a list of str/int is deterministic across processes, which
        keeps this module free of a msgpack import that producer code would
        otherwise pay for.
        """
        digest = hashlib.blake2b(repr(self.registry_rows()).encode(), digest_size=8)
        return int.from_bytes(digest.digest(), "big")


def load_app(spec: str) -> App:
    """Resolve a `module:attr` app spec — the `--app` flag from spec §4.3."""
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise ValueError(f"app spec must be 'module:attr', got {spec!r}")
    app = getattr(importlib.import_module(module_name), attr)
    if not isinstance(app, App):
        raise TypeError(f"{spec} is {type(app).__name__}, not an App")
    return app


class TaskFailed(Exception):
    """The handler raised, and the traceback came back with the result."""

    def __init__(self, error_type: str, traceback: str):
        super().__init__(f"{error_type}\n\n{traceback}".rstrip())
        self.error_type = error_type
        self.traceback = traceback


class AsyncResult:
    """A task's answer, once it has one.

    Nothing is stored unless the task set `result_ttl`, and nothing is stored
    forever. A result that has expired is indistinguishable from one that was
    never kept and from one still running — the broker holds no history to tell
    them apart, so `get()` reports a timeout rather than guessing which.
    """

    def __init__(self, app: App, task_id: str):
        self.app = app
        self.task_id = task_id

    def _fetch(self):
        from . import _proto  # lazy — see App.producer

        blob = self.app.producer().result(self.task_id)
        return None if blob is None else _proto.unpack_result(blob)

    def ready(self) -> bool:
        return self._fetch() is not None

    def get(self, timeout: float = 30.0, poll: float = 0.05):
        """Block until the answer lands, then return it or raise `TaskFailed`.

        ponytail: polls. A blocking read needs pub/sub on the broker, which is
        worth adding the day someone is waiting on tasks often enough to care.
        """
        deadline = time.monotonic() + timeout
        while True:
            found = self._fetch()
            if found is not None:
                ok, payload, error_type, traceback = found
                if ok:
                    return _unpack(payload)
                raise TaskFailed(error_type, traceback)
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"no result for {self.task_id} after {timeout}s — still running, "
                    "never kept, or expired"
                )
            time.sleep(poll)


def _unpack(payload: bytes):
    from . import _proto

    return _proto.unpack_result(payload)
