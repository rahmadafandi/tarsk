"""tarsk — memory-bounded task queue for Python.

Import invariant (spec §4.1): children import this module, so it must never pull
in the broker driver, scheduler, or supervisor. Those live in tarsk._supervisor
and belong to the parent process only.
"""

from __future__ import annotations

import asyncio
import datetime
import functools
import hashlib
import importlib
import inspect
import re
import secrets
import time
from dataclasses import dataclass, field

__all__ = [
    "App", "AsyncResult", "Chain", "Context", "Depends", "Group", "Signature",
    "Task", "TaskFailed", "TaskSpec", "Reject", "Retry", "chain", "group",
    "load_app",
]

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
    # Tokens per second and bucket depth, both zero when unlimited. Kept as
    # numbers rather than the "10/m" the user wrote, so the supervisor — which
    # cannot import their code — never has to parse anything.
    rate_per_sec: float = 0.0
    rate_burst: int = 0
    # Milliseconds a job may wait after becoming runnable before it is dropped
    # instead of run. Zero means never, which is the default: work that
    # disappears by default would be a strange promise for this project.
    expires_ms: int = 0
    # How many of this task may run at once across every worker. Zero is
    # unlimited. Different from a rate limit: that bounds how often something
    # starts, this bounds how many are in progress.
    max_concurrency: int = 0
    # Milliseconds during which an identical send is dropped. Zero is off.
    unique_ms: int = 0
    # Milliseconds during which an identical send is dropped. Zero is off.
    unique_ms: int = 0

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
            self.rate_per_sec,
            self.rate_burst,
            self.expires_ms,
            self.max_concurrency,
        ]


@dataclass(frozen=True, slots=True)
class Context:
    """What a middleware is told about the call it is wrapping.

    Handlers can ask for it too, with `ctx=Depends(Context)`.
    """

    name: str
    task_id: str
    attempt: int
    args: tuple
    kwargs: dict
    #: Whatever the sender attached with `meta=`, and empty when they did not.
    #: Carried beside the arguments rather than inside them, so a listing can
    #: read it without unpacking a call it does not understand.
    meta: dict = field(default_factory=dict)
    #: Set by the worker. Sends progress to the supervisor, which is the only
    #: process here holding a broker connection (spec §4.1).
    _emit: object = None

    def set_progress(self, value) -> None:
        """Publish where this task has got to, readable via AsyncResult.

        Only kept for tasks that set `result_ttl`, under the same expiry: a
        progress record that outlives interest in the result is just a leak
        that reports on itself.
        """
        if self._emit is None:
            raise RuntimeError("set_progress is only available inside a running task")
        self._emit(value)


class Depends:
    """A parameter the caller does not supply and the worker resolves.

    `scope="worker"` resolves once per worker process and is reused — which in
    this design is what a module-level global would have been, except it can be
    replaced in a test. `scope="task"` resolves per call.
    """

    __slots__ = ("provider", "scope")

    def __init__(self, provider, *, scope: str = "worker"):
        if scope not in ("worker", "task"):
            raise ValueError(f"scope must be 'worker' or 'task', got {scope!r}")
        self.provider = provider
        self.scope = scope


class Task:
    def __init__(self, fn, spec: TaskSpec, app: "App"):
        self.fn = fn
        self.spec = spec
        self.app = app
        self.is_async = inspect.iscoroutinefunction(fn)
        # Worked out once, at import, rather than on every dispatch.
        self.depends = {
            name: param.default
            for name, param in inspect.signature(fn).parameters.items()
            if isinstance(param.default, Depends)
        }
        functools.update_wrapper(self, fn)

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def send(self, *args, **kwargs) -> str:
        """Enqueue this task and return its id.

        An id, not a future (spec §4.3). Nothing is written anywhere unless the
        task set `result_ttl`; the id is just how you would ask later.
        """
        return Enqueue(self).send(*args, **kwargs)

    async def send_async(self, *args, **kwargs) -> str:
        """Enqueue without blocking the event loop.

        The Rust producer releases the GIL for the round trip, so handing it to
        a thread frees the loop for the whole wait rather than only for the
        parts Python was not holding. Against a Redis on the same machine that
        wait is tens of microseconds and this is not worth it; against a
        managed one over TLS it is milliseconds, and a web handler that blocks
        for milliseconds per enqueue is a web handler with a throughput ceiling.
        """
        # ponytail: a thread hop, not a native async client. The ceiling is
        # asyncio's default executor, min(32, cpu + 4) threads, so a burst
        # wider than that queues — measured at 500 concurrent enqueues in 57ms.
        # Lifting it means bridging the tokio runtime into the event loop with
        # pyo3-async-runtimes, which is worth doing the day someone is
        # enqueueing fast enough to see the pool rather than the network.
        return await asyncio.to_thread(self.send, *args, **kwargs)

    def s(self, *args, **kwargs) -> "Signature":
        """This task with these arguments, fed the previous step's result."""
        return Signature(self, args, kwargs, feed=True)

    def si(self, *args, **kwargs) -> "Signature":
        """This task with these arguments and nothing else — `i` for immutable."""
        return Signature(self, args, kwargs, feed=False)

    def send_in(self, delay: float, /, *args, **kwargs) -> str:
        """Enqueue, to run no earlier than `delay` seconds from now.

        `delay` is positional on purpose. The spec sketched
        `send_at(doc_id="abc", delay=60)`, which quietly breaks the day someone
        writes a task that takes an argument called `delay`.
        """
        return Enqueue(self, delay=delay).send(*args, **kwargs)

    def send_at(self, when: "datetime.datetime", /, *args, **kwargs) -> str:
        """Enqueue for an absolute time. Must be timezone-aware.

        A naive datetime is the same bug as a local-time cron: it means
        something different depending on where it is read.
        """
        return Enqueue(self, when=when).send(*args, **kwargs)

    def options(
        self,
        *,
        queue: str | None = None,
        timeout: float | None = None,
        delay: float = 0.0,
        when: "datetime.datetime | None" = None,
        task_id: str | None = None,
        dedup_key: str = "",
        dedup_ttl: float | None = None,
        meta: dict | None = None,
    ) -> "Enqueue":
        """Override what this one send does, without touching the registration.

        Keyword-only and on a separate object so that nothing here can collide
        with an argument the task itself takes.
        """
        return Enqueue(self, queue=queue, timeout=timeout, delay=delay, when=when,
                       dedup_key=dedup_key, dedup_ttl=dedup_ttl, meta=meta,
                       task_id=task_id)

    def __repr__(self) -> str:
        return f"<Task {self.spec.name}>"


class Enqueue:
    """One send, with the registration's defaults optionally overridden."""

    def __init__(self, task: Task, *, queue=None, timeout=None, delay=0.0, when=None,
                 chain: bytes = b"", dedup_key: str = "", dedup_ttl: float | None = None,
                 meta: dict | None = None, task_id=None):
        if delay and when is not None:
            raise ValueError("give a delay or a time, not both")
        if when is not None:
            if when.tzinfo is None:
                raise ValueError("send_at needs a timezone-aware datetime")
            delay = max(0.0, when.timestamp() - datetime.datetime.now(datetime.UTC).timestamp())
        if delay < 0:
            raise ValueError("delay must not be negative")
        if timeout is not None and timeout > task.app.max_timeout:
            raise ValueError(
                f"timeout={timeout}s exceeds max_timeout={task.app.max_timeout}s"
            )
        self.task = task
        self.queue = queue or task.spec.queue
        self.timeout_ms = task.spec.timeout_ms if timeout is None else int(timeout * 1000)
        self.delay = delay
        # A caller-supplied id files both sends' results under one key. It is
        # NOT deduplication — the queue takes both jobs and runs them both,
        # and only the answers collide. `dedup_key` or `unique=` is what stops
        # the second send; this used to claim otherwise.
        self.task_id = task_id or secrets.token_hex(8)
        self.chain = chain
        self.dedup_key = dedup_key
        self.dedup_ttl = dedup_ttl
        self.meta = meta or {}

    def send(self, *args, **kwargs) -> str:
        from . import _proto  # lazy — see App.producer

        app = self.task.app
        if app.middlewares:
            # Producer side, so sync only: send() is not a coroutine, and
            # making it one to accommodate a hook would be the tail wagging.
            ctx = Context(
                name=self.task.spec.name,
                task_id=self.task_id,
                attempt=0,  # not run yet
                args=args,
                kwargs=kwargs,
            )
            for middleware in app.middlewares:
                hook = getattr(middleware, "before_send", None)
                if hook is not None:
                    hook(ctx)
            kwargs = ctx.kwargs  # mutable, so a trace id can be attached here
        payload = _proto.pack_args(args, kwargs)
        key, ttl_ms = self._dedup(payload)
        held = self.task.app.producer().send(
            self.task_id, self.queue, self.task.spec.name, payload,
            self.timeout_ms, self.delay, self.chain,
            _proto.pack_result(self.meta) if self.meta else b"",
            key, ttl_ms,
        )
        # A deduplicated send hands back the id of the job already covering it,
        # so the caller waits on the same answer instead of on a job that was
        # never queued.
        return held or self.task_id

    def _dedup(self, payload: bytes) -> tuple[str, int]:
        ttl = self.dedup_ttl if self.dedup_ttl is not None else self.task.spec.unique_ms / 1000
        if not ttl:
            return "", 0
        if self.dedup_key:
            return f"{self.task.spec.name}:{self.dedup_key}", int(ttl * 1000)
        # No key given, so the arguments are the key: same task, same call.
        import hashlib

        digest = hashlib.blake2b(payload, digest_size=16).hexdigest()
        return f"{self.task.spec.name}:{digest}", int(ttl * 1000)


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
        self.start_hooks: list = []
        self.stop_hooks: list = []
        self.middlewares: list = []
        self._provided: dict = {}
        self._overrides: dict = {}
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
        rate_limit: str = "",
        expires: float = 0.0,
        max_concurrency: int = 0,
        unique: float = 0.0,
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
                ]  # a Depends is a default, so injected parameters do not count
                if required:
                    raise ValueError(
                        f"{fn.__qualname__}: a cron task is called with no arguments, but "
                        f"{[p.name for p in required]} have no default"
                    )
            # Parsed here, where a bad string is a startup error the author
            # sees, rather than in the supervisor where it would be a task that
            # silently never runs.
            per_sec, burst = parse_rate(rate_limit) if rate_limit else (0.0, 0)
            if expires < 0:
                raise ValueError(f"{fn.__qualname__}: expires must not be negative")
            spec = TaskSpec(
                task_name, int(t * 1000), retries, backoff, queue,
                int(result_ttl * 1000), cron, per_sec, burst, int(expires * 1000),
                max(0, max_concurrency), int(max(0.0, unique) * 1000),
            )
            task = Task(fn, spec, self)
            self.registry[task_name] = task
            return task

        return decorate

    def on_start(self, fn):
        """Run in each worker process before it takes any work.

        **Once per process, not once per deployment.** Workers are replaced
        whenever they hit a recycle limit, so this runs again every time —
        dozens of times an hour under a tight `--max-rss`. It is the place for
        a connection pool, not for a migration or a "we are up" notification.

        A pool opened at import time instead lands in every worker's baseline
        RSS, which is the number `--max-rss` is measured against, so the
        alternative is not merely untidy: it spends the budget the ceiling
        exists to protect.
        """
        self.start_hooks.append(fn)
        return fn

    def on_stop(self, fn):
        """Run in each worker process as it drains, before it exits.

        Also once per process — its pair above runs just as often. Without it
        the only thing closing a connection is the process ending, which the
        server on the other end experiences as a reset.
        """
        self.stop_hooks.append(fn)
        return fn

    def middleware(self, mw):
        """Wrap every task this worker runs.

        A middleware defines `execute(ctx, call)` and awaits `call()` to run the
        rest — an onion, so tracing and transactions work by holding the call
        open rather than by bracketing it with two callbacks.

        `execute` may be sync or async. A sync one runs in a thread and gets a
        `call()` that blocks it until the inner layers finish, so `with` works
        the way it looks like it should. It costs a thread for the length of
        the task, which is the same trade sync handlers already make.

        `before_send(ctx)` additionally runs in the producer, before the job is
        serialised, and may add to `ctx.kwargs` — which is how a trace id gets
        attached to work that has not left the process yet. It is sync: `send()`
        is not a coroutine.

        Neither can run in the supervisor: that process never
        imports your code, which is what keeps its footprint a constant
        (spec §4.1). There is no hook for "result stored" for the same reason.
        """
        self.middlewares.append(mw)
        return mw

    def override(self, provider, replacement):
        """Swap what a `Depends` provider returns. For tests."""
        self._overrides[provider] = replacement

    async def resolve(self, dep: Depends):
        provider = self._overrides.get(dep.provider, dep.provider)
        if dep.scope == "worker" and provider in self._provided:
            return self._provided[provider]
        value = provider()
        if inspect.isawaitable(value):
            value = await value
        if dep.scope == "worker":
            self._provided[provider] = value
        return value

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

    def cancel(self, task_id: str, *, queue: str = "default", ttl: float = 86400.0) -> None:
        """Stop a job from running. Takes effect within a second.

        A job that has already started is *not* interrupted. The supervisor
        could tell the child to drop it, but a handler that has begun has
        usually begun doing the thing you wanted stopped — written the row,
        called the API — and a queue that reports "cancelled" for work that
        half-happened is worse than one that admits it finished.

        `ttl` is how long the cancellation is remembered, and it has to outlive
        the job: cancelling something scheduled for next week needs a week.
        """
        self.producer().cancel(task_id, queue=queue, ttl=ttl)

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


_RATE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*/\s*(\d*)\s*([smh])\s*$")
_PER = {"s": 1.0, "m": 60.0, "h": 3600.0}


def parse_rate(text: str) -> tuple[float, int]:
    """`"10/s"`, `"100/m"`, `"2/h"`, `"30/5m"` → (tokens per second, burst).

    Burst is the numerator: `"10/s"` lets ten run at once and then refills at
    ten a second, which is what someone protecting a quota of ten per second
    means. A bucket that refills smoothly but never holds more than one token
    would turn a burst limit into a spacing rule nobody asked for.
    """
    match = _RATE.match(text)
    if not match:
        raise ValueError(f"not a rate: {text!r} (try '10/s', '100/m', '30/5m')")
    count, every, unit = match.groups()
    seconds = _PER[unit] * (int(every) if every else 1)
    return float(count) / seconds, max(1, int(float(count)))


class Signature:
    """A task with its arguments, not yet sent.

    `t.s(1)` feeds the previous step's result in front of those arguments when
    it runs in a chain; `t.si(1)` ignores it. Celery spells these the same way,
    and the distinction is load-bearing rather than decorative: most second
    steps want what came before, and the ones that do not would otherwise break
    on an argument they never declared.
    """

    __slots__ = ("task", "args", "kwargs", "feed", "task_id")

    def __init__(self, task: "Task", args: tuple, kwargs: dict, feed: bool):
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.feed = feed
        self.task_id = secrets.token_hex(8)

    def _row(self) -> list:
        from . import _proto

        return [
            self.task_id,
            self.task.spec.name,
            _proto.pack_args(self.args, self.kwargs),
            self.task.spec.timeout_ms,
            self.task.spec.queue,
            self.feed,
        ]

    def __repr__(self) -> str:
        return f"<Signature {self.task.spec.name} feed={self.feed}>"


class Chain:
    """Steps that run one after another, each fed the last one's result."""

    __slots__ = ("steps",)

    def __init__(self, steps: list[Signature]):
        if not steps:
            raise ValueError("a chain needs at least one step")
        self.steps = steps

    @property
    def result_id(self) -> str:
        """The id the final step's result will be filed under.

        Known before anything runs, because every step's id is minted here
        rather than by whichever worker happens to queue it. That is what lets
        you hold a handle to the end of a chain while its first step is still
        being written.
        """
        return self.steps[-1].task_id

    def send(self) -> str:
        from . import _proto

        head, rest = self.steps[0], self.steps[1:]
        chain = _proto.pack_chain([s._row() for s in rest]) if rest else b""
        Enqueue(head.task, task_id=head.task_id, chain=chain).send(*head.args, **head.kwargs)
        return self.result_id

    async def send_async(self) -> str:
        return await asyncio.to_thread(self.send)


class Group:
    """Steps with nothing between them: sent together, run wherever there is room.

    There is no coordination here and none is needed — the queue already runs
    what it can in parallel. What this adds over a list comprehension is that
    the ids exist before the sending does, so the handles can be held while the
    work is still going out.
    """

    __slots__ = ("steps",)

    def __init__(self, steps: list[Signature]):
        if not steps:
            raise ValueError("a group needs at least one step")
        self.steps = steps

    @property
    def result_ids(self) -> list[str]:
        return [s.task_id for s in self.steps]

    def send(self) -> list[str]:
        for step in self.steps:
            Enqueue(step.task, task_id=step.task_id).send(*step.args, **step.kwargs)
        return self.result_ids

    async def send_async(self) -> list[str]:
        return await asyncio.to_thread(self.send)

    def results(self, app: "App") -> list["AsyncResult"]:
        return [app.result(i) for i in self.result_ids]


def group(*steps: Signature) -> Group:
    """`group(resize.s(f) for f in files)` — or just pass them positionally."""
    if len(steps) == 1 and not isinstance(steps[0], Signature):
        steps = tuple(steps[0])  # an iterable, the way people write it
    return Group(list(steps))


def chain(*steps: Signature) -> Chain:
    """`chain(fetch.s(url), parse.s(), store.si("bucket")).send()`

    Only the first step is queued now. Each one queues the next when it
    succeeds, so a chain that fails partway stops there — the remaining steps
    were never enqueued and there is nothing to cancel.
    """
    return Chain(list(steps))


def load_app(spec: str) -> App:
    """Resolve a `module:attr` app spec — the `--app` flag from spec §4.3."""
    module_name, _, attr = spec.partition(":")
    if not module_name or not attr:
        raise ValueError(f"app spec must be 'module:attr', got {spec!r}")
    app = getattr(importlib.import_module(module_name), attr)
    if not isinstance(app, App):
        raise TypeError(f"{spec} is {type(app).__name__}, not an App")
    return app


class Reject(Exception):
    """Give up on this job now: no more retries, straight to the dead letters.

    For work that will never succeed however many times it is tried — a payload
    that cannot be parsed, a row that no longer exists. Spending the retry
    budget on those only delays the moment someone looks at them.
    """


class Retry(Exception):
    """Hand this job back, optionally after `delay` seconds.

    Still charged an attempt. A retry that costs nothing is an infinite loop
    with extra steps, and the thing being waited on is usually the thing least
    able to absorb one.
    """

    def __init__(self, message: str = "", *, delay: float = 0.0):
        super().__init__(message or f"retry in {delay}s")
        self.delay = max(0.0, delay)


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

    def cancel(self, *, queue: str = "default", ttl: float = 86400.0) -> None:
        """Stop this job from running, if it has not started. See `App.cancel`."""
        self.app.cancel(self.task_id, queue=queue, ttl=ttl)

    async def get_async(self, timeout: float = 30.0, poll: float = 0.05):
        """Wait for the answer without blocking the event loop.

        The blocking version sleeps between polls, so a handler awaiting a
        result would hold the loop for up to `timeout` — thirty seconds by
        default, which is not a hiccup but an outage.
        """
        deadline = time.monotonic() + timeout
        while True:
            found = await asyncio.to_thread(self._fetch)
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
            await asyncio.sleep(poll)

    def progress(self):
        """The last value the task published, or None."""
        from . import _proto

        blob = self.app.producer().result(f"progress:{self.task_id}")
        return None if blob is None else _proto.unpack_result(blob)

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
