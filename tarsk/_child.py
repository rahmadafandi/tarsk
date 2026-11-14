"""Child worker: `python -m tarsk._child <address> <module:app> <child-id> [slots]`

Imports the user's task modules and nothing from tarsk beyond the protocol
codec (spec §4.1). Child RSS is therefore CPython + user code.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import os
import signal
import sys
import traceback
from typing import Callable

from . import Context, Reject, Retry, SoftTimeout, Task, _finish, load_app
from . import _proto

# Distinct exit code so the supervisor can tell "recycled after a sync timeout"
# from a crash. Both are unclean; only one is the runtime's own doing.
EXIT_SYNC_TIMEOUT = 75
# A start hook raised. Distinct so the supervisor can say so rather than
# reporting a child that simply never showed up.
EXIT_STARTUP_FAILED = 78


class Wire:
    """Serialises every frame this child sends.

    Progress can arrive from a handler running in a thread while the loop is
    about to send an Ack. Two coroutines writing the same socket interleave
    their bytes, and a torn frame is not a recoverable protocol error.
    """

    def __init__(self, writer):
        self.writer = writer
        self.lock = asyncio.Lock()

    async def send(self, tag, *args):
        async with self.lock:
            await _proto.write(self.writer, tag, *args)

    def send_threadsafe(self, loop, tag, *args):
        """Block the calling thread until the frame is out."""
        asyncio.run_coroutine_threadsafe(self.send(tag, *args), loop).result()


async def _call(app, task: Task, ctx: Context, args: list, kwargs: dict,
                control: "_Control | None" = None):
    """Run one task, through its middleware, under its timeout (spec §4.5).

    The timeout covers the middleware too. A tracing layer that hangs holds the
    lease exactly as a handler would, so bounding only the innermost call would
    bound the wrong thing.
    """

    async def invoke():
        filled = dict(kwargs)
        finishers: list = []
        try:
            for name, dep in task.depends.items():
                if name in filled:
                    continue
                # Depends(Context) hands the handler its own call, rather than
                # making it reach for a global to find out what it is running.
                filled[name] = (ctx if dep.provider is Context
                                else await app.resolve(dep, finishers))
            if task.is_async:
                return await task.fn(*args, **filled)
            return await asyncio.to_thread(task.fn, *args, **filled)
        finally:
            # Whatever a task-scoped provider left after its `yield`, whether
            # the handler returned, raised or was cancelled.
            await _finish(finishers)

    chain = invoke
    for middleware in reversed(app.middlewares):
        if hasattr(middleware, "execute"):
            chain = functools.partial(_layer, middleware, ctx, chain)
    hard = task.spec.timeout_ms / 1000
    soft = task.spec.soft_timeout_ms / 1000
    return await _both_deadlines(chain, ctx, soft, hard, control if task.is_async else None)


class _Control:
    """How a job that is running can be reached from outside it.

    `stop` is filled in once the work has started, and cancels it. Only async
    handlers get one: a thread cannot be interrupted, so for a sync handler
    there is nothing a `stop` could do.
    """

    __slots__ = ("stop",)

    def __init__(self) -> None:
        self.stop: Callable[[], None] | None = None


class _Cancelled(Exception):
    """The job was cancelled while it ran, on the caller's say-so."""


class _Deadline(Exception):
    """The task's own `timeout` ran out.

    Not `TimeoutError`, which is what this used to be: that is also what a
    socket, `asyncio.timeout()` and `Future.result()` raise, so a handler whose
    HTTP call timed out after two seconds was reported as having exceeded its
    thirty — with no traceback, and, for a sync handler, by killing the worker
    and every task sharing it.
    """


async def _both_deadlines(chain, ctx: Context, soft: float, hard: float,
                          control: "_Control | None" = None):
    """Run the work under its deadlines, and report which one stopped it.

    `soft` is zero for a task that set none, and then only the hard one is
    armed.

    Cancellation is the only lever asyncio gives for interrupting a coroutine,
    so the handler sees `CancelledError` and not a type of our choosing. What
    this adds is the reason: `ctx.soft_expired` is set before the cancel lands,
    so a handler cleaning up can tell a passed deadline from a worker shutting
    down, and the exception raised afterwards names which deadline it was.

    Both timers rather than wrapping this in `wait_for`. Nested, the hard
    timeout cancels *this* coroutine while `asked` is already true, so a job
    stopped without warning would be reported as one that had been asked — and
    converting that cancellation into an exception swallows the cancellation
    `wait_for` is relying on. One task, two timers, one place that cancels it.

    The hard deadline still applies to a handler that swallows the ask: the
    soft one is a request, not a second enforcer. Two handlers neither deadline
    reaches, both for the same reason and neither new here. One that swallows
    *every* cancellation, looping on `except CancelledError: pass`, outlives
    both timers — asyncio has no lever past cancellation, and `wait_for` waits
    on the task it cancelled, so that handler hung the same way before there
    was a soft deadline. And a sync middleware anywhere in the chain runs in a
    thread holding a blocking call into the loop, so the cancellation lands on
    the middleware: the job is reported as a soft timeout without the handler
    ever getting the chance the name promises.
    """
    running = asyncio.ensure_future(chain())
    fired = ""

    def ask() -> None:
        nonlocal fired
        fired = "soft"
        # Context is frozen so handlers cannot rewrite their own call. This is
        # the worker writing to it, which is the one exception, and it happens
        # before the cancel so the flag is already true when the handler wakes
        # up inside its except block.
        object.__setattr__(ctx, "soft_expired", True)
        running.cancel()

    def take() -> None:
        nonlocal fired
        fired = "hard"
        running.cancel()

    def stop() -> None:
        # A third reason to cancel, and the same care as the soft deadline:
        # the flag is set first, so the handler waking up in its `except` can
        # already read why.
        nonlocal fired
        if not fired:
            fired = "cancel"
            object.__setattr__(ctx, "cancelled", True)
            running.cancel()

    if control is not None:
        control.stop = stop

    loop = asyncio.get_running_loop()
    timers = [loop.call_later(hard, take)]
    if soft:
        timers.append(loop.call_later(soft, ask))
    try:
        # A handler that catches the cancellation and returns anyway lands
        # here with a value: it was asked, it tidied up, it finished in time.
        # That is the whole point of asking, so it is not an error.
        return await running
    except asyncio.CancelledError:
        if fired == "hard":
            raise _Deadline(f"exceeded timeout of {hard}s") from None
        if fired == "soft":
            raise SoftTimeout(f"exceeded soft_timeout of {soft}s") from None
        if fired == "cancel":
            raise _Cancelled("cancelled while running") from None
        raise  # the worker is going down, not a deadline
    finally:
        for timer in timers:
            timer.cancel()


async def _layer(middleware, ctx: Context, nxt):
    """Run one middleware around the rest of the chain, sync or async.

    A sync `execute` runs in a thread and is handed a blocking `call()`, so it
    can wrap the work with a plain `with` block. The thread waits on the inner
    layers while the event loop runs them — the same trade already made for
    sync handlers, and refusing it here while accepting it there would be an
    inconsistency rather than a principle.
    """
    if inspect.iscoroutinefunction(middleware.execute):
        return await middleware.execute(ctx, nxt)

    loop = asyncio.get_running_loop()

    def blocking_call():
        return asyncio.run_coroutine_threadsafe(nxt(), loop).result()

    return await asyncio.to_thread(middleware.execute, ctx, blocking_call)


async def _connect(address: str):
    """Open the channel the supervisor is listening on.

    A Unix socket where there is one, a named pipe on Windows. Not TCP on
    either: a loopback port is reachable by every process on the machine, and
    what crosses here is task payloads and the word that a job finished.

    asyncio gives Unix sockets a stream helper and named pipes only a transport,
    so the Windows branch assembles the same pair by hand.
    """
    if not address.startswith(r"\\"):
        return await asyncio.open_unix_connection(address)

    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    # Only the Windows event loop has this, and only Windows reaches here.
    connect = getattr(loop, "create_pipe_connection")
    transport, _ = await connect(lambda: protocol, address)
    return reader, asyncio.StreamWriter(transport, protocol, reader, loop)


async def _die(writer: asyncio.StreamWriter, code: int) -> None:
    """Flush what we owe the supervisor, then leave."""
    writer.close()
    try:
        await writer.wait_closed()
    except OSError:
        pass
    os._exit(code)


async def _run_hooks(hooks) -> None:
    for hook in hooks:
        result = hook()
        if inspect.isawaitable(result):
            await result


async def _shutdown(app) -> None:
    """Let the child put its own things down before the process ends."""
    try:
        await _run_hooks(app.stop_hooks)
    except Exception:
        traceback.print_exc()
    # After the stop hooks: a hook may still want the pool a provider opened.
    await app.finish()


# Progress frames in flight from async handlers. The loop keeps only a weak
# reference to a task, so one nobody holds can be collected before it runs.
_reports: set[asyncio.Task] = set()


async def _one(app, wire: Wire, app_spec: str, task_id: int, name: str, payload,
               meta: bytes = b"", attempt: int = 1, job_id: str = "",
               control: "_Control | None" = None) -> tuple[tuple, bool]:
    """Run a single dispatched task. Never raises.

    Returns the frame that reports it, and whether this process can go on —
    which it cannot after a sync handler has outlived its timeout. The caller
    sends the frame, so it can measure what the task left behind once every
    local in here has been let go.
    """
    task = app.registry.get(name)
    if task is None:
        return ("Nack", task_id, "UnknownTask",
                f"no task named {name!r} in {app_spec}", "", 0), True

    try:
        call_args, call_kwargs = _proto.unpack_args(payload)
        loop = asyncio.get_running_loop()

        def emit(value, _id=task_id):
            payload = _proto.pack_result(value)
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                # A sync handler calls this from its executor thread: block
                # that thread until the frame is out, so progress is on the
                # wire before the work continues.
                wire.send_threadsafe(loop, "Progress", _id, payload)
            else:
                # An async handler calls this from the loop thread itself, and
                # blocking here is a deadlock: .result() would wait on a
                # coroutine the blocked loop can never run — freezing not just
                # this task but every slot in the child, since the read loop
                # shares the same thread. Scheduled instead; Wire's lock keeps
                # the frame whole against concurrent Acks.
                report = loop.create_task(wire.send("Progress", _id, payload))
                _reports.add(report)
                report.add_done_callback(_reports.discard)

        ctx = Context(
            name=name,
            # The id `send()` returned, so a trace started in `before_send`
            # continues here. Batch jobs given as bare triples have none, and
            # fall back to the supervisor's counter for the frame.
            task_id=job_id or str(task_id),
            attempt=attempt,
            args=tuple(call_args),
            kwargs=call_kwargs,
            meta=_proto.unpack_result(meta) if meta else {},
            _emit=emit,
        )
        result = await _call(app, task, ctx, call_args, call_kwargs, control)
    except _Cancelled as exc:
        # Settled and dropped: not retried, and not parked where a replay
        # would bring back something its caller asked to be rid of.
        return ("Nack", task_id, "Cancelled", str(exc), "drop", 0), True
    except SoftTimeout as exc:
        # Its own outcome rather than folded into TimeoutError: the two say
        # different things about the handler. This one was asked and stopped
        # anyway; the other was never asked.
        return ("Nack", task_id, "SoftTimeout", f"{type(exc).__name__}: {exc}", "", 0), True
    except Reject as exc:
        # The handler knows this will never work. Skip the remaining attempts
        # rather than spending them to reach the same answer.
        return ("Nack", task_id, "Reject", f"{type(exc).__name__}: {exc}", "reject", 0), True
    except Retry as exc:
        return ("Nack", task_id, "Retry", f"{type(exc).__name__}: {exc}",
                "retry", int(exc.delay * 1000)), True
    except _Deadline:
        # A Python thread cannot be interrupted (spec §4.5). After a sync
        # handler times out the work is still running and still holds whatever
        # it was stopped for, so the process has to go — and with more than
        # one slot it takes its siblings with it. Their leases expire and the
        # supervisor redelivers them; that is the cost of sharing a process.
        return ("Nack", task_id, "TimeoutError",
                f"{name} exceeded timeout of {task.spec.timeout_ms / 1000}s", "", 0), task.is_async
    except Exception as exc:
        # Pre-formatted string — never reconstructed Rust-side (spec §4.2).
        return ("Nack", task_id, type(exc).__name__, traceback.format_exc(), "", 0), True
    else:
        return ("Ack", task_id, _proto.pack_result(result)), True


async def main(socket_path: str, app_spec: str, child_id: int, slots: int = 1) -> None:
    app = load_app(app_spec)
    # Before Register, so whatever a hook opens is inside the baseline the
    # supervisor measures against the ceiling. A pool that does not fit should
    # be refused at startup, not discovered as a child that recycles instantly.
    try:
        await _run_hooks(app.start_hooks)
    except Exception:
        traceback.print_exc()
        sys.exit(EXIT_STARTUP_FAILED)
    reader, raw_writer = await _connect(socket_path)
    wire = Wire(raw_writer)
    writer = raw_writer  # kept for the shutdown path, which owns the socket
    # child_id is assigned by the supervisor before spawn, so it can match this
    # connection to the process it started — and to that process's RSS.
    await wire.send("Register", child_id, app.registry_hash(), app.registry_rows())

    # One Ready per free slot. At slots=1 this is the original strict
    # request/response loop; above it, the supervisor keeps as many tasks in
    # flight here as there are Readys outstanding.
    if slots > 1:
        # asyncio's default executor is min(32, cpu + 4) threads, so a sync
        # handler would cap at that regardless of the slot count — 64 slots
        # quietly running 20 at a time.
        #
        # One thread per slot is not enough. A sync middleware runs in a thread
        # and then *blocks that thread* waiting for the layers inside it, so a
        # sync handler behind two sync middlewares needs three threads at once
        # for one task. Sized to slots alone, every slot takes a thread for its
        # outermost layer and then waits for one that will never come free:
        # a deadlock that lasts until the task timeout, and one that only shows
        # up when the dispatches arrive close enough together to fill the pool
        # before any of them finishes. A fast machine wins that race and a
        # loaded one does not, which is a bug that hides in exactly the place
        # it will eventually be found.
        blocking_layers = sum(
            1
            for m in app.middlewares
            if hasattr(m, "execute") and not inspect.iscoroutinefunction(m.execute)
        )
        asyncio.get_running_loop().set_default_executor(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=slots * (1 + blocking_layers),
                thread_name_prefix="tarsk-task",
            )
        )

    running: set[asyncio.Task] = set()
    # Dispatch id to the handle that can stop that job; see `_Control`.
    controls: dict[int, _Control] = {}
    draining = False
    leaving = False

    # A terminal's ctrl-C and systemd's default stop both signal the whole
    # process group, children included. The supervisor is the one that decides
    # when a child stops — it sends Drain and waits for in-flight work — so a
    # child that died on the same signal turned every graceful shutdown into
    # tasks cut off mid-run. SIGTERM is honoured once this child is already on
    # its way out, which is when the supervisor sends it as an escalation.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    def on_term() -> None:
        if leaving:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGTERM)

    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, on_term)
    except NotImplementedError:  # Windows: no SIGTERM to catch
        pass
    for _ in range(slots):
        await wire.send("Ready")

    async def run_and_report(task_id, name, payload, meta, attempt, job_id):
        # How many objects the interpreter is holding, before and after. What a
        # task leaves behind is what it leaked, and unlike the process's size
        # this is not fooled by the allocator handing one task's freed memory
        # to the next: a block is counted where it is kept, not where the
        # process happened to grow. Two calls that read a counter.
        before = sys.getallocatedblocks()
        control = controls[task_id] = _Control()
        try:
            frame, alive = await _one(app, wire, app_spec, task_id, name, payload, meta,
                                      attempt, job_id, control)
        finally:
            controls.pop(task_id, None)
        await wire.send(*frame, sys.getallocatedblocks() - before)
        if not alive:
            # Nothing above is awaiting this task, so the exit has to happen
            # here — letting it settle into the task object would leave the
            # process running with the thread it could not interrupt.
            await _die(writer, EXIT_SYNC_TIMEOUT)
        if not draining:
            await wire.send("Ready")

    while True:
        frame = await _proto.read(reader)
        if frame is None:
            break  # supervisor went away
        tag, args = frame
        if tag == "Drain":
            draining = True
            break
        if tag == "Cancel":
            # For a job that has already finished, or a sync handler, there is
            # nothing to stop and nothing to say: the supervisor learns how it
            # ended from the frame that reports it.
            control = controls.get(args[0])
            if control is not None and control.stop is not None:
                control.stop()
            continue
        if tag != "Dispatch":
            raise RuntimeError(f"unexpected frame from supervisor: {tag!r}")
        # Indexed rather than unpacked: the frame grew a field for the sender's
        # metadata and then one for the attempt, and an older supervisor still
        # sends the shorter frame.
        task_id, name, payload = args[0], args[1], args[2]
        meta = args[3] if len(args) > 3 else b""
        attempt = args[4] if len(args) > 4 else 1
        job_id = args[5] if len(args) > 5 else ""
        job = asyncio.create_task(
            run_and_report(task_id, name, payload, meta, attempt, job_id)
        )
        running.add(job)
        job.add_done_callback(running.discard)

    leaving = True
    # Whatever is still in flight owns a lease; finishing it is cheaper than
    # letting the supervisor time it out and redeliver.
    if running:
        await asyncio.gather(*running, return_exceptions=True)
    await _shutdown(app)


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        sys.exit("usage: python -m tarsk._child <socket-path> <module:app> <child-id> [slots]")
    slots = int(sys.argv[4]) if len(sys.argv) == 5 else 1
    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3]), slots))
