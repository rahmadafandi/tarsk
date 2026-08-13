"""Child worker: `python -m tarsk._child <socket-path> <module:app> <child-id>`

Imports the user's task modules and nothing from tarsk beyond the protocol
codec (spec §4.1). Child RSS is therefore CPython + user code.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import functools
import inspect
import os
import sys
import traceback

from . import Context, Reject, Retry, Task, load_app
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


async def _call(app, task: Task, ctx: Context, args: list, kwargs: dict):
    """Run one task, through its middleware, under its timeout (spec §4.5).

    The timeout covers the middleware too. A tracing layer that hangs holds the
    lease exactly as a handler would, so bounding only the innermost call would
    bound the wrong thing.
    """

    async def invoke():
        filled = dict(kwargs)
        for name, dep in task.depends.items():
            if name in filled:
                continue
            # Depends(Context) hands the handler its own call, rather than
            # making it reach for a global to find out what it is running.
            filled[name] = ctx if dep.provider is Context else await app.resolve(dep)
        if task.is_async:
            return await task.fn(*args, **filled)
        return await asyncio.to_thread(task.fn, *args, **filled)

    chain = invoke
    for middleware in reversed(app.middlewares):
        if hasattr(middleware, "execute"):
            chain = functools.partial(_layer, middleware, ctx, chain)
    return await asyncio.wait_for(chain(), task.spec.timeout_ms / 1000)


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


async def _shutdown(app, writer) -> None:
    """Let the child put its own things down before the process ends."""
    try:
        await _run_hooks(app.stop_hooks)
    except Exception:
        traceback.print_exc()


async def _one(app, wire: Wire, app_spec: str, task_id: int, name: str, payload) -> None:
    """Run a single dispatched task and report it. Never raises."""
    task = app.registry.get(name)
    if task is None:
        await wire.send(
            "Nack", task_id, "UnknownTask", f"no task named {name!r} in {app_spec}", "", 0
        )
        return

    try:
        call_args, call_kwargs = _proto.unpack_args(payload)
        loop = asyncio.get_running_loop()

        def emit(value, _id=task_id):
            wire.send_threadsafe(loop, "Progress", _id, _proto.pack_result(value))

        ctx = Context(
            name=name,
            task_id=str(task_id),
            attempt=1,
            args=tuple(call_args),
            kwargs=call_kwargs,
            _emit=emit,
        )
        result = await _call(app, task, ctx, call_args, call_kwargs)
    except Reject as exc:
        # The handler knows this will never work. Skip the remaining attempts
        # rather than spending them to reach the same answer.
        await wire.send("Nack", task_id, "Reject", f"{type(exc).__name__}: {exc}", "reject", 0)
    except Retry as exc:
        await wire.send(
            "Nack", task_id, "Retry", f"{type(exc).__name__}: {exc}",
            "retry", int(exc.delay * 1000),
        )
    except TimeoutError:
        await wire.send(
            "Nack", task_id, "TimeoutError",
            f"{name} exceeded timeout of {task.spec.timeout_ms / 1000}s", "", 0,
        )
        if not task.is_async:
            # A Python thread cannot be interrupted (spec §4.5). The work we
            # just timed out on is still running and still holds whatever we
            # killed it for, so the process has to go — and with more than one
            # slot it takes its siblings with it. Their leases expire and the
            # supervisor redelivers them; that is the cost of sharing a process.
            raise _SyncTimeout
    except Exception as exc:
        # Pre-formatted string — never reconstructed Rust-side (spec §4.2).
        await wire.send("Nack", task_id, type(exc).__name__, traceback.format_exc(), "", 0)
    else:
        await wire.send("Ack", task_id, _proto.pack_result(result))


class _SyncTimeout(Exception):
    """A sync handler outlived its timeout, so this process cannot continue."""


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
    reader, raw_writer = await asyncio.open_unix_connection(socket_path)
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
        # quietly running 20 at a time. Threads are cheap next to the
        # interpreter they live in; match the number the caller asked for.
        asyncio.get_running_loop().set_default_executor(
            concurrent.futures.ThreadPoolExecutor(
                max_workers=slots, thread_name_prefix="tarsk-task"
            )
        )

    running: set[asyncio.Task] = set()
    draining = False
    for _ in range(slots):
        await wire.send("Ready")

    async def run_and_report(task_id, name, payload):
        try:
            await _one(app, wire, app_spec, task_id, name, payload)
        except _SyncTimeout:
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
        if tag != "Dispatch":
            raise RuntimeError(f"unexpected frame from supervisor: {tag!r}")
        job = asyncio.create_task(run_and_report(*args))
        running.add(job)
        job.add_done_callback(running.discard)

    # Whatever is still in flight owns a lease; finishing it is cheaper than
    # letting the supervisor time it out and redeliver.
    if running:
        await asyncio.gather(*running, return_exceptions=True)
    await _shutdown(app, writer)


if __name__ == "__main__":
    if len(sys.argv) not in (4, 5):
        sys.exit("usage: python -m tarsk._child <socket-path> <module:app> <child-id> [slots]")
    slots = int(sys.argv[4]) if len(sys.argv) == 5 else 1
    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3]), slots))
