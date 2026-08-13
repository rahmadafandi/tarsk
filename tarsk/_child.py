"""Child worker: `python -m tarsk._child <socket-path> <module:app> <child-id>`

Imports the user's task modules and nothing from tarsk beyond the protocol
codec (spec §4.1). Child RSS is therefore CPython + user code.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import os
import sys
import traceback

from . import Context, Task, load_app
from . import _proto

# Distinct exit code so the supervisor can tell "recycled after a sync timeout"
# from a crash. Both are unclean; only one is the runtime's own doing.
EXIT_SYNC_TIMEOUT = 75
# A start hook raised. Distinct so the supervisor can say so rather than
# reporting a child that simply never showed up.
EXIT_STARTUP_FAILED = 78


async def _call(app, task: Task, ctx: Context, args: list, kwargs: dict):
    """Run one task, through its middleware, under its timeout (spec §4.5).

    The timeout covers the middleware too. A tracing layer that hangs holds the
    lease exactly as a handler would, so bounding only the innermost call would
    bound the wrong thing.
    """

    async def invoke():
        filled = dict(kwargs)
        for name, dep in task.depends.items():
            if name not in filled:
                filled[name] = await app.resolve(dep)
        if task.is_async:
            return await task.fn(*args, **filled)
        return await asyncio.to_thread(task.fn, *args, **filled)

    chain = invoke
    for middleware in reversed(app.middlewares):
        if hasattr(middleware, "execute"):
            chain = functools.partial(_layer, middleware, ctx, chain)
    return await asyncio.wait_for(chain(), task.spec.timeout_ms / 1000)


async def _layer(middleware, ctx: Context, nxt):
    result = middleware.execute(ctx, nxt)
    return await result if inspect.isawaitable(result) else result


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


async def main(socket_path: str, app_spec: str, child_id: int) -> None:
    app = load_app(app_spec)
    # Before Register, so whatever a hook opens is inside the baseline the
    # supervisor measures against the ceiling. A pool that does not fit should
    # be refused at startup, not discovered as a child that recycles instantly.
    try:
        await _run_hooks(app.start_hooks)
    except Exception:
        traceback.print_exc()
        sys.exit(EXIT_STARTUP_FAILED)
    reader, writer = await asyncio.open_unix_connection(socket_path)
    # child_id is assigned by the supervisor before spawn, so it can match this
    # connection to the process it started — and to that process's RSS.
    await _proto.write(writer, "Register", child_id, app.registry_hash(), app.registry_rows())

    # ponytail: one slot per child, so Drain can only arrive while idle and
    # there is no in-flight bookkeeping. Multi-slot means N Readys plus a
    # separate reader task — add it when a child's task mix is I/O-bound
    # enough that one in-flight task wastes the interpreter it paid for.
    while True:
        await _proto.write(writer, "Ready")
        frame = await _proto.read(reader)
        if frame is None:
            await _shutdown(app, writer)
            return  # supervisor went away
        tag, args = frame
        if tag == "Drain":
            await _shutdown(app, writer)
            return
        if tag != "Dispatch":
            raise RuntimeError(f"unexpected frame from supervisor: {tag!r}")

        task_id, name, payload = args
        task = app.registry.get(name)
        if task is None:
            await _proto.write(
                writer, "Nack", task_id, "UnknownTask", f"no task named {name!r} in {app_spec}"
            )
            continue

        try:
            call_args, call_kwargs = _proto.unpack_args(payload)
            ctx = Context(
                name=name,
                task_id=str(task_id),
                attempt=1,
                args=tuple(call_args),
                kwargs=call_kwargs,
            )
            result = await _call(app, task, ctx, call_args, call_kwargs)
        except TimeoutError:
            await _proto.write(
                writer, "Nack", task_id, "TimeoutError",
                f"{name} exceeded timeout of {task.spec.timeout_ms / 1000}s",
            )
            if not task.is_async:
                # A Python thread cannot be interrupted (spec §4.5). The work
                # we just timed out on is still running and still holds
                # whatever we killed it for, so the process has to go.
                await _die(writer, EXIT_SYNC_TIMEOUT)
        except Exception as exc:
            # Pre-formatted string — never reconstructed Rust-side (spec §4.2).
            await _proto.write(writer, "Nack", task_id, type(exc).__name__, traceback.format_exc())
        else:
            await _proto.write(writer, "Ack", task_id, _proto.pack_result(result))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit("usage: python -m tarsk._child <socket-path> <module:app> <child-id>")
    asyncio.run(main(sys.argv[1], sys.argv[2], int(sys.argv[3])))
