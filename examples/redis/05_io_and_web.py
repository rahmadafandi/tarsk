"""Handlers that wait, and enqueueing from inside an event loop.

    Terminal 1:  tarsk worker --app examples.redis.05_io_and_web:app --broker redis://localhost:6379/0
    Terminal 2:  python examples/redis/05_io_and_web.py

Most task queue work waits on something else rather than computing, which is
why `--slots` defaults to 100: one child holds a hundred awaiting tasks in one
interpreter instead of a hundred processes. Raise it for work that waits, set
it to 1 for work that allocates.
"""

import asyncio
import os

from tarsk import App, Context, Depends

BROKER = os.environ.get("TARSK_BROKER", "redis://localhost:6379/0")
app = App(broker=BROKER)

# Every task is given an explicit `name=`. Without one the name is derived from
# where the function was defined, so this file produces `__main__.greet` when
# you run it and `examples.redis.01_basics.greet` when the worker imports it — the
# same function under two names, and the worker rejects what the producer sent.
# An explicit name is the fix and is worth doing in real code for the same
# reason: it survives the module being moved or renamed.


# Opened once per worker process, not per task, and before the child takes any
# work — so whatever it holds is inside the baseline the ceiling measures.
# Never at import: the supervisor imports nothing of yours, but a producer does.
@app.on_start
async def open_pool() -> None:
    print("    [worker] pretending to open a connection pool")


@app.on_stop
async def close_pool() -> None:
    print("    [worker] closing it again")


@app.task(name="io.fetch", result_ttl=120)
async def fetch(url: str) -> str:
    """Awaits, so a hundred of these share one interpreter."""
    await asyncio.sleep(0.5)
    return f"200 {url}"


@app.task(name="io.with_progress", result_ttl=120)
async def with_progress(pages: int, ctx=Depends(Context)) -> str:
    """Progress is readable from AsyncResult while the task is still running.

    Kept under the same expiry as the result: a progress record that outlives
    interest in the answer is a leak that reports on itself.
    """
    for page in range(pages):
        await asyncio.sleep(0.2)
        ctx.set_progress({"page": page + 1, "of": pages})
    return f"read {pages} pages"


async def main() -> None:
    # send() is blocking and would stall an event loop, so async callers get
    # their own. This is what a FastAPI or aiohttp handler should call.
    ids = await asyncio.gather(*(fetch.send_async(f"https://example.com/{n}") for n in range(5)))
    print(f"  queued {len(ids)} fetches without blocking the loop")

    answers = await asyncio.gather(*(app.result(i).get_async(timeout=30) for i in ids))
    print("  answers:", answers)

    job = await with_progress.send_async(5)
    handle = app.result(job)
    # progress() and ready() are sync reads — quick ones, unlike get(), which
    # is the call that would sit in a loop waiting.
    while not handle.ready():
        state = handle.progress()
        if state:
            print("  progress:", state)
        await asyncio.sleep(0.3)
    print("  done:", await handle.get_async(timeout=30))


if __name__ == "__main__":
    asyncio.run(main())
