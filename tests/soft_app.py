"""Tasks for the soft-timeout checks, in an app with no middleware.

Deliberately separate from `demo_app`. That one carries a sync middleware, and
a sync middleware runs in a thread holding a blocking call into the loop — so a
cancellation aimed at the handler lands on the middleware instead and the
handler is never asked. Testing a soft deadline through it would measure that
interaction rather than the deadline.
"""

import asyncio

from tarsk import App, Context, Depends

app = App(default_timeout=5, max_timeout=5)


@app.task(name="tidies_up", timeout=4, soft_timeout=0.3)
async def tidies_up(ctx=Depends(Context)):
    """Asked to stop, saves what it has, finishes inside the hard timeout."""
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        # ctx.soft_expired separates a passed deadline from a worker going
        # down, and only one of those wants partial work kept.
        return ["asked" if ctx.soft_expired else "cancelled", "partial work kept"]
    return "unreachable"


@app.task(name="stops_when_asked", timeout=4, soft_timeout=0.3)
async def stops_when_asked():
    """Lets the cancellation through, which is reported as a soft timeout."""
    await asyncio.sleep(10)


@app.task(name="ignores_the_ask", timeout=1.2, soft_timeout=0.3)
async def ignores_the_ask():
    """Swallows the ask and keeps going, so the hard deadline takes it."""
    try:
        await asyncio.sleep(10)
    except asyncio.CancelledError:
        pass                      # ignores the ask
    await asyncio.sleep(10)       # and is taken by the hard deadline anyway
