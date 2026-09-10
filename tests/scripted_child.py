"""A child that speaks the wire protocol directly, so tests can misbehave.

`python -m tarsk._child` is the real client and it is well behaved by
construction: it acks each task once, for a task it was actually dispatched.
The supervision state machine — `serve`, `slot`, `drain`, `reap` — has to hold
up against a child that does not, and the only way to write that test is to be
the child.

The supervisor spawns `<python> -m tarsk._child <socket> <app> <id> <slots>`
with a command line it owns, so this plugs in as the *interpreter*: `launcher`
writes a shim that drops the `-m` pair and runs this file instead.

    from tests import scripted_child
    sup = Supervisor("tests.demo_app:app", children=1,
                     python=scripted_child.launcher(tmpdir, acks=2))

`acks` is the one knob so far, because one defect needed it. Another misbehaviour
belongs here as another argument, not as another file.
"""

from __future__ import annotations

import asyncio
import stat
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tarsk import _proto, load_app  # noqa: E402


def launcher(directory, acks: int = 1) -> str:
    """Path to an executable the supervisor can spawn in place of `python`.

    `$1 $2` are the `-m tarsk._child` the supervisor always passes; the rest is
    socket, app, child id, slots.
    """
    shim = Path(directory) / "scripted-python"
    shim.write_text(
        f'#!/bin/sh\nexec "{sys.executable}" "{__file__}" "$3" "$4" "$5" "$6" {acks}\n'
    )
    shim.chmod(shim.stat().st_mode | stat.S_IXUSR)
    return str(shim)


async def main(socket_path: str, app_spec: str, child_id: str, slots: str, acks: int) -> None:
    app = load_app(app_spec)
    reader, writer = await asyncio.open_unix_connection(socket_path)
    await _proto.write(
        writer, "Register", int(child_id), app.registry_hash(), app.registry_rows()
    )
    for _ in range(int(slots)):
        await _proto.write(writer, "Ready")

    while True:
        frame = await _proto.read(reader)
        if frame is None:
            break  # supervisor went away
        tag, args = frame
        if tag == "Drain":
            break
        if tag != "Dispatch":
            raise RuntimeError(f"unexpected frame from supervisor: {tag!r}")
        # Nothing is run: what this child is for is the frames it sends back,
        # not the work. The result is a placeholder the supervisor stores.
        for _ in range(acks):
            await _proto.write(writer, "Ack", args[0], _proto.pack_result(None))
        await _proto.write(writer, "Ready")
    writer.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5])))
