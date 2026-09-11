"""Does the supervisor's RSS reading track a child that allocates?

Everything the memory ceiling does rests on this one number. It is right on
Linux and macOS and reads zero for a live child on Windows, which is
indistinguishable from a working reading and a broken trigger unless you look
at the reading itself.
"""

import os
import signal
import subprocess
import sys
import time

from tarsk._core import rss_of

CHILD = """
import os, sys, time
print("child pid", os.getpid(), flush=True)
blocks = []
for i in range(6):
    blocks.append(bytearray(50 * 1024 * 1024))
    for j in range(0, len(blocks[-1]), 4096):
        blocks[-1][j] = 1          # touch every page: reserved is not resident
    print("allocated", (i + 1) * 50, flush=True)
    time.sleep(0.6)
# Hold all of it until killed. The reading is sampled eight times and each
# sample costs a process spawn for the cross-check, which on Windows outlasts
# the allocating loop: without this the last six samples measured a child that
# had already exited and read zero for the honest reason.
time.sleep(60)
"""


def second_opinion(pid: int) -> str:
    """What the operating system says, so a wrong reading can be told from a
    child that never allocated."""
    if sys.platform == "win32":
        # pwsh before powershell: Windows PowerShell 5.1 stopped answering on
        # the September 2026 runner image, and this cross-check went dark with
        # it — printing "no output" for processes that were plainly alive and
        # allocating, which is worse than printing nothing at all. pwsh is the
        # shell the job itself already runs under.
        cmds = [
            [exe, "-NoProfile", "-Command", f"(Get-Process -Id {pid}).WorkingSet64"]
            for exe in ("pwsh", "powershell")
        ]
    else:
        cmds = [["ps", "-o", "rss=", "-p", str(pid)]]  # kilobytes
    detail = "no output"
    for cmd in cmds:
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout.strip()
        except Exception as exc:  # noqa: BLE001 - a diagnostic must not raise
            detail = str(exc)
            continue
        if out:
            value = int(out.split()[0])
            if sys.platform != "win32":
                value *= 1024
            return f"{value / 1e6:.1f} MB"
    return f"unavailable ({detail})"


def main() -> int:
    child = subprocess.Popen(
        [sys.executable, "-c", CHILD], stdout=subprocess.PIPE, text=True
    )
    readings = []
    watched = child.pid
    interpreter = child.pid
    try:
        # A venv's python.exe on Windows is a launcher that starts the real
        # interpreter as a child and waits for it, so the pid Popen returned
        # names a four-megabyte stub and every allocation is in a process it
        # never named. The supervisor is in exactly that position — the spawned
        # pid is the only one it ever knows — so that is the pid this has to
        # pass or fail on, and summing the family is the reading's job.
        said = (child.stdout.readline() if child.stdout else "").split()
        interpreter = int(said[-1]) if said else watched
        if interpreter != watched:
            print(f"  spawned pid {watched} is a launcher stub; the interpreter is "
                  f"{interpreter} (os says {second_opinion(watched)})")
        print(f"  watching pid {watched}, which is what the supervisor holds; "
              f"the child reports {interpreter}")

        for _ in range(8):
            time.sleep(0.5)
            mb = rss_of(watched) / 1e6
            readings.append(mb)
            # Both readings every time: if the family sum is zero while the
            # interpreter read alone is not, the walk is what is broken, and if
            # both are zero it is the reading. One run then says which.
            alone = rss_of(interpreter) / 1e6
            print(f"  child rss {mb:7.1f} MB   (interpreter alone {alone:7.1f} MB, "
                  f"os says {second_opinion(interpreter)})")
    finally:
        child.kill()
        if interpreter != child.pid:
            # Killing the launcher leaves the interpreter holding the pipe, and
            # the read below would wait on it forever.
            try:
                os.kill(interpreter, signal.SIGTERM)
            except OSError:
                pass
        said_all = child.stdout.read() if child.stdout else ""
        child.wait()

    last = said_all.strip().splitlines()[-1] if said_all.strip() else "(nothing)"
    print(f"  child reported: {last}")
    peak = max(readings)
    print(f"  peak {peak:.1f} MB after allocating up to 300 MB")
    if peak < 100:
        print("FAIL: the reading does not track the child's memory on this platform")
        return 1
    print("ok rss_of tracks a growing child")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
