"""Does the supervisor's RSS reading track a child that allocates?

Everything the memory ceiling does rests on this one number. It is right on
Linux and macOS and was reporting 4MB for a child holding hundreds on Windows,
which is indistinguishable from a working reading and a broken trigger unless
you look at the reading itself.
"""

import subprocess
import sys
import time

from tarsk._core import rss_of

CHILD = """
import sys, time
blocks = []
for i in range(6):
    blocks.append(bytearray(50 * 1024 * 1024))
    for j in range(0, len(blocks[-1]), 4096):
        blocks[-1][j] = 1          # touch every page: reserved is not resident
    print("allocated", (i + 1) * 50, flush=True)
    time.sleep(0.6)
"""


def second_opinion(pid: int) -> str:
    """What the operating system says, so a wrong reading can be told from a
    child that never allocated."""
    if sys.platform == "win32":
        cmd = ["powershell", "-NoProfile", "-Command",
               f"(Get-Process -Id {pid}).WorkingSet64"]
    elif sys.platform == "darwin":
        cmd = ["ps", "-o", "rss=", "-p", str(pid)]     # kilobytes
    else:
        cmd = ["ps", "-o", "rss=", "-p", str(pid)]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=20).stdout.strip()
    except Exception as exc:  # noqa: BLE001 - a diagnostic must not raise
        return f"unavailable ({exc})"
    if not out:
        return "unavailable (no output)"
    value = int(out.split()[0])
    if sys.platform != "win32":
        value *= 1024
    return f"{value / 1e6:.1f} MB"


def main() -> int:
    child = subprocess.Popen(
        [sys.executable, "-c", CHILD], stdout=subprocess.PIPE, text=True
    )
    readings = []
    try:
        for _ in range(8):
            time.sleep(0.5)
            mb = rss_of(child.pid) / 1e6
            readings.append(mb)
            print(f"  child rss {mb:7.1f} MB   (os says {second_opinion(child.pid)})")
    finally:
        child.kill()
        said = child.stdout.read() if child.stdout else ""
        child.wait()

    print(f"  child reported: {said.strip().splitlines()[-1] if said.strip() else '(nothing)'}")
    peak = max(readings)
    print(f"  peak {peak:.1f} MB after allocating up to 300 MB")
    if peak < 100:
        print("FAIL: the reading does not track the child's memory on this platform")
        return 1
    print("ok rss_of tracks a growing child")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
