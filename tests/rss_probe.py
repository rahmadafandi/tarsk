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


def main() -> int:
    child = subprocess.Popen([sys.executable, "-c", CHILD], stdout=subprocess.PIPE, text=True)
    readings = []
    try:
        for _ in range(8):
            time.sleep(0.5)
            mb = rss_of(child.pid) / 1e6
            readings.append(mb)
            print(f"  child rss {mb:7.1f} MB")
    finally:
        child.kill()
        child.wait()

    peak = max(readings)
    print(f"  peak {peak:.1f} MB after allocating up to 300 MB")
    if peak < 100:
        print("FAIL: the reading does not track the child's memory on this platform")
        return 1
    print("ok rss_of tracks a growing child")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
