"""The demo spec §6 calls the definition of done.

A leaky handler runs for as long as you ask, against a real broker, under a
configured RSS ceiling. It ends by answering three questions: did the worker
stay under the ceiling, did the sawtooth look like a sawtooth, and did anything
get lost. The trace comes from the supervisor's own /metrics endpoint, so the
picture is what tarsk claims about itself rather than an outside measurement
that agrees with it.

    python demo/run.py --minutes 60 --ceiling 400MB --rate 20
"""

from __future__ import annotations

import argparse
import os
import random
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from tarsk._cli import parse_size  # noqa: E402

MB = 1024 * 1024


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def scrape(port: int) -> dict[str, float]:
    """Pull /metrics and flatten it to {line_without_value: value}."""
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
            sock.sendall(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            chunks = []
            while chunk := sock.recv(65536):
                chunks.append(chunk)
    except OSError:
        return {}
    body = b"".join(chunks).decode().split("\r\n\r\n", 1)[-1]
    out = {}
    for line in body.splitlines():
        if line.startswith("#") or " " not in line:
            continue
        key, _, value = line.rpartition(" ")
        try:
            out[key] = float(value)
        except ValueError:
            pass
    return out


def svg(samples: list[tuple[float, float]], ceiling: float, path: Path) -> None:
    """Hand-drawn because a plotting dependency for one polyline is not a trade."""
    width, height, pad = 1000, 320, 40
    span = max(samples[-1][0], 1.0)
    top = max(ceiling, max(rss for _, rss in samples)) * 1.1
    x = lambda t: pad + (width - 2 * pad) * t / span
    y = lambda v: height - pad - (height - 2 * pad) * v / top
    points = " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in samples)
    ceil_y = y(ceiling)
    ticks = "".join(
        f'<text x="{x(span * i / 5):.0f}" y="{height - pad + 16}" font-size="11" '
        f'text-anchor="middle" fill="#666">{span * i / 5 / 60:.0f}m</text>'
        for i in range(6)
    )
    path.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}"><rect width="{width}" height="{height}" fill="#fff"/>'
        f'<line x1="{pad}" y1="{ceil_y:.1f}" x2="{width - pad}" y2="{ceil_y:.1f}" '
        f'stroke="#c00" stroke-width="1.5" stroke-dasharray="6 4"/>'
        f'<text x="{width - pad}" y="{ceil_y - 6:.1f}" font-size="12" text-anchor="end" '
        f'fill="#c00">ceiling {ceiling / MB:.0f} MB</text>'
        f'<polyline fill="none" stroke="#0a6" stroke-width="1.5" points="{points}"/>'
        f'<line x1="{pad}" y1="{height - pad}" x2="{width - pad}" y2="{height - pad}" '
        f'stroke="#999"/>{ticks}'
        f'<text x="{pad}" y="{pad - 14}" font-size="13" fill="#333">'
        f'child RSS under a leaky handler</text></svg>'
    )


def sparkline(samples: list[tuple[float, float]], ceiling: float) -> str:
    blocks = "▁▂▃▄▅▆▇█"
    if not samples:
        return ""
    step = max(1, len(samples) // 100)
    picked = samples[::step]
    return "".join(blocks[min(7, int(v / ceiling * 7))] for _, v in picked)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=60.0)
    ap.add_argument("--ceiling", type=parse_size, default=400 * MB)
    ap.add_argument("--rate", type=float, default=20.0, help="tasks per second")
    # Sized so a child fills the ceiling in about a minute: the sawtooth has to
    # be slower than the sample interval to look like anything.
    ap.add_argument("--leak-min-kb", type=int, default=100)
    ap.add_argument("--leak-max-kb", type=int, default=600)
    ap.add_argument("--children", type=int, default=1)
    ap.add_argument("--out", type=Path, default=Path("demo/out"))
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)

    workdir = Path(tempfile.mkdtemp(prefix="tarsk-demo-"))
    log = workdir / "done.log"
    log.touch()
    redis_port, metrics_port = free_port(), free_port()
    redis = subprocess.Popen(
        ["redis-server", "--port", str(redis_port), "--save", "", "--appendonly", "no",
         "--dir", str(workdir)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    url = f"redis://127.0.0.1:{redis_port}/0"
    env = {**os.environ, "TARSK_BROKER": url, "DEMO_LOG": str(log), "PYTHONPATH": str(ROOT)}
    for key, value in env.items():
        os.environ[key] = value
    time.sleep(1.0)

    worker = subprocess.Popen(
        [sys.executable, "-m", "tarsk._cli", "worker", "--app", "demo.leaky_app:app",
         "--broker", url, "--children", str(args.children),
         "--max-rss", str(args.ceiling), "--metrics", f"127.0.0.1:{metrics_port}"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True,
    )

    from tarsk import load_app

    app = load_app("demo.leaky_app:app")
    app.broker = url
    app._producer = None
    ingest = app.registry["ingest"]

    rng = random.Random(20260813)
    samples: list[tuple[float, float]] = []
    started = time.time()
    deadline = started + args.minutes * 60
    sent = 0
    next_send = started
    next_sample = started
    try:
        while time.time() < deadline:
            now = time.time()
            if now >= next_send:
                # Payload-dependent: no task count maps to this byte budget
                ingest.send(sent, rng.randint(args.leak_min_kb, args.leak_max_kb))
                sent += 1
                next_send += 1.0 / args.rate
            if now >= next_sample:
                metrics = scrape(metrics_port)
                if metrics:
                    samples.append((now - started, metrics.get("tarsk_child_rss_bytes_max", 0)))
                next_sample += 1.0
            time.sleep(max(0.0, min(next_send, next_sample) - time.time()))

        print(f"sent {sent}, waiting for the queue to drain")
        drain_deadline = time.time() + 300
        while time.time() < drain_deadline:
            if len({line for line in log.read_text().splitlines() if line}) >= sent:
                break
            metrics = scrape(metrics_port)
            if metrics:
                samples.append((time.time() - started, metrics.get("tarsk_child_rss_bytes_max", 0)))
            time.sleep(1.0)
        final = scrape(metrics_port)
    finally:
        try:
            os.killpg(os.getpgid(worker.pid), signal.SIGTERM)
            worker.wait(timeout=60)
        except Exception:
            pass
        redis.terminate()
        redis.wait()

    done = {line for line in log.read_text().splitlines() if line}
    peak = max((rss for _, rss in samples), default=0)
    (args.out / "trace.csv").write_text(
        "seconds,child_rss_bytes\n" + "".join(f"{t:.1f},{int(v)}\n" for t, v in samples)
    )
    if samples:
        svg(samples, args.ceiling, args.out / "trace.svg")

    recycles = int(final.get("tarsk_children_recycled_total", 0))
    prewarmed = int(final.get("tarsk_children_recycled_prewarmed_total", 0))
    print()
    print(sparkline(samples, args.ceiling))
    print()
    print(f"  ran for            {args.minutes:g} min at {args.rate:g} tasks/s")
    print(f"  leak per task      {args.leak_min_kb}-{args.leak_max_kb} KB")
    print(f"  ceiling            {args.ceiling / MB:.0f} MB")
    print(f"  peak child RSS     {peak / MB:.0f} MB")
    print(f"  recycles           {recycles} ({prewarmed} handed over pre-warmed)")
    print(f"  killed             {int(final.get('tarsk_children_killed_total', 0))}")
    print(f"  crashed            {int(final.get('tarsk_children_crashed_total', 0))}")
    print(f"  dead-lettered      {int(final.get('tarsk_tasks_dead_lettered_total', 0))}")
    print(f"  tasks sent         {sent}")
    print(f"  tasks completed    {len(done)}")
    print(f"  lost               {sent - len(done)}")
    print(f"  trace              {args.out}/trace.csv, {args.out}/trace.svg")
    shutil.rmtree(workdir, ignore_errors=True)
    return 0 if len(done) == sent else 1


if __name__ == "__main__":
    raise SystemExit(main())
