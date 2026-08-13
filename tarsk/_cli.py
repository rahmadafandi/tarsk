"""`tarsk worker` — start a supervisor against a broker.

The broker URL comes from `--broker` or `TARSK_BROKER`, never from the App
object. Reading it off the app would mean importing the user's task module in
the supervisor, and the supervisor's constant footprint is exactly the promise
that would break (spec §4.1). The children import it; the parent does not.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

_SIZE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([KMGT]?)i?B?\s*$", re.IGNORECASE)
_SCALE = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}


def parse_size(text: str) -> int:
    """`400MB`, `1.5G`, `512K`, or a plain byte count."""
    match = _SIZE.match(text)
    if not match:
        raise argparse.ArgumentTypeError(f"not a size: {text!r} (try 400MB)")
    return int(float(match.group(1)) * _SCALE[match.group(2).upper()])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tarsk")
    sub = parser.add_subparsers(dest="command", required=True)

    worker = sub.add_parser("worker", help="run a supervisor and its children")
    worker.add_argument("--app", required=True, metavar="module:app",
                        help="where children find your tasks")
    worker.add_argument("--broker", default=os.environ.get("TARSK_BROKER"),
                        help="redis://…, postgres://… (or TARSK_BROKER)")
    worker.add_argument("--queues", default="default", help="comma separated")
    worker.add_argument("--children", type=int, default=2)
    worker.add_argument("--slots", type=int, default=1,
                        help="tasks in flight per child. 1 keeps the memory ceiling "
                             "precise; raise it only for handlers that wait")
    worker.add_argument("--max-rss", type=parse_size, default=0,
                        help="recycle a child above this, e.g. 400MB")
    worker.add_argument("--hard-max-rss", type=parse_size, default=0,
                        help="kill a child that reaches this, mid-task, instead of letting it "
                             "grow. Off by default: it trades one task to protect the box")
    worker.add_argument("--max-tasks", type=int, default=0)
    worker.add_argument("--max-lifetime", type=float, default=0.0, metavar="SECONDS")
    worker.add_argument("--metrics", metavar="HOST:PORT", default=os.environ.get("TARSK_METRICS"),
                        help="serve Prometheus metrics here, e.g. 0.0.0.0:9090")
    worker.add_argument("--lease-grace", type=float, default=30.0, metavar="SECONDS",
                        help="slack on top of a task's own timeout before its lease is dead")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.broker:
        sys.exit("no broker: pass --broker or set TARSK_BROKER")

    from ._supervisor import Supervisor

    supervisor = Supervisor(
        args.app,
        children=args.children,
        slots=args.slots,
        max_rss=args.max_rss,
        max_tasks=args.max_tasks,
        max_lifetime=args.max_lifetime,
        hard_max_rss=args.hard_max_rss,
    )
    queues = [q.strip() for q in args.queues.split(",") if q.strip()]
    stats = supervisor.work(
        args.broker, queues, lease_grace=args.lease_grace, metrics_addr=args.metrics
    )
    for key in sorted(stats):
        print(f"{key}={stats[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
