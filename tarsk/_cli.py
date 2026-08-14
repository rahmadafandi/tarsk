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

    status = sub.add_parser("status", help="how much work is waiting")
    status.add_argument("--broker", default=os.environ.get("TARSK_BROKER"),
                        help="redis://…, postgres://… (or TARSK_BROKER)")
    status.add_argument("--queues", default="default", help="comma separated")

    jobs = sub.add_parser("jobs", help="which jobs are waiting or running")
    jobs.add_argument("--broker", default=os.environ.get("TARSK_BROKER"),
                      help="redis://…, postgres://… (or TARSK_BROKER)")
    jobs.add_argument("--queues", default="default", help="comma separated")
    jobs.add_argument("--state", choices=["ready", "running", "delayed"],
                      help="only this one")
    jobs.add_argument("--limit", type=int, default=50)
    jobs.add_argument("--meta", action="store_true",
                      help="show what the sender attached, on its own line")

    cancel = sub.add_parser("cancel", help="stop queued jobs from running")
    cancel.add_argument("ids", nargs="+", metavar="ID")
    cancel.add_argument("--broker", default=os.environ.get("TARSK_BROKER"),
                        help="redis://…, postgres://… (or TARSK_BROKER)")
    cancel.add_argument("--queue", default="default")
    cancel.add_argument("--ttl", type=float, default=86400.0,
                        help="how long to remember the cancellation. Must outlive the job: "
                             "something scheduled for next week needs a week")

    dead = sub.add_parser("dead", help="inspect and replay the dead letters")
    dead.add_argument("action", choices=["list", "show", "replay", "purge"])
    dead.add_argument("ids", nargs="*", metavar="ID",
                      help="which entries. Empty means all, except for `show`")
    dead.add_argument("--broker", default=os.environ.get("TARSK_BROKER"),
                      help="redis://…, postgres://… (or TARSK_BROKER)")
    dead.add_argument("--queue", default="default")
    dead.add_argument("--limit", type=int, default=50, help="how many to list")

    worker = sub.add_parser("worker", help="run a supervisor and its children")
    worker.add_argument("--app", required=True, metavar="module:app",
                        help="where children find your tasks")
    worker.add_argument("--broker", default=os.environ.get("TARSK_BROKER"),
                        help="redis://…, postgres://… (or TARSK_BROKER)")
    worker.add_argument("--queues", default="default", help="comma separated")
    worker.add_argument("--children", type=int, default=2)
    worker.add_argument("--slots", type=int, default=DEFAULT_SLOTS,
                        help=f"tasks in flight per child (default {DEFAULT_SLOTS}). "
                             "Use 1 for the tightest memory ceiling, more for handlers "
                             "that wait")
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


# One number, whatever else is set. An earlier version derived this from
# whether --max-rss was given, which made one flag silently change another's
# default and needed a line of runtime output to explain itself. A default that
# has to be explained at runtime is the wrong default.
DEFAULT_SLOTS = 100


def run_dead(args) -> int:
    """Read, replay or drop what the retries gave up on.

    No app module: a dead letter is a name, a payload and a traceback, none of
    which needs the user's code to be importable. That matters when the reason
    the tasks died is that the code does not import.
    """
    from ._core import Producer

    producer = Producer(broker_url=args.broker)

    if args.action == "purge":
        gone = producer.dead_purge(queue=args.queue, ids=args.ids)
        print(f"purged {gone}")
        return 0

    if args.action == "replay":
        moved = producer.dead_replay(queue=args.queue, ids=args.ids)
        print(f"replayed {moved}")
        return 0

    entries = producer.dead_list(queue=args.queue, limit=args.limit)
    if not entries:
        print(f"no dead letters in {args.queue!r}")
        return 0

    if args.action == "show":
        if not args.ids:
            sys.exit("show needs at least one id — `tarsk dead list` prints them")
        wanted = [e for e in entries if e[0] in args.ids]
        missing = set(args.ids) - {e[0] for e in wanted}
        for entry_id, name, error, traceback, died in wanted:
            print(f"{entry_id}  {name}  {_when(died)}")
            print(f"  {error}")
            for line in traceback.rstrip().splitlines():
                print(f"  {line}")
            print()
        if missing:
            # Not an error: it may simply be older than --limit.
            print(f"not in the last {args.limit}: {', '.join(sorted(missing))}", file=sys.stderr)
        return 0

    width = max(len(e[1]) for e in entries)
    for entry_id, name, error, _tb, died in entries:
        print(f"{entry_id}  {_when(died)}  {name:<{width}}  {error.splitlines()[0][:60]}")
    print(f"\n{len(entries)} shown. `tarsk dead show <id>` for a traceback, "
          f"`replay` to requeue, `purge` to drop.", file=sys.stderr)
    return 0


def _span(millis: int) -> str:
    """`4s`, `12m`, `3h` — a duration, not a clock time.

    A listing is read to find what is stuck, and "how long" answers that where
    a timestamp makes the reader do the subtraction.
    """
    seconds = max(0, millis) / 1000
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 5400:
        return f"{seconds / 60:.0f}m"
    return f"{seconds / 3600:.1f}h"


def _when(millis: int) -> str:
    if not millis:
        return "unknown" + " " * 9
    import datetime

    stamp = datetime.datetime.fromtimestamp(millis / 1000, datetime.timezone.utc)
    return stamp.strftime("%Y-%m-%d %H:%M:%S")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.broker:
        sys.exit("no broker: pass --broker or set TARSK_BROKER")
    if args.command == "status":
        from ._core import Producer

        wanted = [q.strip() for q in args.queues.split(",") if q.strip()]
        rows = Producer(broker_url=args.broker).depth(wanted)
        width = max((len(r[0]) for r in rows), default=5)
        print(f"{'queue':<{width}}  {'ready':>8} {'running':>8} {'delayed':>8} {'dead':>8}")
        for queue, ready, running, delayed, dead in rows:
            print(f"{queue:<{width}}  {ready:>8} {running:>8} {delayed:>8} {dead:>8}")
        if not rows:
            print("(no such queue, or nothing has ever been sent to it)")
        return 0
    if args.command == "jobs":
        from ._core import Producer

        wanted = [q.strip() for q in args.queues.split(",") if q.strip()]
        rows = Producer(broker_url=args.broker).jobs(wanted, args.limit)
        if args.state:
            rows = [r for r in rows if r[3] == args.state]
        if not rows:
            print("nothing waiting or running")
            return 0
        width = max(len(r[2]) for r in rows)
        for job_id, queue, name, state, attempt, age_ms, worker, meta in rows:
            when = (f"due in {_span(-age_ms)}" if state == "delayed"
                    else f"{_span(age_ms)} ago")
            tries = f"  attempt {attempt}" if attempt > 1 else ""
            # Only running jobs have a holder, and only then is it worth a column.
            held = f"  on {worker}" if worker else ""
            print(f"{job_id}  {state:<7}  {name:<{width}}  {when}{tries}{held}")
            if args.meta and meta:
                from . import _proto

                print(f"    {_proto.unpack_result(meta)}")
        print(f"\n{len(rows)} shown of at most {args.limit}. "
              f"`tarsk status` for the totals.", file=sys.stderr)
        return 0
    if args.command == "cancel":
        from ._core import Producer

        producer = Producer(broker_url=args.broker)
        for job_id in args.ids:
            producer.cancel(job_id, queue=args.queue, ttl=args.ttl)
        print(f"cancelled {len(args.ids)}; a job already running is not interrupted")
        return 0
    if args.command == "dead":
        return run_dead(args)

    from ._supervisor import Supervisor

    slots = args.slots
    print(f"tarsk: {args.children} children x {slots} "
          f"{'slot' if slots == 1 else 'slots'}", file=sys.stderr)
    if args.max_rss and slots > 1:
        # Not a warning about a choice this made — a fact about the one the
        # caller made. The ceiling still fires; what it cannot promise is that
        # overshoot stops at a single task.
        print(f"tarsk: --max-rss with {slots} slots — a child is retired when it crosses, "
              f"but up to {slots} tasks are running when it does, so overshoot is their "
              "peak, not one task's. --slots 1 for the tightest bound.", file=sys.stderr)

    supervisor = Supervisor(
        args.app,
        children=args.children,
        slots=slots,
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
