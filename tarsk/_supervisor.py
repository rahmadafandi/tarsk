"""Python face of the Rust supervisor (tarsk._core).

Thin on purpose: this module reshapes jobs and results, and owns nothing. All
scheduling, child supervision, RSS monitoring, and recycling live in Rust and
run with the GIL released.

Not imported by children (spec §4.1) — importing this pulls in the extension
module, which is exactly what child RSS must not carry.
"""

from __future__ import annotations

import sys
from typing import Any

from . import _core, _proto

__all__ = ["Supervisor"]


class Supervisor:
    def __init__(
        self,
        app_spec: str,
        children: int = 2,
        slots: int = 1,
        max_rss: int = 0,          # bytes, 0 = unbounded
        max_tasks: int = 0,        # 0 = unlimited
        max_lifetime: float = 0.0, # seconds, 0 = unlimited
        hard_max_rss: int = 0,      # bytes, 0 = never kill a running task
        max_dead: int = 10_000,     # per queue, 0 = keep every failure forever
        python: str | None = None,
    ):
        if hard_max_rss and max_rss and hard_max_rss <= max_rss:
            raise ValueError(
                f"hard_max_rss={hard_max_rss} must exceed max_rss={max_rss}; below it the "
                "graceful ceiling can never fire and every recycle kills a running task"
            )
        self.app_spec = app_spec
        self.children = children
        self.slots = slots
        self.max_rss = max_rss
        self.max_tasks = max_tasks
        self.max_lifetime = max_lifetime
        self.hard_max_rss = hard_max_rss
        self.max_dead = max_dead
        self.python = python or sys.executable
        self.stats: dict[str, int] = {}
        self.exits: list[int] = []

    def run(self, jobs: list[tuple[str, tuple, dict]]) -> dict[int, tuple[str, Any]]:
        """Run `jobs` to completion over the in-memory broker.

        Returns {task_id: ("ack", value) | ("nack", (error_type, traceback))}.
        This is the batch path — tests and benchmarks. Production uses `work`,
        which consumes a real broker and does not return on its own.
        """
        packed = [(name, _proto.pack_args(args, kwargs)) for name, args, kwargs in jobs]
        outcomes, self.stats, self.exits = _core.run(
            self.app_spec,
            packed,
            self.python,
            self.children,
            self.slots,
            self.max_rss,
            self.max_tasks,
            self.max_lifetime,
            self.hard_max_rss,
        )
        return {
            task_id: ("ack", _proto.unpack_result(result)) if ok else ("nack", (error_type, tb))
            for task_id, ok, result, error_type, tb in outcomes
        }

    def work(
        self,
        broker_url: str,
        queues: list[str],
        lease_grace: float = 30.0,
        metrics_addr: str | None = None,
    ) -> dict[str, int]:
        """Consume `queues` until SIGINT or SIGTERM, then drain and return stats.

        A job's lease is its own timeout plus `lease_grace`, so how long a
        stranded job waits for redelivery follows the task, not the slowest
        task in the system.

        No results come back: without a result backend there is nowhere to put
        them, and a worker that accumulated every outcome would leak by design.
        """
        self.stats = _core.work(
            self.app_spec,
            broker_url,
            queues,
            self.python,
            self.children,
            self.slots,
            self.max_rss,
            self.max_tasks,
            self.max_lifetime,
            lease_grace,
            metrics_addr,
            self.hard_max_rss,
            self.max_dead,
        )
        return self.stats
