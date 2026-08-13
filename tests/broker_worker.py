"""Worker entrypoint used by the broker integration tests."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from tarsk._supervisor import Supervisor  # noqa: E402

Supervisor("tests.broker_app:app", children=int(os.environ.get("TARSK_CHILDREN", "1"))).work(
    os.environ["TARSK_BROKER"],
    ["default"],
    lease_grace=float(os.environ.get("TARSK_LEASE_GRACE", "30")),
    metrics_addr=os.environ.get("TARSK_METRICS"),
)
