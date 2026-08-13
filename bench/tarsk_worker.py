"""Run the tarsk supervisor as its own process, so it can be sampled like a
Celery master. Config arrives as a JSON file to keep argv small.
"""

import json
import sys
from pathlib import Path

from tarsk._supervisor import Supervisor

spec = json.loads(Path(sys.argv[1]).read_text())
sup = Supervisor(
    "bench.tarsk_app:app",
    children=spec["children"],
    max_rss=spec.get("max_rss", 0),
    max_tasks=spec.get("max_tasks", 0),
)
results = sup.run([(spec["task"], (i, *spec["args"]), {}) for i in range(spec["count"])])
Path(spec["out"]).write_text(
    json.dumps(
        {
            "stats": sup.stats,
            "exits": sup.exits,
            "acked": sum(1 for kind, _ in results.values() if kind == "ack"),
            "total": len(results),
        }
    )
)
