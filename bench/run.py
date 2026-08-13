"""Benchmark harness: tarsk vs Celery vs taskiq.

What this measures and why (spec §2 sets the terms):

  footprint   idle RSS of the worker that runs user code
  imports     which process carries your imports, and what that costs
  memory      worker RSS under a leaky handler, fixed and variable leak sizes
  oom         tasks lost when the box enforces a hard memory limit
  gap         inter-task gaps caused by worker recycling
  throughput  tasks/sec, for a trivial handler and a realistic 50ms one

Only `memory`, `oom` and `gap` test claims tarsk actually makes. `throughput`
is here to be honest about the cost, not to win: tarsk has no broker yet
(step 3), so its throughput number omits a hop the others pay and is an upper
bound rather than a comparison.

Linux only — it reads /proc for RSS and uses systemd-run for the OOM case.

Run: python bench/run.py [scenario ...]
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

PY = sys.executable
# Point BENCH_REDIS at your own instance to use it instead of a throwaway one.
# Nothing here calls FLUSHALL, so a database number is respected: pass
# redis://host:6379/10 and db 0 is left alone.
EXTERNAL_REDIS = os.environ.get("BENCH_REDIS")
REDIS_PORT = 0  # assigned when the harness starts its own
REDIS_URL = EXTERNAL_REDIS or ""
PAGE = os.sysconf("SC_PAGE_SIZE")
MB = 1024 * 1024

# --------------------------------------------------------------- proc utils


def rss_of(pid: int) -> float:
    try:
        with open(f"/proc/{pid}/statm") as fh:
            return int(fh.read().split()[1]) * PAGE
    except (OSError, ValueError, IndexError):
        return 0


def smaps(pid: int, key: str) -> float:
    """A field from /proc/<pid>/smaps_rollup, in bytes.

    Pss is the one that can be summed across a process tree: it divides each
    shared page among the processes mapping it, where Rss counts it in full for
    every one of them. A runtime that forks therefore looks much heavier under
    summed Rss than it is.
    """
    try:
        for line in open(f"/proc/{pid}/smaps_rollup"):
            if line.startswith(key):
                return int(line.split()[1]) * 1024
    except (OSError, ValueError, IndexError):
        pass
    return 0.0


def parent_map() -> dict[int, int]:
    tree = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as fh:
                data = fh.read()
        except OSError:
            continue
        # comm may contain spaces and parens; everything after the last ')'
        # is state, ppid, ...
        try:
            fields = data[data.rindex(")") + 2 :].split()
            tree[int(entry)] = int(fields[1])
        except (ValueError, IndexError):
            continue
    return tree


def descendants(root: int) -> list[int]:
    tree = parent_map()
    kids: dict[int, list[int]] = {}
    for pid, ppid in tree.items():
        kids.setdefault(ppid, []).append(pid)
    out, stack = [], list(kids.get(root, []))
    while stack:
        pid = stack.pop()
        out.append(pid)
        stack.extend(kids.get(pid, []))
    return out


def kill_tree(pid: int) -> None:
    for target in descendants(pid) + [pid]:
        try:
            os.kill(target, 9)
        except OSError:
            pass


class Sampler(threading.Thread):
    """Sample the RSS of a process tree every `interval` seconds."""

    def __init__(self, root: int, interval: float = 0.05):
        super().__init__(daemon=True)
        self.root = root
        self.interval = interval
        self.stop = threading.Event()
        self.peak_root = 0
        self.peak_worker = 0  # largest single descendant
        self.peak_total = 0
        self.trace: list[tuple[float, int]] = []

    def run(self) -> None:
        origin = time.time()
        while not self.stop.is_set():
            kids = descendants(self.root)
            root_rss = rss_of(self.root)
            per_kid = [rss_of(pid) for pid in kids]
            total = root_rss + sum(per_kid)
            self.peak_root = max(self.peak_root, root_rss)
            self.peak_worker = max(self.peak_worker, max(per_kid, default=0))
            self.peak_total = max(self.peak_total, total)
            self.trace.append((time.time() - origin, max(per_kid, default=root_rss)))
            time.sleep(self.interval)


# ------------------------------------------------------------------- redis


class Redis:
    """A throwaway server, unless BENCH_REDIS points somewhere already running."""

    def __enter__(self):
        global REDIS_PORT, REDIS_URL
        self.external = bool(EXTERNAL_REDIS)
        if self.external:
            REDIS_URL = EXTERNAL_REDIS
            REDIS_PORT = int(EXTERNAL_REDIS.rsplit(":", 1)[1].split("/")[0])
            self.dir = None
            self.proc = None
            return self
        self.dir = tempfile.mkdtemp(prefix="bench-redis-")
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            REDIS_PORT = probe.getsockname()[1]
        REDIS_URL = f"redis://127.0.0.1:{REDIS_PORT}/0"
        self.proc = subprocess.Popen(
            ["redis-server", "--port", str(REDIS_PORT), "--save", "", "--appendonly", "no",
             "--dir", self.dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(100):
            if subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "ping"],
                              capture_output=True).stdout.strip() == b"PONG":
                return self
            time.sleep(0.1)
        raise RuntimeError("redis did not come up")

    def flush(self) -> None:
        """FLUSHDB, never FLUSHALL.

        FLUSHALL empties every database in the instance regardless of which one
        is selected, so a harness that used it could not be pointed at a Redis
        anybody cared about — measured: a key in db 0 does not survive a
        FLUSHALL issued from db 10.
        """
        db = REDIS_URL.rsplit("/", 1)[-1] or "0"
        subprocess.run(["redis-cli", "-p", str(REDIS_PORT), "-n", db, "flushdb"],
                       capture_output=True)

    def __exit__(self, *exc):
        if self.proc is not None:
            self.proc.terminate()
            self.proc.wait()
        if self.dir:
            shutil.rmtree(self.dir, ignore_errors=True)


# ----------------------------------------------------------------- results


@dataclass
class Result:
    framework: str
    label: str
    expected: int
    completed: int = 0      # distinct task ids that finished at least once
    runs: int = 0           # executions, including re-runs after a kill
    pids: int = 0           # distinct worker processes that ran a task
    launches: int = 1       # times the worker had to be (re)started from outside
    wall: float = 0.0
    peak_worker: int = 0    # max(self-reported high-water, /proc sampling)
    peak_sampled: int = 0
    peak_total: int = 0
    peak_root: int = 0
    gaps: list[float] = field(default_factory=list)
    note: str = ""

    @property
    def lost(self) -> int:
        return self.expected - self.completed

    @property
    def rate(self) -> float:
        return self.completed / self.wall if self.wall else 0.0


def read_log(path: Path) -> list[tuple[int, int, int, float, float]]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text().splitlines():
        parts = line.split("\t")
        if len(parts) != 5:
            continue  # a torn final line from a killed worker
        try:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2]), float(parts[3]), float(parts[4])))
        except ValueError:
            continue
    return rows


# ----------------------------------------------------------------- runners


def base_env(log: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(BENCH_LOG=str(log), BENCH_REDIS=REDIS_URL, PYTHONPATH=str(ROOT))
    return env


def celery_cmd(max_tasks_per_child: int | None) -> list[str]:
    cmd = [PY, "-m", "celery", "-A", "bench.celery_app", "worker", "-c", "1", "-P", "prefork",
           "--loglevel", "error", "--without-gossip", "--without-mingle", "--without-heartbeat"]
    if max_tasks_per_child:
        cmd += ["--max-tasks-per-child", str(max_tasks_per_child)]
    return cmd


def taskiq_cmd() -> list[str]:
    return [PY, "-m", "taskiq", "worker", "bench.taskiq_app:broker", "--workers", "1",
            "--max-async-tasks", "1", "--max-threadpool-threads", "1", "--log-level", "ERROR"]


def tarsk_cmd(spec: dict, tmp: Path) -> list[str]:
    path = tmp / "tarsk-spec.json"
    path.write_text(json.dumps(spec))
    return [PY, "bench/tarsk_worker.py", str(path)]


def submit_celery(task: str, count: int, args: list) -> None:
    from bench.celery_app import app

    for i in range(count):
        app.send_task(task, args=[i, *args])


def submit_taskiq(task: str, count: int, args: list) -> None:
    import asyncio

    import bench.taskiq_app as mod

    async def go():
        await mod.broker.startup()
        handle = getattr(mod, task)
        for i in range(count):
            await handle.kiq(i, *args)
        await mod.broker.shutdown()

    asyncio.run(go())


def run_worker(cmd: list[str], env: dict, expected: int, log: Path, timeout: float,
               memory_max: str | None = None, restarts: int = 0) -> Result:
    """Run the worker to completion, optionally restarting it when it dies.

    `restarts` stands in for the orchestrator: Kubernetes restarts a pod its OOM
    killer took, and a queue configured to ack late gets its in-flight work
    redelivered to the replacement. Without that, a benchmark measures the
    absence of a supervisor rather than the queue.
    """
    if memory_max:
        cmd = ["systemd-run", "--user", "--scope", "-q", "-p", f"MemoryMax={memory_max}",
               "-p", "MemorySwapMax=0", "--"] + cmd
    started = time.time()
    deadline = started + timeout
    peaks = [0, 0, 0]
    launches = 0

    while time.time() < deadline and launches <= restarts:
        launches += 1
        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)
        sampler = Sampler(proc.pid)
        sampler.start()
        while time.time() < deadline:
            if len({row[0] for row in read_log(log)}) >= expected:
                break
            if proc.poll() is not None:
                break  # died — the orchestrator's turn
            time.sleep(0.05)
        sampler.stop.set()
        sampler.join()
        peaks = [max(peaks[0], sampler.peak_worker), max(peaks[1], sampler.peak_total),
                 max(peaks[2], sampler.peak_root)]
        kill_tree(proc.pid)
        proc.wait()
        if len({row[0] for row in read_log(log)}) >= expected:
            break
    wall = time.time() - started

    rows = read_log(log)
    # Two lower bounds on the true peak, each blind in a different way:
    # ru_maxrss cannot miss a spike but stops at the last completed task, so it
    # under-reports a process that was killed mid-task; /proc sampling sees a
    # kill coming but misses any spike shorter than the 50ms interval — which a
    # fast handover makes likely. The larger of the two is the tighter bound.
    self_peak = max((row[2] for row in rows), default=0)
    result = Result(framework="", label="", expected=expected,
                    completed=len({row[0] for row in rows}), runs=len(rows),
                    pids=len({row[1] for row in rows}), launches=launches, wall=wall,
                    peak_worker=max(self_peak, peaks[0]), peak_sampled=peaks[0],
                    peak_total=peaks[1], peak_root=peaks[2])
    ends = sorted(row[4] for row in rows)
    warm = ends[len(ends) // 10 :]  # drop the first 10%: worker startup, not a gap
    result.gaps = [b - a for a, b in zip(warm, warm[1:])]
    return result


def case(framework: str, label: str, task: str, count: int, args: list, timeout: float,
         redis: Redis, tmp: Path, *, celery_mtpc=None, tarsk_kw=None, memory_max=None,
         extra_env=None, restarts=0) -> Result:
    log = tmp / f"{framework}-{label}-{task}.log".replace(" ", "_")
    log.unlink(missing_ok=True)
    log.touch()
    env = base_env(log)
    env.update(extra_env or {})

    if framework == "tarsk":
        spec = {"children": 1, "task": task, "args": args, "count": count,
                "out": str(tmp / "tarsk-out.json"), **(tarsk_kw or {})}
        cmd = tarsk_cmd(spec, tmp)
    else:
        redis.flush()
        if framework == "celery":
            submit_celery(task, count, args)
            cmd = celery_cmd(celery_mtpc)
        else:
            submit_taskiq(task, count, args)
            cmd = taskiq_cmd()

    result = run_worker(cmd, env, count, log, timeout, memory_max, restarts)
    result.framework, result.label = framework, label
    return result


# --------------------------------------------------------------- scenarios


def table(title: str, note: str, rows: list[Result], columns: list[str]) -> None:
    print(f"\n### {title}\n")
    if note:
        print(note + "\n")
    header = {"peak": "peak worker RSS", "total": "peak tree RSS", "done": "completed",
              "lost": "lost", "rate": "tasks/sec", "wall": "wall", "p50gap": "p50 gap",
              "p99gap": "p99 gap", "maxgap": "max gap", "runs": "executions",
              "pids": "worker pids", "launches": "worker restarts"}
    print("| runtime | " + " | ".join(header[c] for c in columns) + " |")
    print("|---" * (len(columns) + 1) + "|")
    for r in rows:
        cells = []
        for column in columns:
            if column == "peak":
                cells.append(f"{r.peak_worker / MB:.0f} MB")
            elif column == "total":
                cells.append(f"{r.peak_total / MB:.0f} MB")
            elif column == "done":
                cells.append(f"{r.completed}/{r.expected}")
            elif column == "lost":
                cells.append("**" + str(r.lost) + "**" if r.lost else "0")
            elif column == "runs":
                cells.append(str(r.runs) + ("" if r.runs == r.completed else f" (+{r.runs - r.completed})"))
            elif column == "pids":
                cells.append(str(r.pids))
            elif column == "launches":
                cells.append(str(r.launches - 1))
            elif column == "rate":
                cells.append(f"{r.rate:.0f}")
            elif column == "wall":
                cells.append(f"{r.wall:.1f}s")
            elif column == "p50gap":
                cells.append(f"{statistics.median(r.gaps) * 1000:.1f} ms" if r.gaps else "—")
            elif column == "p99gap":
                cells.append(f"{quantile(r.gaps, 0.99) * 1000:.0f} ms" if r.gaps else "—")
            elif column == "maxgap":
                cells.append(f"{max(r.gaps) * 1000:.0f} ms" if r.gaps else "—")
        print(f"| {r.framework} {r.label} | " + " | ".join(cells) + " |")


def quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def measure_imports(cmd: list[str], env: dict, tmp: Path, tag: str) -> tuple[float, float, bool]:
    """Coordinator RSS, importer RSS, and whether the coordinator is the importer.

    The importing process names itself in a log, because picking "the biggest
    child" guesses wrong the moment a runtime keeps more than one process
    around. RSS is sampled repeatedly and kept at its maximum: a worker that
    dies and is respawned would otherwise be caught mid-import and read low.
    """
    log = tmp / f"imports-{tag}.log"
    log.write_text("")
    env = {**env, "BENCH_IMPORT_LOG": str(log)}
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, start_new_session=True)
    coordinator = importer = 0.0
    importers: set[int] = set()
    ever_alive = False
    deadline = time.time() + 8
    while time.time() < deadline:
        importers |= {int(line) for line in log.read_text().split() if line.isdigit()}
        coordinator = max(coordinator, rss_of(proc.pid))
        live = [rss_of(pid) for pid in importers if rss_of(pid) > 0]
        ever_alive |= bool(live)
        importer = max(importer, max(live, default=0.0))
        time.sleep(0.2)
    root_imports = proc.pid in importers
    family = [proc.pid, *descendants(proc.pid)]
    tree_rss = sum(smaps(pid, "Rss:") for pid in family)
    tree_pss = sum(smaps(pid, "Pss:") for pid in family)
    kill_tree(proc.pid)
    proc.wait()
    return coordinator, importer if ever_alive else None, root_imports, tree_rss, tree_pss


def scenario_imports(redis: Redis, tmp: Path) -> None:
    runtimes = {
        "celery": [PY, "-m", "celery", "-A", "bench.footprint_celery", "worker", "-c", "1",
                   "--loglevel", "error", "--without-gossip", "--without-mingle",
                   "--without-heartbeat"],
        "taskiq": [PY, "-m", "taskiq", "worker", "bench.footprint_taskiq:broker",
                   "--workers", "1", "--log-level", "ERROR"],
        "tarsk": [PY, "-m", "tarsk._cli", "worker", "--app", "bench.footprint_app:app",
                  "--broker", REDIS_URL, "--children", "1"],
    }
    print("\n### Where your imports land\n")
    print("The same two app modules under each runtime: one trivial, one importing "
          "celery + taskiq + redis,\nwhich costs 47MB in a bare interpreter and stands in "
          "for a real dependency tree. `heavy − light`\nis what your code costs that "
          "process.\n")
    print("| runtime | coordinator (light → heavy) | runs your code (light → heavy) | "
          "coordinator imports it | whole tree, heavy (Rss / Pss) |")
    print("|---|---|---|---|---|")
    for name, cmd in runtimes.items():
        redis.flush()
        base = {**os.environ, "BENCH_REDIS": REDIS_URL, "PYTHONPATH": str(ROOT)}
        light = measure_imports(cmd, base, tmp, f"{name}-light")
        heavy = measure_imports(cmd, {**base, "BENCH_HEAVY": "1"}, tmp, f"{name}-heavy")
        def arrow(a, b):
            if a is None or b is None:
                # Better an admitted gap than a zero that reads as a result.
                return "n/a — the process did not stay up to be measured"
            return f"{a / MB:.0f} → {b / MB:.0f} MB ({(b - a) / MB:+.0f})"
        # Rss summed over a tree counts every shared page once per process;
        # Pss splits it. The gap is how much the summed figure overstates.
        tree = f"{heavy[3] / MB:.0f} / **{heavy[4] / MB:.0f}** MB"
        print(f"| {name} | {arrow(light[0], heavy[0])} | {arrow(light[1], heavy[1])} | "
              f"{'**yes**' if light[2] else 'no'} | {tree} |")


def scenario_footprint(redis: Redis, tmp: Path) -> None:
    rows = [
        case("tarsk", "", "noop", 1, [], 60, redis, tmp),
        case("celery", "", "noop", 1, [], 60, redis, tmp),
        case("taskiq", "", "noop", 1, [], 60, redis, tmp),
    ]
    table("Footprint — RSS after one trivial task",
          "The process that runs user code. tarsk children never import the broker "
          "driver (spec §4.1); Celery and taskiq workers carry theirs.",
          rows, ["peak", "total"])


CEILING = 200 * MB
MTPC = 6  # tuned so 6 × 20MB + interpreter ≈ the same 200MB budget


def scenario_memory(redis: Redis, tmp: Path) -> None:
    count, leak = 40, 20
    rows = [
        case("celery", "(no recycling)", "leak", count, [leak], 300, redis, tmp),
        case("taskiq", "(no recycling available)", "leak", count, [leak], 300, redis, tmp),
        case("celery", f"--max-tasks-per-child={MTPC}", "leak", count, [leak], 300, redis, tmp,
             celery_mtpc=MTPC),
        case("tarsk", "--max-rss=200MB", "leak", count, [leak], 300, redis, tmp,
             tarsk_kw={"max_rss": CEILING}),
    ]
    table(f"Bounded memory — {count} tasks, each retaining {leak}MB",
          f"Both limits aim at the same ~200MB budget: `--max-tasks-per-child={MTPC}` was "
          "chosen by dividing that budget by this workload's known leak rate. Celery bounds "
          "it tighter here, and that is the fair result to report — when you know the leak "
          "per task, counting tasks works.", rows, ["peak", "done", "lost"])


def scenario_memory_workload_shift(redis: Redis, tmp: Path) -> None:
    # Same configuration as scenario_memory, only the workload changed. This is
    # the part a task-count proxy cannot survive: it encodes an assumption about
    # bytes-per-task that nothing enforces.
    count, leak = 30, 40
    rows = [
        case("celery", f"--max-tasks-per-child={MTPC} (unchanged)", "leak", count, [leak], 300,
             redis, tmp, celery_mtpc=MTPC),
        case("tarsk", "--max-rss=200MB (unchanged)", "leak", count, [leak], 300, redis, tmp,
             tarsk_kw={"max_rss": CEILING}),
    ]
    table(f"Same config, heavier workload — {count} tasks now retaining {leak}MB",
          "Nothing was reconfigured; the tasks got bigger, as tasks do. A task count is a "
          "guess about bytes-per-task, and the guess is now wrong. An RSS ceiling is not a "
          "guess about anything.", rows, ["peak", "done", "lost"])


def scenario_memory_variable(redis: Redis, tmp: Path) -> None:
    count = 40
    rows = [
        case("celery", f"--max-tasks-per-child={MTPC}", "leak", count, [-1], 300, redis, tmp,
             celery_mtpc=MTPC),
        case("tarsk", "--max-rss=200MB", "leak", count, [-1], 300, redis, tmp,
             tarsk_kw={"max_rss": CEILING}),
    ]
    table(f"Payload-dependent memory — {count} tasks retaining 2–80MB each",
          "The realistic case: memory follows the payload, so no task count maps to a byte "
          "budget. Identical deterministic sequence for both runtimes.",
          rows, ["peak", "done", "lost"])


def scenario_oom(redis: Redis, tmp: Path) -> None:
    count, leak = 30, 20
    limit = "400M"
    late = dict(os.environ)
    rows = [
        case("celery", "(defaults)", "leak", count, [leak], 90, redis, tmp,
             memory_max=limit),
        case("taskiq", "(no recycling available)", "leak", count, [leak], 90, redis, tmp,
             memory_max=limit),
        case("celery", "task_acks_late=True", "leak", count, [leak], 120, redis, tmp,
             memory_max=limit, extra_env={"BENCH_ACKS_LATE": "1"}),
        case("celery", "task_acks_late=True + restarted", "leak", count, [leak], 180, redis, tmp,
             memory_max=limit, extra_env={"BENCH_ACKS_LATE": "1"}, restarts=10),
        case("celery", f"--max-tasks-per-child={MTPC} (tuned)", "leak", count, [leak], 90,
             redis, tmp, celery_mtpc=MTPC, memory_max=limit),
        case("tarsk", "--max-rss=200MB", "leak", count, [leak], 90, redis, tmp,
             tarsk_kw={"max_rss": 200 * MB}, memory_max=limit),
    ]
    del late
    table(f"Task loss under a hard {limit} cgroup limit — {count} tasks leaking {leak}MB",
          "What Kubernetes does to a leaky worker. The OOM killer takes in-flight work "
          "with it; a runtime that recycles below the limit does not.",
          rows, ["done", "lost", "runs", "launches", "maxgap", "peak"])


def scenario_gap(redis: Redis, tmp: Path) -> None:
    count = 120
    rows = [
        case("celery", "--max-tasks-per-child=20", "work5", count, [], 300, redis, tmp,
             celery_mtpc=20),
        case("tarsk", "--max-tasks=20", "work5", count, [], 300, redis, tmp,
             tarsk_kw={"max_tasks": 20}),
        case("taskiq", "(no recycling available)", "work5", count, [], 300, redis, tmp),
    ]
    table(f"Recycling stalls — {count} × 5ms tasks, recycled every 20",
          "Gap between consecutive completions. Kill-then-spawn leaves the slot empty "
          "for a whole interpreter startup; overlap replacement does not (spec §4.4).",
          rows, ["p50gap", "p99gap", "maxgap", "done"])


def scenario_throughput(redis: Redis, tmp: Path) -> None:
    rows = [
        case("celery", "noop", "noop", 500, [], 300, redis, tmp),
        case("taskiq", "noop", "noop", 500, [], 300, redis, tmp),
        case("tarsk", "noop (no broker yet)", "noop", 500, [], 300, redis, tmp),
        case("celery", "50ms handler", "work50", 200, [], 300, redis, tmp),
        case("taskiq", "50ms handler", "work50", 200, [], 300, redis, tmp),
        case("tarsk", "50ms handler (no broker yet)", "work50", 200, [], 300, redis, tmp),
    ]
    table("Throughput — single worker, concurrency 1",
          "Not a claim (spec §2, §5). tarsk has no broker yet, so its rows skip a hop "
          "the others pay: treat them as an upper bound, not a win. The 50ms rows are "
          "the point — with a realistic handler the runtimes converge.",
          rows, ["rate", "wall", "done"])


SCENARIOS = {
    "footprint": scenario_footprint,
    "imports": scenario_imports,
    "memory": scenario_memory,
    "shift": scenario_memory_workload_shift,
    "variable": scenario_memory_variable,
    "oom": scenario_oom,
    "gap": scenario_gap,
    "throughput": scenario_throughput,
}


def main() -> None:
    wanted = sys.argv[1:] or list(SCENARIOS)
    unknown = [name for name in wanted if name not in SCENARIOS]
    if unknown:
        sys.exit(f"unknown scenario(s): {unknown}. pick from {list(SCENARIOS)}")
    tmp = Path(tempfile.mkdtemp(prefix="bench-"))
    try:
        with Redis() as redis:
            for name in wanted:
                SCENARIOS[name](redis, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    main()
