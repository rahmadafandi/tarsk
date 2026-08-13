"""The thesis, in miniature: a leaky handler stays under a ceiling, losing nothing.

The MVP demo (spec §6) is this run stretched to an hour with an RSS trace. This
is the version that fits in a test suite.

Run: python tests/test_recycle.py
"""

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from tarsk._supervisor import Supervisor  # noqa: E402

CEILING = 120 * 1024 * 1024  # 120MB
LEAK_MB = 20
JOBS = 20


def test_leaky_handler_is_bounded():
    sup = Supervisor("tests.leaky_app:app", children=1, max_rss=CEILING)
    results = sup.run([("leak", (LEAK_MB,), {}) for _ in range(JOBS)])

    assert len(results) == JOBS, f"lost tasks: got {len(results)} of {JOBS}"
    lost = {tid: v for tid, v in results.items() if v[0] != "ack"}
    assert not lost, f"tasks did not survive recycling: {lost}"

    # Unbounded, one child would end at ~{JOBS * LEAK_MB}MB. Distinct pids mean
    # the ceiling actually forced replacements.
    pids = {pid for _, (pid, _) in results.values()}
    assert len(pids) > 1, f"never recycled — one child ran all {JOBS} jobs"
    assert sup.stats.get("recycle_max_rss", 0) > 0, sup.stats

    # The real contract: the ceiling is checked while a child is idle, so a
    # worker can overshoot by at most the peak allocation of one task. Nothing
    # weaker is achievable without killing running work.
    bound = CEILING + LEAK_MB * 1024 * 1024
    peak = max(rss for _, (_, rss) in results.values())
    assert peak <= bound, f"peak child RSS {peak / 1e6:.1f}MB exceeded {bound / 1e6:.1f}MB"

    # Overlap replacement, not kill-then-spawn: nothing should have needed a signal.
    assert sup.stats["children_killed"] == 0, f"a child had to be killed: {sup.stats}"
    assert sup.stats["children_crashed"] == 0, f"a child died unexpectedly: {sup.stats}"


def test_recycling_is_overlapped():
    """Every recycle should hand over to a child that is already running.

    Spec §4.4's whole objection to `max-tasks-per-child` is the capacity gap;
    a replacement started *after* the trigger reproduces it. The spawn count is
    the sharper assertion: one child per recycle and not one more means no
    replacement was ever started twice.
    """
    sup = Supervisor("tests.demo_app:app", children=1, max_tasks=10)
    results = sup.run([("add", (i, 1), {}) for i in range(60)])

    assert all(kind == "ack" for kind, _ in results.values()), results
    recycles = sup.stats["children_recycled"]
    assert recycles >= 4, f"expected ~5 recycles at 60 tasks / 10, got {recycles}"
    assert sup.stats["children_recycled_prewarmed"] == recycles, (
        f"{recycles - sup.stats['children_recycled_prewarmed']} recycles paid a cold spawn: {sup.stats}"
    )
    assert sup.stats["children_spawned"] == recycles + 1, f"redundant spawns: {sup.stats}"


def test_baseline_above_ceiling_is_refused():
    """A ceiling below the interpreter itself is a config error, not a leak.

    Without this the supervisor recycles a child that never ran a task, spawns
    an identical replacement, and repeats until it trips its spawn cap — a lot
    of noise for a wrong number in a flag.
    """
    sup = Supervisor("tests.demo_app:app", children=1, max_rss=1024 * 1024)
    try:
        sup.run([("add", (1, 2), {})])
    except ValueError as exc:
        assert "baseline" in str(exc), exc
        assert "max_rss" in str(exc), exc
    else:
        raise AssertionError("a child that cannot fit its own ceiling must fail, not thrash")


def test_hard_ceiling_kills_and_dead_letters():
    """The opt-in ceiling: one task dies so the box does not.

    retries=1, so it is killed twice and then parked — a task that cannot fit
    must stop being retried, or the hard ceiling is just a slower crash loop.
    """
    sup = Supervisor("tests.demo_app:app", children=1, hard_max_rss=200 * 1024 * 1024)
    results = sup.run([("add", (1, 1), {}), ("glutton", (400,), {}), ("add", (2, 2), {})])

    assert results[0] == ("ack", 2), results[0]
    assert results[2] == ("ack", 4), results[2]
    kind, (error_type, message) = results[1]
    assert (kind, error_type) == ("nack", "HardMemoryLimit"), results[1]
    assert "200 MB" in message, message
    assert sup.stats["children_hard_killed"] == 2, sup.stats
    assert sup.stats["tasks_dead_lettered"] == 1, sup.stats


def test_hard_ceiling_must_exceed_the_soft_one():
    try:
        Supervisor("tests.demo_app:app", max_rss=200, hard_max_rss=100)
    except ValueError as exc:
        assert "must exceed" in str(exc), exc
    else:
        raise AssertionError("a hard ceiling below the soft one must be refused")


def test_child_hooks_bracket_every_child():
    """Each child runs the hooks exactly once, including the ones it replaced.

    A pool opened at import instead of in a hook lands in the baseline RSS the
    ceiling is measured against, so getting this right is not housekeeping.
    """
    import tempfile

    hook_log = Path(tempfile.mktemp(suffix=".hooks"))
    os.environ["TARSK_HOOK_LOG"] = str(hook_log)
    try:
        sup = Supervisor("tests.demo_app:app", children=1, max_tasks=10)
        results = sup.run([("add", (i, 1), {}) for i in range(30)])
        assert all(kind == "ack" for kind, _ in results.values()), results

        lines = [line.split() for line in hook_log.read_text().splitlines() if line]
        started = [pid for kind, pid in lines if kind == "start"]
        stopped = [pid for kind, pid in lines if kind == "stop"]
        assert len(started) == len(set(started)), f"a child ran start twice: {started}"
        # Every child that drained also stopped; the last one may still be
        # draining as run() returns, so stops trail starts by at most one.
        assert 0 <= len(started) - len(stopped) <= 1, f"{started} vs {stopped}"
        assert set(stopped) <= set(started), "a child stopped without starting"
        assert len(started) >= 3, f"expected several children at 30 tasks / 10: {started}"
    finally:
        hook_log.unlink(missing_ok=True)
        os.environ.pop("TARSK_HOOK_LOG", None)


if __name__ == "__main__":
    for check in (test_leaky_handler_is_bounded, test_recycling_is_overlapped,
                  test_baseline_above_ceiling_is_refused,
                  test_hard_ceiling_kills_and_dead_letters,
                  test_hard_ceiling_must_exceed_the_soft_one,
                  test_child_hooks_bracket_every_child):
        check()
        print("ok", check.__name__)
