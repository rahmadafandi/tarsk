"""End-to-end check of the step-1 IPC protocol.

Run: python tests/test_ipc.py   (or pytest tests/test_ipc.py)
"""

import os
import sys
import tempfile
from pathlib import Path
from unittest import SkipTest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)  # children resolve `tests.demo_app` from cwd

from tarsk import App  # noqa: E402
from tarsk._child import EXIT_SYNC_TIMEOUT  # noqa: E402
from tarsk._supervisor import Supervisor  # noqa: E402

JOBS = [
    ("add", (2, 3), {}),
    ("boom", (), {}),
    ("naps", (), {}),
    ("slow_sync", (), {}),
    ("hard_crash", (), {}),
    ("tests.demo_app.missing", (), {}),
]
FLAKY_MARKER = Path(tempfile.gettempdir()) / "tarsk-flaky-marker"
ATTEMPT_MARKER = Path(tempfile.gettempdir()) / "tarsk-attempt-marker"


def test_registry_hash():
    a, b = App(), App()
    a.task(name="x", timeout=10)(lambda: None)
    b.task(name="x", timeout=20)(lambda: None)
    assert a.registry_hash() != b.registry_hash(), "timeout change must move the hash"

    c = App()
    c.task(name="x", timeout=10)(lambda: None)
    assert a.registry_hash() == c.registry_hash(), "identical registries must agree"


def test_timeout_cap():
    app = App(default_timeout=5, max_timeout=5)
    try:
        app.task(timeout=6)(lambda: None)
    except ValueError:
        pass
    else:
        raise AssertionError("timeout above max_timeout must be rejected at registration")


def test_slots_overlap_inside_one_child():
    """Four awaiting tasks, one child: concurrent, or the slot count is a lie."""
    import time

    jobs = [("waits", (), {}) for _ in range(4)]

    start = time.monotonic()
    serial = Supervisor("tests.demo_app:app", children=1, slots=1)
    serial_results = serial.run(jobs)
    serial_wall = time.monotonic() - start

    start = time.monotonic()
    parallel = Supervisor("tests.demo_app:app", children=1, slots=4)
    parallel_results = parallel.run(jobs)
    parallel_wall = time.monotonic() - start

    assert len(parallel_results) == 4, parallel_results
    assert all(r[0] == "ack" for r in parallel_results.values()), parallel_results
    # All four ran in the same process — this is concurrency, not more children.
    assert len({r[1] for r in parallel_results.values()}) == 1, parallel_results

    # Four 0.2s waits: 0.8s serialised, ~0.2s overlapped. Interpreter startup is
    # in both figures, so compare them against each other rather than a constant.
    assert parallel_wall < serial_wall - 0.3, (
        f"slots=4 took {parallel_wall:.2f}s against slots=1 at {serial_wall:.2f}s — "
        "the tasks did not overlap"
    )
    assert len(serial_results) == 4, serial_results


def test_slots_overlap_sync_handlers_too():
    """A sync handler holds a thread, and asyncio's default pool is small.

    Without sizing the executor to the slot count, twelve slots would run
    min(32, cpu + 4) at a time — silently, and only on sync handlers.

    Timed against itself rather than a constant: the first version asserted a
    wall-clock ceiling tuned to one laptop and failed on macOS, where a slower
    interpreter startup pushed a fully overlapped run past it.
    """
    import time

    jobs = [("blocks", (), {}) for _ in range(12)]

    start = time.monotonic()
    Supervisor("tests.demo_app:app", children=1, slots=1).run(jobs)
    serial = time.monotonic() - start

    start = time.monotonic()
    results = Supervisor("tests.demo_app:app", children=1, slots=12).run(jobs)
    parallel = time.monotonic() - start

    assert len(results) == 12, results
    assert all(r[0] == "ack" for r in results.values()), results
    # Twelve 0.2s sleeps: 2.4s in a row, 0.2s at once, with the same startup in
    # both. Compared as a ratio rather than a margin, because a busy machine
    # stretches both numbers and only a fixed difference notices.
    assert parallel < serial / 2, (
        f"twelve sync slots took {parallel:.2f}s against {serial:.2f}s at one slot — "
        "the thread pool is the cap, not the slot count"
    )


def test_end_to_end():
    sup = Supervisor("tests.demo_app:app", children=2)
    results = sup.run(JOBS)

    assert len(results) == len(JOBS), f"missing results: {results}"
    assert sup.stats["registry_hash"] != 0, "no child ever registered"
    # Compare against the app itself: the point is that the registry crossed
    # the wire intact, not that it happens to have six entries today.
    from tests.demo_app import app as demo

    assert sup.stats["registry_len"] == len(demo.registry), sup.stats

    assert results[0] == ("ack", 5), results[0]

    status, (error_type, tb) = results[1]
    assert (status, error_type) == ("nack", "ValueError"), results[1]
    assert "kaboom" in tb, "traceback must cross the socket pre-formatted"

    assert results[2][0] == "nack" and results[2][1][0] == "TimeoutError", results[2]
    assert results[3][0] == "nack" and results[3][1][0] == "TimeoutError", results[3]
    # spec §4.5: a sync handler cannot be interrupted, so its timeout costs the child
    assert EXIT_SYNC_TIMEOUT in sup.exits, sup.exits

    # retries=1, so it is redelivered once and then dead-lettered
    assert results[4] == ("nack", ("ChildDied", "hard_crash did not survive its child")), results[4]
    assert results[5][1][0] == "UnknownTask", results[5]


def test_retry_then_succeed():
    """retries=2: two failures are absorbed, the third attempt is the answer."""
    FLAKY_MARKER.unlink(missing_ok=True)
    sup = Supervisor("tests.demo_app:app", children=1)
    results = sup.run([("flaky", (str(FLAKY_MARKER),), {})])
    try:
        assert results[0] == ("ack", 3), results[0]
        assert sup.stats["task_retries"] == 2, sup.stats
        assert sup.stats["tasks_dead_lettered"] == 0, sup.stats
    finally:
        FLAKY_MARKER.unlink(missing_ok=True)


def test_retries_run_out():
    """A task that never succeeds stops after its allowance, not forever."""
    sup = Supervisor("tests.demo_app:app", children=1)
    results = sup.run([("boom", (), {})])
    kind, (error_type, _) = results[0]
    assert (kind, error_type) == ("nack", "ValueError"), results[0]
    assert sup.stats["task_retries"] == 0, "boom has no retries configured"
    assert sup.stats["tasks_dead_lettered"] == 1, sup.stats


def test_reject_skips_the_remaining_retries():
    sup = Supervisor("tests.demo_app:app", children=1)
    results = sup.run([("hopeless", (), {})])
    kind, (error_type, _) = results[0]
    assert (kind, error_type) == ("nack", "Reject"), results[0]
    # retries=5, so the policy alone would have run it six times
    assert sup.stats["task_retries"] == 0, sup.stats
    assert sup.stats["tasks_dead_lettered"] == 1, sup.stats


def test_a_duplicate_ack_is_not_counted():
    """The child hosts user code, so it is not a trusted counter of its own work.

    Read through the max_tasks recycle rather than the counter itself: two jobs
    under a limit of four is a run that never recycles, and only counting an Ack
    the supervisor never dispatched gets it there. The metrics endpoint reports
    the same number but wants a live broker and an HTTP scrape to say so.
    """
    from tests import scripted_child

    jobs = [("add", (1, 1), {}), ("add", (2, 2), {})]

    def ran_the_jobs(sup, results):
        """A child that never started recycles nothing, which is not the point.

        `recycle_max_tasks == 0` is what a working supervisor and a dead
        harness both produce, so it has to be read alongside proof that two
        tasks were actually dispatched and answered.
        """
        assert len(results) == len(jobs), f"the scripted child never ran: {results}"
        assert all(kind == "ack" for kind, _ in results.values()), (
            f"nothing below counts anything if the child did not run: {results}"
        )
        assert sup.stats["children_spawned"] >= 1, sup.stats

    with tempfile.TemporaryDirectory() as tmp:
        # The control: four is genuinely out of reach for two tasks, so the
        # assertion below is about the duplicate and not about the limit.
        honest = Supervisor("tests.demo_app:app", children=1, max_tasks=4,
                            python=scripted_child.launcher(tmp, acks=1))
        ran_the_jobs(honest, honest.run(jobs))
        assert honest.stats.get("recycle_max_tasks", 0) == 0, honest.stats

        liar = Supervisor("tests.demo_app:app", children=1, max_tasks=4,
                          python=scripted_child.launcher(tmp, acks=2))
        ran_the_jobs(liar, liar.run(jobs))
        assert liar.stats.get("recycle_max_tasks", 0) == 0, liar.stats


def test_retry_hands_the_job_back():
    marker = Path(tempfile.gettempdir()) / "tarsk-retry-marker"
    marker.unlink(missing_ok=True)
    sup = Supervisor("tests.demo_app:app", children=1)
    try:
        results = sup.run([("asks_again", (str(marker),), {})])
        assert results[0] == ("ack", 2), results[0]
        assert sup.stats["task_retries"] == 1, sup.stats
    finally:
        marker.unlink(missing_ok=True)


def test_attempt_counts_up_across_retries():
    """ctx.attempt must be the broker's attempt number, not a constant 1.

    The retry *count* was already asserted elsewhere and stayed right while
    this was wrong: the number the supervisor uses for backoff and for the
    dead-letter decision never crossed the IPC boundary, so a handler
    branching on ctx.attempt saw 1 on every attempt.
    """
    ATTEMPT_MARKER.unlink(missing_ok=True)
    sup = Supervisor("tests.demo_app:app", children=1)
    try:
        sup.run([("records_attempts", (str(ATTEMPT_MARKER),), {})])
        seen = ATTEMPT_MARKER.read_text().split()
        assert seen == ["1", "2", "3"], f"ctx.attempt across a retry run: {seen}"
    finally:
        ATTEMPT_MARKER.unlink(missing_ok=True)


if __name__ == "__main__":
    ran, skipped = [], []
    for check in (test_slots_overlap_inside_one_child, test_slots_overlap_sync_handlers_too, test_registry_hash, test_timeout_cap, test_end_to_end,
                  test_retry_then_succeed, test_retries_run_out,
                  test_reject_skips_the_remaining_retries, test_retry_hands_the_job_back,
                  test_a_duplicate_ack_is_not_counted,
                  test_attempt_counts_up_across_retries):
        try:
            check()
        except SkipTest as why:
            # Never "ok": a check that did not run must not print like one.
            skipped.append(check.__name__)
            print(f"skip {check.__name__} ({why})")
        else:
            ran.append(check.__name__)
            print("ok", check.__name__)
    print(f"\n{len(ran)} ran, {len(skipped)} skipped"
          + (f": {', '.join(skipped)}" if skipped else ""))
