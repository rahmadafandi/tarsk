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
    # Carrying the stats matters when this runs where its author cannot: the
    # supervisor's own peak reading says whether the ceiling was never crossed
    # or crossed and ignored, and those want different fixes.
    assert len(pids) > 1, (
        f"never recycled — one child ran all {JOBS} jobs. "
        f"ceiling {CEILING / 1e6:.0f}MB, supervisor saw a peak of "
        f"{sup.stats.get('child_rss_peak', 0) / 1e6:.1f}MB across "
        f"{sup.stats.get('children_spawned', 0)} spawns; {sup.stats}"
    )
    assert sup.stats.get("recycle_max_rss", 0) > 0, sup.stats

    # The real contract: the ceiling is checked while a child is idle, so a
    # worker can overshoot by at most the peak allocation of one task. Nothing
    # weaker is achievable without killing running work.
    bound = CEILING + LEAK_MB * 1024 * 1024
    peak = max(rss for _, (_, rss) in results.values())
    if peak == 0:
        # Windows has no `resource`, so the handler cannot weigh itself and
        # every reading is zero — which would satisfy `peak <= bound` while
        # measuring nothing. Say so instead of collecting the pass. The ceiling
        # itself is still proven above by recycle_max_rss, which the supervisor
        # reports from its own reading.
        print("  (overshoot bound not checked: no per-handler RSS on this platform)")
    else:
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


def test_hooks_bracket_every_worker():
    """Each worker process runs the hooks once, including every replacement.

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


def test_before_send_can_attach_to_the_payload():
    """The producer-side hook, which the README claimed before it existed."""
    from unittest.mock import Mock

    import msgpack

    from tarsk import App

    app = App(broker="memory://")

    class Tracer:
        def before_send(self, ctx):
            ctx.kwargs["trace_id"] = f"trace-of-{ctx.name}"

    app.middleware(Tracer())

    @app.task(name="carried")
    def carried(x, trace_id=None):
        return trace_id

    app._producer = Mock()
    app.registry["carried"].send(1)
    payload = app._producer.send.call_args[0][3]
    assert msgpack.unpackb(payload) == [[1], {"trace_id": "trace-of-carried"}], payload


def test_middleware_wraps_and_dependencies_inject():
    """Layers nest outside-in and unwind inside-out, failures included.

    The worker-scoped provider must resolve once for the process, not once per
    task — that is the whole difference between it and a plain default value.
    """
    import tempfile

    trace = Path(tempfile.mktemp(suffix=".trace"))
    os.environ["TARSK_TRACE_LOG"] = str(trace)
    try:
        sup = Supervisor("tests.demo_app:app", children=1)
        results = sup.run([
            ("uses_pool", ("a",), {}),
            ("uses_pool", ("b",), {}),
            ("wrapped_boom", (), {}),
        ])

        # Injected, and the same instance both times: one provider call.
        assert results[0] == ("ack", ["a", 1, 1]), results[0]
        assert results[1] == ("ack", ["b", 1, 1]), results[1]
        assert results[2][1][0] == "KeyError", results[2]

        lines = trace.read_text().splitlines()
        first = lines[: lines.index("outer<uses_pool") + 1]
        # A sync layer sandwiched between two async ones still nests: it runs
        # in a thread holding a blocking call() while the loop runs the rest.
        assert first == [
            "outer>uses_pool", "sync>uses_pool", "inner>uses_pool",
            "inner<uses_pool", "sync<uses_pool", "outer<uses_pool",
        ], f"layers did not nest: {first}"
        assert "inner!KeyError" in lines and "outer!KeyError" in lines, lines
    finally:
        trace.unlink(missing_ok=True)
        os.environ.pop("TARSK_TRACE_LOG", None)


def test_soft_timeout_asks_before_it_takes():
    """Three handlers, three answers, one deadline pair.

    The value is in the gap between being asked and being stopped, so all three
    rows come from one run: what a handler does with the ask is the only thing
    that differs.
    """
    sup = Supervisor("tests.soft_app:app", children=1)
    results = sup.run([
        ("tidies_up", (), {}),
        ("stops_when_asked", (), {}),
        ("ignores_the_ask", (), {}),
    ])

    # Cleaned up and returned in time: an ack, not a failure. The handler was
    # asked and answered, which is the whole point of asking.
    assert results[0] == ("ack", ["asked", "partial work kept"]), results[0]

    # Let the cancellation through: reported as its own kind, not as the hard
    # timeout, because "asked and stopped" is not "never asked".
    assert results[1][0] == "nack", results[1]
    assert results[1][1][0] == "SoftTimeout", results[1]

    # Swallowed the ask: the hard timeout still takes it, and it is reported as
    # a timeout rather than relabelled. A soft deadline is a request; the hard
    # one is not, and a handler cannot talk its way out of the second.
    assert results[2][0] == "nack", results[2]
    assert results[2][1][0] == "TimeoutError", results[2]


def test_soft_timeout_is_refused_where_it_cannot_work():
    """Rejected at decoration rather than accepted and quietly ignored."""
    from tarsk import App

    probe = App(default_timeout=5, max_timeout=5)

    # A thread cannot be interrupted, so there is nowhere to deliver the ask.
    try:
        @probe.task(name="sync_soft", timeout=4, soft_timeout=1)
        def sync_soft():
            pass
    except ValueError as exc:
        assert "async handler" in str(exc), exc
    else:
        raise AssertionError("a sync handler accepted a soft_timeout")

    # A soft deadline at or past the hard one never fires.
    try:
        @probe.task(name="never_fires", timeout=4, soft_timeout=4)
        async def never_fires():
            pass
    except ValueError as exc:
        assert "never fire" in str(exc), exc
    else:
        raise AssertionError("soft_timeout >= timeout was accepted")


def test_memory_broker_carries_the_full_record():
    """Chains, concurrency caps and expiry on the in-memory broker.

    These three lived only on Redis and Postgres for a while: batch jobs
    carried no id/queue/chain, acquire_slot always said yes, and the memory
    broker never stamped when a job became runnable — so expiry read every
    job as too young to drop. A chain bug once had to be chased on Postgres
    because this broker could not express a chain at all.
    """
    import time as _time

    from tarsk import chain
    import tests.demo_app as demo

    # A chain ends the batch only after its tail has settled, and the tail is
    # fed the head's result.
    sup = Supervisor("tests.demo_app:app", children=1, slots=4)
    results = sup.run([chain(demo.add.s(1, 2), demo.add.s(10))])
    answers = sorted(v for kind, v in results.values() if kind == "ack")
    assert answers == [3, 13], f"chain on memory broker returned {results}"

    # max_concurrency=1 must serialise three 0.3s tasks even with slots free.
    sup = Supervisor("tests.demo_app:app", children=2, slots=4)
    started = _time.time()
    results = sup.run([("one_lane", (n,), {}) for n in range(3)])
    wall = _time.time() - started
    assert all(kind == "ack" for kind, _ in results.values()), results
    assert wall >= 0.75, (
        f"three capped 0.3s tasks finished in {wall:.2f}s — the cap did not hold"
    )

    # A job that waited past its expires is dropped, not run. Needs the memory
    # broker to know when the job became runnable; it used to say "no idea",
    # which no expiry check treats as expired.
    sup = Supervisor("tests.demo_app:app", children=1, slots=1)
    results = sup.run([("slow_lane", (), {}), ("milk", (), {})])
    assert results[0] == ("ack", "done"), results[0]
    kind, detail = results[1]
    assert kind == "nack" and detail[0] == "Expired", (
        f"a job 1.2s past expires=0.5 came back as {results[1]}"
    )


if __name__ == "__main__":
    for check in (test_leaky_handler_is_bounded, test_recycling_is_overlapped,
                  test_baseline_above_ceiling_is_refused,
                  test_hard_ceiling_kills_and_dead_letters,
                  test_hard_ceiling_must_exceed_the_soft_one,
                  test_hooks_bracket_every_worker,
                  test_middleware_wraps_and_dependencies_inject,
                  test_before_send_can_attach_to_the_payload,
                  test_soft_timeout_asks_before_it_takes,
                  test_soft_timeout_is_refused_where_it_cannot_work,
                  test_memory_broker_carries_the_full_record):
        check()
        print("ok", check.__name__)
