"""The thesis, in miniature: a leaky handler stays under a ceiling, losing nothing.

The MVP demo (spec §6) is this run stretched to an hour with an RSS trace. This
is the version that fits in a test suite.

Run: python tests/test_recycle.py
"""

import os
import sys
import time
from pathlib import Path
from unittest import SkipTest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from tarsk import App  # noqa: E402
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


def test_growth_is_charged_to_the_task_that_caused_it():
    """Two tasks, one leaking and one not, and the supervisor can say which.

    The ceiling tells you a worker grew; it cannot tell you what grew it. One
    slot, so each reading has exactly one task between it and the last.
    """
    MB = 1024 * 1024
    sup = Supervisor("tests.leaky_app:app", children=1, max_rss=CEILING)
    jobs = [("leak", (8,), {}), ("tidy", (8,), {})] * 6
    results = sup.run(jobs)
    assert all(kind == "ack" for kind, _ in results.values()), results

    # Who: the leak keeps what it allocated, run after run, and the tidy task
    # hands everything back. Counted in objects, where the allocator's habit
    # of giving one task's freed memory to the next cannot move the blame.
    leak, tidy = sup.stats["kept_objects:leak"], sup.stats["kept_objects:tidy"]
    assert leak >= 6, f"six runs that each keep a bytearray kept {leak} objects"
    assert tidy < leak, f"the tidy task kept {tidy} objects against the leak's {leak}"

    # How much: between them the two were charged about what was leaked. Which
    # of the two is not asserted, because the bytes do not know — taking turns
    # like this, the tidy task is charged most of it.
    grown = sup.stats.get("rss_growth_bytes:leak", 0) + sup.stats.get("rss_growth_bytes:tidy", 0)
    assert grown >= 24 * MB, f"48 MB was leaked and {grown / MB:.1f} MB of growth was seen"


def test_a_task_is_not_charged_for_its_neighbour():
    """Two slots, and a task that keeps nothing running beside one that hoards.

    The count is the whole process's, so the bystander used to be charged the
    twenty thousand objects kept while it waited. A run that had company now
    reports nothing, and one that ran alone still does.
    """
    sup = Supervisor("tests.leaky_app:app", children=1, slots=2)
    results = sup.run([("bystander", (), {}), ("hoard", (20_000,), {})])
    assert all(kind == "ack" for kind, _ in results.values()), results
    for name in ("bystander", "hoard"):
        kept = sup.stats.get(f"kept_objects:{name}", 0)
        assert kept < 1_000, f"{name} ran beside another task and was charged {kept} objects"

    alone = Supervisor("tests.leaky_app:app", children=1, slots=2)
    alone.run([("hoard", (20_000,), {})])
    assert alone.stats["kept_objects:hoard"] >= 20_000, alone.stats


def test_counting_objects_stays_within_its_budget():
    """The count walks the heap, so it is rationed rather than taken every run."""
    from tarsk._child import _Census

    census = _Census()
    assert census.affordable(), "a fresh child could not afford its first count"
    before = census.credit
    census.count()
    assert census.credit < before, "a count cost nothing"

    # As after one count of a very large heap: nothing more until it is earned
    # back, and it is earned back at SHARE of the clock.
    census.credit = -census.SHARE * 0.05
    assert not census.affordable(), "counted again while in debt"
    time.sleep(0.1)
    assert census.affordable(), "0.1s did not earn back 0.05s worth of credit"
    assert census.credit <= census.CAP


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


def test_a_recycled_child_loses_no_acks():
    """Settling runs off the frame loop; recycling under it must lose nothing.

    Eight slots, a five-task limit and a child that acks the instant it is
    dispatched: eight recycles, each with the settle queue in use. Every job
    still has to come back.

    It cannot prove the drain on its own. The in-memory broker settles faster
    than the child can ack, so nothing is ever outstanding when a child retires
    and this passes with the drain removed. What proves that is
    `settle_tests::a_retiring_child_settles_everything_it_queued`, which can
    hold the consumer still. This is the end-to-end half: real children, real
    recycles, no answer missing.
    """
    import tempfile

    from tests import scripted_child

    jobs = [("add", (i, 1), {}) for i in range(40)]
    with tempfile.TemporaryDirectory() as tmp:
        sup = Supervisor("tests.demo_app:app", children=1, slots=8, max_tasks=5,
                         python=scripted_child.launcher(tmp))
        results = sup.run(jobs)

    assert len(results) == len(jobs), f"lost acks across recycles: {results}"
    assert all(kind == "ack" for kind, _ in results.values()), results
    # The point is the recycles: without several of them this asserts nothing
    # about what a retiring child owes.
    assert sup.stats.get("recycle_max_tasks", 0) >= 3, sup.stats


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


def test_hard_ceiling_holds_past_the_soft_one():
    """Both ceilings set, and a task that grows through one on its way to the other.

    This is how memory actually runs away, and the hard ceiling did nothing
    about it: crossing the soft one retired the child, a retiring child was no
    longer measured, and the task went on to 400 MB under a 220 MB limit. The
    test above never saw it — one allocation jumps both ceilings between two
    readings.
    """
    import tempfile

    marker = Path(tempfile.gettempdir()) / "tarsk-balloon-marker"
    marker.unlink(missing_ok=True)
    MB = 1024 * 1024
    sup = Supervisor("tests.demo_app:app", children=1,
                     max_rss=120 * MB, hard_max_rss=220 * MB)
    try:
        results = sup.run([("balloons", (400, str(marker)), {})])
        kind, (error_type, message) = results[0]
        assert (kind, error_type) == ("nack", "HardMemoryLimit"), results[0]
        assert sup.stats["children_hard_killed"] == 1, sup.stats
        # Ten megabytes every tenth of a second: stopped near the limit, not
        # at the end of the run.
        # The last whole line: the kill can land in the middle of the next.
        held = int([line for line in marker.read_text().split("\n") if line][-1])
        assert held <= 260, f"it was holding {held} MB when it was stopped"
        assert sup.stats["child_rss_peak"] < 320 * MB, sup.stats["child_rss_peak"] // MB
    finally:
        marker.unlink(missing_ok=True)


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


def test_a_provider_cleans_up_after_its_yield():
    """A provider that yields is resumed when its value is done with.

    Per call for `scope="task"` — including a call that raised — and once, as
    the worker drains, for the default `scope="worker"`.
    """
    import tempfile

    log = Path(tempfile.mktemp(suffix=".providers"))
    os.environ["TARSK_PROVIDER_LOG"] = str(log)
    try:
        sup = Supervisor("tests.demo_app:app", children=1)
        out = sup.run([("uses_a_session", (False,), {}), ("uses_a_session", (True,), {}),
                       ("uses_a_client", (), {}), ("uses_a_client", (), {})])
        assert out[0] == ("ack", "session") and out[2] == ("ack", "client"), out
        assert out[1][0] == "nack", out[1]
        lines = log.read_text().split()
        # Opened and closed around each call, the failed one included.
        assert lines[:6] == ["session-open", "handler-session", "session-closed"] * 2, lines
        # Opened once for the process, used twice, and closed at the end.
        assert lines[6:] == ["client-open", "handler-client", "handler-client",
                             "client-closed"], lines
    finally:
        log.unlink(missing_ok=True)
        os.environ.pop("TARSK_PROVIDER_LOG", None)


def test_backoff_is_checked_where_it_is_written():
    app = App()
    for bad in ({"backoff": "linear"}, {"backoff_base": 0}, {"backoff_base": 5, "backoff_max": 1}):
        try:
            app.task(name=f"bad-{len(app.registry)}", **bad)(lambda: None)
        except ValueError:
            pass
        else:
            raise AssertionError(f"{bad} was accepted")
    spec = app.task(name="ok", backoff_base=0.05, backoff_max=2)(lambda: None).spec
    assert (spec.backoff_base_ms, spec.backoff_max_ms) == (50, 2000), spec
    assert spec.as_row()[12:14] == [50, 2000], spec.as_row()


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
    ran, skipped = [], []
    for check in (test_leaky_handler_is_bounded, test_growth_is_charged_to_the_task_that_caused_it,
                  test_recycling_is_overlapped,
                  test_a_recycled_child_loses_no_acks,
                  test_baseline_above_ceiling_is_refused,
                  test_a_task_is_not_charged_for_its_neighbour,
                  test_counting_objects_stays_within_its_budget,
                  test_hard_ceiling_kills_and_dead_letters,
                  test_hard_ceiling_holds_past_the_soft_one,
                  test_a_provider_cleans_up_after_its_yield,
                  test_backoff_is_checked_where_it_is_written,
                  test_hard_ceiling_must_exceed_the_soft_one,
                  test_hooks_bracket_every_worker,
                  test_middleware_wraps_and_dependencies_inject,
                  test_before_send_can_attach_to_the_payload,
                  test_soft_timeout_asks_before_it_takes,
                  test_soft_timeout_is_refused_where_it_cannot_work,
                  test_memory_broker_carries_the_full_record):
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
