"""Redis Streams and Postgres brokers, end to end.

Each backend is checked for the three things a broker has to get right:
delivery, redelivery when a child dies holding a job, and redelivery when the
whole worker disappears and the lease has to expire on its own.

Spins up its own redis-server and Postgres cluster, so it touches nothing you
already have running. Skips a backend whose server binaries are missing.

Run: python tests/test_brokers.py
"""

import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

def _find_pg() -> Path | None:
    """Where initdb lives, on Debian and on Homebrew.

    Only the Debian path was searched, so on macOS this returned None and the
    Postgres test printed "skip" — which reads exactly like a pass in CI output
    and would have hidden the backend being broken there entirely.
    """
    roots = [Path("/usr/lib/postgresql"), Path("/opt/homebrew/opt"), Path("/usr/local/opt")]
    for root in roots:
        if not root.exists():
            continue
        for candidate in sorted(root.glob("*/bin"), reverse=True):
            if (candidate / "initdb").exists():
                return candidate
    found = shutil.which("initdb")
    return Path(found).parent if found else None


PG_BIN = _find_pg()


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Redis:
    def __enter__(self):
        self.dir = tempfile.mkdtemp(prefix="tarsk-redis-")
        self.port = free_port()
        self.proc = subprocess.Popen(
            ["redis-server", "--port", str(self.port), "--save", "", "--appendonly", "no",
             "--dir", self.dir],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(100):
            with socket.socket() as probe:
                if probe.connect_ex(("127.0.0.1", self.port)) == 0:
                    return self
            time.sleep(0.1)
        raise RuntimeError("redis did not come up")

    @property
    def url(self):
        return f"redis://127.0.0.1:{self.port}/0"

    def __exit__(self, *exc):
        self.proc.terminate()
        self.proc.wait()
        shutil.rmtree(self.dir, ignore_errors=True)


class Postgres:
    def __enter__(self):
        self.dir = tempfile.mkdtemp(prefix="tarsk-pg-")
        self.port = free_port()
        env = {**os.environ, "PATH": f"{PG_BIN}:{os.environ['PATH']}"}
        subprocess.run([str(PG_BIN / "initdb"), "-D", f"{self.dir}/data", "-U", "tarsk",
                        "--auth=trust", "-N"], check=True, env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([str(PG_BIN / "pg_ctl"), "-D", f"{self.dir}/data", "-w", "start",
                        "-l", f"{self.dir}/log",
                        # TCP only: a socket path under a temp dir blows the 107-byte limit
                        "-o", f"-p {self.port} -h 127.0.0.1 -k /tmp"],
                       check=True, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run([str(PG_BIN / "createdb"), "-h", "127.0.0.1", "-p", str(self.port),
                        "-U", "tarsk", "tarsk"], check=True, env=env,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return self

    @property
    def url(self):
        return f"postgres://tarsk@127.0.0.1:{self.port}/tarsk"

    def __exit__(self, *exc):
        subprocess.run([str(PG_BIN / "pg_ctl"), "-D", f"{self.dir}/data", "-w", "-m", "immediate",
                        "stop"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.rmtree(self.dir, ignore_errors=True)


def start_worker(env, **overrides):
    return subprocess.Popen(
        [sys.executable, "tests/broker_worker.py"],
        env={**env, **{k: str(v) for k, v in overrides.items()}},
        stdout=subprocess.DEVNULL,
        stderr=(open(os.environ["TARSK_WORKER_ERR"], "a") if os.environ.get("TARSK_WORKER_ERR")
                else subprocess.DEVNULL),
        start_new_session=True,
    )


def stop_worker(proc, hard=False):
    """SIGTERM drains; SIGKILL of the whole group strands every lease it held."""
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL if hard else signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        pass
    proc.wait(timeout=30)


def tags_in(log: Path) -> list[str]:
    if not log.exists():
        return []
    return [line.split("\t")[0] for line in log.read_text().splitlines() if line]


def wait_for(log: Path, needed: set[str], timeout: float) -> list[str]:
    """Wait for specific tags, not a count.

    A count is only right while nothing else is running, and a cron schedule
    ticking in the background is exactly something else running.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        seen = tags_in(log)
        if needed <= set(seen):
            return seen
        time.sleep(0.05)
    return tags_in(log)


class _AnyPid:
    """The pid is whichever child ran it; the point is that the dict crossed."""

    def __eq__(self, other):
        return isinstance(other, int) and other > 0


ANY_PID = _AnyPid()


def scrape_metrics(port: int, timeout: float = 15.0) -> str:
    # A freshly started worker needs a moment to bind; refusing once is normal.
    deadline = time.time() + timeout
    while True:
        try:
            return _scrape_once(port)
        except OSError:
            if time.time() >= deadline:
                raise
            time.sleep(0.1)


def _scrape_once(port: int) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        chunks = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks).decode()


def count_dead(url: str) -> int:
    """How many jobs the backend has parked in its dead-letter store."""
    if url.startswith("redis://"):
        port = url.rsplit(":", 1)[1].split("/")[0]
        out = subprocess.run(["redis-cli", "-p", port, "XLEN", "tarsk:default:dead"],
                             capture_output=True, text=True).stdout.strip()
        return int(out or 0)
    port = url.rsplit(":", 1)[1].split("/")[0]
    out = subprocess.run(["psql", "-h", "127.0.0.1", "-p", port, "-U", "tarsk", "-d", "tarsk",
                          "-tAc", "select count(*) from tarsk_dead"],
                         capture_output=True, text=True).stdout.strip()
    return int(out or 0)


def check_broker(url: str, label: str) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="tarsk-broker-"))
    log = workdir / "run.log"
    log.touch()
    env = {**os.environ, "TARSK_BROKER": url, "TARSK_LOG": str(log), "PYTHONPATH": str(ROOT)}
    for key, value in env.items():
        os.environ[key] = value

    from tarsk import load_app

    # The app module is imported once, so its cached producer still points at
    # whichever broker ran first. Re-point it.
    app = load_app("tests.broker_app:app")
    app.broker = url
    app._producer = None
    try:
        # --- delivery ---------------------------------------------------
        for i in range(6):
            app.registry["note"].send(f"job-{i}")
        app.registry["crash_once"].send("crashy")

        expected = {f"job-{i}" for i in range(6)} | {"crashy"}
        worker = start_worker(env)
        seen = wait_for(log, expected, timeout=60)
        stop_worker(worker)

        assert expected <= set(seen), f"{label}: missing {sorted(expected - set(seen))}"
        # crash_once dies once before it logs, so it must have been redelivered
        assert "crashy" in seen, f"{label}: crashed job never came back"

        # --- lease expiry: the whole worker vanishes mid-task -----------
        log.write_text("")
        app.registry["slow"].send("stranded")
        victim = start_worker(env, TARSK_LEASE_GRACE=1)
        time.sleep(4)  # long enough to have claimed it and be sitting in the handler
        stop_worker(victim, hard=True)
        assert "stranded" not in tags_in(log), f"{label}: the slow job should not have finished"

        # Nobody nacked anything — the lease has to time out by itself.
        app.registry["note"].send("after-expiry")
        rescuer = start_worker(env, TARSK_LEASE_GRACE=1)
        seen = wait_for(log, {"after-expiry", "stranded"}, timeout=90)
        stop_worker(rescuer, hard=True)
        assert "after-expiry" in seen, f"{label}: fresh job never ran"
        assert "stranded" in seen, f"{label}: stranded lease was never reclaimed ({seen})"

        # --- a delayed job waits, then runs -----------------------------
        log.write_text("")
        app.registry["note"].send_in(6, "later")
        app.registry["note"].send("now")
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        early = wait_for(log, {"now"}, timeout=30)
        assert "later" not in early, f"{label}: delayed job jumped the queue ({early})"
        seen = wait_for(log, {"now", "later"}, timeout=60)
        stop_worker(worker)
        assert "later" in seen, f"{label}: delayed job never arrived ({seen})"

        # --- results, for the tasks that asked for them -----------------
        log.write_text("")
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        answer_id = app.registry["answers"].send(2, 3)
        boom_id = app.registry["explodes"].send()
        forgotten_id = app.registry["note"].send("unstored")

        from tarsk import TaskFailed

        assert app.result(answer_id).get(timeout=60) == {"sum": 5, "pid": ANY_PID}, label
        try:
            app.result(boom_id).get(timeout=60)
        except TaskFailed as exc:
            assert exc.error_type == "ValueError", exc.error_type
            assert "always going to" in exc.traceback, exc.traceback
        else:
            raise AssertionError(f"{label}: a failed task must raise, not return")
        # note has no result_ttl, so nothing was written for it
        assert app.result(forgotten_id).ready() is False, f"{label}: stored an unasked result"

        # The async pair has to leave the event loop free, which is the whole
        # reason it exists. Measured as the longest a neighbouring coroutine
        # went without running, while awaiting a task that takes a second: if
        # either call blocked, that gap is the length of the block.
        #
        # Counting the coroutine's turns instead — which this did first — fails
        # on a fast machine for the opposite reason, because work that finishes
        # before the loop comes round twice looks exactly like work that wedged
        # it. A free-threaded build reported one turn and was simply quick.
        import asyncio

        async def check_async_path():
            gaps = []
            stop = asyncio.Event()

            async def ticker():
                last = time.perf_counter()
                while not stop.is_set():
                    await asyncio.sleep(0.001)
                    now = time.perf_counter()
                    gaps.append(now - last)
                    last = now

            spin = asyncio.create_task(ticker())
            await asyncio.sleep(0.05)
            gaps.clear()
            job = await app.registry["unhurried"].send_async("async")
            answer = await app.result(job).get_async(timeout=60)
            stop.set()
            await spin
            return answer, max(gaps) if gaps else 99.0

        answer, worst = asyncio.run(check_async_path())
        assert answer == "async", f"{label}: {answer!r}"
        # The handler alone is a second. Anything approaching that means the
        # loop sat inside a call instead of around it.
        assert worst < 0.3, f"{label}: the loop stalled for {worst:.2f}s"

        stop_worker(worker)

        # --- per-call overrides: a different queue, an absolute time ----
        import datetime

        log.write_text("")
        note = app.registry["note"]
        fixed = note.options(task_id="chosen-by-the-caller").send("by-id")
        assert fixed == "chosen-by-the-caller", fixed
        soon = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=5)
        note.send_at(soon, "absolute")
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        early = wait_for(log, {"by-id"}, timeout=30)
        assert "absolute" not in early, f"{label}: send_at jumped the queue ({early})"
        seen = wait_for(log, {"by-id", "absolute"}, timeout=60)
        stop_worker(worker)
        assert "absolute" in seen, f"{label}: send_at never arrived ({seen})"

        # --- progress, published from inside a running task -------------
        log.write_text("")
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        job = app.registry["reports"].send()
        handle = app.result(job)
        seen_progress = []
        deadline = time.time() + 60
        while time.time() < deadline and not handle.ready():
            step = handle.progress()
            if step and step not in seen_progress:
                seen_progress.append(step)
            time.sleep(0.05)
        assert handle.get(timeout=30) == "finished", label
        stop_worker(worker)
        assert seen_progress, f"{label}: no progress was ever visible"
        assert seen_progress == sorted(seen_progress, key=lambda s: s["step"]), seen_progress
        assert seen_progress[-1]["of"] == 3, seen_progress

        # --- cron fires once a minute, once across the fleet ------------
        log.write_text("")
        # Two workers, so a schedule fired twice would show up as two ticks.
        pair = [start_worker(env, TARSK_LEASE_GRACE=1) for _ in range(2)]
        seen = wait_for(log, {"tick"}, timeout=150)
        time.sleep(3)  # let a duplicate arrive if one is going to
        ticks = [t for t in tags_in(log) if t == "tick"]
        for worker in pair:
            stop_worker(worker)
        assert ticks == ["tick"], f"{label}: expected exactly one tick, got {ticks}"

        # --- identical sends collapse into one job ------------------------
        log.write_text("")
        first = [app.registry["once"].send("dup") for _ in range(4)]
        assert len(set(first)) == 1, f"{label}: four identical sends gave {set(first)}"
        # A different argument is a different job, not a collision.
        app.registry["once"].send("other")
        # An explicit key overrides the argument hash, so these two collapse
        # even though their arguments differ.
        keyed = [
            app.registry["once"].options(dedup_key="k", dedup_ttl=30).send(f"keyed-{i}")
            for i in range(3)
        ]
        assert len(set(keyed)) == 1, f"{label}: explicit keys gave {set(keyed)}"

        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and len(tags_in(log)) < 3:
                time.sleep(0.2)
            time.sleep(0.5)
        finally:
            stop_worker(worker)
        ran = sorted(tags_in(log))
        assert ran == ["once-dup", "once-keyed-0", "once-other"], f"{label}: {ran}"

        # --- a concurrency cap holds across workers, not per worker -------
        #
        # Four children with four slots each is sixteen places a job could run.
        # The cap says two, and the proof is overlap: each job logs its own
        # start and end, so the deepest nesting is measured rather than inferred
        # from how long the whole thing took.
        log.write_text("")
        for i in range(8):
            app.registry["capped"].send(f"c{i}")
        worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_CHILDREN=4, TARSK_SLOTS=4)
        try:
            deadline = time.time() + 60
            while time.time() < deadline and \
                    len([t for t in tags_in(log) if t.startswith("end-")]) < 8:
                time.sleep(0.2)
        finally:
            stop_worker(worker)
        live = peak = 0
        for tag in tags_in(log):
            if tag.startswith("start-"):
                live += 1
                peak = max(peak, live)
            elif tag.startswith("end-"):
                live -= 1
        finished = len([t for t in tags_in(log) if t.startswith("end-")])
        assert finished == 8, f"{label}: only {finished}/8 capped jobs finished"
        assert peak <= 2, f"{label}: max_concurrency=2 but {peak} ran at once"
        assert peak == 2, f"{label}: never reached the cap ({peak}) — is anything parallel?"

        # --- the types people actually pass survive the round trip --------
        #
        # Through the broker, the socket and a child interpreter, not just
        # through the codec in one process: msgpack carries these as extension
        # types, and the chain splicer in Rust decodes and re-encodes payloads
        # on the way past.
        import datetime as _dt
        import decimal as _dec
        import uuid as _uuid

        log.write_text("")
        sent = {
            "when": _dt.datetime(2026, 8, 14, 7, 30, tzinfo=_dt.timezone.utc),
            "day": _dt.date(2026, 8, 14),
            "wait": _dt.timedelta(days=1, microseconds=7),
            "amount": _dec.Decimal("1.50"),
            "who": _uuid.UUID(int=99),
            "tags": {"a", "b"},
        }
        echo_id = app.registry["echoes"].send(sent)
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            back = app.result(echo_id).get(timeout=60)
        finally:
            stop_worker(worker)
        assert back == sent, f"{label}: round trip changed the value: {back!r}"
        for key, value in sent.items():
            assert type(back[key]) is type(value), (
                f"{label}: {key} came back as {type(back[key]).__name__}, "
                f"sent {type(value).__name__}"
            )
        # A Decimal that returned as a float would be the exact bug Decimal exists to avoid.
        assert str(back["amount"]) == "1.50", f"{label}: {back['amount']!r}"

        # And through the chain splicer, which is the part written in Rust: it
        # decodes a payload, inserts the previous result and re-encodes, so an
        # extension type it did not understand would be lost right there.
        from tarsk import chain as _chain

        piped = _chain(
            app.registry["echoes"].s(sent["when"]),
            app.registry["echoes"].s(),
        )
        piped_id = piped.send()
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            spliced = app.result(piped_id).get(timeout=60)
        finally:
            stop_worker(worker)
        assert spliced == sent["when"], f"{label}: the splicer changed it: {spliced!r}"
        assert type(spliced) is _dt.datetime, f"{label}: came back {type(spliced).__name__}"

        # --- the dead letters stop growing at the cap ---------------------
        #
        # Everything else here expires on its own; this one grew forever, one
        # payload and one full traceback per row. Twelve failures against a cap
        # of three must leave three.
        log.write_text("")
        from tarsk._core import Producer

        producer = Producer(broker_url=url)
        producer.dead_purge(queue="default")
        worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_MAX_DEAD=3)
        try:
            for i in range(12):
                app.registry["always_fails"].send(f"doomed-{i}")
            deadline = time.time() + 90
            while time.time() < deadline and len(producer.dead_list(queue="default", limit=50)) < 3:
                time.sleep(0.3)
            time.sleep(2.0)                       # let the rest arrive and be trimmed
            kept = producer.dead_list(queue="default", limit=50)
        finally:
            stop_worker(worker)
        # Redis trims approximately — it drops whole nodes — so the cap is a
        # bound rather than an exact count. What matters is that twelve
        # failures did not leave twelve rows.
        assert 0 < len(kept) <= 8, f"{label}: cap of 3 left {len(kept)} dead letters"
        assert len(kept) < 12, f"{label}: nothing was trimmed"

        # --- what the sender attached reaches the handler and the listing --
        #
        # Carried beside the arguments, not inside them: the listing reads it
        # without unpacking a call it does not understand, and a handler that
        # never declared a parameter for it still gets it through Context.
        log.write_text("")
        from tarsk import _proto

        tagged = app.registry["labelled"].options(
            meta={"trace": "abc123", "tenant": 7}
        ).send("t")
        rows = {r[0]: r for r in producer.jobs(["default"], 50)}
        assert tagged in rows, f"{label}: the tagged job is not listed"
        assert _proto.unpack_result(rows[tagged][7]) == {"trace": "abc123", "tenant": 7}, \
            f"{label}: listing lost the meta: {rows[tagged][7]!r}"

        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            answer = app.result(tagged).get(timeout=60)
        finally:
            stop_worker(worker)
        assert answer == {"trace": "abc123", "tenant": 7}, f"{label}: handler saw {answer!r}"
        assert "labelled-abc123" in tags_in(log), f"{label}: {tags_in(log)}"

        # --- the individual jobs are visible, not only the count ----------
        #
        # The depth counters say how far behind; this says behind on what. A
        # delayed job is listed too, and reports time remaining rather than
        # time waited — it has not started waiting yet.
        log.write_text("")
        soon = app.registry["note"].send("listed-now")
        later = app.registry["note"].options(delay=600).send("listed-later")
        listed = {row[0]: row for row in producer.jobs(["default"], 50)}
        assert soon in listed, f"{label}: a queued job is not in the listing"
        assert later in listed, f"{label}: a delayed job is not in the listing"
        assert listed[soon][2] == "note", f"{label}: wrong name {listed[soon][2]!r}"
        assert listed[soon][3] == "ready", f"{label}: {listed[soon][3]!r}"
        assert listed[later][3] == "delayed", f"{label}: {listed[later][3]!r}"
        # Ten minutes out, so the age is negative by roughly that much.
        assert listed[later][5] < -500_000, f"{label}: due-in looks wrong: {listed[later][5]}"
        # The name has to survive: the payload beside it is msgpack, and
        # reading the whole record as text used to blank every field.
        assert listed[later][2] == "note", f"{label}: delayed name {listed[later][2]!r}"

        app.cancel(later)
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and "listed-now" not in tags_in(log):
                time.sleep(0.2)
        finally:
            stop_worker(worker)

        # --- the backlog is visible before, during and after -------------
        log.write_text("")

        def depth():
            rows = producer.depth(["default"])
            return rows[0][1:] if rows else (0, 0, 0, 0)

        before = depth()
        for i in range(4):
            app.registry["note"].send(f"depth-{i}")
        ready, running, delayed, _dead = depth()
        assert ready == before[0] + 4, f"{label}: four sent, ready went {before[0]} → {ready}"
        assert delayed == before[2], f"{label}: nothing was delayed, but delayed={delayed}"
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and len(tags_in(log)) < 4:
                time.sleep(0.2)
        finally:
            stop_worker(worker)
        ready, running, _delayed, _dead = depth()
        assert (ready, running) == (0, 0), \
            f"{label}: queue drained but reports ready={ready} running={running}"

        # --- a chain runs in order and feeds each result to the next ------
        log.write_text("")
        from tarsk import chain

        pipeline = chain(
            app.registry["double"].s(5),      # 10
            app.registry["add_to"].s(3),      # 10 + 3 = 13
            app.registry["shout"].si("done"), # ignores 13, returns "DONE"
        )
        final_id = pipeline.send()
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            answer = app.result(final_id).get(timeout=60)
        finally:
            stop_worker(worker)
        assert answer == "DONE", f"{label}: chain ended with {answer!r}"
        ran = tags_in(log)
        assert ran == ["double-5", "add_to-10+3", "shout-done"], f"{label}: {ran}"

        # --- a group hands back every id before any of them has run -------
        log.write_text("")
        from tarsk import group

        fan = group(app.registry["double"].s(n) for n in (1, 2, 3))
        ids = fan.send()
        assert len(set(ids)) == 3, f"{label}: group reused an id: {ids}"
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            answers = sorted(h.get(timeout=60) for h in fan.results(app))
        finally:
            stop_worker(worker)
        assert answers == [2, 4, 6], f"{label}: group returned {answers}"

        # --- a job that waited too long is dropped, not run ---------------
        #
        # Enqueued with no worker running, so the wait is real queue time
        # rather than something contrived. Both jobs are identical; only the
        # gap before the worker starts differs.
        log.write_text("")
        app.registry["perishable"].send("stale")
        time.sleep(3.0)                      # expires is 2s
        app.registry["perishable"].send("fresh")
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and "fresh" not in tags_in(log):
                time.sleep(0.2)
            time.sleep(1.0)                  # give the stale one every chance
        finally:
            stop_worker(worker)
        ran = tags_in(log)
        assert "fresh" in ran, f"{label}: the fresh job did not run: {ran}"
        assert "stale" not in ran, f"{label}: a job three seconds past expires=2 still ran"

        # --- expires per send, not only per registration ------------------
        #
        # `durable` registers none, so anything dropped here was dropped by
        # what the caller asked for. Three sends, one worker start: the only
        # difference between them is the deadline each one carried.
        log.write_text("")
        app.registry["durable"].options(expires=1).send("asked-and-stale")
        app.registry["durable"].send("no-deadline")
        app.registry["durable"].options(expires=60).send("asked-and-fresh")
        time.sleep(2.5)                      # past the 1s, nowhere near the 60s
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and "asked-and-fresh" not in tags_in(log):
                time.sleep(0.2)
            time.sleep(1.0)                  # give the dropped one every chance
        finally:
            stop_worker(worker)
        ran = tags_in(log)
        assert "asked-and-fresh" in ran, f"{label}: expires=60 did not survive 2.5s: {ran}"
        # A task with no registered expiry still runs when nobody asked for one.
        assert "no-deadline" in ran, f"{label}: a job with no deadline was dropped: {ran}"
        assert "asked-and-stale" not in ran, (
            f"{label}: expires=1 on the send did not drop a job 2.5s old: {ran}"
        )

        # --- --queues high,low is a priority, not a preference ------------
        #
        # Twenty low jobs are queued first and given a head start, then five
        # high ones. A worker that merely prefers the first queue within one
        # read drains the low batch it already fetched; a strict one runs
        # every high job before any low job it has not yet claimed.
        #
        # The assertion is on the *last* high job's position, not the first:
        # one high job jumping the queue proves nothing if the other four
        # trail behind the backlog.
        log.write_text("")
        for n in range(20):
            app.registry["whenever"].send(f"low{n:02d}")
        time.sleep(0.5)                      # let them settle in the stream
        for n in range(5):
            app.registry["urgent"].send(f"high{n}")
        worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_QUEUES="high,low")
        try:
            deadline = time.time() + 60
            while time.time() < deadline and len(tags_in(log)) < 25:
                time.sleep(0.2)
        finally:
            stop_worker(worker)
        ran = tags_in(log)
        assert len(ran) == 25, f"{label}: only {len(ran)} of 25 ran: {ran}"
        order = [t for t in log.read_text().splitlines() if t]
        highs = [i for i, t in enumerate(order) if t.startswith("high")]
        lows = [i for i, t in enumerate(order) if t.startswith("low")]
        assert len(highs) == 5, f"{label}: {len(highs)} high jobs: {order}"
        # Every high job before every low one. Both queues had work when the
        # worker started, so nothing was claimed before the priority applied
        # and there is no batch of slack to allow for. A worker that merely
        # prefers within one read interleaves them instead.
        assert max(highs) < min(lows), (
            f"{label}: high jobs did not all precede low ones, last high at "
            f"{max(highs)} with lows from {min(lows)}: {order}"
        )

        # --- a chain against an idle high-slot worker is not paced by hz --
        #
        # A nonblocking claim used to send `XREADGROUP … BLOCK 1`, and Redis
        # rounds every BLOCK timeout up to its serverCron sweep (`hz`, default
        # 10 — so ~100ms). A worker with N slots pays that stall once per
        # Ready, serially, and the Ack of a chain's first step queues behind
        # all of them: at 200 slots the second step dispatched ~20 seconds
        # after the first finished, which read as a hang. The worker starts
        # first and goes idle on purpose — that is the shape that stalls.
        #
        # Teeth, measured on the reverted fix: 200 slots stall this exact
        # shape for 9.9 seconds, so the 6-second budget fails it. With the
        # fix the same chain answers in well under a second.
        log.write_text("")
        worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_SLOTS=200)
        try:
            time.sleep(3.0)  # idle: the Ready burst must be in flight first
            paced = chain(
                app.registry["double"].s(4),      # 8
                app.registry["add_to"].s(1),      # 9
            )
            answer = app.result(paced.send()).get(timeout=6)
        finally:
            stop_worker(worker)
        assert answer == 9, f"{label}: paced chain returned {answer!r}"

        # --- a delayed send keeps what was attached to it -----------------
        #
        # Delayed jobs wait in a hash beside the stream rather than in it, and
        # every field has to be written there by hand. `meta` was not, so it
        # arrived empty after the sweep promoted the job — the kind of gap that
        # only shows up when something reads it on the far side.
        log.write_text("")
        app.registry["carries_meta"].options(delay=1, meta={"trace": "kept"}).send()
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            deadline = time.time() + 30
            while time.time() < deadline and not tags_in(log):
                time.sleep(0.2)
        finally:
            stop_worker(worker)
        assert "meta-kept" in tags_in(log), (
            f"{label}: a delayed send lost its meta: {tags_in(log)}"
        )

        # --- a rate limit holds across workers, not per worker ------------
        #
        # Four children with eight slots each is thirty-two things that could
        # run at once. Celery's rate_limit is per worker and would let each of
        # them have the full allowance; the point of putting the bucket in the
        # broker is that they share one.
        #
        # Timed rather than counted, and drained rather than cut off: fifteen
        # tasks at five a second cannot finish sooner than two seconds however
        # many slots are free, and leaving leftovers in the queue would show up
        # as noise in whichever phase runs next.
        log.write_text("")
        for i in range(15):
            app.registry["metered"].send(f"metered-{i}")
        worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_CHILDREN=4, TARSK_SLOTS=8)
        started = time.time()
        try:
            deadline = time.time() + 60
            while time.time() < deadline and len(tags_in(log)) < 15:
                time.sleep(0.1)
        finally:
            stop_worker(worker)
        took = time.time() - started
        assert len(tags_in(log)) == 15, f"{label}: only {len(tags_in(log))}/15 ever ran"
        # Burst of five, then five a second: ten more take two seconds. Without
        # a limit, thirty-two slots would have this done in well under one.
        assert took >= 1.5, f"{label}: 15 tasks at 5/s finished in {took:.2f}s"

        # --- a queued job can be cancelled while the worker is running ----
        #
        # Ordering matters: the cancellation is sent *after* the worker is up
        # and already chewing through the queue, which is when a caller would
        # actually send it. Cancelling before the worker starts would test the
        # easy half.
        log.write_text("")
        first = app.registry["medium"].send("running")   # occupies the child
        doomed = [app.registry["note"].send(f"cancelled-{i}") for i in range(5)]
        alive = [app.registry["note"].send(f"kept-{i}") for i in range(5)]
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        try:
            time.sleep(1.0)                              # worker is on `medium`
            for job_id in doomed:
                app.cancel(job_id)
            deadline = time.time() + 40
            while time.time() < deadline and len(tags_in(log)) < 6:
                time.sleep(0.2)
        finally:
            stop_worker(worker)
        ran = tags_in(log)
        assert not [t for t in ran if t.startswith("cancelled-")], \
            f"{label}: cancelled jobs ran anyway: {ran}"
        assert len([t for t in ran if t.startswith("kept-")]) == 5, \
            f"{label}: cancelling took uncancelled jobs with it: {ran}"
        assert "running" in ran, f"{label}: the in-flight job did not finish: {ran}"
        del first, alive

        # --- retries run out, job is dead-lettered ----------------------
        log.write_text("")
        # Count the delta: earlier phases have already parked a job in there.
        dead_before = count_dead(url)
        app.registry["always_fails"].send("doomed")
        metrics_port = free_port()
        worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_METRICS=f"127.0.0.1:{metrics_port}")
        deadline = time.time() + 60
        while time.time() < deadline and count_dead(url) <= dead_before:
            time.sleep(0.2)
        attempts = len(tags_in(log))
        scrape = scrape_metrics(metrics_port)
        stop_worker(worker)
        assert attempts == 2, f"{label}: retries=1 should run it twice, ran {attempts}"
        assert count_dead(url) == dead_before + 1, f"{label}: nothing reached the dead-letter store"
        for line in ("tarsk_tasks_total{task=\"always_fails\",outcome=\"failed\"} 2",
                     "tarsk_tasks_dead_lettered_total 1",
                     "tarsk_task_duration_seconds_count 2",
                     "tarsk_supervisor_rss_bytes"):
            assert line in scrape, f"{label}: /metrics missing {line!r}\n{scrape}"

        # --- the dead letters can be read, replayed and dropped -----------
        from tarsk._core import Producer

        producer = Producer(broker_url=url)
        parked = producer.dead_list(queue="default", limit=50)
        assert len(parked) == dead_before + 1, f"{label}: dead_list saw {len(parked)}"
        entry_id, name, error, traceback, died_ms = parked[-1]
        assert name == "always_fails", f"{label}: wrong name in dead_list: {name!r}"
        assert "doomed never works" in traceback, \
            f"{label}: traceback did not survive: {traceback!r}"
        assert died_ms > 0, f"{label}: no timestamp on the dead letter"

        # Replaying puts the work back where a worker will find it. Without the
        # payload and timeout stored alongside, this is where it would fail:
        # the job would reappear unrunnable rather than not reappear at all.
        log.write_text("")
        moved = producer.dead_replay(queue="default", ids=[entry_id])
        assert moved == 1, f"{label}: replayed {moved}"
        assert not any(e[0] == entry_id for e in producer.dead_list(queue="default", limit=50)), \
            f"{label}: replayed entry is still in the dead letters"

        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        deadline = time.time() + 60
        while time.time() < deadline and not tags_in(log):
            time.sleep(0.2)
        stop_worker(worker)
        assert tags_in(log), f"{label}: a replayed job never ran"

        producer.dead_replay(queue="default")  # everything else, so purge has work
        gone = producer.dead_purge(queue="default")
        assert producer.dead_list(queue="default", limit=50) == [], \
            f"{label}: purge left {gone} behind"
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_redis():
    if not shutil.which("redis-server"):
        print("skip test_redis (no redis-server)")
        return
    with Redis() as redis:
        check_broker(redis.url, "redis")


def test_postgres():
    if PG_BIN is None:
        print("skip test_postgres (no postgres binaries)")
        return
    with Postgres() as pg:
        check_broker(pg.url, "postgres")


def test_postgres_tls_is_compiled_in():
    """A Postgres URL asking for TLS must not be refused for lack of support.

    The connector is always supplied, so sslmode decides. Before it was, this
    failed with "error performing TLS handshake" against every managed Postgres
    there is — loudly rather than silently, but no less unusable.
    """
    from tarsk._core import Producer

    for mode in ("require", "verify-full"):
        try:
            Producer(broker_url=f"postgres://u:p@127.0.0.1:1/db?sslmode={mode}")
        except ValueError as exc:
            text = str(exc)
            assert "invalid connection string" not in text, f"{mode} rejected outright: {text}"
            assert "TLS handshake" not in text, f"{mode} has no TLS support: {text}"
        else:
            raise AssertionError("connecting to a dead port should not succeed")


def test_tls_is_compiled_in():
    """rediss:// must fail like a connection, not like a missing feature.

    Two things break silently here and only when someone actually uses TLS:
    dropping the redis TLS feature from Cargo.toml, and leaving rustls without
    a crypto provider — the second aborts the process rather than raising.
    Neither shows up in any test that talks plaintext.
    """
    from tarsk._core import Producer

    # Port 1 is reserved and nothing listens there, so a TLS handshake cannot
    # get far enough to succeed for the wrong reason.
    try:
        Producer(broker_url="rediss://127.0.0.1:1/0")
    except ValueError as exc:
        assert "feature is not enabled" not in str(exc), "redis built without TLS"
        assert "CryptoProvider" not in str(exc), "rustls has no crypto provider selected"
    else:
        raise AssertionError("connecting to a dead port should not succeed")


if __name__ == "__main__":
    for check in (test_redis, test_postgres, test_tls_is_compiled_in,
                  test_postgres_tls_is_compiled_in):
        check()
        print("ok", check.__name__)
