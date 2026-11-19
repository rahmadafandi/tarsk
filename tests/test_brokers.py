"""Redis Streams and Postgres brokers, end to end.

Each backend is checked for the three things a broker has to get right:
delivery, redelivery when a child dies holding a job, and redelivery when the
whole worker disappears and the lease has to expire on its own.

Spins up its own redis-server and Postgres cluster, so it touches nothing you
already have running. Skips a backend whose server binaries are missing —
unless TARSK_REDIS_URL, TARSK_PG_URL or TARSK_AMQP_URL names one to use, which
is how a machine without the binaries still tests the backend rather than
printing a skip that reads like a pass.

Run: python tests/test_brokers.py
"""

import os
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from unittest import SkipTest
from urllib.parse import urlsplit


def scheme_is(url: str, schemes: tuple[str, ...]) -> bool:
    return url.startswith(schemes)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from tarsk import BrokerError  # noqa: E402

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
    """A private redis-server, or TARSK_REDIS_URL when one is named.

    The override mirrors TARSK_AMQP_URL and exists for the same reason: a
    machine with no redis-server binary but a Redis to point at would
    otherwise skip the backend, and a skip is not a pass. Give it a database
    of its own — the tests write real keys and delete only their own.
    """

    def __enter__(self):
        self.external = os.environ.get("TARSK_REDIS_URL")
        if self.external:
            return self
        # Here rather than in the tests: two of them need a server, and a
        # missing binary has to skip both instead of crashing the second.
        if not shutil.which("redis-server"):
            raise SkipTest("no redis-server; set TARSK_REDIS_URL")
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
        return self.external or f"redis://127.0.0.1:{self.port}/0"

    def __exit__(self, *exc):
        if self.external:
            return
        self.proc.terminate()
        self.proc.wait()
        shutil.rmtree(self.dir, ignore_errors=True)


class Postgres:
    """A private cluster, or TARSK_PG_URL when one is named. See Redis above."""

    def __enter__(self):
        self.external = os.environ.get("TARSK_PG_URL")
        if self.external:
            return self
        if PG_BIN is None:
            raise SkipTest("no postgres binaries; set TARSK_PG_URL")
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
        return self.external or f"postgres://tarsk@127.0.0.1:{self.port}/tarsk"

    def __exit__(self, *exc):
        if self.external:
            return
        subprocess.run([str(PG_BIN / "pg_ctl"), "-D", f"{self.dir}/data", "-w", "-m", "immediate",
                        "stop"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        shutil.rmtree(self.dir, ignore_errors=True)


def start_worker(env, stderr=None, **overrides):
    """`stderr` is a path to keep what the worker prints, for a test that reads it."""
    sink = stderr or os.environ.get("TARSK_WORKER_ERR")
    worker = subprocess.Popen(
        [sys.executable, "tests/broker_worker.py"],
        env={**env, **{k: str(v) for k, v in overrides.items()}},
        stdout=subprocess.DEVNULL,
        stderr=open(sink, "a") if sink else subprocess.DEVNULL,
        start_new_session=True,
    )
    _started.append(worker)
    return worker


# Every worker this run has started, so one a failed block left behind can be
# found and stopped before it eats the next block's jobs.
_started: list = []


def _stop_stray_workers() -> None:
    for worker in _started:
        if worker.poll() is None:
            try:
                os.killpg(os.getpgid(worker.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            worker.wait(timeout=30)
    _started.clear()


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


def _resp(args) -> bytes:
    out = f"*{len(args)}\r\n".encode()
    for arg in args:
        raw = str(arg).encode()
        out += b"$%d\r\n%s\r\n" % (len(raw), raw)
    return out


def _redis_xlen(url: str, key: str) -> int:
    """XLEN, spoken to the server directly.

    Three lines of RESP rather than a redis-cli subprocess: it needs no client
    installed, and it reads whichever database the URL names — a shared server
    with the tests on db 4 was counted on db 0 before.
    """
    parts = urlsplit(url)
    commands = []
    if parts.password:
        commands.append(("AUTH", *[p for p in (parts.username, parts.password) if p]))
    commands.append(("SELECT", parts.path.lstrip("/") or "0"))
    commands.append(("XLEN", key))
    with socket.create_connection((parts.hostname or "127.0.0.1", parts.port or 6379), 10) as sock:
        sock.sendall(b"".join(_resp(c) for c in commands))
        buf = b""
        while buf.count(b"\r\n") < len(commands):
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
    reply = buf.split(b"\r\n")[len(commands) - 1]
    assert reply.startswith(b":"), f"XLEN {key} said {reply!r}"
    return int(reply[1:])


_told_you_once: set[str] = set()
_producers: dict = {}


def _producer_for(url: str):
    """One producer per URL, kept: count_dead runs in a polling loop, and a
    fresh connection every 200ms is how a backend answers "db error"."""
    if url not in _producers:
        from tarsk._core import Producer

        _producers[url] = Producer(broker_url=url)
    return _producers[url]


def count_dead(url: str) -> int:
    """How many jobs the backend has parked in its dead-letter store.

    Read from the store itself, not through `dead_list`: block 26 asserts one
    against the other, and a count taken through the code under test would
    make that assertion true by construction. AMQP has no store to query, and
    a Postgres needs psql on the machine — where the independent count is not
    available this says so rather than quietly becoming a tautology.
    """
    if scheme_is(url, ("redis://", "rediss://")):
        return _redis_xlen(url, "tarsk:default:dead")
    if scheme_is(url, ("postgres://", "postgresql://")) and shutil.which("psql"):
        # The whole URL: it carries the database, the user and the password,
        # which a rebuilt host/port pair did not.
        out = subprocess.run(["psql", url, "-tAc", "select count(*) from tarsk_dead"],
                             capture_output=True, text=True).stdout.strip()
        return int(out or 0)
    if not scheme_is(url, ("amqp://", "amqps://")) and url not in _told_you_once:
        _told_you_once.add(url)
        print("  note: no psql here, so the dead-letter count comes from dead_list "
              "and block 26 cross-checks it against itself")
    return len(_producer_for(url).dead_list("default", 100_000))


def _b01_delivery(url, label, skip, app, env, log, producer):
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


def _b02_lease_expiry_the_whole(url, label, skip, app, env, log, producer):
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


def _b03_a_delayed_job_waits(url, label, skip, app, env, log, producer):
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


def _b04_results_for_the_tasks(url, label, skip, app, env, log, producer):
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


def _b05_per_call_overrides_a(url, label, skip, app, env, log, producer):
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


def _b06_progress_published_from_inside(url, label, skip, app, env, log, producer):
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
    # One more read once the loop has stopped. It stops on ready(), so a report
    # published between the last poll and the result is never looked at
    # otherwise — and asserting on the final report would then be a race the
    # poller loses on a slow runner rather than anything about the feature.
    step = handle.progress()
    if step and step not in seen_progress:
        seen_progress.append(step)
    assert handle.get(timeout=30) == "finished", label
    stop_worker(worker)
    assert seen_progress, f"{label}: no progress was ever visible"
    assert seen_progress == sorted(seen_progress, key=lambda s: s["step"]), seen_progress
    assert seen_progress[-1]["of"] == 3, seen_progress


def _b07_cron_election(url, label, skip, app, env, log, producer):
    if "cron-election" in skip:
        print(f"  {label}: skip cron-election — cron election is per-worker here: no shared store to elect through, so two workers would both fire")
    else:
        # --- cron fires once a minute, once across the fleet ------------
        log.write_text("")
        # Two workers, so a schedule fired twice would show up as two ticks.
        pair = [start_worker(env, TARSK_LEASE_GRACE=1) for _ in range(2)]
        wait_for(log, {"tick"}, timeout=150)
        time.sleep(3)  # let a duplicate arrive if one is going to
        ticks = [t for t in tags_in(log) if t == "tick"]
        for worker in pair:
            stop_worker(worker)
        assert ticks == ["tick"], f"{label}: expected exactly one tick, got {ticks}"


def _b08_dedup(url, label, skip, app, env, log, producer):
    if "dedup" in skip:
        print(f"  {label}: skip dedup — send dedup needs a shared SET-NX; producers on this broker cannot see each other's sends")
    else:
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


def _b09_shared_concurrency(url, label, skip, app, env, log, producer):
    if "shared-concurrency" in skip:
        print(f"  {label}: skip shared-concurrency — max_concurrency is held per worker here, Celery's shape — the cross-worker half of this test cannot hold")
    else:
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


def _b10_the_types_people_actually(url, label, skip, app, env, log, producer):
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


def _b11_the_dead_letters_stop(url, label, skip, app, env, log, producer):
    # --- the dead letters stop growing at the cap ---------------------
    #
    # Everything else here expires on its own; this one grew forever, one
    # payload and one full traceback per row. Twelve failures against a cap
    # of three must leave three.
    log.write_text("")

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


def _b12_what_the_sender_attached(url, label, skip, app, env, log, producer):
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


def _b13_listing_and_cancel(url, label, skip, app, env, log, producer):
    if "listing-and-cancel" in skip:
        print(f"  {label}: skip listing-and-cancel — delayed jobs live in TTL queues this broker cannot enumerate, and the block ends by cancelling one")
    else:
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


def _b14_the_backlog_is_visible(url, label, skip, app, env, log, producer):
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


def _b15_a_chain_runs_in(url, label, skip, app, env, log, producer):
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


def _b16_a_group_hands_back(url, label, skip, app, env, log, producer):
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


def _b17_a_job_that_waited(url, label, skip, app, env, log, producer):
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


def _b18_expires_per_send_not(url, label, skip, app, env, log, producer):
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


def _b19_queues_high_low_is(url, label, skip, app, env, log, producer):
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


def _b20_a_chain_against_an(url, label, skip, app, env, log, producer):
    from tarsk import chain
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


def _b21_set_progress_works_from(url, label, skip, app, env, log, producer):
    # --- set_progress works from both kinds of handler ----------------
    #
    # From an async handler it used to block the loop thread on a frame
    # the blocked loop could never send — freezing every slot in the
    # child, not just the task that called it. From a sync handler it
    # must keep blocking its executor thread, so progress is on the wire
    # before the work continues.
    log.write_text("")
    worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_SLOTS=8)
    try:
        for name, expect in (("async_progress", "done-async"),
                             ("sync_progress", "done-sync")):
            rid = app.registry[name].send()
            handle = app.result(rid)
            seen = None
            deadline = time.time() + 20
            while time.time() < deadline and not handle.ready():
                seen = handle.progress() or seen
                time.sleep(0.05)
            # The loop stops on ready(), so the last report can land after the
            # final poll. See the same read in the earlier progress block.
            seen = handle.progress() or seen
            assert handle.get(timeout=10) == expect, f"{label}: {name} result"
            assert seen == 2, (
                f"{label}: {name} progress read {seen!r}, wanted the last report"
            )
    finally:
        stop_worker(worker)


def _b22_a_delayed_send_keeps(url, label, skip, app, env, log, producer):
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


def _b23_a_rate_limit_holds(url, label, skip, app, env, log, producer):
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


def _b24_cancel(url, label, skip, app, env, log, producer):
    if "cancel" in skip:
        print(f"  {label}: skip cancel — cancellation needs state every worker can read; this broker has no shared store to hold it")
    else:
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


def _b25_retries_run_out_job(url, label, skip, app, env, log, producer):
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


def _b26_the_dead_letters_can(url, label, skip, app, env, log, producer):
    # --- the dead letters can be read, replayed and dropped -----------

    # dead_list must see everything the store actually holds — asserted
    # against the store's own count rather than a baseline smuggled over
    # from the previous block, which is what this used to do.
    parked = producer.dead_list(queue="default", limit=50)
    assert len(parked) == count_dead(url), f"{label}: dead_list saw {len(parked)}"
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


def _b27_the_handler_knows_its_id(url, label, skip, app, env, log, producer):
    # --- ctx.task_id is the id send() returned ------------------------
    # It was the supervisor's own dispatch counter, so a trace begun in
    # before_send had nothing to join on in the handler.
    sent = app.registry["knows_itself"].send()
    worker = start_worker(env)
    try:
        seen = app.result(sent).get(timeout=60)
    finally:
        stop_worker(worker)
    assert seen == sent, f"{label}: sent {sent!r}, the handler saw {seen!r}"


def _b28_stop_means_stop(url, label, skip, app, env, log, producer):
    # --- SIGTERM with a backlog ends the worker, not the backlog -------
    # The supervisor alone gets the signal, as an orchestrator sends it. It
    # used to keep claiming for as long as the queue had anything in it.
    log.write_text("")
    tags = {f"backlog-{i}" for i in range(60)}
    for tag in sorted(tags):
        app.registry["brief"].send(tag)
    worker = start_worker(env)
    try:
        wait_for(log, {"backlog-0"}, timeout=60)
        os.kill(worker.pid, signal.SIGTERM)
        try:
            worker.wait(timeout=15)
        except subprocess.TimeoutExpired:
            raise AssertionError(
                f"{label}: still running 15s after SIGTERM, having worked through "
                f"{len(tags_in(log))} of {len(tags)} queued jobs") from None
    finally:
        stop_worker(worker, hard=True)
    done = set(tags_in(log))
    assert len(done) < len(tags), f"{label}: the backlog was gone before the signal"

    # Nothing it left behind is lost: another worker finishes the queue.
    rescuer = start_worker(env, TARSK_SLOTS=50, TARSK_LEASE_GRACE=1)
    seen = wait_for(log, tags, timeout=120)
    stop_worker(rescuer)
    assert tags <= set(seen), f"{label}: lost {sorted(tags - set(seen))}"


def _b29_a_replay_is_the_same_job(url, label, skip, app, env, log, producer):
    # --- a replayed dead letter keeps its id and the rest of its chain --
    from tarsk import chain

    marker = Path(env["TARSK_LOG"]).with_name("deployed")
    marker.unlink(missing_ok=True)
    steps = chain(app.registry["needs_marker"].si(str(marker)),
                  app.registry["shout"].si("after"))
    steps.send()
    worker = start_worker(env, TARSK_LEASE_GRACE=1)
    try:
        deadline = time.time() + 60
        parked = []
        while time.time() < deadline and not parked:
            parked = [e for e in producer.dead_list(queue="default", limit=50)
                      if e[1] == "needs_marker"]
            time.sleep(0.2)
        assert parked, f"{label}: the failing step never reached the dead letters"

        marker.write_text("")  # the fix ships
        assert producer.dead_replay(queue="default", ids=[parked[-1][0]]) == 1
        # The last step's id was minted before anything ran. Only a replay
        # that kept the chain can ever put a result under it.
        assert app.result(steps.result_id).get(timeout=60) == "AFTER", \
            f"{label}: the chain did not resume after the replay"
    finally:
        stop_worker(worker)
        marker.unlink(missing_ok=True)


def _redis_do(url: str, *commands) -> list[bytes]:
    """Run commands on the URL's database and hand back the raw replies' text."""
    parts = urlsplit(url)
    setup = []
    if parts.password:
        setup.append(("AUTH", *[p for p in (parts.username, parts.password) if p]))
    setup.append(("SELECT", parts.path.lstrip("/") or "0"))
    with socket.create_connection((parts.hostname or "127.0.0.1", parts.port or 6379), 10) as sock:
        sock.sendall(b"".join(_resp(c) for c in (*setup, *commands)))
        sock.settimeout(1.0)
        buf = b""
        try:
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                buf += chunk
        except OSError:
            pass  # quiet for a second: everything asked for has been answered
    return buf.split(b"\r\n")


def _drop_connections(url: str, label: str) -> bool:
    """Close every connection the broker's clients hold, from the server's side.

    False where this machine has no way to do it, said out loud.
    """
    if scheme_is(url, ("redis://", "rediss://")):
        db = urlsplit(url).path.lstrip("/") or "0"
        # Only this database's clients: a shared server has other tenants.
        ids = [field.split(b"=")[1].decode()
               for reply in _redis_do(url, ("CLIENT", "LIST"))
               for line in reply.split(b"\n")  # one bulk string, a client per line
               if b" db=%s " % db.encode() in line and b" cmd=client" not in line
               for field in line.split() if field.startswith(b"id=")]
        assert ids, f"{label}: found no connections to drop"
        _redis_do(url, *[("CLIENT", "KILL", "ID", i) for i in ids])
        return True
    if scheme_is(url, ("postgres://", "postgresql://")):
        if not shutil.which("psql"):
            print(f"  {label}: skip reconnect — no psql here to end the server's connections with")
            return False
        ended = subprocess.run(
            ["psql", url, "-tAc",
             "select pg_terminate_backend(pid) from pg_stat_activity "
             "where datname = current_database() and pid <> pg_backend_pid()"],
            capture_output=True, text=True, check=True).stdout.split()
        assert "t" in ended, f"{label}: found no connections to drop"
        return True
    return _drop_amqp_connections(url, label)


def _drop_amqp_connections(url: str, label: str) -> bool:
    """The same, through RabbitMQ's management API, which is the only way in."""
    import base64
    import json
    import urllib.error
    import urllib.request
    from urllib.parse import quote, unquote

    parts = urlsplit(url)
    api = f"http://{parts.hostname}:{os.environ.get('TARSK_AMQP_MANAGEMENT_PORT', '15672')}/api"
    vhost = unquote(parts.path.lstrip("/")) or "/"
    login = base64.b64encode(
        f"{unquote(parts.username or 'guest')}:{unquote(parts.password or 'guest')}".encode())

    def call(path, method="GET"):
        request = urllib.request.Request(
            f"{api}{path}", method=method, headers={"Authorization": f"Basic {login.decode()}"})
        with urllib.request.urlopen(request, timeout=5) as reply:
            return reply.read()

    try:
        call("/overview")
    except (urllib.error.URLError, OSError):
        print(f"  {label}: skip reconnect — no management API here to close connections with")
        return False
    # The API learns of a connection a few seconds after it opens.
    deadline = time.time() + 15
    while True:
        names = [c["name"] for c in json.loads(call("/connections")) if c.get("vhost") == vhost]
        if names or time.time() > deadline:
            break
        time.sleep(1)
    assert names, f"{label}: found no connections to drop"
    for name in names:
        try:
            call(f"/connections/{quote(name, safe='')}", "DELETE")
        except urllib.error.HTTPError:
            pass  # gone between the listing and the request
    return True


def _send_until_it_lands(send, label: str, tries: int = 5) -> str:
    """The call that finds the connection gone still fails; the next reconnects."""
    for attempt in range(tries):
        try:
            return send()
        except Exception as why:  # noqa: BLE001 — any broker error, by design
            last = why
            time.sleep(0.3)
    raise AssertionError(f"{label}: still failing after {tries} sends: {last}")


def _b30_a_signal_to_the_group_drains(url, label, skip, app, env, log, producer):
    # --- ctrl-C and systemd signal the children too, and work still finishes
    # Both go to the whole process group. A child that died on them cut its
    # task off mid-run instead of letting the supervisor drain it.
    for sig in (signal.SIGINT, signal.SIGTERM):
        log.write_text("")
        tag = f"through-{sig.name}"
        app.registry["announces"].send(tag)
        worker = start_worker(env)
        try:
            wait_for(log, {f"{tag}-start"}, timeout=60)
            os.killpg(os.getpgid(worker.pid), sig)
            worker.wait(timeout=30)
        finally:
            stop_worker(worker, hard=True)
        assert f"{tag}-end" in tags_in(log), \
            f"{label}: {sig.name} to the group cut the running task off: {tags_in(log)}"


def _b31_a_wait_is_not_an_attempt(url, label, skip, app, env, log, producer):
    # --- queueing behind max_concurrency does not spend the retry budget
    ids = [app.registry["queued_attempt"].send() for _ in range(4)]
    worker = start_worker(env, TARSK_SLOTS=4, TARSK_LEASE_GRACE=1)
    try:
        attempts = [app.result(i).get(timeout=60) for i in ids]
    finally:
        stop_worker(worker)
    assert attempts == [1, 1, 1, 1], \
        f"{label}: jobs that only waited their turn were run as attempts {attempts}"


def _b32_a_cancel_needs_no_queue(url, label, skip, app, env, log, producer):
    if "cancel" in skip:
        print(f"  {label}: skip cancel-by-id — this broker refuses cancellation outright")
        return
    # --- a cancellation finds its job whatever queue the caller named ----
    # Naming the wrong one used to report success and change nothing — and
    # "default" is the wrong one for every job on any other queue.
    log.write_text("")
    doomed = app.registry["note"].send_in(3, "cancelled-by-id")
    app.cancel(doomed, queue="some-other-queue")
    app.registry["note"].send_in(3, "left-alone")
    worker = start_worker(env, TARSK_LEASE_GRACE=1)
    try:
        seen = wait_for(log, {"left-alone"}, timeout=60)
        time.sleep(1.5)  # room for the cancelled one to run, if it is going to
        seen = tags_in(log)
    finally:
        stop_worker(worker)
    assert "left-alone" in seen, f"{label}: the job beside it never ran"
    assert "cancelled-by-id" not in seen, f"{label}: a cancelled job ran anyway"


def _b33_jobs_filters_before_it_limits(url, label, skip, app, env, log, producer):
    if "listing-and-cancel" in skip:
        print(f"  {label}: skip jobs-by-state — this broker cannot enumerate delayed jobs")
        return
    # --- `tarsk jobs --state X --limit N` is N jobs in X, not N jobs filtered
    log.write_text("")
    for tag in ("ahead-1", "ahead-2"):
        app.registry["note"].send(tag)
    later = app.registry["note"].send_in(120, "far-off")
    try:
        out = subprocess.run(
            [sys.executable, "-m", "tarsk._cli", "jobs", "--state", "delayed", "--limit", "1"],
            env=env, capture_output=True, text=True, timeout=60)
        # One row, and a delayed one. Not necessarily this block's: on a server
        # that keeps its data between runs an older delayed job sorts first.
        rows = [line for line in out.stdout.splitlines() if line.strip()]
        assert len(rows) == 1 and "  delayed  " in rows[0], \
            f"{label}: the delayed job was cut off by the limit:\n{out.stdout}{out.stderr}"
    finally:
        app.cancel(later, ttl=600)
        worker = start_worker(env, TARSK_LEASE_GRACE=1)
        wait_for(log, {"ahead-1", "ahead-2"}, timeout=60)
        stop_worker(worker)


def _console(port: int, request: bytes, then: bytes = b"") -> str:
    """One request to the console; `then` is sent as a later segment."""
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall(request)
        if then:
            time.sleep(0.4)
            sock.sendall(then)
        chunks = []
        while True:
            chunk = sock.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks).decode(errors="replace")


def _b34_the_console_acts_on_what_it_was_sent(url, label, skip, app, env, log, producer):
    # --- one dead letter purged from the console is one dead letter -----
    log.write_text("")
    before = count_dead(url)
    for tag in ("console-a", "console-b"):
        app.registry["always_fails"].send(tag)
    port = free_port()
    worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_METRICS=f"127.0.0.1:{port}",
                          TARSK_ADMIN_ACTIONS=1)
    try:
        deadline = time.time() + 60
        while time.time() < deadline and count_dead(url) < before + 2:
            time.sleep(0.2)
        parked = producer.dead_list(queue="default", limit=50)
        assert len(parked) >= 2, f"{label}: expected two dead letters, found {len(parked)}"
        scrape_metrics(port)  # the listener is up

        # No token, so it answers only as the machine itself. A page that has
        # re-pointed its own name at 127.0.0.1 still arrives under that name.
        own = _console(port, f"GET / HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n\r\n".encode())
        assert own.startswith("HTTP/1.1 200"), f"{label}: the console refused its own host"
        rebound = _console(port, b"GET / HTTP/1.1\r\nHost: evil.example\r\n\r\n")
        assert rebound.startswith("HTTP/1.1 403"), \
            f"{label}: a rebound hostname was served: {rebound[:80]!r}"

        # The body arrives after the headers, as it does through some proxies.
        # Read as empty, the form meant "every dead letter in default".
        victim = parked[0][0]
        body = f"queue=default&id={victim}".encode()
        head = (f"POST /purge HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                f"Content-Type: application/x-www-form-urlencoded\r\n"
                f"Content-Length: {len(body)}\r\n\r\n").encode()
        reply = _console(port, head, then=body)
        assert reply.startswith("HTTP/1.1 303"), f"{label}: purge said {reply[:80]!r}"
        left = [e[0] for e in producer.dead_list(queue="default", limit=50)]
        assert victim not in left, f"{label}: the purged entry is still there"
        assert len(left) == len(parked) - 1, \
            f"{label}: purging one dead letter left {len(left)} of {len(parked)}"

        # And a form that names no queue is refused, not pointed at "default".
        bare = b"id=whatever"
        reply = _console(port, (f"POST /purge HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                                f"Content-Length: {len(bare)}\r\n\r\n").encode() + bare)
        assert reply.startswith("HTTP/1.1 400"), f"{label}: a queueless purge said {reply[:80]!r}"
        assert len(producer.dead_list(queue="default", limit=50)) == len(left), \
            f"{label}: a refused purge still purged"
    finally:
        stop_worker(worker)
        producer.dead_purge(queue="default")


def _b35_a_dropped_connection_comes_back(url, label, skip, app, env, log, producer):
    # --- the worker and the producer both survive losing their connection
    log.write_text("")
    if scheme_is(url, ("amqp://", "amqps://")):
        # No way to close a client from outside without the management API,
        # so the client is made to close its own channel: declaring a queue
        # that exists with other arguments is a channel error, and a closed
        # channel used to stay closed for the rest of the process's life.
        held = app.registry["echoes"].send("kept")
        worker = start_worker(env)
        try:
            app.result(held).get(timeout=60)
        finally:
            stop_worker(worker)
        try:
            producer.depth([f"result:{held}"])
        except BrokerError:
            pass
        else:
            raise AssertionError(f"{label}: the conflicting declare was expected to fail")
        rows = producer.depth(["default"])
        assert rows and rows[0][0] == "default", f"{label}: the channel never came back: {rows}"

    app.registry["note"].send("before-the-drop")
    said = Path(env["TARSK_LOG"]).with_name("worker.err")
    said.unlink(missing_ok=True)
    worker = start_worker(env, stderr=str(said), TARSK_LEASE_GRACE=1)
    try:
        wait_for(log, {"before-the-drop"}, timeout=60)
        if not _drop_connections(url, label):
            return
        time.sleep(1.0)
        # A producer notices before it sends, so not even the first call
        # after the drop is lost. It used to be: the call that found the
        # connection gone was the one that paid for finding out.
        try:
            producer.depth(["default"])
            app.registry["note"].send("after-the-drop")
        except BrokerError as why:
            raise AssertionError(
                f"{label}: the first call after the drop still failed: {why}") from None
        # The suite's other producers lost their connections to the same
        # sweep, and each owes the one failed call that discovers it.
        for other in (producer, _producers.get(url)):
            if other is not None:
                _send_until_it_lands(lambda: other.depth(["default"]), label)
        seen = wait_for(log, {"after-the-drop"}, timeout=60)
    finally:
        stop_worker(worker)
    assert "after-the-drop" in seen, \
        f"{label}: nothing ran after the connections were dropped — the worker never " \
        f"reconnected. It printed:\n{said.read_text()}"
    if scheme_is(url, ("redis://", "rediss://")):
        # Its blocking read was in flight when the server hung up, so this
        # worker did see an error — and has to have said what it was. The
        # counter it used to move instead told an operator nothing.
        printed = said.read_text()
        assert "tarsk: broker error during claim:" in printed, \
            f"{label}: the worker lost its connection and printed nothing about it:\n{printed}"


def _b36_a_failed_send_frees_its_reservation(url, label, skip, app, env, log, producer):
    if "dedup" in skip:
        print(f"  {label}: skip dedup-release — this broker holds no reservations to release")
        return
    # --- a send that reserved its dedup window and then failed gives it back
    log.write_text("")
    once = app.registry["once"]
    # A key nobody has used: on a server that keeps its data, a window left
    # over from an earlier run would decide this instead of the send under test.
    key = f"released-{os.urandom(4).hex()}"
    if scheme_is(url, ("redis://", "rediss://")):
        # A queue whose stream key is already a string: the reservation
        # succeeds and the XADD after it cannot.
        held = app.registry["echoes"].send("poison")
        worker = start_worker(env)
        try:
            app.result(held).get(timeout=60)
        finally:
            stop_worker(worker)
        broken = f"result:{held}"
    else:
        broken = "no\x00such"  # Postgres refuses a NUL in text
    try:
        once.options(queue=broken, dedup_key=key, dedup_ttl=60).send("freed")
    except BrokerError:
        pass
    else:
        raise AssertionError(f"{label}: the send to {broken!r} was expected to fail")

    # --- and a window that has lapsed is reserved again by the next send --
    # On Postgres the first send after an expiry used to win without leaving
    # a reservation behind, so the one after it won as well.
    lapsing = f"lapsed-{os.urandom(4).hex()}"
    first = once.options(dedup_key=lapsing, dedup_ttl=1).send("lapsed")
    time.sleep(1.5)
    second = once.options(dedup_key=lapsing, dedup_ttl=60).send("lapsed")
    third = once.options(dedup_key=lapsing, dedup_ttl=60).send("lapsed")
    assert second != first, f"{label}: an expired window still answered with its old job"
    assert third == second, f"{label}: two sends inside one window were both queued"

    # The same key again. Had the reservation been kept, this would be answered
    # with the id of a job that was never queued, and nothing would run.
    once.options(dedup_key=key, dedup_ttl=60).send("freed")
    worker = start_worker(env, TARSK_LEASE_GRACE=1)
    try:
        seen = wait_for(log, {"once-freed"}, timeout=30)
    finally:
        stop_worker(worker)
    assert "once-freed" in seen, \
        f"{label}: the retry was swallowed by the failed send's reservation"


def _b37_many_workers_share_one_cap(url, label, skip, app, env, log, producer):
    if "shared-concurrency" in skip:
        print(f"  {label}: skip shared-cap-across-workers — the cap is per worker on this broker")
        return
    # --- three supervisors, three connections, one cap and one bucket ----
    # One worker pipelines everything down a single connection, which hides a
    # check that is only atomic per connection. Separate processes do not.
    log.write_text("")
    for i in range(12):
        app.registry["capped"].send(f"m{i}")
    workers = [start_worker(env, TARSK_LEASE_GRACE=1, TARSK_CHILDREN=2, TARSK_SLOTS=4)
               for _ in range(3)]
    try:
        deadline = time.time() + 90
        while time.time() < deadline and \
                len([t for t in tags_in(log) if t.startswith("end-")]) < 12:
            time.sleep(0.2)
    finally:
        for worker in workers:
            stop_worker(worker)
    live = peak = 0
    for tag in tags_in(log):
        if tag.startswith("start-"):
            live += 1
            peak = max(peak, live)
        elif tag.startswith("end-"):
            live -= 1
    finished = len([t for t in tags_in(log) if t.startswith("end-")])
    assert finished == 12, f"{label}: only {finished}/12 capped jobs finished"
    assert peak <= 2, f"{label}: max_concurrency=2 across three workers, but {peak} ran at once"

    log.write_text("")
    for i in range(15):
        app.registry["metered"].send(f"shared-{i}")
    workers = [start_worker(env, TARSK_LEASE_GRACE=1, TARSK_CHILDREN=2, TARSK_SLOTS=4)
               for _ in range(3)]
    started = time.time()
    try:
        deadline = time.time() + 60
        while time.time() < deadline and len(tags_in(log)) < 15:
            time.sleep(0.1)
    finally:
        for worker in workers:
            stop_worker(worker)
    took = time.time() - started
    assert len(tags_in(log)) == 15, f"{label}: only {len(tags_in(log))}/15 ever ran"
    assert took >= 1.5, f"{label}: 15 tasks at 5/s across three workers took {took:.2f}s"


def _b40_limits_hold_across_connections(url, label, skip, app, env, log, producer):
    if "shared-concurrency" in skip:
        print(f"  {label}: skip limits-across-connections — the limits are per worker on this broker")
        return
    # --- sixteen connections at once cannot overrun a cap or a bucket ----
    # Three workers sharing a cap (block 37) rarely collide on the same
    # instant. Sixteen producers released from one barrier always do, and each
    # has its own connection, which is where a check that reads and then
    # writes comes apart.
    import threading

    from tarsk._core import Producer

    if not hasattr(Producer, "_acquire_slot"):
        print(f"  {label}: skip limits-across-connections — this build has no test hooks "
              "(maturin develop -F test-hooks)")
        return
    callers = [Producer(broker_url=url) for _ in range(16)]
    stamp = os.urandom(4).hex()
    for round_no in range(20):
        barrier = threading.Barrier(len(callers))
        got = []

        def take(caller, n, task=f"cap-{stamp}-{round_no}"):
            barrier.wait()
            got.append(caller._acquire_slot(task, f"job-{n}", 2, 30_000))

        threads = [threading.Thread(target=take, args=(c, n)) for n, c in enumerate(callers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sum(got) == 2, \
            f"{label}: a cap of 2 let {sum(got)} of 16 simultaneous callers in (round {round_no})"

    for round_no in range(20):
        barrier = threading.Barrier(len(callers))
        waits = []

        def spend(caller, task=f"rate-{stamp}-{round_no}"):
            barrier.wait()
            # Three tokens, and a refill too slow to add a fourth meanwhile.
            waits.append(caller._take_token(task, 0.001, 3))

        threads = [threading.Thread(target=spend, args=(c,)) for c in callers]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        served = sum(1 for wait in waits if wait == 0)
        assert served == 3, \
            f"{label}: a bucket of 3 served {served} of 16 simultaneous callers (round {round_no})"


def _record_last_run(url: str, task: str, minute: int) -> bool:
    """Write the scheduler's own record of when `task` last ran. False if this
    machine has no way to reach the store."""
    if scheme_is(url, ("redis://", "rediss://")):
        _redis_do(url, ("SET", f"tarsk:cron:last:{task}", minute))
        return True
    if scheme_is(url, ("postgres://", "postgresql://")) and shutil.which("psql"):
        subprocess.run(
            ["psql", url, "-tAc",
             f"insert into tarsk_cron_runs (name, minute) values ('{task}', {minute}) "
             f"on conflict (name) do update set minute = excluded.minute"],
            capture_output=True, check=True)
        return True
    return False


def _b41_a_missed_cron_run_is_caught_up(url, label, skip, app, env, log, producer):
    if "cron-election" in skip:
        print(f"  {label}: skip cron-catch-up — with no shared store there is no record of "
              "the last run to catch up from")
        return
    # --- a run that came due while no worker was up fires when one returns
    import datetime

    log.write_text("")
    now = datetime.datetime.now(datetime.UTC)
    due = now - datetime.timedelta(minutes=10)
    schedule = f"{due.minute} {due.hour} * * *"   # once a day, ten minutes ago
    this_minute = int(now.timestamp()) // 60

    # Never seen before: that run was not missed, it predates the schedule.
    # Forgetting any record a previous run of this suite left is what makes
    # "never seen" true on a server that keeps its data.
    if not _record_last_run(url, "catches_up", this_minute + 10_000):
        print(f"  {label}: skip cron-catch-up — no psql here to set the last run with")
        return
    worker = start_worker(env, TARSK_LEASE_GRACE=1, TARSK_TEST_CRON=schedule)
    try:
        time.sleep(8)
        assert "caught-up" not in tags_in(log), \
            f"{label}: fired with the last run recorded as later than the one due"
    finally:
        stop_worker(worker)

    # Last ran twenty minutes ago, and came due ten minutes ago with nobody up.
    _record_last_run(url, "catches_up", this_minute - 20)
    pair = [start_worker(env, TARSK_LEASE_GRACE=1, TARSK_TEST_CRON=schedule) for _ in range(2)]
    try:
        seen = wait_for(log, {"caught-up"}, timeout=30)
        time.sleep(8)  # room for a second firing, from either worker
        seen = tags_in(log)
    finally:
        for worker in pair:
            stop_worker(worker)
    fired = seen.count("caught-up")
    assert fired == 1, f"{label}: a run missed ten minutes ago fired {fired} times, wanted once"


def _b42_connecting_beside_a_busy_worker(url, label, skip, app, env, log, producer):
    if not scheme_is(url, ("postgres://", "postgresql://")):
        return
    # --- thirty producers connect while a worker is claiming, and none fails
    # Every connection used to run the schema, taking a lock on the jobs table
    # that a claim in progress could deadlock with — Postgres then killed one
    # of the two, and it was as likely to be the worker's claim.
    import threading

    from tarsk._core import Producer

    log.write_text("")
    tags = {f"busy-{i}" for i in range(60)}
    for tag in sorted(tags):
        app.registry["note"].send(tag)
    said = Path(env["TARSK_LOG"]).with_name("busy.err")
    said.unlink(missing_ok=True)
    failed = []

    def connect():
        try:
            Producer(broker_url=url).depth(["default"])
        except BrokerError as why:
            failed.append(str(why))

    worker = start_worker(env, stderr=str(said), TARSK_SLOTS=8, TARSK_LEASE_GRACE=1)
    try:
        threads = [threading.Thread(target=connect) for _ in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        seen = wait_for(log, tags, timeout=60)
    finally:
        stop_worker(worker)
    assert not failed, f"{label}: {len(failed)} of 30 connections failed: {failed[0]}"
    assert tags <= set(seen), f"{label}: lost {sorted(tags - set(seen))[:5]}"
    assert "deadlock" not in said.read_text(), \
        f"{label}: the worker hit a deadlock while others connected:\n{said.read_text()}"


def _b43_a_batch_arrives_whole(url, label, skip, app, env, log, producer):
    # --- three hundred jobs in one call, and every one of them runs ------
    log.write_text("")
    note = app.registry["note"]
    tags = [f"bulk-{i}" for i in range(300)]
    ids = app.send_many(note.si(tag) for tag in tags)
    assert len(set(ids)) == 300, f"{label}: 300 sends were given {len(set(ids))} ids"
    worker = start_worker(env, TARSK_SLOTS=50, TARSK_LEASE_GRACE=1)
    try:
        seen = wait_for(log, set(tags), timeout=60)
    finally:
        stop_worker(worker)
    assert set(tags) <= set(seen), f"{label}: lost {len(set(tags) - set(seen))} of the batch"

    # One job no broker may take, in the middle: none of the batch may be
    # queued, whichever backend. Sent one by one, the first half would have
    # been, and Redis used to queue the rest where Postgres refused the lot.
    log.write_text("")
    batch = [note.si(f"half-{i}") for i in range(10)]
    batch[5] = type(batch[0])(note, ("refused",), {}, feed=False)
    batch[5].task = _on_queue(note, "no\x00such")
    try:
        app.send_many(batch)
    except BrokerError:
        pass
    else:
        raise AssertionError(f"{label}: a batch with a job no broker can take was accepted")
    worker = start_worker(env, TARSK_LEASE_GRACE=1)
    try:
        time.sleep(4)
    finally:
        stop_worker(worker)
    ran = [t for t in tags_in(log) if t.startswith("half-")]
    assert not ran, f"{label}: a refused batch still queued {ran}"


def _b47_a_job_handed_back_is_not_left_behind(url, label, skip, app, env, log, producer):
    # --- a retry keeps its place, far below where the worker now looks ---
    # Postgres starts a claim just under the newest id it has taken, and a job
    # handed back with no delay sits where it was. It has to be found anyway.
    log.write_text("")
    app.registry["comes_back"].send("came-back")
    app.send_many(app.registry["note"].si(f"ahead-{i}") for i in range(3000))
    worker = start_worker(env, TARSK_SLOTS=50, TARSK_LEASE_GRACE=1)
    try:
        seen = wait_for(log, {"came-back"}, timeout=90)
    finally:
        stop_worker(worker)
    ahead = sum(1 for tag in seen if tag.startswith("ahead-"))
    assert "came-back" in seen, \
        f"{label}: handed back after {ahead} newer jobs ran, and never taken again"


class _on_queue:
    """A task as `send_many` sees it, with its queue swapped for one job."""

    def __init__(self, task, queue):
        import dataclasses

        self.app, self.fn = task.app, task.fn
        self.spec = dataclasses.replace(task.spec, queue=queue)


def _b44_a_paused_queue_waits(url, label, skip, app, env, log, producer):
    if "cancel" in skip:
        print(f"  {label}: skip pause — this broker has no store every worker can read")
        return
    # --- a paused queue is left alone, its neighbour is not, and it resumes
    log.write_text("")
    app.pause("default")
    try:
        assert producer.paused() == ["default"], producer.paused()
        app.registry["note"].send("held-back")
        app.registry["urgent"].send("unaffected")       # the queue called "high"
        worker = start_worker(env, TARSK_QUEUES="high,default", TARSK_LEASE_GRACE=1)
        try:
            seen = wait_for(log, {"unaffected"}, timeout=60)
            time.sleep(3)
            seen = tags_in(log)
            assert "unaffected" in seen, f"{label}: pausing one queue stopped another"
            assert "held-back" not in seen, f"{label}: a paused queue was served: {seen}"
            out = subprocess.run([sys.executable, "-m", "tarsk._cli", "status"],
                                 env=env, capture_output=True, text=True, timeout=60)
            assert "paused" in out.stdout, f"{label}: status does not say so:\n{out.stdout}"

            # Let go from the command line, and the same worker picks it up.
            subprocess.run([sys.executable, "-m", "tarsk._cli", "resume"],
                           env=env, check=True, capture_output=True, timeout=60)
            seen = wait_for(log, {"held-back"}, timeout=30)
            assert "held-back" in seen, f"{label}: the queue never resumed"
        finally:
            stop_worker(worker)
    finally:
        app.resume("default")
    assert producer.paused() == [], producer.paused()


def _b45_a_running_job_can_be_stopped(url, label, skip, app, env, log, producer):
    if "cancel" in skip:
        print(f"  {label}: skip interrupt — this broker refuses cancellation outright")
        return
    # --- cancel(interrupt=True) reaches a job that has already started ---
    from tarsk import TaskFailed

    log.write_text("")
    before = count_dead(url)
    waiting = app.registry["stoppable"].send("halfway")
    blocking = app.registry["medium"].send("sync-runs-on")
    looking = app.registry["stoppable_sync"].send("thread")
    worker = start_worker(env, TARSK_SLOTS=4, TARSK_LEASE_GRACE=1)
    try:
        wait_for(log, {"halfway-start", "thread-start"}, timeout=60)
        app.cancel(waiting, interrupt=True)
        app.cancel(blocking, interrupt=True)
        app.cancel(looking, interrupt=True)
        seen = wait_for(log, {"halfway-stopped-True", "thread-saw-it"}, timeout=15)
        assert "halfway-stopped-True" in seen, \
            f"{label}: the running job was not interrupted, or was not told why: {seen}"
        # A thread cannot be interrupted, but it can be told.
        assert "thread-saw-it" in seen, \
            f"{label}: a sync handler was never told it had been cancelled: {seen}"
        for stopped in (waiting, looking):
            try:
                app.result(stopped).get(timeout=15)
            except TaskFailed as why:
                assert why.error_type == "Cancelled", why.error_type
            else:
                raise AssertionError(f"{label}: an interrupted job reported a result")
        # One that never looks finishes its work.
        seen = wait_for(log, {"sync-runs-on"}, timeout=30)
        assert "sync-runs-on" in seen, f"{label}: a sync handler was cut off: {seen}"
        time.sleep(1.5)
    finally:
        stop_worker(worker)
    seen = tags_in(log)
    # It had two retries to spend and spent none: cancelled is not failed.
    assert seen.count("halfway-start") == 1, f"{label}: a cancelled job was retried: {seen}"
    assert seen.count("thread-start") == 1, f"{label}: a cancelled job was retried: {seen}"
    assert "thread-end" not in seen, f"{label}: {seen}"
    assert "halfway-end" not in seen, f"{label}: {seen}"
    assert count_dead(url) == before, \
        f"{label}: a cancelled job was left in the dead letters for a replay to revive"


def _b46_a_chord_joins(url, label, skip, app, env, log, producer):
    if "cancel" in skip:
        print(f"  {label}: skip chord — this broker has no store to count finished members in")
        return
    # --- twelve members across two workers, and one callback after them ---
    from tarsk import chord, group

    log.write_text("")
    double, adds_up = app.registry["double"], app.registry["adds_up"]
    work = chord(group(double.si(n) for n in range(12)), adds_up.s(extra=1))
    answer = work.send()
    pair = [start_worker(env, TARSK_SLOTS=4, TARSK_LEASE_GRACE=1) for _ in range(2)]
    try:
        # 2 * (0 + 1 + … + 11) + 1, which also says the list arrived whole.
        assert app.result(answer).get(timeout=60) == 133, f"{label}: the callback's answer"
        time.sleep(1.5)  # room for a second callback, if two members both fired it
    finally:
        for worker in pair:
            stop_worker(worker)
    fired = [t for t in tags_in(log) if t.startswith("adds_up-")]
    assert fired == ["adds_up-12"], f"{label}: the callback ran {len(fired)} times: {fired}"


def _b38_redis_only(url, label, skip, app, env, log, producer):
    if not scheme_is(url, ("redis://", "rediss://")):
        return
    from tarsk._core import Producer

    # --- delayed jobs from two producers do not overwrite each other -----
    # Their member in the delayed set was <pid>-<counter>, both counting from
    # zero: two producers in one process are the two containers that are both
    # pid 1.
    log.write_text("")
    for n, sender in enumerate((Producer(broker_url=url), Producer(broker_url=url))):
        app._producer, kept = sender, app._producer
        try:
            app.registry["note"].send_in(1, f"delayed-by-{n}")
        finally:
            app._producer = kept
    worker = start_worker(env, TARSK_LEASE_GRACE=1)
    try:
        seen = wait_for(log, {"delayed-by-0", "delayed-by-1"}, timeout=30)
    finally:
        stop_worker(worker)
    assert {"delayed-by-0", "delayed-by-1"} <= set(seen), \
        f"{label}: one producer's delayed job replaced the other's: {seen}"

    # --- what a stopping worker read ahead is handed back, not held ------
    # Four children, each busy for seconds. The read that fed the fourth
    # fetched four entries and could place one, so three sit in the worker's
    # buffer with nobody free to take them — and that is when it is told to
    # stop. It still owned those: they sat out a whole lease, half a minute
    # for these, before anyone else could run them.
    log.write_text("")
    tags = [f"ahead-{i}" for i in range(12)]
    for tag in tags:
        app.registry["lingers"].send(tag)
    worker = start_worker(env, TARSK_CHILDREN=4)
    try:
        deadline = time.time() + 60
        while time.time() < deadline and \
                len([t for t in tags_in(log) if t.endswith("-start")]) < 4:
            time.sleep(0.05)
        time.sleep(0.5)  # all four are mid-task, and the buffer is as full as it gets
        os.kill(worker.pid, signal.SIGTERM)
        worker.wait(timeout=30)
    finally:
        stop_worker(worker, hard=True)
    ended = {f"{tag}-end" for tag in tags}
    rescuer = start_worker(env, TARSK_SLOTS=50, TARSK_LEASE_GRACE=1)
    try:
        seen = wait_for(log, ended, timeout=20)
    finally:
        stop_worker(rescuer)
    held = sorted(ended - set(seen))
    assert not held, \
        f"{label}: {len(held)} jobs the stopped worker had read ahead were still held " \
        f"20s later: {held}"

    # --- the lease sweep reaches an entry behind 64 others ---------------
    # Seventy jobs in flight and inside their lease, then one stranded behind
    # them by a killed worker. The sweep's filter is the shortest timeout, so
    # its first page is those seventy; the stranded one is on the second.
    log.write_text("")
    release = Path(env["TARSK_LOG"] + ".release")
    stranded = Path(env["TARSK_LOG"] + ".stranded")
    release.unlink(missing_ok=True)
    stranded.unlink(missing_ok=True)
    for i in range(70):
        app.registry["holds"].send(f"held-{i}")
    holder = start_worker(env, TARSK_SLOTS=70, TARSK_LEASE_GRACE=1)
    try:
        deadline = time.time() + 60
        while time.time() < deadline and producer.depth(["default"])[0][2] < 70:
            time.sleep(0.2)
        assert producer.depth(["default"])[0][2] == 70, \
            f"{label}: the holder never took all seventy: {producer.depth(['default'])}"
    finally:
        stop_worker(holder, hard=True)  # their leases stay, for twenty seconds more
    held_since = time.time()

    app.registry["strands_once"].send("behind-the-page")
    victim = start_worker(env, TARSK_LEASE_GRACE=1)
    try:
        deadline = time.time() + 30
        while time.time() < deadline and not stranded.exists():
            time.sleep(0.1)
        assert stranded.exists(), f"{label}: the job to strand never started"
    finally:
        stop_worker(victim, hard=True)

    rescuer = start_worker(env, TARSK_SLOTS=70, TARSK_LEASE_GRACE=1)
    try:
        seen = wait_for(log, {"behind-the-page"}, timeout=12)
        waited = time.time() - held_since
        assert "behind-the-page" in seen, \
            f"{label}: a stranded job behind 64 pending entries was not swept in {waited:.0f}s"
        # Before the seventy in front of it were themselves due, or this
        # proved only that the queue eventually empties.
        assert waited < 19, f"{label}: swept only once the page ahead had expired ({waited:.0f}s)"
        release.touch()
        # Ready and running only: a delayed job parked by an earlier block is
        # not this one's to wait for.
        deadline = time.time() + 90
        while time.time() < deadline and sum(producer.depth(["default"])[0][1:3]) > 0:
            time.sleep(0.5)
        assert sum(producer.depth(["default"])[0][1:3]) == 0, \
            f"{label}: the held jobs never drained: {producer.depth(['default'])}"
    finally:
        stop_worker(rescuer)
        release.unlink(missing_ok=True)
        stranded.unlink(missing_ok=True)


def _b39_large_things_travel(url, label, skip, app, env, log, producer):
    # --- a chain, a meta and a traceback each too big for an AMQP frame ----
    # RabbitMQ carries a message's headers in one frame of at most 131,072
    # bytes, and that is where all three used to ride: the send failed, or a
    # job that could not be dead-lettered came back for ever.
    from tarsk import chain

    big = 200_000
    before = count_dead(url)
    steps = chain(app.registry["shout"].si("first"), app.registry["shout"].si("y" * big))
    steps.send()
    tagged = app.registry["labelled"].options(meta={"trace": "t", "blob": "m" * big}).send("big")
    app.registry["fails_loudly"].send(big)
    worker = start_worker(env, TARSK_LEASE_GRACE=1)
    try:
        tail = app.result(steps.result_id).get(timeout=60)
        assert tail == "Y" * big, f"{label}: the chain's large step arrived as {len(tail)} bytes"
        meta = app.result(tagged).get(timeout=60)
        assert len(meta["blob"]) == big, f"{label}: the meta arrived as {len(meta['blob'])} bytes"
        deadline = time.time() + 60
        while time.time() < deadline and count_dead(url) <= before:
            time.sleep(0.2)
        parked = [e for e in producer.dead_list(queue="default", limit=50)
                  if e[1] == "fails_loudly"]
        assert parked, f"{label}: the job with the large traceback never reached the dead letters"
        assert len(parked[-1][3]) > big, \
            f"{label}: its traceback was cut to {len(parked[-1][3])} bytes"
    finally:
        stop_worker(worker)
        producer.dead_purge(queue="default")


def check_broker(url: str, label: str, skip: frozenset = frozenset()) -> None:
    """`skip` names capability blocks a broker cannot express, by design.

    Skipping is loud, not silent: each skipped block prints why, so a suite
    run says "cannot" rather than looking like it forgot."""
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
    # One producer for every block. Two blocks used to build their own and
    # four more borrowed the first one across block boundaries — the single
    # piece of cross-block state this function had grown, now explicit.
    from tarsk._core import Producer

    producer = Producer(broker_url=url)
    # Every block, in the order its number gives it. A block that fails is
    # reported and the rest still run: one assertion used to end the whole
    # suite, so a run told you about the first thing broken and nothing about
    # the others. `TARSK_BLOCKS=27,35` runs only those.
    blocks = sorted(
        (name, fn) for name, fn in globals().items()
        if re.fullmatch(r"_b\d\d_\w+", name) and callable(fn)
    )
    only = {int(n) for n in os.environ.get("TARSK_BLOCKS", "").replace(",", " ").split()}
    failed = []
    try:
        for name, block in blocks:
            if only and int(name[2:4]) not in only:
                continue
            began = time.time()
            try:
                block(url, label, skip, app, env, log, producer)
            except Exception as why:  # noqa: BLE001 — reported below, by name
                failed.append(name)
                first = str(why).strip().splitlines()[0] if str(why).strip() else ""
                print(f"  FAIL {label} {name[1:]} ({time.time() - began:.0f}s): "
                      f"{type(why).__name__}: {first}")
                if os.environ.get("TARSK_TRACEBACKS"):
                    import traceback

                    traceback.print_exc()
                _stop_stray_workers()
            else:
                if os.environ.get("TARSK_VERBOSE"):
                    print(f"  ok   {label} {name[1:]} ({time.time() - began:.0f}s)")
        if failed:
            raise AssertionError(
                f"{label}: {len(failed)} of {len(blocks)} blocks failed: "
                + ", ".join(name[1:] for name in failed))
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# memory:// — the batch path.
#
# check_broker cannot reach this broker, and not by oversight: it starts
# `tarsk worker` as a separate process, and an in-process queue is empty seen
# from over there. Batch mode is the only way work reaches memory://, so the
# parity worth guarding is asserted through Supervisor.run instead.
#
# The blocks below are the capability blocks batch mode can express. The ones
# missing need an actor outside the run — cancelling work already submitted,
# electing a cron leader against another worker, deduplicating across
# producers — and a single run() call that returns when the work is done has
# nowhere to put them.


def _m01_delivery_and_results(label, log, run):
    # --- every job runs and its return value comes back ----------------
    out = run([("note", ("n0",), {}), ("answers", (2, 3), {}),
               ("shout", ("hello",), {})])
    assert len(out) == 3, f"{label}: {len(out)} outcomes for 3 jobs"
    acks = [v for kind, v in out.values() if kind == "ack"]
    assert "n0" in acks and "HELLO" in acks, f"{label}: got {acks}"
    # A dict, so this also says the codec carried a structure and not just
    # scalars — and the pid inside says a child ran it, not this process.
    answer = next(v for v in acks if isinstance(v, dict))
    assert answer["sum"] == 5, f"{label}: got {answer}"
    assert answer["pid"] != os.getpid(), f"{label}: the handler ran in the parent"


def _m02_failures_come_back_as_nacks(label, log, run):
    # --- a handler that raises settles, it does not vanish -------------
    out = run([("explodes", (), {}), ("note", ("survivor",), {})])
    nacks = [v for kind, v in out.values() if kind == "nack"]
    assert len(nacks) == 1, f"{label}: expected one nack, got {out}"
    error_type, traceback = nacks[0]
    assert "Error" in error_type or error_type, f"{label}: no error type: {error_type!r}"
    assert traceback, f"{label}: nack carried no traceback"
    assert ("ack", "survivor") in out.values(), f"{label}: the sibling job was lost"


def _m03_a_raising_handler_is_retried(label, log, run):
    # --- retries=1 means two attempts, then the failure settles --------
    #
    # always_fails logs on the way in and then raises, so the count is the
    # number of attempts and not a guess from how long the run took.
    log.write_text("")
    out = run([("always_fails", ("doomed",), {})])
    attempts = len([t for t in tags_in(log) if t == "doomed"])
    assert attempts == 2, f"{label}: retries=1 should run it twice, ran {attempts}"
    nacks = [v for kind, v in out.values() if kind == "nack"]
    assert len(nacks) == 1, f"{label}: expected the failure to settle once: {out}"
    assert "never works" in nacks[0][1], f"{label}: lost the traceback: {nacks[0]}"


def _m04_a_child_that_dies_hands_its_job_back(label, log, run):
    # --- the job outlives the process that was running it --------------
    #
    # crash_once kills its child outright the first time — no exception to
    # catch, no ack, no nack. In batch mode the in-process broker has to
    # notice the delivery is unfinished and hand it to another child, which
    # is the whole reason a job record exists here rather than a callback.
    #
    # It logs only on the attempt that survives, so one line *is* the
    # evidence: the first attempt never got that far.
    log.write_text("")
    Path(str(log) + ".crashed").unlink(missing_ok=True)
    out = run([("crash_once", ("r0",), {})])
    assert ("ack", "r0") in out.values(), f"{label}: the job died with its child: {out}"
    assert tags_in(log) == ["r0"], f"{label}: expected one surviving attempt: {tags_in(log)}"


def _m05_a_chain_runs_in_order(label, log, run):
    # --- each step feeds the next, and immutable steps do not ----------
    #
    # This is the block that earned the batch path its job record: a chain
    # bug once had to be chased on Postgres because memory:// could not
    # carry a chain at all.
    from tarsk import chain

    import tests.broker_app as mod

    log.write_text("")
    out = run([chain(mod.double.s(5), mod.add_to.s(3), mod.shout.si("done"))])
    acks = [v for kind, v in out.values() if kind == "ack"]
    assert 10 in acks, f"{label}: first step did not run: {acks}"
    assert 13 in acks, f"{label}: 5*2+3 never landed: {acks}"
    assert "DONE" in acks, f"{label}: immutable step missing or fed: {acks}"
    order = [t for t in tags_in(log) if not t.startswith("note")]
    assert order == ["double-5", "add_to-10+3", "shout-done"], f"{label}: order was {order}"


def _m07_a_concurrency_cap_holds(label, log, run):
    # --- max_concurrency binds in batch mode as well -------------------
    #
    # Two children of three slots is six places a job could run; the cap
    # says two. Overlap is measured from each job's own start and end,
    # not inferred from how long the batch took. Held in-process here,
    # which for this broker is the whole world.
    log.write_text("")
    run([("capped", (f"c{i}",), {}) for i in range(6)], children=2, slots=3)
    live = peak = 0
    for tag in tags_in(log):
        if tag.startswith("start-"):
            live += 1
            peak = max(peak, live)
        elif tag.startswith("end-"):
            live -= 1
    finished = len([t for t in tags_in(log) if t.startswith("end-")])
    assert finished == 6, f"{label}: only {finished}/6 capped jobs finished"
    assert peak <= 2, f"{label}: max_concurrency=2 but {peak} ran at once"
    assert peak == 2, f"{label}: never reached the cap ({peak}) — is anything parallel?"


def _m08_a_job_that_waited_too_long_is_dropped(label, log, run):
    # --- expiry is enforced against the time the job became ready ------
    #
    # One child with one slot, three one-second jobs ahead of it: the
    # perishable job is claimed about three seconds in, past its two.
    # `durable` sits behind the same queue with no expiry and must still
    # run — otherwise this block would pass just as well if batch mode
    # were dropping the tail of every queue.
    #
    # The measurement this guards is the ready-at stamp: while it stayed
    # zero, `expires` was silently unenforceable on this broker.
    log.write_text("")
    out = run([("unhurried", (f"u{i}",), {}) for i in range(3)]
              + [("perishable", ("gone",), {}), ("durable", ("kept",), {})],
              children=1, slots=1)
    dropped = [(t, tb) for t, tb in
               (v for kind, v in out.values() if kind == "nack")]
    assert len(dropped) == 1, f"{label}: expected one expiry, got {out}"
    error_type, why = dropped[0]
    assert error_type == "Expired", f"{label}: dropped for the wrong reason: {error_type}"
    assert "past its expires" in why, f"{label}: unhelpful expiry message: {why!r}"
    assert ("ack", "kept") in out.values(), f"{label}: the unexpiring job was dropped too"
    assert "gone" not in tags_in(log), f"{label}: an expired job ran anyway"


def check_batch_broker(label: str = "memory") -> None:
    """The capability blocks batch mode can express, run against memory://."""
    from tarsk._supervisor import Supervisor

    workdir = Path(tempfile.mkdtemp(prefix="tarsk-batch-"))
    log = workdir / "run.log"
    log.touch()
    # The children are separate interpreters that import the task module, so
    # these have to be on the environment rather than passed in.
    os.environ["TARSK_LOG"] = str(log)
    os.environ["PYTHONPATH"] = str(ROOT)

    def run(jobs, children=2, slots=2):
        return Supervisor("tests.broker_app:app", children=children, slots=slots).run(jobs)

    try:
        _m01_delivery_and_results(label, log, run)
        _m02_failures_come_back_as_nacks(label, log, run)
        _m03_a_raising_handler_is_retried(label, log, run)
        _m04_a_child_that_dies_hands_its_job_back(label, log, run)
        _m05_a_chain_runs_in_order(label, log, run)
        _m07_a_concurrency_cap_holds(label, log, run)
        _m08_a_job_that_waited_too_long_is_dropped(label, log, run)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def test_memory():
    check_batch_broker()


def test_redis():
    with Redis() as redis:
        check_broker(redis.url, "redis")


def test_cli_broker_from_env_and_dotenv():
    """--broker beats TARSK_BROKER beats ./.env, and each works alone.

    The precedence every dotenv tool trains people to expect; getting it
    backwards would make a shell export impossible to override per-project.
    """
    import tempfile

    with Redis() as redis:
        redis_url = redis.url
        base = {k: v for k, v in os.environ.items() if k != "TARSK_BROKER"}
        base["PYTHONPATH"] = str(ROOT)
        tarsk_bin = str(ROOT / ".venv" / "bin" / "tarsk")

        def status(cwd, env, *extra):
            return subprocess.run(
                [tarsk_bin, "status", *extra],
                cwd=cwd, env=env, capture_output=True, text=True, timeout=30,
            )

        with tempfile.TemporaryDirectory() as workdir:
            # Nothing anywhere: refused, and the message names all three ways.
            p = status(workdir, dict(base))
            assert p.returncode != 0 and ".env" in (p.stdout + p.stderr), (p.stdout, p.stderr)

            # ./.env alone is enough — with quotes, an export prefix, a
            # comment, and a non-TARSK key that must stay unexported.
            (Path(workdir) / ".env").write_text(
                "# local dev\n"
                f"export TARSK_BROKER='{redis_url}'\n"
                "DATABASE_URL=postgres://not/mine\n"
            )
            p = status(workdir, dict(base))
            assert p.returncode == 0 and "queue" in p.stdout, (p.stdout, p.stderr)

            # A real environment variable beats the file.
            (Path(workdir) / ".env").write_text("TARSK_BROKER=redis://127.0.0.1:1/9\n")
            p = status(workdir, {**base, "TARSK_BROKER": redis_url})
            assert p.returncode == 0, (p.stdout, p.stderr)

            # And the flag beats the environment.
            p = status(workdir, {**base, "TARSK_BROKER": "redis://127.0.0.1:1/9"},
                       "--broker", redis_url)
            assert p.returncode == 0, (p.stdout, p.stderr)

        # The library half: App() with no URL reads the variable, so producer
        # code deployed twelve ways needs no per-deployment edit.
        probe = subprocess.run(
            [sys.executable, "-c",
             "from tarsk import App; import sys; sys.exit(0 if App().broker == 'redis://x/1' else 1)"],
            env={**base, "TARSK_BROKER": "redis://x/1"},
            capture_output=True, text=True, timeout=30,
        )
        assert probe.returncode == 0, (probe.stdout, probe.stderr)


def _amqp_url() -> str | None:
    """TARSK_AMQP_URL, or whatever answers on the conventional ports."""
    if os.environ.get("TARSK_AMQP_URL"):
        return os.environ["TARSK_AMQP_URL"]
    for port in (5672, 5680):
        with socket.socket() as probe:
            probe.settimeout(0.5)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                return f"amqp://guest:guest@127.0.0.1:{port}/%2f"
    return None


def test_amqp():
    url = _amqp_url()
    if url is None:
        raise SkipTest("no rabbitmq listening; set TARSK_AMQP_URL")
    check_broker(url, "amqp", skip=frozenset({
        # All one root cause: AMQP moves messages and holds no shared state,
        # so anything needing a store every worker can read is per-worker
        # (Celery's own shape on RabbitMQ) or refused.
        "cron-election", "dedup", "shared-concurrency",
        "listing-and-cancel", "cancel",
    }))


def test_postgres():
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
        except BrokerError as exc:
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
    except BrokerError as exc:
        assert "feature is not enabled" not in str(exc), "redis built without TLS"
        assert "CryptoProvider" not in str(exc), "rustls has no crypto provider selected"
    else:
        raise AssertionError("connecting to a dead port should not succeed")


if __name__ == "__main__":
    # `python tests/test_brokers.py` runs everything; naming backends runs
    # those — `… redis postgres` — which is how CI gives each its own job.
    # memory first: it needs no server, so a broken build says so in seconds
    # rather than after Postgres has finished initdb.
    suites = {
        "memory": [test_memory],
        "redis": [test_redis],
        "cli": [test_cli_broker_from_env_and_dotenv],
        "amqp": [test_amqp],
        "postgres": [test_postgres],
        "tls": [test_tls_is_compiled_in, test_postgres_tls_is_compiled_in],
    }
    unknown = [name for name in sys.argv[1:] if name not in suites]
    if unknown:
        sys.exit(f"no such suite: {', '.join(unknown)} (there is {', '.join(suites)})")
    ran, skipped, broken = [], [], []
    for check in [c for name in (sys.argv[1:] or suites) for c in suites[name]]:
        try:
            check()
        except SkipTest as why:
            # Never "ok": a backend whose server is missing did not run, and
            # printing a pass for it is how a broken backend hides in CI.
            skipped.append(check.__name__)
            print(f"skip {check.__name__} ({why})")
        except AssertionError as why:
            # Reported and carried on from, so one backend's failure does not
            # hide whether the next one works.
            broken.append(check.__name__)
            print(f"FAIL {check.__name__}: {why}")
        else:
            ran.append(check.__name__)
            print("ok", check.__name__)
    print(f"\n{len(ran)} ran, {len(skipped)} skipped"
          + (f": {', '.join(skipped)}" if skipped else "")
          + (f", {len(broken)} FAILED: {', '.join(broken)}" if broken else ""))
    if broken:
        sys.exit(1)
