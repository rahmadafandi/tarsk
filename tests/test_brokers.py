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

PG_BIN = next((p for p in sorted(Path("/usr/lib/postgresql").glob("*/bin"), reverse=True)
               if (p / "initdb").exists()), None) if Path("/usr/lib/postgresql").exists() else None


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
        # reason it exists — so count how often a neighbouring coroutine got to
        # run while the enqueue and the wait were in flight. If either blocked,
        # the counter barely moves.
        import asyncio

        async def check_async_path():
            ticks = 0
            stop = asyncio.Event()

            async def ticker():
                nonlocal ticks
                while not stop.is_set():
                    ticks += 1
                    await asyncio.sleep(0.001)

            spin = asyncio.create_task(ticker())
            job = await app.registry["answers"].send_async(4, 5)
            answer = await app.result(job).get_async(timeout=60)
            stop.set()
            await spin
            return answer, ticks

        answer, ticks = asyncio.run(check_async_path())
        assert answer == {"sum": 9, "pid": ANY_PID}, f"{label}: {answer}"
        assert ticks > 20, f"{label}: the loop only ran {ticks} times — something blocked it"
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
    for check in (test_redis, test_postgres, test_tls_is_compiled_in):
        check()
        print("ok", check.__name__)
