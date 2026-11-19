"""What the compiled half of tarsk exposes to the Python half.

The extension is built from Rust and carries no annotations of its own, so
this is where a type checker — and anyone reading — learns what it takes.
"""

class Producer:
    def __init__(self, broker_url: str) -> None: ...
    def send(
        self,
        id: str,
        queue: str,
        name: str,
        payload: bytes,
        timeout_ms: int,
        delay: float = 0.0,
        chain: bytes = b"",
        meta: bytes = b"",
        dedup_key: str = "",
        dedup_ttl_ms: int = 0,
        expires_ms: int = 0,
    ) -> str | None: ...
    def send_many(
        self, jobs: list[tuple[str, str, str, bytes, int, bytes, bytes, int]]
    ) -> None: ...
    def result(self, id: str) -> bytes | None: ...
    def wait_result(self, id: str, timeout: float) -> bytes | None: ...
    def cancel(self, id: str, queue: str = "default", ttl: float = 86400.0) -> None: ...
    def pause(self, queue: str = "default", paused: bool = True) -> None: ...
    def paused(self) -> list[str]: ...
    def depth(self, queues: list[str]) -> list[tuple[str, int, int, int, int, int | None]]: ...
    def jobs(
        self, queues: list[str], limit: int = 50
    ) -> list[tuple[str, str, str, str, int, int, str, bytes]]: ...
    def dead_list(
        self, queue: str = "default", limit: int = 50
    ) -> list[tuple[str, str, str, str, int]]: ...
    def dead_replay(self, queue: str = "default", ids: list[str] = ...) -> int: ...
    def dead_purge(self, queue: str = "default", ids: list[str] = ...) -> int: ...

def run(
    app_spec: str,
    jobs: list[tuple[str, bytes, str, str, int, bytes, bytes, int]],
    python: str,
    children: int = 2,
    slots: int = 1,
    max_rss: int = 0,
    max_tasks: int = 0,
    max_lifetime: float = 0.0,
    hard_max_rss: int = 0,
) -> tuple[list[tuple[int, bool, bytes, str, str]], dict[str, int], list[int]]: ...
def work(
    app_spec: str,
    broker_url: str,
    queues: list[str],
    python: str,
    children: int = 2,
    slots: int = 1,
    max_rss: int = 0,
    max_tasks: int = 0,
    max_lifetime: float = 0.0,
    lease_grace: float = 30.0,
    metrics_addr: str | None = None,
    hard_max_rss: int = 0,
    max_dead: int = 1_000,
) -> dict[str, int]: ...
def console(broker_url: str, queues: list[str], addr: str) -> None: ...
def rss_of(pid: int) -> int: ...
def backends() -> list[str]: ...
