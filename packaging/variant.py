#!/usr/bin/env python3
"""Point pyproject.toml at one of the four tarsk distributions.

    python packaging/variant.py redis     # build tarsk-redis next
    python packaging/variant.py memory    # back to plain tarsk
    python packaging/variant.py --check   # is the file the committed one?

Same Rust, same `tarsk` package, same `tarsk._core`, different cargo features:

    tarsk            memory
    tarsk-redis      memory, redis
    tarsk-postgres   memory, postgres
    tarsk-amqp       memory, amqp

Why one file edited in place rather than four under packaging/: maturin finds
pyproject.toml by walking up from the Cargo manifest, not from the working
directory. With crates/tarsk-python/Cargo.toml as the manifest, the file it
finds is always /pyproject.toml — a packaging/tarsk-redis/pyproject.toml is
read for its manifest-path and then overridden, so the wheel comes out named
`tarsk` with the root's features. Verified with maturin 1.15; the same is true
of a staging directory outside the tree.

That constraint is also the answer to metadata drift, which four files would
have had: there is one set of authors, classifiers, keywords and URLs, and no
copy of it to fall behind. Only the three lines below differ per distribution.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"

# backend -> (distribution, what to append to the shared description)
VARIANTS = {
    "memory": ("tarsk", ""),
    "redis": ("tarsk-redis", " — Redis Streams build"),
    "postgres": ("tarsk-postgres", " — Postgres build"),
    "amqp": ("tarsk-amqp", " — RabbitMQ build"),
}
DESCRIPTION = "Memory-bounded task queue for Python with a Rust runtime"


def apply(backend: str) -> str:
    dist, suffix = VARIANTS[backend]
    features = ["pyo3/extension-module"] + ([backend] if backend != "memory" else [])
    text = PYPROJECT.read_text(encoding="utf-8")
    for pattern, value in (
        (r'^name = "tarsk[\w-]*"$', f'name = "{dist}"'),
        (r"^description = .*$", f'description = "{DESCRIPTION}{suffix}"'),
        (r"^features = \[.*\]$", f"features = {features!r}".replace("'", '"')),
    ):
        text, hits = re.subn(pattern, value, text, count=1, flags=re.MULTILINE)
        if hits != 1:
            sys.exit(f"pyproject.toml has no line matching {pattern!r}")
    return text


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) == 2 else ""
    if arg == "--check":
        # The committed file is the plain `tarsk` one. A build that forgot to
        # put it back would publish the next distribution under the wrong name.
        if PYPROJECT.read_text(encoding="utf-8") != apply("memory"):
            sys.exit("pyproject.toml is left on a variant: run variant.py memory")
        print("pyproject.toml is the committed `tarsk` form")
        return
    if arg not in VARIANTS:
        sys.exit(__doc__)
    # encoding and newline both spelled out: Windows would otherwise write the
    # locale encoding (cp1252 mangles the em-dashes in this file's comments and
    # in the descriptions below, and maturin then refuses to read it) and turn
    # every LF into CRLF. The round trip has to be byte-identical everywhere.
    PYPROJECT.write_text(apply(arg), encoding="utf-8", newline="\n")
    print(f"pyproject.toml now builds {VARIANTS[arg][0]} ({arg})")


if __name__ == "__main__":
    main()
