"""Length-prefixed msgpack framing for the supervisor↔child socket (spec §4.2).

Wire format: 4-byte big-endian length, then a msgpack array `[tag, *args]`.
Flat arrays rather than maps — smaller, and positionally decodable from Rust
(rmpv) without both sides agreeing on a serde enum representation.
"""

from __future__ import annotations

import asyncio
import struct
from typing import Any

import msgpack

_LEN = struct.Struct(">I")

# ponytail: fixed ceiling. Raise it, or make it a supervisor flag, only once a
# real payload trips it — an unbounded length prefix is a remote OOM.
MAX_FRAME = 32 * 1024 * 1024


def encode(tag: str, *args: Any) -> bytes:
    body = msgpack.packb([tag, *args], use_bin_type=True)
    return _LEN.pack(len(body)) + body


async def read(reader: asyncio.StreamReader) -> tuple[str, list] | None:
    """Return `(tag, args)`, or None on a clean EOF at a frame boundary."""
    try:
        header = await reader.readexactly(_LEN.size)
    except asyncio.IncompleteReadError:
        return None
    (size,) = _LEN.unpack(header)
    if size > MAX_FRAME:
        raise ValueError(f"frame of {size} bytes exceeds MAX_FRAME")
    # A truncated body is corruption, not a clean shutdown — let it raise.
    body = await reader.readexactly(size)
    tag, *args = msgpack.unpackb(body, raw=False)
    return tag, args


async def write(writer: asyncio.StreamWriter, tag: str, *args: Any) -> None:
    writer.write(encode(tag, *args))
    await writer.drain()


# --- user payload codec -------------------------------------------------
# Separate from the framing above on purpose: to the supervisor this is opaque
# `bytes` (spec §4.2). Swapping msgpack for msgspec here changes nothing in Rust.


def pack_args(args: tuple, kwargs: dict) -> bytes:
    return msgpack.packb([list(args), kwargs], use_bin_type=True)


def unpack_args(payload: bytes) -> tuple[list, dict]:
    args, kwargs = msgpack.unpackb(payload, raw=False)
    return args, kwargs


def pack_result(value: Any) -> bytes:
    # ponytail: `default=repr` degrades an unserializable return value instead
    # of failing. Nacking here would redeliver a task that already ran its side
    # effects; the result backend is off by default anyway (spec §4.3).
    return msgpack.packb(value, use_bin_type=True, default=repr)


def unpack_result(payload: bytes) -> Any:
    return msgpack.unpackb(payload, raw=False)
