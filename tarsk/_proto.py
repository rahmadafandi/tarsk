"""Length-prefixed msgpack framing for the supervisor↔child socket (spec §4.2).

Wire format: 4-byte big-endian length, then a msgpack array `[tag, *args]`.
Flat arrays rather than maps — smaller, and positionally decodable from Rust
(rmpv) without both sides agreeing on a serde enum representation.
"""

from __future__ import annotations

import asyncio
import datetime
import decimal
import struct
import uuid
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


# msgpack extension codes, ours to choose in 0..127. These exist because the
# complaint behind "why can't I use pickle" is almost never about pickle: it is
# that `send(datetime.now())` raised TypeError. Pickle would fix that by making
# every worker execute whatever is in the queue; these fix it by teaching the
# codec eight types.
_DATETIME, _DATE, _TIME, _TIMEDELTA = 1, 2, 3, 4
_DECIMAL, _UUID, _SET, _FROZENSET = 5, 6, 7, 8


def _extend(obj: Any) -> msgpack.ExtType:
    """Encode the types people actually pass, and refuse the rest.

    datetime before date, because the first is a subclass of the second and the
    wrong order would quietly drop the time of day.
    """
    if isinstance(obj, datetime.datetime):
        # isoformat rather than a POSIX timestamp: it keeps the offset a naive
        # datetime does not have and a timestamp cannot represent.
        return msgpack.ExtType(_DATETIME, obj.isoformat().encode())
    if isinstance(obj, datetime.date):
        return msgpack.ExtType(_DATE, obj.isoformat().encode())
    if isinstance(obj, datetime.time):
        return msgpack.ExtType(_TIME, obj.isoformat().encode())
    if isinstance(obj, datetime.timedelta):
        # The three fields rather than total_seconds(): microseconds survive a
        # round trip that a float would round off.
        return msgpack.ExtType(
            _TIMEDELTA,
            msgpack.packb([obj.days, obj.seconds, obj.microseconds]),
        )
    if isinstance(obj, decimal.Decimal):
        # As text. A Decimal that came back as a float would be the exact bug
        # people reach for Decimal to avoid.
        return msgpack.ExtType(_DECIMAL, str(obj).encode())
    if isinstance(obj, uuid.UUID):
        return msgpack.ExtType(_UUID, obj.bytes)
    if isinstance(obj, frozenset):
        return msgpack.ExtType(_FROZENSET, msgpack.packb(list(obj), default=_extend))
    if isinstance(obj, set):
        return msgpack.ExtType(_SET, msgpack.packb(list(obj), default=_extend))
    raise TypeError(
        f"cannot send a {type(obj).__name__}: tarsk carries JSON-shaped values plus "
        "datetime, date, time, timedelta, Decimal, UUID, set and frozenset. Convert it "
        "at the call site, or send an id and load it in the handler."
    )


def _revive(code: int, data: bytes) -> Any:
    if code == _DATETIME:
        return datetime.datetime.fromisoformat(data.decode())
    if code == _DATE:
        return datetime.date.fromisoformat(data.decode())
    if code == _TIME:
        return datetime.time.fromisoformat(data.decode())
    if code == _TIMEDELTA:
        days, seconds, micros = msgpack.unpackb(data)
        return datetime.timedelta(days=days, seconds=seconds, microseconds=micros)
    if code == _DECIMAL:
        return decimal.Decimal(data.decode())
    if code == _UUID:
        return uuid.UUID(bytes=data)
    if code == _SET:
        return set(msgpack.unpackb(data, raw=False, ext_hook=_revive))
    if code == _FROZENSET:
        return frozenset(msgpack.unpackb(data, raw=False, ext_hook=_revive))
    # An unknown code is a newer sender talking to an older worker. Handing back
    # the raw ExtType keeps the rest of the payload readable, which beats
    # failing the whole call over one argument.
    return msgpack.ExtType(code, data)


def pack_args(args: tuple, kwargs: dict) -> bytes:
    # Raises on an unknown type, unlike pack_result below. The difference is
    # deliberate: here nothing has happened yet and the caller can fix the call,
    # there the task has already run and its side effects are already spent.
    return msgpack.packb([list(args), kwargs], use_bin_type=True, default=_extend)


def unpack_args(payload: bytes) -> tuple[list, dict]:
    args, kwargs = msgpack.unpackb(payload, raw=False, ext_hook=_revive)
    return args, kwargs


def pack_chain(rows: list) -> bytes:
    """The steps after the first, as the supervisor will read them."""
    return msgpack.packb(rows, use_bin_type=True, default=_extend)


def _extend_or_describe(obj: Any) -> Any:
    """The extension types, then `repr` for anything else."""
    try:
        return _extend(obj)
    except TypeError:
        return repr(obj)


def pack_result(value: Any) -> bytes:
    # ponytail: degrades an unserializable return value instead of failing.
    # Nacking here would redeliver a task that already ran its side effects; the
    # result backend is off by default anyway (spec §4.3).
    return msgpack.packb(value, use_bin_type=True, default=_extend_or_describe)


def unpack_result(payload: bytes) -> Any:
    return msgpack.unpackb(payload, raw=False, ext_hook=_revive)
