"""Version 1: bounded length-prefixed UTF-8 JSON with tagged base64 bytes."""

import asyncio
import base64
import binascii
import json
import struct

from .errors import CacheError

MAX_FRAME = 8 * 1024**2
HEADER = struct.Struct("!I")


def _encode(value):
    if isinstance(value, bytes):
        return {"$bytes": base64.b64encode(value).decode("ascii")}
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _decode(obj):
    if "$bytes" in obj:
        if set(obj) != {"$bytes"} or not isinstance(obj["$bytes"], str):
            raise ValueError("Invalid binary field")
        return base64.b64decode(obj["$bytes"], validate=True)
    return obj


def payload(value: object) -> bytes:
    return json.dumps(value, default=_encode, allow_nan=False, separators=(",", ":")).encode()


def unpack(data: bytes):
    try:
        return json.loads(
            data,
            object_hook=_decode,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Nonfinite JSON")),
        )
    except (ValueError, UnicodeError, binascii.Error, RecursionError) as exc:
        raise CacheError("INVALID_REQUEST", "Malformed JSON or base64") from exc


def frame(value: object) -> bytes:
    data = payload(value)
    validate_length(len(data))
    return HEADER.pack(len(data)) + data


def validate_length(length: int) -> None:
    if not 0 < length <= MAX_FRAME:
        raise CacheError("INVALID_REQUEST", "Frame length outside permitted bounds")


async def read_frame(reader: asyncio.StreamReader, *, with_size: bool = False):
    length = HEADER.unpack(await reader.readexactly(4))[0]
    validate_length(length)
    value = unpack(await reader.readexactly(length))
    return (value, length + 4) if with_size else value


def request(command: str, args: dict, request_id: str) -> dict:
    return {"version": 1, "request_id": request_id, "command": command, "args": args}


def validate_request(value) -> tuple[str, dict, str]:
    if not isinstance(value, dict):
        raise CacheError("INVALID_REQUEST", "Request must be an object")
    if type(value.get("version")) is not int or value["version"] != 1:
        raise CacheError("UNSUPPORTED_VERSION", "Only protocol version 1 is supported")
    rid, command, args = value.get("request_id"), value.get("command"), value.get("args")
    if not isinstance(rid, str) or len(rid) > 128:
        raise CacheError("INVALID_REQUEST", "request_id must be a string of at most 128 characters")
    if not isinstance(command, str) or not isinstance(args, dict):
        raise CacheError("INVALID_REQUEST", "command must be a string and args an object")
    return command.upper(), args, rid
