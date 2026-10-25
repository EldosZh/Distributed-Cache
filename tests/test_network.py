import asyncio
import struct

import pytest

from distributed_cache.client import AsyncCacheClient, CacheClient
from distributed_cache.engine import CacheEngine
from distributed_cache.errors import CacheError
from distributed_cache.protocol import MAX_FRAME, frame, read_frame, request
from distributed_cache.server import CacheServer


async def test_commands_pool_counters_sync_and_pipeline():
    async with CacheServer(port=0) as server:
        async with AsyncCacheClient(port=server.port, max_connections=3) as c:
            assert await c.ping() == "PONG"
            assert await c.set(b"\x00\xff", b"\x00\xff\r\n")
            assert await c.get(b"\x00\xff") == b"\x00\xff\r\n"
            assert await c.ttl(b"\x00\xff") == -1
            assert await c.expire(b"\x00\xff", 10)
            assert 0 < await c.ttl(b"\x00\xff") <= 10
            assert await c.persist(b"\x00\xff")
            assert await c.exists(b"\x00\xff")
            assert await c.delete(b"\x00\xff")
            assert await c.get(b"\x00\xff") is None
            assert await c.mset({"a": "1", "b": "2"}) == [True, True]
            assert await c.mget(["b", "missing", "a"]) == [b"2", None, b"1"]
            await asyncio.gather(*(c.incr("counter") for _ in range(300)))
            assert await c.get("counter") == b"300"
            assert await c.decr("counter", 3) == 297
            assert await c.pipeline([("PING", {}), ("GET", {"key": b"a"})]) == ["PONG", b"1"]
            with pytest.raises(CacheError, match="Unknown"):
                await c.command("BOGUS")
            assert await c.ping() == "PONG"
            assert (await c.stats())["active_connections"] <= 3

        def sync_operations():
            with CacheClient(port=server.port) as sync:
                assert sync.get("a") == b"1"
                assert sync.pipeline([("PING", {}), ("INCR", {"key": b"a"})]) == ["PONG", 2]

        await asyncio.to_thread(sync_operations)


async def test_fragmented_coalesced_and_malformed():
    async with CacheServer(port=0) as s:
        reader, writer = await asyncio.open_connection(s.host, s.port)
        encoded = frame(request("SET", {"key": b"a", "value": b"b"}, "1"))
        for byte in encoded:
            writer.write(bytes([byte]))
            await writer.drain()
        assert (await read_frame(reader))["data"] is True
        writer.write(frame(request("GET", {"key": b"a"}, "2")) + frame(request("PING", {}, "3")))
        assert (await read_frame(reader))["data"] == b"b"
        assert (await read_frame(reader))["request_id"] == "3"
        writer.close()
        await writer.wait_closed()
        bad_messages = [
            struct.pack("!I", MAX_FRAME + 1),
            struct.pack("!I", 0),
            struct.pack("!I", 1) + b"{",
            frame([1]),
            frame(request("SET", {"key": 12, "value": b"a"}, "x")),
            frame(request("SET", {"key": b"a", "value": b"a", "nx": "yes"}, "x")),
            frame(request("EXPIRE", {"key": b"a", "ttl": "bad"}, "x")),
            frame(request("SET", {"key": b"a", "value": b"v", "ttl": 10**400}, "x")),
            frame(request("GET", {"key": b"a" * 4097}, "x")),
            frame(request("IMPORT", {"key": b"a", "value": b"v", "expires_wall": 10**400}, "x")),
        ]
        raw = b'{"version":1,"request_id":"x","command":"GET","args":{"key":{"$bytes":"!!"}}}'
        bad_messages.append(struct.pack("!I", len(raw)) + raw)
        for bad in bad_messages:
            r, w = await asyncio.open_connection(s.host, s.port)
            w.write(bad)
            assert (await read_frame(r))["ok"] is False
            w.close()
            await w.wait_closed()
        r, w = await asyncio.open_connection(s.host, s.port)
        w.write(struct.pack("!I", 100) + b"abc")
        w.close()
        await w.wait_closed()
        async with AsyncCacheClient(port=s.port) as c:
            assert await c.ping() == "PONG"


async def test_active_ttl_and_limits_over_network():
    async with CacheServer(CacheEngine(max_value=10), port=0) as s:
        async with AsyncCacheClient(port=s.port) as c:
            with pytest.raises(CacheError) as error:
                await c.set("a", b"v" * 11)
            assert error.value.code == "VALUE_TOO_LARGE"
            await c.set("a", "b", ttl=0.03)
            await asyncio.sleep(0.12)
            assert (await c.stats())["keys"] == 0
            assert await c.get("a") is None


async def test_disconnect_is_explicit_and_not_retried():
    s = await CacheServer(port=0).start()
    async with AsyncCacheClient(port=s.port, request_timeout=0.2) as c:
        await c.ping()
        await s.close()
        with pytest.raises(CacheError) as error:
            await c.incr("counter")
        assert error.value.code == "NODE_UNAVAILABLE"
