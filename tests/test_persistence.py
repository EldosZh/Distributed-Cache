import struct

import pytest

from distributed_cache.client import AsyncCacheClient
from distributed_cache.engine import CacheEngine
from distributed_cache.persistence import Persistence
from distributed_cache.server import CacheServer


@pytest.mark.parametrize("mode", ["snapshot", "aof", "snapshot+aof"])
async def test_restart_all_modes(tmp_path, mode):
    async with CacheServer(port=0, persistence=Persistence(tmp_path, mode)) as s:
        async with AsyncCacheClient(port=s.port) as c:
            await c.set(b"\x00\xff", b"\xff\x00", ttl=30)
            await c.incr("counter", 42)
            await c.set("removed", "x")
            await c.delete("removed")
            await c.set("persisted", "x", ttl=30)
            await c.persist("persisted")
            if "snapshot" in mode:
                await c.snapshot()
            await c.incr("counter", 1)
    async with CacheServer(port=0, persistence=Persistence(tmp_path, mode)) as s:
        async with AsyncCacheClient(port=s.port) as c:
            assert await c.get(b"\x00\xff") == b"\xff\x00"
            assert 0 < await c.ttl(b"\x00\xff") < 30
            assert await c.get("counter") == b"43"
            assert await c.get("removed") is None
            assert await c.ttl("persisted") == -1


@pytest.mark.parametrize("tail", [b"\x00\x00", struct.pack("!Q", 30) + b"abc"])
async def test_aof_crash_tail_and_continue(tmp_path, tail):
    p = Persistence(tmp_path, "aof")
    s = await CacheServer(port=0, persistence=p).start()
    async with AsyncCacheClient(port=s.port) as c:
        await c.set("a", "1")
        await c.incr("a")
    await s.close()
    with p.aof_path.open("ab") as stream:
        stream.write(tail)
    async with CacheServer(port=0, persistence=Persistence(tmp_path, "aof")) as s:
        async with AsyncCacheClient(port=s.port) as c:
            assert await c.get("a") == b"2"
            await c.set("b", "3")
    async with CacheServer(port=0, persistence=Persistence(tmp_path, "aof")) as s:
        async with AsyncCacheClient(port=s.port) as c:
            assert await c.get("b") == b"3"


def test_snapshot_expired_skipped_and_atomic_failure(tmp_path, monkeypatch):
    now = [10.0]
    e = CacheEngine(clock=lambda: now[0], wall=lambda: now[0])
    p = Persistence(tmp_path, "snapshot")
    e.set(b"a", b"x", ttl=1)
    e.set(b"b", b"y")
    p.snapshot(e)
    original = p.snapshot_path.read_bytes()
    import distributed_cache.persistence as module

    def fail(*args):
        raise OSError("injected replace failure")

    monkeypatch.setattr(module.os, "replace", fail)
    with pytest.raises(OSError):
        p.snapshot(e)
    assert p.snapshot_path.read_bytes() == original
    now[0] = 12
    restored = CacheEngine(clock=lambda: now[0], wall=lambda: now[0])
    p.recover(restored)
    assert restored.get(b"a") is None
    assert restored.get(b"b") == b"y"


def test_corrupt_snapshot_and_aof_refused(tmp_path):
    e = CacheEngine()
    e.set(b"a", b"b")
    p = Persistence(tmp_path, "snapshot")
    p.snapshot(e)
    data = bytearray(p.snapshot_path.read_bytes())
    data[-1] ^= 1
    p.snapshot_path.write_bytes(data)
    with pytest.raises(ValueError, match="checksum"):
        p.recover(CacheEngine())
    p = Persistence(tmp_path, "aof")
    p.recover(e)
    p.append([{"key": b"a", "value": b"b", "expires_wall": None}])
    p.close(e)
    data = bytearray(p.aof_path.read_bytes())
    data[-1] ^= 1
    p.aof_path.write_bytes(data)
    with pytest.raises(ValueError, match="checksum"):
        p.recover(CacheEngine())


async def test_aof_tracks_evictions_and_expire(tmp_path):
    async with CacheServer(
        CacheEngine(max_memory=260), port=0, persistence=Persistence(tmp_path, "aof")
    ) as s:
        async with AsyncCacheClient(port=s.port) as c:
            await c.set("a", "1")
            await c.set("b", "2")
            await c.get("a")
            await c.set("c", "3")
            await c.expire("a", 0.01)
    import asyncio

    await asyncio.sleep(0.03)
    async with CacheServer(port=0, persistence=Persistence(tmp_path, "aof")) as s:
        async with AsyncCacheClient(port=s.port) as c:
            assert await c.get("a") is None
            assert await c.get("b") is None
            assert await c.get("c") == b"3"
