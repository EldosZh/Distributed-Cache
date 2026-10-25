import asyncio
import random

import pytest

from distributed_cache import AsyncCacheClient, CacheError
from distributed_cache.engine import CacheEngine
from distributed_cache.persistence import Persistence
from distributed_cache.server import CacheServer


def test_randomized_accounting_and_expiration():
    rng, now = random.Random(42), [0.0]
    engine = CacheEngine(max_memory=3000, clock=lambda: now[0])
    for _ in range(2000):
        key = bytes([rng.randrange(40)])
        choice = rng.randrange(5)
        if choice == 0:
            engine.set(key, b"v" * rng.randrange(1, 80), ttl=1 if rng.randrange(2) else None)
        elif choice == 1:
            engine.get(key)
        elif choice == 2:
            engine.delete(key)
        elif choice == 3:
            now[0] += 0.1
            engine.expire_due(4)
        else:
            engine.expire(key, 2)
        assert engine.memory_used == sum(e.size_bytes for e in engine.entries.values())
        assert 0 <= engine.memory_used <= engine.max_memory
        assert set(engine.entries) == set(engine.key_ids) == set(engine.scan_keys.values())
    now[0] += 100
    for key in list(engine.entries):
        entry = engine.entries[key]
        if entry.expires_at is not None:
            assert engine.get(key) is None


async def test_partial_mset_is_journaled(tmp_path):
    async with CacheServer(
        CacheEngine(max_memory=260, eviction="noeviction"),
        port=0,
        persistence=Persistence(tmp_path, "aof"),
    ) as server:
        async with AsyncCacheClient(port=server.port) as client:
            with pytest.raises(CacheError) as error:
                await client.mset({"a": "1", "b": "2", "c": "3"})
            assert error.value.code == "OUT_OF_MEMORY"
    async with CacheServer(port=0, persistence=Persistence(tmp_path, "aof")) as server:
        async with AsyncCacheClient(port=server.port) as client:
            assert await client.mget(["a", "b", "c"]) == [b"1", b"2", None]
            await client.command("FLUSH")
    async with CacheServer(port=0, persistence=Persistence(tmp_path, "aof")) as server:
        assert not server.engine.entries


async def test_failed_journal_stops_further_commands(tmp_path, monkeypatch):
    persistence = Persistence(tmp_path, "aof")
    async with CacheServer(port=0, persistence=persistence) as server:

        def fail(changes):
            raise OSError("injected disk failure")

        monkeypatch.setattr(persistence, "append", fail)
        async with AsyncCacheClient(port=server.port) as client:
            with pytest.raises(CacheError) as error:
                await client.set("key", "value")
            assert error.value.code == "PERSISTENCE_ERROR"
            with pytest.raises(CacheError) as error:
                await client.get("key")
            assert error.value.code == "PERSISTENCE_ERROR"


async def test_cancelled_request_releases_pool_slot():
    async with CacheServer(port=0) as server:
        async with AsyncCacheClient(port=server.port, max_connections=1) as client:
            async with server.lock:
                task = asyncio.create_task(client.incr("key"))
                await asyncio.sleep(0.02)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert await client.ping() == "PONG"


async def test_dead_connection_recovers_on_next_explicit_request():
    async with CacheServer(port=0, idle_timeout=0.03) as server:
        async with AsyncCacheClient(port=server.port) as client:
            assert await client.ping() == "PONG"
            await asyncio.sleep(0.08)
            with pytest.raises(CacheError):
                await client.ping()
            assert await client.ping() == "PONG"


async def test_request_timeout_releases_pool_slot():
    async with CacheServer(port=0) as server:
        async with AsyncCacheClient(
            port=server.port, max_connections=1, request_timeout=0.03
        ) as client:
            async with server.lock:
                with pytest.raises(CacheError) as error:
                    await client.get("key")
                assert error.value.code == "NODE_UNAVAILABLE"
            assert await client.ping() == "PONG"


def test_recovery_refuses_smaller_memory_limit(tmp_path):
    persistence = Persistence(tmp_path, "snapshot")
    engine = CacheEngine()
    engine.set(b"a", b"v" * 300)
    persistence.snapshot(engine)
    with pytest.raises(CacheError) as error:
        persistence.recover(CacheEngine(max_memory=200))
    assert error.value.code == "OUT_OF_MEMORY"
