import asyncio
from collections import Counter
from contextlib import AsyncExitStack

import pytest

from distributed_cache.cluster import AsyncClusterClient, ClusterClient, HashRing, stable_hash
from distributed_cache.config import Node
from distributed_cache.engine import CacheEngine
from distributed_cache.errors import CacheError
from distributed_cache.rebalance import migrate, plan
from distributed_cache.server import CacheServer


def test_ring_distribution_movement_and_placement():
    nodes = [Node(f"node-{c}", port=7001 + i) for i, c in enumerate("abcd")]
    old, new = HashRing(nodes[:3]), HashRing(nodes)
    reordered = HashRing(list(reversed(nodes[:3])))
    keys = [f"key:{i}".encode() for i in range(10000)]
    distribution = Counter(old.owner(k).id for k in keys)
    assert all(2000 < n < 4500 for n in distribution.values())
    assert all(old.owner(k) == reordered.owner(k) for k in keys)
    moved = [k for k in keys if old.owner(k) != new.owner(k)]
    assert all(new.owner(k).id == "node-d" for k in moved)
    assert 0.1 < len(moved) / len(keys) < 0.4
    modulo = sum(stable_hash(k) % 3 != stable_hash(k) % 4 for k in keys)
    assert modulo > len(moved)
    assert len({n.id for n in old.placement(b"key", 3)}) == 3
    one = HashRing(nodes[:1], 1)
    assert all(one.owner(k) == nodes[0] for k in keys)
    with pytest.raises(ValueError):
        old.placement(b"x", 4)


async def test_cluster_thousands_failure_health_and_migration():
    async with AsyncExitStack() as stack:
        servers = [
            await stack.enter_async_context(
                CacheServer(CacheEngine(eviction="noeviction"), port=0, node_id=f"node-{i}")
            )
            for i in range(4)
        ]
        nodes = [Node(s.node_id, s.host, s.port) for s in servers]
        old = await stack.enter_async_context(AsyncClusterClient(nodes[:3]))
        new = await stack.enter_async_context(AsyncClusterClient(nodes))
        items = [(f"key:{i}", f"value:{i}".encode()) for i in range(2000)]
        await old.mset(items)
        for start in range(0, 2000, 100):
            batch = items[start : start + 100]
            assert await old.mget([k for k, _ in batch]) == [v for _, v in batch]
        assert all(len(s.engine.entries) > 300 for s in servers[:3])
        await old.set("expiring", "v", ttl=100)
        scanned, moves = await plan(old, new)
        assert scanned == 2001 and moves
        assert sum(len(s.engine.entries) for s in servers) == 2001
        copied = await migrate(old, new, moves)
        assert copied == len(moves)
        for start in range(0, 2000, 100):
            batch = items[start : start + 100]
            assert await new.mget([k for k, _ in batch]) == [v for _, v in batch]
        assert await new.get("expiring") == b"v"
        assert 90 < await new.ttl("expiring") <= 100
        assert sum(len(s.engine.entries) for s in servers) == 2001

        def sync_cluster():
            with ClusterClient(nodes) as c:
                assert c.mget(["key:0", "key:1"]) == [b"value:0", b"value:1"]

        await asyncio.to_thread(sync_cluster)
        await new.check_health()
        assert all(state == "healthy" for state in (await new.check_health()).values())
        failed = new.ring.owner("key:0")
        await servers[int(failed.id.split("-")[1])].close()
        with pytest.raises(CacheError) as error:
            await new.get("key:0")
        assert error.value.code == "NODE_UNAVAILABLE"
        for _ in range(3):
            status = await new.check_health()
        assert status[failed.id] == "unavailable"


async def test_migration_failure_keeps_source():
    async with (
        CacheServer(port=0) as a,
        CacheServer(
            CacheEngine(max_memory=129, eviction="noeviction"),
            port=0,
        ) as b,
    ):
        nodes = [Node("a", port=a.port), Node("b", port=b.port)]
        async with AsyncClusterClient(nodes[:1]) as old, AsyncClusterClient(nodes) as new:
            key = next(f"k:{i}" for i in range(100) if new.ring.owner(f"k:{i}").id == "b")
            await old.set(key, "data")
            _, moves = await plan(old, new)
            with pytest.raises(CacheError):
                await migrate(old, new, moves)
            assert await old.get(key) == b"data"


async def test_migration_rejects_endpoint_alias_and_changed_identity():
    async with (
        AsyncClusterClient([Node("a", port=7001)]) as old,
        AsyncClusterClient([Node("b", port=7001)]) as alias,
        AsyncClusterClient([Node("a", port=7002)]) as changed,
    ):
        with pytest.raises(ValueError, match="identity"):
            await plan(old, alias)
        with pytest.raises(ValueError, match="endpoints"):
            await plan(old, changed)
