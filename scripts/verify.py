"""Reproduce local hashing and in-memory TCP benchmark measurements."""

import asyncio
import json
import platform
import sys
from collections import Counter
from pathlib import Path

from distributed_cache.benchmark import benchmark
from distributed_cache.cluster import HashRing, stable_hash
from distributed_cache.config import Node
from distributed_cache.server import CacheServer


async def main():
    nodes = [Node(f"node-{c}", port=7001 + i) for i, c in enumerate("abcd")]
    before, after = HashRing(nodes[:3]), HashRing(nodes)
    keys = [f"key:{i}".encode() for i in range(10000)]
    report = {
        "environment": {"python": platform.python_version(), "os": platform.system()},
        "hashing": {
            "keys": len(keys),
            "virtual_nodes_per_node": 128,
            "distribution": dict(Counter(before.owner(k).id for k in keys)),
            "moved_percent": sum(before.owner(k) != after.owner(k) for k in keys) / 100,
            "modulo_moved_percent": sum(stable_hash(k) % 3 != stable_hash(k) % 4 for k in keys)
            / 100,
        },
    }
    async with CacheServer(port=0) as server:
        report["benchmark"] = {
            "configuration": {
                "clients": 10,
                "requests": 1000,
                "value_size": 128,
                "mode": "mixed",
                "get_percent": 80,
                "persistence": "none",
            },
            "result": await benchmark(port=server.port),
        }
    destination = Path(sys.argv[1] if len(sys.argv) > 1 else "benchmark-data/verification.json")

    def save():
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    await asyncio.to_thread(save)
    sys.stdout.write(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    asyncio.run(main())
