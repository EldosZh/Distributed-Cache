import asyncio
import math
import time
import uuid

from .client import AsyncCacheClient
from .errors import CacheError


def percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[max(0, math.ceil(fraction * len(values)) - 1)] if values else 0


async def benchmark(
    host="127.0.0.1",
    port=7000,
    requests=1000,
    clients=10,
    mode="mixed",
    value_size=128,
    get_percent=80,
) -> dict:
    if requests < 1 or clients < 1 or not 0 <= get_percent <= 100 or not 1 <= value_size <= 1024**2:
        raise ValueError("Invalid benchmark limits")
    if mode not in {"get", "set", "mixed"}:
        raise ValueError("mode must be get, set, or mixed")
    value = b"x" * value_size
    prefix = f"benchmark:{uuid.uuid4().hex}:"
    latencies, failures, transferred = [], 0, 0
    async with AsyncCacheClient(host, port, max_connections=clients) as client:
        # Seed a bounded private working set; GET results must be real hits.
        key_count = min(100, requests)
        for i in range(key_count):
            await client.set(f"{prefix}{i:08}", value, ttl=300)

        async def worker(worker_id):
            nonlocal failures, transferred
            for i in range(worker_id, requests, clients):
                key = f"{prefix}{i % key_count:08}"
                start = time.perf_counter()
                try:
                    read = mode == "get" or (mode == "mixed" and i % 100 < get_percent)
                    if read:
                        if await client.get(key) != value:
                            raise CacheError("BENCHMARK_MISS", "Working-set key missing")
                    else:
                        await client.set(key, value, ttl=300)
                    transferred += len(value)
                    latencies.append((time.perf_counter() - start) * 1000)
                except CacheError:
                    failures += 1

        started = time.perf_counter()
        await asyncio.gather(*(worker(i) for i in range(clients)))
        elapsed = time.perf_counter() - started
        for i in range(key_count):
            await client.delete(f"{prefix}{i:08}")
    return {
        "requests": requests,
        "successful": len(latencies),
        "failed": failures,
        "elapsed_seconds": elapsed,
        "requests_per_second": requests / elapsed,
        "payload_MiB_per_second": transferred / 1024**2 / elapsed,
        "average_ms": sum(latencies) / len(latencies) if latencies else 0,
        "p50_ms": percentile(latencies, 0.5),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "min_ms": min(latencies, default=0),
        "max_ms": max(latencies, default=0),
    }
