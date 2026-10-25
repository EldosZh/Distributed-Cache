"""Subprocess helper for real process termination and restart tests."""

import asyncio
import json
import sys

from distributed_cache.persistence import Persistence
from distributed_cache.server import CacheServer


async def main():
    async with CacheServer(port=0, persistence=Persistence(sys.argv[1], sys.argv[2])) as server:
        sys.stdout.write(json.dumps({"port": server.port}) + "\n")
        sys.stdout.flush()
        await asyncio.to_thread(sys.stdin.readline)


if __name__ == "__main__":
    asyncio.run(main())
