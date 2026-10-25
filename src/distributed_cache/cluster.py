import asyncio
import bisect
import hashlib
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from .client import AsyncCacheClient, CacheClient, ClientCommands, binary
from .config import Node, load_topology
from .errors import CacheError

log = logging.getLogger(__name__)


def stable_hash(key: bytes) -> int:
    return int.from_bytes(hashlib.sha256(key).digest(), "big")


class HashRing:
    def __init__(self, nodes: list[Node], virtual_nodes: int = 128):
        if not nodes or len({n.id for n in nodes}) != len(nodes):
            raise ValueError("Topology requires nonempty, unique node IDs")
        if len({(n.host, n.port) for n in nodes}) != len(nodes):
            raise ValueError("Physical nodes must have distinct endpoints")
        if type(virtual_nodes) is not int or not 1 <= virtual_nodes <= 4096:
            raise ValueError("virtual_nodes must be 1..4096")
        for node in nodes:
            if (
                not node.id
                or not node.host
                or type(node.port) is not int
                or not 1 <= node.port <= 65535
            ):
                raise ValueError("Invalid node identity/endpoint")
        self.nodes = {n.id: n for n in nodes}
        self.virtual_nodes = virtual_nodes
        points = sorted(
            (stable_hash(f"{len(n.id.encode())}:{n.id}:{i}".encode()), n.id)
            for n in nodes
            for i in range(virtual_nodes)
        )
        self.positions = [p for p, _ in points]
        self.owners = [n for _, n in points]

    def owner(self, key: str | bytes) -> Node:
        index = bisect.bisect_left(self.positions, stable_hash(binary(key))) % len(self.positions)
        return self.nodes[self.owners[index]]

    def placement(self, key: str | bytes, count: int) -> list[Node]:
        """Placement calculation only; this release does not replicate writes."""
        if not 1 <= count <= len(self.nodes):
            raise ValueError("Placement count exceeds physical nodes")
        index = bisect.bisect_left(self.positions, stable_hash(binary(key))) % len(self.positions)
        result, seen = [], set()
        for offset in range(len(self.positions)):
            node_id = self.owners[(index + offset) % len(self.positions)]
            if node_id not in seen:
                result.append(self.nodes[node_id])
                seen.add(node_id)
                if len(result) == count:
                    return result
        return result


@dataclass
class Health:
    status: str = "unknown"
    failures: int = 0
    successes: int = 0

    def observe(self, success: bool):
        before = self.status
        if success:
            self.failures = 0
            self.successes += 1
            if self.successes >= 2:
                self.status = "healthy"
        else:
            self.successes = 0
            self.failures += 1
            self.status = "unavailable" if self.failures >= 3 else "suspected"
        if before != self.status:
            log.info("Node health transitioned %s -> %s", before, self.status)


class ClusterClient(ClientCommands):
    def __init__(self, nodes: list[Node], virtual_nodes=128, **client_options):
        self.ring = HashRing(nodes, virtual_nodes)
        self.clients = {n.id: CacheClient(n.host, n.port, **client_options) for n in nodes}
        self.health = {n.id: Health() for n in nodes}

    @classmethod
    def from_config(cls, path, **options):
        nodes, vnodes = load_topology(path)
        return cls(nodes, vnodes, **options)

    def command(self, command, **args):
        if "key" not in args:
            raise CacheError("INVALID_REQUEST", "Use per-node clients for administrative commands")
        node = self.ring.owner(args["key"])
        try:
            result = self.clients[node.id].command(command, **args)
        except CacheError as exc:
            if exc.code == "NODE_UNAVAILABLE":
                self.health[node.id].observe(False)
            raise
        self.health[node.id].observe(True)
        return result

    def mget(self, keys):
        keys = [binary(k) for k in keys]
        groups = defaultdict(list)
        for i, key in enumerate(keys):
            groups[self.ring.owner(key).id].append((i, key))
        result = [None] * len(keys)

        def fetch(item):
            node, indexed = item
            values = self.clients[node].mget([key for _, key in indexed])
            return indexed, values

        with ThreadPoolExecutor(max_workers=max(1, len(groups))) as executor:
            for indexed, values in executor.map(fetch, groups.items()):
                for (index, _), value in zip(indexed, values, strict=True):
                    result[index] = value
        return result

    def mset(self, items):
        pairs = items.items() if isinstance(items, dict) else items
        return [self.set(k, v) for k, v in pairs]

    def check_health(self):
        for node_id, client in self.clients.items():
            try:
                client.ping()
                success = True
            except CacheError:
                success = False
            self.health[node_id].observe(success)
        return {node: state.status for node, state in self.health.items()}

    def close(self):
        for client in self.clients.values():
            client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class AsyncClusterClient(ClientCommands):
    def __init__(self, nodes: list[Node], virtual_nodes=128, **client_options):
        self.ring = HashRing(nodes, virtual_nodes)
        self.clients = {n.id: AsyncCacheClient(n.host, n.port, **client_options) for n in nodes}
        self.health = {n.id: Health() for n in nodes}

    @classmethod
    def from_config(cls, path, **options):
        nodes, vnodes = load_topology(path)
        return cls(nodes, vnodes, **options)

    async def command(self, command, **args):
        if "key" not in args:
            raise CacheError("INVALID_REQUEST", "Use per-node clients for administrative commands")
        node = self.ring.owner(args["key"])
        try:
            result = await self.clients[node.id].command(command, **args)
        except CacheError as exc:
            if exc.code == "NODE_UNAVAILABLE":
                self.health[node.id].observe(False)
            raise
        self.health[node.id].observe(True)
        return result

    async def mget(self, keys):
        keys = [binary(k) for k in keys]
        groups = defaultdict(list)
        for index, key in enumerate(keys):
            groups[self.ring.owner(key).id].append((index, key))
        result = [None] * len(keys)

        async def fetch(node, indexed):
            values = await self.clients[node].mget([key for _, key in indexed])
            for (index, _), value in zip(indexed, values, strict=True):
                result[index] = value

        await asyncio.gather(*(fetch(node, indexed) for node, indexed in groups.items()))
        return result

    async def mset(self, items):
        pairs = items.items() if isinstance(items, dict) else items
        return [await self.set(k, v) for k, v in pairs]

    async def check_health(self):
        async def check(node, client):
            try:
                await client.ping()
                success = True
            except CacheError:
                success = False
            self.health[node].observe(success)

        await asyncio.gather(*(check(n, c) for n, c in self.clients.items()))
        return {node: state.status for node, state in self.health.items()}

    async def close(self):
        await asyncio.gather(*(client.close() for client in self.clients.values()))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
