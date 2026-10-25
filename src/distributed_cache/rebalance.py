"""Maintenance-window migration. Stop all application writes before planning/executing."""

from collections import Counter
from dataclasses import dataclass

from .cluster import AsyncClusterClient
from .errors import CacheError


@dataclass(frozen=True)
class Move:
    source: str
    target: str
    key: bytes


def validate_transition(old, new):
    for node_id in old.ring.nodes.keys() & new.ring.nodes.keys():
        if old.ring.nodes[node_id] != new.ring.nodes[node_id]:
            raise ValueError("Existing node IDs must retain their endpoints during migration")
    old_endpoints = {(n.host, n.port): n.id for n in old.ring.nodes.values()}
    for node in new.ring.nodes.values():
        previous = old_endpoints.get((node.host, node.port))
        if previous is not None and previous != node.id:
            raise ValueError("An endpoint cannot change node identity during migration")


async def plan(old: AsyncClusterClient, new: AsyncClusterClient) -> tuple[int, list[Move]]:
    validate_transition(old, new)
    scanned, moves = 0, []
    for node, client in old.clients.items():
        cursor = 0
        while True:
            page = await client.scan(cursor)
            for key in page["keys"]:
                if old.ring.owner(key).id != node:
                    continue
                scanned += 1
                target = new.ring.owner(key).id
                if target != node:
                    moves.append(Move(node, target, key))
            cursor = page["cursor"]
            if not cursor:
                break
    return scanned, moves


def describe(scanned, moves):
    return {
        "keys_scanned": scanned,
        "keys_moving": len(moves),
        "routes": {
            f"{a}->{b}": n for (a, b), n in Counter((m.source, m.target) for m in moves).items()
        },
    }


async def migrate(old: AsyncClusterClient, new: AsyncClusterClient, moves: list[Move]) -> int:
    validate_transition(old, new)
    for move in moves:
        if (
            move.source != old.ring.owner(move.key).id
            or move.target != new.ring.owner(move.key).id
            or move.source == move.target
        ):
            raise ValueError("Migration plan does not match these topologies")
    for target in {m.target for m in moves}:
        if (await new.clients[target].stats())["eviction"] != "noeviction":
            raise CacheError("INVALID_REQUEST", "Migration targets must use noeviction")
    copied = 0
    for move in moves:
        source, target = old.clients[move.source], new.clients[move.target]
        record = await source.command("EXPORT", key=move.key)
        if record is None:
            continue
        await target.command("IMPORT", **record)
        verified = await target.command("EXPORT", key=move.key)
        if verified != record:
            # Expiration during transfer is safe, but never delete an unverified source.
            raise CacheError("MIGRATION_FAILED", "Destination verification failed; source retained")
        await source.delete(move.key)
        copied += 1
    return copied
