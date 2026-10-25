"""Synchronous, non-yielding operations; the server serializes calls with an async lock."""

import heapq
import re
import time
from collections import OrderedDict
from collections.abc import Callable

from .errors import CacheError
from .models import CacheEntry


class CacheEngine:
    ENTRY_OVERHEAD = 128

    def __init__(
        self,
        max_memory: int = 64 * 1024**2,
        eviction: str = "lru",
        max_key: int = 4096,
        max_value: int = 1024**2,
        clock: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
    ):
        if max_memory <= 0 or max_key <= 0 or max_value <= 0:
            raise ValueError("Limits must be positive")
        if eviction not in {"lru", "noeviction"}:
            raise ValueError("eviction must be lru or noeviction")
        self.max_memory, self.eviction = max_memory, eviction
        self.max_key, self.max_value = max_key, max_value
        self.clock, self.wall = clock, wall
        self.entries: OrderedDict[bytes, CacheEntry] = OrderedDict()
        self.expirations: list[tuple[float, int, bytes]] = []
        self.memory_used = self.generation = 0
        self.hits = self.misses = self.evictions = self.expired_keys = 0
        self.started = clock()
        # Stable integer IDs permit bounded SCAN without copying/sorting all keys.
        self.scan_keys: dict[int, bytes] = {}
        self.key_ids: dict[bytes, int] = {}
        self.next_id = 0
        self.dirty: set[bytes] = set()

    def validate_key(self, key: bytes) -> None:
        if not isinstance(key, bytes):
            raise CacheError("INVALID_REQUEST", "Key must be bytes")
        if len(key) > self.max_key:
            raise CacheError("KEY_TOO_LARGE", "Key exceeds configured limit")

    @staticmethod
    def validate_ttl(ttl: float | None) -> None:
        if ttl is not None and (
            isinstance(ttl, bool)
            or not isinstance(ttl, (int, float))
            or not 0 < ttl <= 10 * 365 * 86400
        ):
            raise CacheError("INVALID_TTL", "TTL must be positive and at most ten years")

    def _remove(self, key: bytes) -> bool:
        entry = self.entries.pop(key, None)
        if entry is None:
            return False
        self.memory_used -= entry.size_bytes
        self.dirty.add(key)
        del self.scan_keys[self.key_ids.pop(key)]
        return True

    def _lookup(self, key: bytes) -> CacheEntry | None:
        self.validate_key(key)
        entry = self.entries.get(key)
        if entry and entry.expires_at is not None and entry.expires_at <= self.clock():
            self._remove(key)
            self.expired_keys += 1
            return None
        return entry

    def _compact_heap(self) -> None:
        if len(self.expirations) > max(64, 2 * len(self.entries)):
            self.expirations = [
                (e.expires_at, e.generation, k)
                for k, e in self.entries.items()
                if e.expires_at is not None
            ]
            heapq.heapify(self.expirations)

    def expire_due(self, budget: int = 256) -> int:
        now, removed = self.clock(), 0
        for _ in range(budget):
            if not self.expirations or self.expirations[0][0] > now:
                break
            _, generation, key = heapq.heappop(self.expirations)
            entry = self.entries.get(key)
            if entry and entry.generation == generation and entry.expires_at is not None:
                self._remove(key)
                self.expired_keys += 1
                removed += 1
        self._compact_heap()
        return removed

    def get(self, key: bytes) -> bytes | None:
        entry = self._lookup(key)
        if entry is None:
            self.misses += 1
            return None
        self.hits += 1
        entry.last_accessed_at = self.clock()
        entry.access_count += 1
        self.entries.move_to_end(key)
        return entry.value

    def set(
        self,
        key: bytes,
        value: bytes,
        ttl: float | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        self.validate_key(key)
        self.validate_ttl(ttl)
        if not isinstance(value, bytes):
            raise CacheError("INVALID_REQUEST", "Value must be bytes")
        if len(value) > self.max_value:
            raise CacheError("VALUE_TOO_LARGE", "Value exceeds configured limit")
        if not isinstance(nx, bool) or not isinstance(xx, bool) or (nx and xx):
            raise CacheError("INVALID_REQUEST", "NX and XX must be mutually exclusive booleans")
        size = len(key) + len(value) + self.ENTRY_OVERHEAD
        if size > self.max_memory:
            raise CacheError("OUT_OF_MEMORY", "Entry cannot fit in an empty cache")
        self.expire_due()
        old = self._lookup(key)
        if (nx and old) or (xx and not old):
            return False
        required = self.memory_used - (old.size_bytes if old else 0) + size
        if required > self.max_memory and self.eviction == "noeviction":
            raise CacheError("OUT_OF_MEMORY", "Insufficient accounted memory")
        while required > self.max_memory:
            victim = next(iter(self.entries))
            if victim == key:
                self.entries.move_to_end(key)
                continue
            required -= self.entries[victim].size_bytes
            self._remove(victim)
            self.evictions += 1
        self._remove(key)
        self.generation += 1
        now = self.clock()
        deadline = None if ttl is None else now + ttl
        wall = None if ttl is None else self.wall() + ttl
        self.entries[key] = CacheEntry(value, now, deadline, wall, now, 0, size, self.generation)
        self.dirty.add(key)
        self.memory_used += size
        self.next_id += 1
        self.key_ids[key] = self.next_id
        self.scan_keys[self.next_id] = key
        if deadline is not None:
            heapq.heappush(self.expirations, (deadline, self.generation, key))
        self._compact_heap()
        return True

    def delete(self, key: bytes) -> bool:
        return bool(self._lookup(key)) and self._remove(key)

    def exists(self, key: bytes) -> bool:
        return self._lookup(key) is not None

    def ttl(self, key: bytes) -> float:
        entry = self._lookup(key)
        if entry is None:
            return -2
        return -1 if entry.expires_at is None else max(0, entry.expires_at - self.clock())

    def expire(self, key: bytes, ttl: float | None) -> bool:
        self.validate_ttl(ttl)
        entry = self._lookup(key)
        if entry is None:
            return False
        self.generation += 1
        entry.generation = self.generation
        self.dirty.add(key)
        entry.expires_at = None if ttl is None else self.clock() + ttl
        entry.expires_wall = None if ttl is None else self.wall() + ttl
        if entry.expires_at is not None:
            heapq.heappush(self.expirations, (entry.expires_at, entry.generation, key))
        self._compact_heap()
        return True

    def incr(self, key: bytes, amount: int = 1) -> int:
        if type(amount) is not int or not -(2**63) <= amount < 2**63:
            raise CacheError("INVALID_INTEGER", "Increment must be a signed 64-bit integer")
        entry = self._lookup(key)
        raw = entry.value if entry else b"0"
        if len(raw) > 20 or not re.fullmatch(rb"-?[0-9]+", raw):
            raise CacheError("INVALID_INTEGER", "Value is not a signed 64-bit integer")
        result = int(raw) + amount
        if not -(2**63) <= int(raw) < 2**63 or not -(2**63) <= result < 2**63:
            raise CacheError("INVALID_INTEGER", "Counter overflow")
        self.set(key, str(result).encode())
        if entry is not None and entry.expires_at is not None:
            updated = self.entries[key]
            updated.expires_at, updated.expires_wall = entry.expires_at, entry.expires_wall
            heapq.heappush(self.expirations, (updated.expires_at, updated.generation, key))
            self._compact_heap()
        return result

    def scan(self, cursor: int = 0, count: int = 100) -> dict:
        if (
            type(cursor) is not int
            or cursor < 0
            or type(count) is not int
            or not 1 <= count <= 1000
        ):
            raise CacheError("INVALID_REQUEST", "Invalid cursor/count")
        upper = min(self.next_id, cursor + count)
        keys = []
        for index in range(cursor + 1, upper + 1):
            key = self.scan_keys.get(index)
            if key is not None and self.exists(key):
                keys.append(key)
        return {"cursor": 0 if upper >= self.next_id else upper, "keys": keys}

    def flush(self) -> bool:
        self.dirty.update(self.entries)
        self.entries.clear()
        self.expirations.clear()
        self.scan_keys.clear()
        self.key_ids.clear()
        self.memory_used = 0
        return True

    def stats(self) -> dict:
        return {
            "uptime": self.clock() - self.started,
            "keys": len(self.entries),
            "memory_used": self.memory_used,
            "memory_limit": self.max_memory,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / (self.hits + self.misses) if self.hits + self.misses else 0,
            "evictions": self.evictions,
            "expired_keys": self.expired_keys,
            "expiration_heap": len(self.expirations),
            "eviction": self.eviction,
        }

    def records(self) -> list[dict]:
        self.expire_due(len(self.expirations))
        return [
            {"key": k, "value": e.value, "expires_wall": e.expires_wall}
            for k, e in self.entries.items()
        ]

    def restore(self, records: list[dict]) -> None:
        self.flush()
        policy = self.eviction
        self.eviction = "noeviction"
        try:
            for record in records:
                wall = record["expires_wall"]
                ttl = None if wall is None else wall - self.wall()
                if ttl is None or ttl > 0:
                    self.set(record["key"], record["value"], ttl=ttl)
                    self.entries[record["key"]].expires_wall = wall
        finally:
            self.eviction = policy
