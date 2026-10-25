from dataclasses import dataclass


@dataclass(slots=True)
class CacheEntry:
    value: bytes
    created_at: float
    expires_at: float | None
    expires_wall: float | None
    last_accessed_at: float
    access_count: int
    size_bytes: int
    generation: int
