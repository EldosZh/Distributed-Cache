"""Checksummed binary containers with JSON/base64 bodies; never executable data."""

import hashlib
import logging
import os
import struct
from pathlib import Path

from .protocol import payload, unpack

log = logging.getLogger(__name__)
MAGIC = b"DCACHE\x00\x01"
LENGTH = struct.Struct("!Q")
MAX_RECORD = 32 * 1024**2


def atomic_replace(path: Path, data: bytes) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    # POSIX directory fsync persists the rename. Windows does not expose this through os.open.
    if os.name != "nt":
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class Persistence:
    def __init__(self, directory: str | Path, mode="snapshot+aof", fsync="always"):
        if mode not in {"snapshot", "aof", "snapshot+aof"}:
            raise ValueError("Unknown persistence mode")
        if fsync not in {"always", "no"}:
            raise ValueError("fsync must be always or no")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.mode, self.fsync = mode, fsync
        self.sequence = 0
        self.stream = None

    @property
    def snapshot_path(self):
        return self.directory / "dump.snapshot"

    @property
    def aof_path(self):
        return self.directory / "cache.aof"

    @staticmethod
    def changes(engine):
        result = []
        for key in engine.dirty:
            entry = engine.entries.get(key)
            result.append(
                {
                    "key": key,
                    "value": entry.value if entry else None,
                    "expires_wall": entry.expires_wall if entry else None,
                }
            )
        return result

    def append(self, changes):
        if "aof" not in self.mode:
            return
        self.sequence += 1
        data = payload(
            {"sequence": self.sequence, "changes": changes or [], "clear": changes is None}
        )
        if len(data) > MAX_RECORD:
            raise OSError("AOF transaction exceeds limit")
        self.stream.write(LENGTH.pack(len(data)) + data + hashlib.sha256(data).digest())
        self.stream.flush()
        if self.fsync == "always":
            os.fsync(self.stream.fileno())

    def snapshot(self, engine):
        if "snapshot" not in self.mode:
            raise ValueError("Snapshot persistence is not configured")
        records = engine.records()
        data = payload({"sequence": self.sequence, "count": len(records), "records": records})
        atomic_replace(
            self.snapshot_path,
            MAGIC + LENGTH.pack(len(data)) + data + hashlib.sha256(data).digest(),
        )
        log.info("Snapshot saved: %s entries", len(records))

    def recover(self, engine):
        if "snapshot" in self.mode and self.snapshot_path.exists():
            with self.snapshot_path.open("rb") as stream:
                if stream.read(8) != MAGIC:
                    raise ValueError("Unsupported snapshot magic/version")
                header = stream.read(8)
                if len(header) != 8:
                    raise ValueError("Truncated snapshot")
                length = LENGTH.unpack(header)[0]
                # Local files still need a sanity bound before allocation.
                if length > self.snapshot_path.stat().st_size - 48:
                    raise ValueError("Invalid snapshot length")
                data = stream.read(length)
                if hashlib.sha256(data).digest() != stream.read(32) or stream.read(1):
                    raise ValueError("Snapshot checksum/length mismatch")
                obj = unpack(data)
                if obj["count"] != len(obj["records"]):
                    raise ValueError("Snapshot record count mismatch")
                engine.restore(obj["records"])
                self.sequence = obj["sequence"]
        checkpoint = self.sequence
        if "aof" in self.mode:
            if self.aof_path.exists():
                self._replay(engine, checkpoint)
            else:
                atomic_replace(self.aof_path, MAGIC)
            self.stream = self.aof_path.open("ab")
        engine.dirty.clear()
        log.info("Persistence recovered through sequence %s", self.sequence)

    def _replay(self, engine, checkpoint):
        with self.aof_path.open("r+b") as stream:
            if stream.read(8) != MAGIC:
                raise ValueError("Unsupported AOF magic/version")
            previous = 0
            while True:
                start = stream.tell()
                header = stream.read(8)
                if not header:
                    break
                if len(header) < 8:
                    stream.truncate(start)
                    log.warning("Truncated incomplete AOF length tail")
                    break
                length = LENGTH.unpack(header)[0]
                if length > MAX_RECORD:
                    raise ValueError("Invalid AOF record length; refusing corruption")
                data = stream.read(length)
                checksum = stream.read(32)
                if len(data) != length or len(checksum) != 32:
                    stream.truncate(start)
                    log.warning("Truncated incomplete AOF record tail")
                    break
                if hashlib.sha256(data).digest() != checksum:
                    raise ValueError("AOF checksum mismatch; refusing corruption")
                obj = unpack(data)
                sequence = obj["sequence"]
                if sequence <= previous:
                    raise ValueError("Non-monotonic AOF sequence")
                previous = sequence
                self.sequence = max(self.sequence, sequence)
                if sequence <= checkpoint:
                    continue
                if obj.get("clear"):
                    engine.flush()
                # Delete the entire change set first: replay order must not cause extra evictions.
                for record in obj["changes"]:
                    engine.delete(record["key"])
                policy = engine.eviction
                engine.eviction = "noeviction"
                try:
                    for record in obj["changes"]:
                        key, value, wall = record["key"], record["value"], record["expires_wall"]
                        ttl = None if wall is None else wall - engine.wall()
                        if value is not None and (ttl is None or ttl > 0):
                            engine.set(key, value, ttl=ttl)
                            engine.entries[key].expires_wall = wall
                finally:
                    engine.eviction = policy

    def close(self, engine):
        try:
            if "snapshot" in self.mode:
                self.snapshot(engine)
        finally:
            if self.stream:
                self.stream.flush()
                if self.fsync == "always":
                    os.fsync(self.stream.fileno())
                self.stream.close()
                self.stream = None
