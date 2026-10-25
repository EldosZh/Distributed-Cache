import asyncio
import contextlib
import inspect
import logging
import math
import time
from collections import defaultdict, deque

from .engine import CacheEngine
from .errors import CacheError
from .protocol import frame, read_frame, validate_request

log = logging.getLogger(__name__)
MUTATIONS = {
    "SET",
    "DELETE",
    "EXPIRE",
    "PERSIST",
    "INCR",
    "DECR",
    "INCRBY",
    "DECRBY",
    "MSET",
    "FLUSH",
    "IMPORT",
}


class CacheServer:
    def __init__(
        self,
        engine: CacheEngine | None = None,
        host: str = "127.0.0.1",
        port: int = 7000,
        node_id: str = "node-local",
        persistence=None,
        idle_timeout: float = 30,
        max_connections: int = 256,
    ):
        self.engine = engine or CacheEngine()
        self.host, self.port, self.node_id = host, port, node_id
        self.persistence = persistence
        self.idle_timeout, self.max_connections = idle_timeout, max_connections
        self.lock = asyncio.Lock()
        self.server = None
        self.background = None
        self.writers = set()
        self.handlers = set()
        self.commands_processed = self.bytes_received = self.bytes_sent = 0
        self.latencies = defaultdict(lambda: deque(maxlen=1024))
        self.persistence_failed = False

    async def start(self):
        if self.persistence:
            await asyncio.to_thread(self.persistence.recover, self.engine)
        self.server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self.server.sockets[0].getsockname()[1]
        self.background = asyncio.create_task(self._expire())
        log.info("Node %s listening on %s:%s", self.node_id, self.host, self.port)
        return self

    async def _expire(self):
        while True:
            await asyncio.sleep(0.05)
            async with self.lock:
                self.engine.expire_due()

    async def close(self):
        if self.server:
            self.server.close()
        if self.background:
            self.background.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.background
        for writer in tuple(self.writers):
            writer.close()
        if self.handlers:
            await asyncio.gather(*tuple(self.handlers), return_exceptions=True)
        if self.server:
            await self.server.wait_closed()
        if self.persistence:
            async with self.lock:
                await asyncio.to_thread(self.persistence.close, self.engine)
        log.info("Node %s stopped", self.node_id)

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, *args):
        await self.close()

    async def _handle(self, reader, writer):
        if len(self.writers) >= self.max_connections:
            writer.close()
            return
        task = asyncio.current_task()
        self.handlers.add(task)
        self.writers.add(writer)
        try:
            while True:
                rid = None
                try:
                    req, received = await asyncio.wait_for(
                        read_frame(reader, with_size=True),
                        self.idle_timeout,
                    )
                    command, args, rid = validate_request(req)
                    self.bytes_received += received
                    started = time.perf_counter()
                    data = await self.execute(command, args)
                    self.latencies[command].append(time.perf_counter() - started)
                    response = {"version": 1, "request_id": rid, "ok": True, "data": data}
                except CacheError as exc:
                    response = {
                        "version": 1,
                        "request_id": rid,
                        "ok": False,
                        "error": {"code": exc.code, "message": str(exc)},
                    }
                    # Framing and envelope errors close the stream: resynchronization is unsafe.
                    encoded = frame(response)
                    writer.write(encoded)
                    self.bytes_sent += len(encoded)
                    await asyncio.wait_for(writer.drain(), self.idle_timeout)
                    if rid is None:
                        break
                    continue
                encoded = frame(response)
                writer.write(encoded)
                self.bytes_sent += len(encoded)
                await asyncio.wait_for(writer.drain(), self.idle_timeout)
        except (asyncio.IncompleteReadError, ConnectionError, TimeoutError):
            pass
        except Exception:
            log.exception("Connection handler failed")
        finally:
            self.writers.discard(writer)
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            self.handlers.discard(task)

    async def execute(self, command: str, args: dict):
        async with self.lock:
            self.commands_processed += 1
            if self.persistence_failed:
                raise CacheError(
                    "PERSISTENCE_ERROR", "Node requires restart after persistence failure"
                )
            if command == "SNAPSHOT":
                if args:
                    raise CacheError("INVALID_REQUEST", "SNAPSHOT takes no arguments")
                if not self.persistence or "snapshot" not in self.persistence.mode:
                    raise CacheError("INVALID_REQUEST", "Persistence is disabled")
                try:
                    await asyncio.to_thread(self.persistence.snapshot, self.engine)
                except OSError as exc:
                    raise CacheError(
                        "PERSISTENCE_ERROR", "Snapshot failed; previous file retained"
                    ) from exc
                return True
            self.engine.dirty.clear()
            command_error = None
            try:
                result = self._dispatch(command, args)
            except TypeError as exc:
                command_error = CacheError(
                    "INVALID_REQUEST", "Wrong argument names, types, or count"
                )
                command_error.__cause__ = exc
            except CacheError as exc:
                command_error = exc
            if self.persistence and command in MUTATIONS and self.engine.dirty:
                try:
                    changes = None if command == "FLUSH" else self.persistence.changes(self.engine)
                    await asyncio.to_thread(self.persistence.append, changes)
                except OSError as exc:
                    self.persistence_failed = True
                    log.exception("Persistence failed; refusing further commands")
                    raise CacheError(
                        "PERSISTENCE_ERROR", "Write outcome uncertain; restart required"
                    ) from exc
            if command_error:
                raise command_error
            return result

    def _dispatch(self, command: str, args: dict):
        e = self.engine
        if command == "EXPORT":
            if set(args) != {"key"}:
                raise TypeError()
            key = args["key"]
            entry = e._lookup(key)
            return (
                None
                if entry is None
                else {
                    "key": key,
                    "value": entry.value,
                    "expires_wall": entry.expires_wall,
                }
            )
        if command == "IMPORT":
            if set(args) != {"key", "value", "expires_wall"}:
                raise TypeError()
            wall = args["expires_wall"]
            if wall is not None and (
                type(wall) not in {int, float} or not -1e12 < wall < 1e12 or not math.isfinite(wall)
            ):
                raise CacheError("INVALID_TTL", "Invalid wall-clock deadline")
            ttl = None if wall is None else wall - e.wall()
            if ttl is not None and ttl <= 0:
                return False
            e.set(args["key"], args["value"], ttl=ttl)
            e.entries[args["key"]].expires_wall = wall
            return True
        methods = {
            "GET": e.get,
            "SET": e.set,
            "DELETE": e.delete,
            "EXISTS": e.exists,
            "TTL": e.ttl,
            "EXPIRE": e.expire,
            "SCAN": e.scan,
            "FLUSH": e.flush,
        }
        if command in methods:
            method = methods[command]
            inspect.signature(method).bind(**args)
            return method(**args)
        if command == "PERSIST":
            if set(args) != {"key"}:
                raise TypeError()
            return e.expire(args["key"], None)
        if command in {"INCR", "DECR", "INCRBY", "DECRBY"}:
            expected = {"key", "amount"} if command.endswith("BY") else {"key"}
            if set(args) != expected:
                raise TypeError()
            amount = args.get("amount", 1)
            if type(amount) is not int:
                raise CacheError("INVALID_INTEGER", "amount must be an integer")
            return e.incr(args["key"], -amount if command.startswith("DECR") else amount)
        if command == "MGET":
            if (
                set(args) != {"keys"}
                or not isinstance(args["keys"], list)
                or len(args["keys"]) > 1000
            ):
                raise TypeError()
            # Bound the response before reading values; split large batches client-side.
            total = 0
            for key in args["keys"]:
                entry = e._lookup(key)
                total += len(entry.value) if entry else 0
            if total > 4 * 1024**2:
                raise CacheError("INVALID_REQUEST", "MGET result too large; use smaller batches")
            return [e.get(key) for key in args["keys"]]
        if command == "MSET":
            if (
                set(args) != {"items"}
                or not isinstance(args["items"], list)
                or len(args["items"]) > 1000
            ):
                raise TypeError()
            # Sequential semantics, deliberately not advertised as a transaction.
            for pair in args["items"]:
                if not isinstance(pair, list) or len(pair) != 2:
                    raise TypeError()
                e.validate_key(pair[0])
                if not isinstance(pair[1], bytes) or len(pair[1]) > e.max_value:
                    raise CacheError("VALUE_TOO_LARGE", "Invalid MSET value")
            return [e.set(k, v) for k, v in args["items"]]
        if command in {"PING", "STATS", "INFO"}:
            if args:
                raise TypeError()
            if command == "PING":
                return "PONG"
            return {
                **e.stats(),
                "node_id": self.node_id,
                "commands_processed": self.commands_processed,
                "active_connections": len(self.writers),
                "bytes_received": self.bytes_received,
                "bytes_sent": self.bytes_sent,
                "latency_window": {
                    name: {"count": len(values), "average_ms": 1000 * sum(values) / len(values)}
                    for name, values in self.latencies.items()
                    if values
                },
            }
        raise CacheError("UNKNOWN_COMMAND", "Unknown command")
