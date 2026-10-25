"""Bounded lazy connection pools. Failed requests are never retried automatically."""

import asyncio
import contextlib
import queue
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from .errors import CacheError
from .protocol import HEADER, frame, read_frame, request, unpack, validate_length


def binary(value: str | bytes) -> bytes:
    if isinstance(value, str):
        return value.encode()
    if not isinstance(value, bytes):
        raise TypeError("Expected str or bytes")
    return value


def response_data(response, rid):
    if (
        not isinstance(response, dict)
        or response.get("request_id") != rid
        or response.get("version") != 1
    ):
        raise CacheError("INVALID_RESPONSE", "Response correlation failed")
    if response.get("ok") is not True:
        error = response.get("error", {})
        raise CacheError(
            error.get("code", "INVALID_RESPONSE"), error.get("message", "Invalid response")
        )
    return response.get("data")


class ClientCommands:
    def ping(self):
        return self.command("PING")

    def get(self, key):
        return self.command("GET", key=binary(key))

    def set(self, key, value, ttl=None, nx=False, xx=False):
        return self.command("SET", key=binary(key), value=binary(value), ttl=ttl, nx=nx, xx=xx)

    def delete(self, key):
        return self.command("DELETE", key=binary(key))

    def exists(self, key):
        return self.command("EXISTS", key=binary(key))

    def ttl(self, key):
        return self.command("TTL", key=binary(key))

    def expire(self, key, ttl):
        return self.command("EXPIRE", key=binary(key), ttl=ttl)

    def persist(self, key):
        return self.command("PERSIST", key=binary(key))

    def incr(self, key, amount=1):
        return self.command("INCRBY", key=binary(key), amount=amount)

    def decr(self, key, amount=1):
        return self.command("DECRBY", key=binary(key), amount=amount)

    def mget(self, keys):
        return self.command("MGET", keys=[binary(k) for k in keys])

    def mset(self, items):
        pairs = items.items() if isinstance(items, dict) else items
        return self.command("MSET", items=[[binary(k), binary(v)] for k, v in pairs])

    def stats(self):
        return self.command("STATS")

    def scan(self, cursor=0, count=100):
        return self.command("SCAN", cursor=cursor, count=count)

    def snapshot(self):
        return self.command("SNAPSHOT")


class CacheClient(ClientCommands):
    def __init__(
        self,
        host="127.0.0.1",
        port=7000,
        max_connections=4,
        connect_timeout=2.0,
        request_timeout=5.0,
    ):
        if max_connections < 1 or connect_timeout <= 0 or request_timeout <= 0:
            raise ValueError("Pool size and timeouts must be positive")
        self.host, self.port = host, port
        self.connect_timeout, self.request_timeout = connect_timeout, request_timeout
        self.pool = queue.LifoQueue(max_connections)
        for _ in range(max_connections):
            self.pool.put(None)
        self.closed = False
        self.guard = threading.Lock()

    def command(self, command, **args):
        return self.pipeline([(command, args)])[0]

    @staticmethod
    def _read(sock, count, deadline):
        data = bytearray()
        while len(data) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Request deadline exceeded")
            sock.settimeout(remaining)
            chunk = sock.recv(count - len(data))
            if not chunk:
                raise ConnectionError("Peer disconnected")
            data.extend(chunk)
        return bytes(data)

    def pipeline(self, commands):
        if not 1 <= len(commands) <= 1000:
            raise ValueError("Pipeline must contain 1..1000 commands")
        ids = [uuid.uuid4().hex for _ in commands]
        encoded = b"".join(
            frame(request(c, a, rid)) for (c, a), rid in zip(commands, ids, strict=True)
        )
        if len(encoded) > 8 * 1024**2:
            raise ValueError("Pipeline exceeds 8 MiB")
        if self.closed:
            raise CacheError("CLIENT_CLOSED", "Client is closed")
        try:
            sock = self.pool.get(timeout=self.request_timeout)
        except queue.Empty as exc:
            raise CacheError("POOL_TIMEOUT", "Connection pool exhausted") from exc
        healthy = False
        try:
            if self.closed:
                raise CacheError("CLIENT_CLOSED", "Client is closed")
            if sock is None:
                sock = socket.create_connection((self.host, self.port), self.connect_timeout)
            sock.settimeout(self.request_timeout)
            deadline = time.monotonic() + self.request_timeout
            replies = []

            def receive():
                for _ in commands:
                    length = HEADER.unpack(self._read(sock, 4, deadline))[0]
                    validate_length(length)
                    replies.append(unpack(self._read(sock, length, deadline)))

            if len(commands) == 1:
                sock.sendall(encoded)
                receive()
            else:
                # Drain responses concurrently with sending a bidirectional pipeline.
                with ThreadPoolExecutor(max_workers=1) as executor:
                    sent = executor.submit(sock.sendall, encoded)
                    receive()
                    sent.result()
            healthy = True
            return [response_data(r, rid) for r, rid in zip(replies, ids, strict=True)]
        except (OSError, TimeoutError) as exc:
            raise CacheError(
                "NODE_UNAVAILABLE", "Transport failed; mutation outcome may be uncertain"
            ) from exc
        finally:
            with self.guard:
                if sock is not None and (not healthy or self.closed):
                    sock.close()
                    sock = None
                self.pool.put(sock)

    def close(self):
        with self.guard:
            self.closed = True
            while True:
                try:
                    sock = self.pool.get_nowait()
                except queue.Empty:
                    break
                if sock:
                    sock.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class AsyncCacheClient(ClientCommands):
    def __init__(
        self,
        host="127.0.0.1",
        port=7000,
        max_connections=4,
        connect_timeout=2.0,
        request_timeout=5.0,
    ):
        if max_connections < 1 or connect_timeout <= 0 or request_timeout <= 0:
            raise ValueError("Pool size and timeouts must be positive")
        self.host, self.port = host, port
        self.connect_timeout, self.request_timeout = connect_timeout, request_timeout
        self.pool = asyncio.LifoQueue(max_connections)
        for _ in range(max_connections):
            self.pool.put_nowait(None)
        self.closed = False

    async def command(self, command, **args):
        return (await self.pipeline([(command, args)]))[0]

    async def pipeline(self, commands):
        if not 1 <= len(commands) <= 1000:
            raise ValueError("Pipeline must contain 1..1000 commands")
        ids = [uuid.uuid4().hex for _ in commands]
        encoded = b"".join(
            frame(request(c, a, rid)) for (c, a), rid in zip(commands, ids, strict=True)
        )
        if len(encoded) > 8 * 1024**2:
            raise ValueError("Pipeline exceeds 8 MiB")
        if self.closed:
            raise CacheError("CLIENT_CLOSED", "Client is closed")
        try:
            connection = await asyncio.wait_for(self.pool.get(), self.request_timeout)
        except TimeoutError as exc:
            raise CacheError("POOL_TIMEOUT", "Connection pool exhausted") from exc
        healthy = False
        sender = None
        try:
            if self.closed:
                raise CacheError("CLIENT_CLOSED", "Client is closed")
            if connection is None:
                connection = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    self.connect_timeout,
                )
            reader, writer = connection
            async with asyncio.timeout(self.request_timeout):
                writer.write(encoded)
                sender = asyncio.create_task(writer.drain())
                replies = [await read_frame(reader) for _ in commands]
                await sender
            healthy = True
            return [response_data(r, rid) for r, rid in zip(replies, ids, strict=True)]
        except (OSError, TimeoutError, asyncio.IncompleteReadError) as exc:
            raise CacheError(
                "NODE_UNAVAILABLE", "Transport failed; mutation outcome may be uncertain"
            ) from exc
        finally:
            if sender is not None:
                if not sender.done():
                    sender.cancel()
                with contextlib.suppress(asyncio.CancelledError, ConnectionError):
                    await sender
            if connection and (not healthy or self.closed):
                connection[1].close()
                with contextlib.suppress(ConnectionError):
                    await connection[1].wait_closed()
                connection = None
            self.pool.put_nowait(connection)

    async def close(self):
        self.closed = True
        while not self.pool.empty():
            connection = self.pool.get_nowait()
            if connection:
                connection[1].close()
                with contextlib.suppress(ConnectionError):
                    await connection[1].wait_closed()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()
