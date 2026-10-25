import asyncio
import json
import logging
from pathlib import Path

import typer

from . import __version__
from .benchmark import benchmark as run_benchmark
from .client import CacheClient
from .cluster import AsyncClusterClient, HashRing
from .config import load_topology, memory_size
from .engine import CacheEngine
from .errors import CacheError
from .protocol import payload
from .rebalance import describe, migrate, plan
from .server import CacheServer

app = typer.Typer(
    help="Educational distributed cache; custom TCP protocol.",
    no_args_is_help=True,
    invoke_without_command=True,
)
server_app = typer.Typer()
cluster_app = typer.Typer()
app.add_typer(server_app, name="server")
app.add_typer(cluster_app, name="cluster")


@app.callback()
def main(version: bool = typer.Option(False, "--version", is_eager=True)):
    if version:
        typer.echo(__version__)
        raise typer.Exit()


def output(value):
    typer.echo(json.dumps(json.loads(payload(value)), indent=2))


def call(command, host, port, **args):
    try:
        with CacheClient(host, port) as client:
            output(client.command(command, **args))
    except CacheError as exc:
        typer.echo(f"{exc.code}: {exc}", err=True)
        raise typer.Exit(1) from exc


@server_app.command("start")
def start(
    host: str = "127.0.0.1",
    port: int = 7000,
    node_id: str = "node-local",
    max_memory: str = "64MB",
    eviction: str = "lru",
    max_key: int = 4096,
    max_value: int = 1024**2,
    persistence: str = "none",
    data_dir: Path = Path("cache-data"),
    fsync: str = "always",
):
    """Run a node in the foreground. Ctrl+C closes sockets and saves configured persistence."""
    from .persistence import Persistence

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        engine = CacheEngine(memory_size(max_memory), eviction, max_key, max_value)
        store = None if persistence == "none" else Persistence(data_dir, persistence, fsync)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    async def serve():
        async with CacheServer(engine, host, port, node_id, store):
            await asyncio.Event().wait()

    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass


@app.command()
def ping(host: str = "127.0.0.1", port: int = 7000):
    call("PING", host, port)


@app.command()
def get(key: str, host: str = "127.0.0.1", port: int = 7000):
    call("GET", host, port, key=key.encode())


@app.command("set")
def set_value(
    key: str,
    value: str,
    ttl: float | None = None,
    nx: bool = False,
    xx: bool = False,
    host: str = "127.0.0.1",
    port: int = 7000,
):
    call("SET", host, port, key=key.encode(), value=value.encode(), ttl=ttl, nx=nx, xx=xx)


@app.command()
def delete(key: str, host: str = "127.0.0.1", port: int = 7000):
    call("DELETE", host, port, key=key.encode())


@app.command()
def exists(key: str, host: str = "127.0.0.1", port: int = 7000):
    call("EXISTS", host, port, key=key.encode())


@app.command()
def ttl(key: str, host: str = "127.0.0.1", port: int = 7000):
    call("TTL", host, port, key=key.encode())


@app.command()
def expire(key: str, seconds: float, host: str = "127.0.0.1", port: int = 7000):
    call("EXPIRE", host, port, key=key.encode(), ttl=seconds)


@app.command()
def persist(key: str, host: str = "127.0.0.1", port: int = 7000):
    call("PERSIST", host, port, key=key.encode())


@app.command()
def incr(key: str, amount: int = 1, host: str = "127.0.0.1", port: int = 7000):
    call("INCRBY", host, port, key=key.encode(), amount=amount)


@app.command()
def decr(key: str, amount: int = 1, host: str = "127.0.0.1", port: int = 7000):
    call("DECRBY", host, port, key=key.encode(), amount=amount)


@app.command()
def mget(keys: list[str], host: str = "127.0.0.1", port: int = 7000):
    call("MGET", host, port, keys=[k.encode() for k in keys])


@app.command()
def mset(pairs: list[str], host: str = "127.0.0.1", port: int = 7000):
    """Set alternating KEY VALUE arguments; sequential, not transactional."""
    if len(pairs) % 2:
        raise typer.BadParameter("Expected alternating KEY VALUE pairs")
    call(
        "MSET",
        host,
        port,
        items=[[k.encode(), v.encode()] for k, v in zip(pairs[::2], pairs[1::2], strict=True)],
    )


@app.command()
def stats(host: str = "127.0.0.1", port: int = 7000):
    call("STATS", host, port)


@app.command()
def info(host: str = "127.0.0.1", port: int = 7000):
    call("INFO", host, port)


@app.command()
def scan(cursor: int = 0, count: int = 100, host: str = "127.0.0.1", port: int = 7000):
    call("SCAN", host, port, cursor=cursor, count=count)


@app.command()
def snapshot(host: str = "127.0.0.1", port: int = 7000):
    call("SNAPSHOT", host, port)


@app.command()
def flush(yes: bool = typer.Option(False, "--yes"), host: str = "127.0.0.1", port: int = 7000):
    if not yes:
        raise typer.BadParameter("FLUSH deletes every key on this node; supply --yes")
    call("FLUSH", host, port)


@app.command()
def benchmark(
    host: str = "127.0.0.1",
    port: int = 7000,
    requests: int = 1000,
    clients: int = 10,
    mode: str = "mixed",
    value_size: int = 128,
    get_percent: int = 80,
):
    """Measure successful-request latency, excluding working-set setup and cleanup."""
    try:
        output(
            asyncio.run(run_benchmark(host, port, requests, clients, mode, value_size, get_percent))
        )
    except (ValueError, CacheError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@cluster_app.command("hash")
def hash_key(key: str, config: Path = Path("examples/cluster.toml")):
    nodes, vnodes = load_topology(config)
    output({"owner": HashRing(nodes, vnodes).owner(key).id})


@cluster_app.command()
def status(config: Path = Path("examples/cluster.toml")):
    async def run():
        async with AsyncClusterClient.from_config(config) as client:
            await client.check_health()
            return await client.check_health()

    output(asyncio.run(run()))


@cluster_app.command()
def rebalance(old: Path, new: Path, execute: bool = False, writes_paused: bool = False):
    """Plan by default. Migration requires --execute --writes-paused and noeviction targets."""
    if execute and not writes_paused:
        raise typer.BadParameter("Pause application writes, then pass --writes-paused")

    async def run():
        async with (
            AsyncClusterClient.from_config(old) as before,
            AsyncClusterClient.from_config(new) as after,
        ):
            scanned, moves = await plan(before, after)
            result = describe(scanned, moves)
            if execute:
                result["copied_and_removed"] = await migrate(before, after, moves)
            return result

    try:
        output(asyncio.run(run()))
    except (ValueError, CacheError) as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc
