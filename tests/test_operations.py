import asyncio

import pytest
from typer.testing import CliRunner

from distributed_cache.benchmark import benchmark
from distributed_cache.cli import app
from distributed_cache.config import memory_size
from distributed_cache.server import CacheServer


def test_cli_help_version_and_safety():
    runner = CliRunner()
    assert runner.invoke(app, ["--help"]).exit_code == 0
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0 and "0.1.0" in result.stdout
    assert runner.invoke(app, ["flush"]).exit_code != 0
    assert (
        runner.invoke(
            app, ["cluster", "rebalance", "--old", "a", "--new", "b", "--execute"]
        ).exit_code
        != 0
    )


@pytest.mark.parametrize("text,expected", [("1KB", 1024), ("2MiB", 2 * 1024**2), ("10", 10)])
def test_memory_parser(text, expected):
    assert memory_size(text) == expected


@pytest.mark.parametrize("text", ["0", "-1", "1.5MB", "bogus", "1TB"])
def test_invalid_memory_parser(text):
    with pytest.raises(ValueError):
        memory_size(text)


async def test_cli_against_live_node():
    async with CacheServer(port=0) as s:

        def run():
            runner = CliRunner()
            for args in (
                ["ping"],
                ["set", "hello", "world"],
                ["get", "hello"],
                ["mset", "a", "1", "b", "2"],
                ["mget", "b", "a"],
                ["incr", "a"],
                ["decr", "a"],
                ["exists", "a"],
                ["expire", "a", "10"],
                ["ttl", "a"],
                ["persist", "a"],
                ["stats"],
                ["info"],
                ["scan"],
                ["delete", "a"],
                ["flush", "--yes"],
            ):
                result = runner.invoke(app, [*args, "--port", str(s.port)])
                assert result.exit_code == 0, (args, result.output, result.exception)

        await asyncio.to_thread(run)


async def test_small_benchmark():
    async with CacheServer(port=0) as s:
        result = await benchmark(port=s.port, requests=50, clients=3)
        assert result["successful"] == 50 and result["failed"] == 0
        assert 0 < result["p50_ms"] <= result["p95_ms"] <= result["p99_ms"]
        assert not s.engine.entries
