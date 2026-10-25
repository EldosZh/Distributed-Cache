import json
import subprocess
import sys
from pathlib import Path

import pytest

from distributed_cache import CacheClient


def launch(directory, mode):
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).with_name("node_process.py")), str(directory), mode],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    line = process.stdout.readline()
    if not line:
        _, error = process.communicate(timeout=5)
        raise AssertionError(error)
    return process, json.loads(line)["port"]


@pytest.mark.parametrize("mode", ["snapshot", "aof", "snapshot+aof"])
def test_real_process_restart(tmp_path, mode):
    process, port = launch(tmp_path, mode)
    try:
        with CacheClient(port=port) as client:
            client.set("binary", b"\x00\xff\n", ttl=60)
            client.incr("counter", 9)
            if "snapshot" in mode:
                client.snapshot()
            client.incr("counter")
        if "aof" in mode:
            # No server.close(): only acknowledged AOF writes are available after this kill.
            process.kill()
            process.communicate(timeout=5)
            with (tmp_path / "cache.aof").open("ab") as stream:
                stream.write(b"\x00\x00")
        else:
            process.communicate("stop\n", timeout=5)
            assert process.returncode == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
    process, port = launch(tmp_path, mode)
    try:
        with CacheClient(port=port) as client:
            assert client.get("binary") == b"\x00\xff\n"
            assert client.get("counter") == b"10"
            assert 0 < client.ttl("binary") < 60
        process.communicate("stop\n", timeout=5)
        assert process.returncode == 0
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
