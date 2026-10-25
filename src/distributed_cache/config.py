import re
import tomllib
from dataclasses import dataclass
from pathlib import Path


def memory_size(value: str) -> int:
    match = re.fullmatch(r"(\d+)\s*(B|KB|MB|GB|KiB|MiB|GiB)?", value, re.IGNORECASE)
    if not match:
        raise ValueError("Expected a positive byte size, such as 64MB")
    unit = (match[2] or "B").upper().replace("I", "")
    result = int(match[1]) * {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3}[unit]
    if result <= 0:
        raise ValueError("Memory limit must be positive")
    return result


@dataclass(frozen=True, slots=True)
class Node:
    id: str
    host: str = "127.0.0.1"
    port: int = 7000


def load_topology(path: str | Path) -> tuple[list[Node], int]:
    with open(path, "rb") as stream:
        config = tomllib.load(stream)
    if config.get("cluster", {}).get("replication_factor", 1) != 1:
        raise ValueError("This release supports sharding only; replication_factor must be 1")
    nodes = [Node(**item) for item in config["nodes"]]
    return nodes, config.get("cluster", {}).get("virtual_nodes", 128)
