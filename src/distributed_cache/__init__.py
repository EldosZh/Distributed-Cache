"""Educational cache. Custom protocol; no Redis compatibility."""

__version__ = "0.1.0"

from .client import AsyncCacheClient, CacheClient
from .cluster import AsyncClusterClient, ClusterClient
from .errors import CacheError

__all__ = ["AsyncCacheClient", "CacheClient", "AsyncClusterClient", "ClusterClient", "CacheError"]
