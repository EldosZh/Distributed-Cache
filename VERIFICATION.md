# Local verification

Verified on Windows with CPython 3.13.16, using the project's `.venv`.
The client calendar date was 2026-10-26. These are local observations, not hosted CI results.

| Check | Observed result |
| --- | --- |
| Editable installation, `python -m pip install -e .` | Successful build and install, version 0.1.0 |
| `python -m pytest` | **45 passed in 26.55 seconds** |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 24 files already formatted |
| `distributed-cache --help` | Successful, commands listed |
| `distributed-cache --version` | 0.1.0 |
| Source audit | No TODO/FIXME, executable deserialization, debug prints, personal paths, or secret-like fields found in source/tests/scripts |

The system's unqualified `python` command resolved to an unusable Windows application alias.
A working installed Python 3.13 interpreter created `.venv`; all checks used its executables.
The restricted default temporary directory was replaced with ignored workspace `.test-tmp`
through process-local TEMP/TMP environment variables. No global configuration was changed.

## Coverage exercised

- Binary-safe network values, all basic commands, NX/XX, size/type/TTL limits, integer validation.
- Lazy/active expiration, stale heap generations, heap bounds, exact counter deadlines.
- Memory overwrite/delete/expiration accounting, LRU order, noeviction, oversized-entry preservation,
  randomized accounting, and expiration during replacement.
- Three hundred concurrent increments produce exactly 300.
- Byte-fragmented requests, coalesced frames, pipelines, malformed JSON/base64/envelopes/types,
  zero/oversized frames and mid-frame disconnects; server remains usable.
- Bounded pooled connections, sync/async APIs, cancellation, timeout, explicit reconnect behavior.
- Snapshot, AOF, combined persistence, TTL recovery, eviction/deletion recovery, partial MSET logging,
  compact FLUSH logging, corrupted checksums, incomplete AOF tails, and smaller-memory recovery refusal.
- Failure injection preserves the prior snapshot; journal failure makes the node refuse further commands.
- Separate OS process tests terminate AOF-enabled servers without graceful shutdown, append an
  incomplete tail, restart, verify acknowledged state, then stop gracefully. Snapshot-only restart
  is tested after a graceful stop. Combined recovery includes a snapshot followed by newer AOF writes.
- Three live TCP nodes store/retrieve 2,000 deterministic keys; cross-node MGET preserves order.
- Four-node migration verifies every value, retained TTL, and no lost keys; failed copy retains source.
- Invalid endpoint/identity changes are rejected before migration.
- Stopping an owner produces NODE_UNAVAILABLE; repeated probes mark it unavailable.
- CLI command execution and destructive-command confirmation checks.
- Small benchmark completes successfully and cleans up its working set.

## Hashing measurements

SHA-256, 128 virtual nodes per physical node, 10,000 deterministic `key:<index>` keys:

| Owner | Keys |
| --- | ---: |
| node-a | 3,392 |
| node-b | 3,782 |
| node-c | 2,826 |

Adding node-d moves **27.73%** of keys; naive SHA-256 modulo count moves **75.16%**.
The ring test also verifies that keys which move on addition move only to the added node.

## Benchmark observation

Actual local TCP run with server/client on the same asyncio event loop, persistence disabled,
10 clients, 1,000 requests, 128-byte values, 80% GET / 20% SET, a preseeded 100-key working set.
Setup and cleanup are excluded. Percentiles describe successful requests, including pool waits.

| Metric | Value |
| --- | ---: |
| Successful / failed | 1,000 / 0 |
| Elapsed seconds | 0.424520 |
| Requests/sec | 2,355.60 |
| Successful value MiB/sec | 0.28755 |
| Average latency ms | 4.22056 |
| P50 ms | 4.14950 |
| P95 ms | 6.40650 |
| P99 ms | 8.05260 |
| Minimum / maximum ms | 1.06140 / 13.32770 |

Reproduce with `python scripts/verify.py`. Raw local output is ignored at
`benchmark-data/verification.json`. A new run will produce different timing numbers.
This small run is not a capacity estimate, production guarantee, or Redis comparison.

## Scope and remaining work

Replication is deliberately deferred: no write ACK modes, replica ordering, replica reads,
or fallback guarantees are exposed or claimed. No replication verification was run.
Rebalancing is implemented and verified only with paused writes and noeviction destinations.
There is no distributed transaction, production authentication, TLS, automatic failover,
consensus, AOF compaction, or exact process-memory cap. See README for persistence loss windows,
topology requirements, scan semantics, and failure limitations.

During implementation verification, no Git commits or pushes were made. Runtime data, virtual environments, test caches,
package caches and raw benchmark output are ignored. All test/benchmark servers exited.
