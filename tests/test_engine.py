import pytest

from distributed_cache.engine import CacheEngine
from distributed_cache.errors import CacheError


def test_expiration_and_stale_heap():
    now = [0.0]
    e = CacheEngine(clock=lambda: now[0], wall=lambda: 1000 + now[0])
    e.set(b"a", b"one", ttl=1)
    e.expire(b"a", 10)
    now[0] = 2
    e.expire_due()
    assert e.get(b"a") == b"one"
    e.expire(b"a", None)
    now[0] = 20
    e.expire_due()
    assert e.ttl(b"a") == -1
    e.expire(b"a", 1)
    now[0] = 22
    assert e.get(b"a") is None
    assert e.memory_used == 0
    assert e.ttl(b"a") == -2


def test_active_expiration():
    now = [0.0]
    e = CacheEngine(clock=lambda: now[0])
    e.set(b"a", b"x", ttl=1)
    now[0] = 2
    assert e.expire_due() == 1
    assert not e.entries and e.memory_used == 0


def test_lru_and_accounting():
    e = CacheEngine(max_memory=260)
    e.set(b"a", b"1")
    e.set(b"b", b"2")
    assert e.get(b"a") == b"1"
    assert e.exists(b"b") and e.ttl(b"b") == -1
    e.set(b"c", b"3")
    assert not e.exists(b"b")
    assert e.memory_used == 260
    e.set(b"a", b"12")
    assert e.memory_used == 131
    assert e.delete(b"a") and not e.delete(b"a")
    assert e.memory_used == 0


def test_noeviction_and_oversize_preserve_state():
    e = CacheEngine(max_memory=260, eviction="noeviction")
    e.set(b"a", b"1")
    e.set(b"b", b"2")
    for value in (b"12", b"x" * 300):
        with pytest.raises(CacheError, match="memory|fit"):
            e.set(b"a", value)
        assert e.get(b"a") == b"1" and e.get(b"b") == b"2"


def test_counters_conditions_and_limits():
    e = CacheEngine(max_value=30)
    assert e.incr(b"n") == 1
    assert e.incr(b"n", -2) == -1
    assert not e.set(b"n", b"x", nx=True)
    assert not e.set(b"missing", b"x", xx=True)
    assert e.set(b"n", b"x", xx=True)
    with pytest.raises(CacheError, match="integer"):
        e.incr(b"n")
    for ttl in (-1, 0, True, float("nan"), float("inf"), 10**400):
        with pytest.raises(CacheError):
            e.set(b"a", b"x", ttl=ttl)
    with pytest.raises(CacheError):
        e.set(b"a", b"x" * 31)


def test_heap_is_bounded_and_scan_terminates():
    e = CacheEngine()
    e.set(b"a", b"v", ttl=5)
    for _ in range(500):
        e.expire(b"a", 5)
    assert len(e.expirations) <= 64
    for i in range(50):
        e.set(str(i).encode(), b"v")
    cursor, found = 0, []
    while True:
        page = e.scan(cursor, 7)
        found.extend(page["keys"])
        cursor = page["cursor"]
        if not cursor:
            break
    assert set(found) == set(e.entries)


def test_counter_preserves_exact_deadlines():
    now = [10.0]
    e = CacheEngine(clock=lambda: now[0], wall=lambda: 1000 + now[0])
    e.set(b"n", b"1", ttl=10)
    original = e.entries[b"n"]
    now[0] = 12
    e.incr(b"n")
    current = e.entries[b"n"]
    assert current.expires_at == original.expires_at
    assert current.expires_wall == original.expires_wall
    now[0] = 21
    e.expire_due()
    assert e.get(b"n") is None


def test_expiration_during_replacement_does_not_underestimate_memory(monkeypatch):
    now = [0.0]
    e = CacheEngine(max_memory=260, clock=lambda: now[0])
    e.set(b"a", b"1", ttl=1)
    e.set(b"b", b"2")
    original = e.expire_due

    def advance_and_expire():
        now[0] = 2
        return original()

    monkeypatch.setattr(e, "expire_due", advance_and_expire)
    e.set(b"a", b"v" * 100)
    assert e.memory_used == 229
    assert not e.exists(b"b")
    assert e.get(b"a") == b"v" * 100
