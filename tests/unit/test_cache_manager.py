"""
Unit Tests for CacheManager (MESI Protocol)
Tests the local state-machine logic only — Redis & HTTP calls are mocked.
"""
import pytest
from unittest.mock import AsyncMock, patch

# Import at module level — conftest.py already set env vars
import src.nodes.cache_node as cache_module
from src.nodes.cache_node import CacheManager, CacheState, BusMessage


# ─── Fixture ──────────────────────────────────────────────────────────────────
@pytest.fixture()
def cache_manager():
    """Returns a fresh CacheManager with Redis and HTTP fully mocked."""
    mgr = CacheManager(capacity=3)

    # Replace instance-level clients with mocks
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock()
    mgr.redis = mock_redis

    mgr.peer_urls = {}  # No peers → no bus traffic
    mgr.http_client = AsyncMock()

    return mgr, CacheState


# ── State Tests ───────────────────────────────────────────────────────────────

class TestMESIStates:
    @pytest.mark.asyncio
    async def test_write_creates_modified_state(self, cache_manager):
        """Writing to a new key should set state to MODIFIED."""
        mgr, CacheState = cache_manager
        await mgr.write("price", "1000")
        assert mgr.cache["price"].state == CacheState.MODIFIED
        assert mgr.cache["price"].value == "1000"

    @pytest.mark.asyncio
    async def test_read_miss_creates_exclusive_when_no_sharers(self, cache_manager):
        """Cache miss with no peers holding copy → EXCLUSIVE state."""
        mgr, CacheState = cache_manager
        mgr.redis.get = AsyncMock(return_value="42")
        val = await mgr.read("key_x")
        assert val == "42"
        assert mgr.cache["key_x"].state == CacheState.EXCLUSIVE

    @pytest.mark.asyncio
    async def test_read_miss_returns_none_when_not_in_memory(self, cache_manager):
        """Cache miss with nothing in Redis returns None."""
        mgr, CacheState = cache_manager
        mgr.redis.get = AsyncMock(return_value=None)
        val = await mgr.read("nonexistent")
        assert val is None

    @pytest.mark.asyncio
    async def test_write_then_read_hits_cache(self, cache_manager):
        """After writing, a read should hit the local cache (no Redis call)."""
        mgr, CacheState = cache_manager
        await mgr.write("name", "Laptop ROG")
        mgr.redis.get = AsyncMock(return_value=None)  # Redis has nothing
        val = await mgr.read("name")
        assert val == "Laptop ROG"

    @pytest.mark.asyncio
    async def test_overwrite_stays_modified(self, cache_manager):
        """Overwriting an existing MODIFIED entry stays MODIFIED."""
        mgr, CacheState = cache_manager
        await mgr.write("price", "1000")
        await mgr.write("price", "5000")
        assert mgr.cache["price"].state == CacheState.MODIFIED
        assert mgr.cache["price"].value == "5000"


# ── LRU Eviction Tests ────────────────────────────────────────────────────────

class TestLRUEviction:
    @pytest.mark.asyncio
    async def test_eviction_triggers_at_capacity(self, cache_manager):
        """Cache should evict when it reaches capacity."""
        mgr, CacheState = cache_manager  # capacity = 3
        await mgr.write("k1", "v1")
        await mgr.write("k2", "v2")
        await mgr.write("k3", "v3")
        # Writing k4 should evict k1 (LRU)
        await mgr.write("k4", "v4")
        assert len(mgr.cache) == 3
        assert "k1" not in mgr.cache
        assert "k4" in mgr.cache

    @pytest.mark.asyncio
    async def test_eviction_writes_modified_to_redis(self, cache_manager):
        """Evicting a MODIFIED entry must flush it to Redis."""
        mgr, CacheState = cache_manager  # capacity = 3
        await mgr.write("k1", "important_data")
        await mgr.write("k2", "v2")
        await mgr.write("k3", "v3")
        # Trigger eviction of k1 (which is MODIFIED)
        await mgr.write("k4", "v4")
        mgr.redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_eviction_count_increments(self, cache_manager):
        """Eviction metrics counter should increment."""
        mgr, CacheState = cache_manager  # capacity = 3
        await mgr.write("k1", "v1")
        await mgr.write("k2", "v2")
        await mgr.write("k3", "v3")
        await mgr.write("k4", "v4")
        assert mgr.metrics["evictions"] == 1


# ── Metrics Tests ─────────────────────────────────────────────────────────────

class TestMetrics:
    @pytest.mark.asyncio
    async def test_hit_counter_increments_on_cache_hit(self, cache_manager):
        """metrics['hits'] must increment when the read hits local cache."""
        mgr, _ = cache_manager
        await mgr.write("k", "val")
        await mgr.read("k")
        assert mgr.metrics["hits"] >= 1

    @pytest.mark.asyncio
    async def test_miss_counter_increments_on_cache_miss(self, cache_manager):
        """metrics['misses'] must increment when the read misses local cache."""
        mgr, _ = cache_manager
        mgr.redis.get = AsyncMock(return_value="redis_val")
        await mgr.read("unknown_key")
        assert mgr.metrics["misses"] >= 1

    @pytest.mark.asyncio
    async def test_get_metrics_returns_correct_structure(self, cache_manager):
        """get_metrics() should return a dict with required keys."""
        mgr, _ = cache_manager
        metrics = mgr.get_metrics()
        assert "node_id" in metrics
        assert "cache_size" in metrics
        assert "capacity" in metrics
        assert "metrics" in metrics
        assert "cache_state" in metrics


# ── Snoop (Bus) Tests ─────────────────────────────────────────────────────────

class TestBusSnoop:
    @pytest.mark.asyncio
    async def test_snoop_busrd_invalidates_modified_and_flushes(self, cache_manager):
        """BusRd on a MODIFIED line: flush to Redis + downgrade to SHARED."""
        mgr, CacheState = cache_manager
        await mgr.write("shared_key", "100")  # state = MODIFIED
        from src.nodes.cache_node import BusMessage
        msg = BusMessage(type="BusRd", key="shared_key", sender_id="node_2")
        response = await mgr.handle_snoop(msg)
        assert response["has_copy"] is True
        assert mgr.cache["shared_key"].state == CacheState.SHARED
        mgr.redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_snoop_busrdx_invalidates_our_copy(self, cache_manager):
        """BusRdX: another cache wants exclusive write, we must invalidate."""
        mgr, CacheState = cache_manager
        await mgr.write("exclusive_key", "data")
        from src.nodes.cache_node import BusMessage
        msg = BusMessage(type="BusRdX", key="exclusive_key", sender_id="node_2")
        await mgr.handle_snoop(msg)
        assert "exclusive_key" not in mgr.cache

    @pytest.mark.asyncio
    async def test_snoop_on_unknown_key_returns_no_copy(self, cache_manager):
        """Snooping for a key we don't have should return has_copy=False."""
        mgr, CacheState = cache_manager
        from src.nodes.cache_node import BusMessage
        msg = BusMessage(type="BusRd", key="missing_key", sender_id="node_2")
        response = await mgr.handle_snoop(msg)
        assert response["has_copy"] is False
