"""
Unit Tests for LockState (Distributed Lock Manager)
Tests the core lock state machine without any Raft/network dependencies.
"""
import time
import pytest
from src.nodes.lock_manager import LockState


# ── Helper ────────────────────────────────────────────────────────────────────

def make_cmd(resource="res_A", client_id="client_1", lock_type="exclusive", ttl=30):
    return {"resource": resource, "client_id": client_id, "type": lock_type, "ttl": ttl}


# ── Exclusive Lock Tests ───────────────────────────────────────────────────────

class TestExclusiveLock:
    def test_acquire_exclusive_lock_success(self):
        """First client can acquire a free exclusive lock."""
        state = LockState()
        cmd = make_cmd(client_id="alice")
        assert state.acquire(cmd) is True
        assert "res_A" in state.locks

    def test_second_client_blocked_by_exclusive(self):
        """Second client cannot acquire an already exclusively-held lock."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice"))
        assert state.acquire(make_cmd(client_id="bob")) is False

    def test_reentrant_exclusive_lock(self):
        """Same client can re-acquire its own exclusive lock (renew)."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice", ttl=5))
        assert state.acquire(make_cmd(client_id="alice", ttl=60)) is True

    def test_reentrant_renews_ttl(self):
        """Re-acquiring the lock should extend the TTL."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice", ttl=5))
        before = state.locks["res_A"]["expires_at"]
        state.acquire(make_cmd(client_id="alice", ttl=300))
        after = state.locks["res_A"]["expires_at"]
        assert after > before

    def test_acquire_after_ttl_expiry(self):
        """A new client can acquire a lock after the previous one expires."""
        state = LockState()
        cmd = make_cmd(client_id="alice", ttl=0)
        state.acquire(cmd)
        # TTL=0 means it expired immediately
        time.sleep(0.01)
        assert state.acquire(make_cmd(client_id="bob", ttl=30)) is True
        assert state.locks["res_A"]["client_id"] == "bob"


# ── Exclusive Release Tests ────────────────────────────────────────────────────

class TestExclusiveRelease:
    def test_release_by_owner_succeeds(self):
        """Lock owner can release the lock."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice"))
        assert state.release(make_cmd(client_id="alice")) is True
        assert "res_A" not in state.locks

    def test_release_by_non_owner_fails(self):
        """Non-owner cannot release someone else's lock."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice"))
        assert state.release(make_cmd(client_id="bob")) is False
        assert "res_A" in state.locks

    def test_release_nonexistent_lock_returns_false(self):
        """Releasing a lock that doesn't exist should return False."""
        state = LockState()
        assert state.release(make_cmd(client_id="alice")) is False

    def test_acquire_after_release_succeeds(self):
        """A new client can acquire the lock after it has been released."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice"))
        state.release(make_cmd(client_id="alice"))
        assert state.acquire(make_cmd(client_id="bob")) is True
        assert state.locks["res_A"]["client_id"] == "bob"


# ── Shared Lock Tests ──────────────────────────────────────────────────────────

class TestSharedLock:
    def test_multiple_clients_can_hold_shared_lock(self):
        """Multiple clients can hold a shared lock simultaneously."""
        state = LockState()
        assert state.acquire(make_cmd(client_id="alice", lock_type="shared")) is True
        assert state.acquire(make_cmd(client_id="bob", lock_type="shared")) is True
        assert "alice" in state.locks["res_A"]["shared_clients"]
        assert "bob" in state.locks["res_A"]["shared_clients"]

    def test_exclusive_blocked_by_shared_lock(self):
        """Exclusive lock request is rejected when a shared lock is held."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice", lock_type="shared"))
        assert state.acquire(make_cmd(client_id="bob", lock_type="exclusive")) is False

    def test_shared_blocked_by_exclusive_lock(self):
        """Shared lock request is rejected when an exclusive lock is held."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice", lock_type="exclusive"))
        assert state.acquire(make_cmd(client_id="bob", lock_type="shared")) is False

    def test_shared_lock_release_by_one_client(self):
        """Releasing one shared client should keep the lock alive for others."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice", lock_type="shared"))
        state.acquire(make_cmd(client_id="bob", lock_type="shared"))
        state.release(make_cmd(client_id="alice", lock_type="shared"))
        assert "res_A" in state.locks
        assert "bob" in state.locks["res_A"]["shared_clients"]

    def test_shared_lock_fully_released_when_last_client_leaves(self):
        """Lock entry is deleted when last shared client releases it."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice", lock_type="shared"))
        state.release(make_cmd(client_id="alice", lock_type="shared"))
        assert "res_A" not in state.locks

    def test_duplicate_shared_client_not_added_twice(self):
        """Same client acquiring shared lock twice should not duplicate entry."""
        state = LockState()
        state.acquire(make_cmd(client_id="alice", lock_type="shared"))
        state.acquire(make_cmd(client_id="alice", lock_type="shared"))
        assert state.locks["res_A"]["shared_clients"].count("alice") == 1


# ── Multiple Resources ─────────────────────────────────────────────────────────

class TestMultipleResources:
    def test_independent_resources_dont_conflict(self):
        """Locks on different resources are independent."""
        state = LockState()
        cmd_a = make_cmd(resource="res_A", client_id="alice")
        cmd_b = make_cmd(resource="res_B", client_id="alice")
        assert state.acquire(cmd_a) is True
        assert state.acquire(cmd_b) is True
