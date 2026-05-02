"""
Unit Tests for Security module (RBAC + Audit Log)
Tests API key validation and role enforcement without a live server.
"""
import os
import pytest
from unittest.mock import patch, mock_open
from fastapi import HTTPException


# ─── Import functions under test ──────────────────────────────────────────────
from src.utils.security import get_current_role, require_admin, audit_log, API_KEYS


# ── get_current_role Tests ────────────────────────────────────────────────────

class TestGetCurrentRole:
    def test_admin_key_returns_admin_role(self):
        """The admin API key should return the 'admin' role."""
        role = get_current_role(api_key="admin-secret-key-123")
        assert role == "admin"

    def test_viewer_key_returns_viewer_role(self):
        """The viewer API key should return the 'viewer' role."""
        role = get_current_role(api_key="viewer-secret-key-456")
        assert role == "viewer"

    def test_missing_api_key_raises_401(self):
        """Missing API key (None) must raise HTTP 401."""
        with pytest.raises(HTTPException) as exc_info:
            get_current_role(api_key=None)
        assert exc_info.value.status_code == 401

    def test_invalid_api_key_raises_403(self):
        """An unrecognised API key must raise HTTP 403."""
        with pytest.raises(HTTPException) as exc_info:
            get_current_role(api_key="wrong-key-xyz")
        assert exc_info.value.status_code == 403

    def test_empty_string_key_raises_401(self):
        """An empty string (falsy) is treated as missing, raising 401."""
        with pytest.raises(HTTPException) as exc_info:
            get_current_role(api_key="")
        assert exc_info.value.status_code == 401


# ── require_admin Tests ───────────────────────────────────────────────────────

class TestRequireAdmin:
    def test_admin_role_passes(self):
        """Admin role should pass require_admin without raising."""
        result = require_admin(role="admin")
        assert result == "admin"

    def test_viewer_role_raises_403(self):
        """Viewer role should be rejected by require_admin."""
        with pytest.raises(HTTPException) as exc_info:
            require_admin(role="viewer")
        assert exc_info.value.status_code == 403

    def test_unknown_role_raises_403(self):
        """Any unrecognised role should be rejected."""
        with pytest.raises(HTTPException) as exc_info:
            require_admin(role="superuser")
        assert exc_info.value.status_code == 403


# ── API_KEYS Registry Tests ───────────────────────────────────────────────────

class TestAPIKeysRegistry:
    def test_all_keys_have_roles(self):
        """Every entry in API_KEYS must map to a non-empty role string."""
        for key, role in API_KEYS.items():
            assert isinstance(role, str) and len(role) > 0

    def test_at_least_one_admin_key_exists(self):
        """There must be at least one admin key registered."""
        admin_keys = [k for k, v in API_KEYS.items() if v == "admin"]
        assert len(admin_keys) >= 1


# ── audit_log Tests ───────────────────────────────────────────────────────────

class TestAuditLog:
    def test_audit_log_writes_to_file(self):
        """audit_log() should write a line to security_audit.log."""
        m = mock_open()
        with patch("builtins.open", m):
            audit_log("ACQUIRE_LOCK", "database_master", "Klien_A")
        m.assert_called_once_with("security_audit.log", "a")
        handle = m()
        handle.write.assert_called_once()

    def test_audit_log_entry_contains_expected_fields(self):
        """The written log entry must contain the action, resource, and user."""
        written_data = []
        m = mock_open()
        with patch("builtins.open", m):
            with patch("builtins.print"):  # suppress stdout
                audit_log("RELEASE_LOCK", "res_X", "Klien_B")
            handle = m()
            # Collect write arguments
            for call in handle.write.call_args_list:
                written_data.append(call[0][0])

        full_log = "".join(written_data)
        assert "RELEASE_LOCK" in full_log
        assert "res_X" in full_log
        assert "Klien_B" in full_log
        assert "AUDIT_EVENT" in full_log

    def test_audit_log_does_not_raise_on_file_error(self):
        """audit_log() must not propagate file-write exceptions."""
        with patch("builtins.open", side_effect=PermissionError("no access")):
            with patch("builtins.print"):  # suppress stdout
                # Should NOT raise
                audit_log("TEST_ACTION", "res", "user")
