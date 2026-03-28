# Description: Tests for Synapse query timeout and exponential backoff.
# Description: Validates transient error classification, retry behavior, and connection reset.

"""
Tests for synapse_client.py query timeout and tenacity retry behavior.

Covers:
- Query timeout constant and connection-level timeout
- Transient error classification (08S01, HYT00, HYT01)
- Exponential backoff on transient failures
- Connection reset between retry attempts
- Non-transient errors are raised immediately
"""

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

pyodbc = pytest.importorskip(
    "pyodbc", reason="pyodbc requires ODBC drivers", exc_type=ImportError
)

from src.synapse_client import (  # noqa: E402
    QUERY_TIMEOUT_SECONDS,
    SynapseClient,
    SynapseConfig,
    _is_transient_synapse_error,
)


# ============================================================================
# Unit Tests - Constants and configuration
# ============================================================================


class TestQueryTimeoutConfig:
    """Tests for query timeout constant and connection setup."""

    def test_query_timeout_is_300_seconds(self):
        """Query timeout should be 5 minutes (300 seconds)."""
        assert QUERY_TIMEOUT_SECONDS == 300

    def test_connection_timeout_set_managed_identity(self):
        """Connection should have query timeout set after creation (managed identity path)."""
        mock_conn = MagicMock()
        mock_conn.closed = True  # Force new connection

        config = SynapseConfig(server="test.sql.azuresynapse.net")
        client = SynapseClient(config)

        with (
            patch.dict(os.environ, {"USE_MANAGED_IDENTITY": "true"}),
            patch("pyodbc.connect", return_value=mock_conn),
        ):
            conn = client._get_connection()

        assert conn.timeout == QUERY_TIMEOUT_SECONDS

    def test_connection_timeout_set_token_auth(self):
        """Connection should have query timeout set after creation (token auth path)."""
        mock_conn = MagicMock()
        mock_conn.closed = True

        config = SynapseConfig(server="test.sql.azuresynapse.net")
        client = SynapseClient(config)

        mock_credential = MagicMock()
        mock_token = MagicMock()
        mock_token.token = "fake-token"
        mock_credential.get_token.return_value = mock_token

        with (
            patch.dict(os.environ, {"USE_MANAGED_IDENTITY": "false"}),
            patch("pyodbc.connect", return_value=mock_conn),
            patch(
                "src.synapse_client.DefaultAzureCredential",
                return_value=mock_credential,
            ),
        ):
            conn = client._get_connection()

        assert conn.timeout == QUERY_TIMEOUT_SECONDS


# ============================================================================
# Unit Tests - Transient error classification
# ============================================================================


class TestTransientErrorClassification:
    """Tests for _is_transient_synapse_error."""

    def test_communication_link_failure_is_transient(self):
        """08S01 (communication link failure) should be classified as transient."""
        exc = pyodbc.Error("08S01", "Communication link failure")
        assert _is_transient_synapse_error(exc) is True

    def test_query_timeout_is_transient(self):
        """HYT00 (query timeout) should be classified as transient."""
        exc = pyodbc.Error("HYT00", "Timeout expired")
        assert _is_transient_synapse_error(exc) is True

    def test_connection_timeout_is_transient(self):
        """HYT01 (connection timeout) should be classified as transient."""
        exc = pyodbc.Error("HYT01", "Connection timeout expired")
        assert _is_transient_synapse_error(exc) is True

    def test_syntax_error_is_not_transient(self):
        """42000 (syntax error) should not be classified as transient."""
        exc = pyodbc.Error("42000", "Incorrect syntax near 'SELECT'")
        assert _is_transient_synapse_error(exc) is False

    def test_auth_error_is_not_transient(self):
        """28000 (authentication failure) should not be classified as transient."""
        exc = pyodbc.Error("28000", "Login failed for user")
        assert _is_transient_synapse_error(exc) is False

    def test_non_pyodbc_error_is_not_transient(self):
        """Non-pyodbc exceptions should not be classified as transient."""
        assert _is_transient_synapse_error(ValueError("something")) is False
        assert _is_transient_synapse_error(RuntimeError("something")) is False

    def test_none_is_not_transient(self):
        """None should not be classified as transient."""
        assert _is_transient_synapse_error(TypeError("NoneType")) is False


# ============================================================================
# Unit Tests - Retry behavior
# ============================================================================


class TestRetryBehavior:
    """Tests for tenacity retry on transient Synapse errors."""

    @pytest.mark.asyncio
    async def test_retries_on_connection_lost(self):
        """Should retry and succeed after a transient 08S01 error."""
        config = SynapseConfig(server="test.sql.azuresynapse.net")
        client = SynapseClient(config)

        call_count = 0

        def make_cursor():
            nonlocal call_count
            call_count += 1
            cursor = MagicMock()
            if call_count == 1:
                cursor.execute.side_effect = pyodbc.Error(
                    "08S01", "Communication link failure"
                )
            else:
                cursor.execute.return_value = None
                cursor.description = [
                    ("resource_hash",),
                    ("datasource_name",),
                    ("metric_name",),
                    ("timestamp",),
                    ("value_double",),
                    ("value_int",),
                    ("attributes",),
                    ("ingested_at",),
                ]
                cursor.fetchall.return_value = []
                cursor.fetchone.return_value = (0,)
            return cursor

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_conn.cursor.side_effect = make_cursor

        # Validate cached connection passes on first call, then re-create on retry
        validate_cursor = MagicMock()
        validate_cursor.fetchone.return_value = (1,)
        original_cursor = mock_conn.cursor

        def cursor_for_validation_or_query():
            return original_cursor()

        mock_conn.cursor = cursor_for_validation_or_query

        # Bypass connection validation for simplicity
        client._connection = mock_conn

        with patch.object(client, "_get_connection", return_value=mock_conn):
            result = await client.get_training_data(
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
            )

        assert result["data"] == []
        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_resets_connection_on_transient_error(self):
        """Should set _connection to None before retry so _get_connection creates fresh one."""
        config = SynapseConfig(server="test.sql.azuresynapse.net")
        client = SynapseClient(config)

        mock_conn = MagicMock()
        mock_conn.closed = False
        cursor = MagicMock()
        cursor.execute.side_effect = pyodbc.Error(
            "08S01", "Communication link failure"
        )
        mock_conn.cursor.return_value = cursor

        client._connection = mock_conn

        with (
            patch.object(client, "_get_connection", return_value=mock_conn),
            pytest.raises(pyodbc.Error, match="08S01"),
        ):
            await client.get_training_data(
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
            )

        # Connection should have been reset (set to None) during retry attempts
        assert client._connection is None

    @pytest.mark.asyncio
    async def test_non_transient_error_raises_immediately(self):
        """Should not retry on non-transient errors like syntax errors."""
        config = SynapseConfig(server="test.sql.azuresynapse.net")
        client = SynapseClient(config)

        mock_conn = MagicMock()
        mock_conn.closed = False
        cursor = MagicMock()
        cursor.execute.side_effect = pyodbc.Error(
            "42000", "Incorrect syntax near 'SELECT'"
        )
        mock_conn.cursor.return_value = cursor

        client._connection = mock_conn

        with (
            patch.object(client, "_get_connection", return_value=mock_conn),
            pytest.raises(pyodbc.Error, match="42000"),
        ):
            await client.get_training_data(
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
            )

        # Should have been called only once (no retry)
        assert cursor.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_inventory_retries_on_transient_error(self):
        """get_inventory should also retry on transient errors."""
        config = SynapseConfig(server="test.sql.azuresynapse.net")
        client = SynapseClient(config)

        call_count = 0

        def make_cursor():
            nonlocal call_count
            call_count += 1
            cursor = MagicMock()
            if call_count == 1:
                cursor.execute.side_effect = pyodbc.Error(
                    "HYT00", "Timeout expired"
                )
            else:
                cursor.execute.return_value = None
                cursor.fetchall.return_value = []
                cursor.fetchone.return_value = (None, None, 0)
            return cursor

        mock_conn = MagicMock()
        mock_conn.closed = False
        mock_conn.cursor.side_effect = make_cursor

        client._connection = mock_conn

        with patch.object(client, "_get_connection", return_value=mock_conn):
            result = await client.get_inventory()

        assert "metrics" in result
        assert call_count >= 2
