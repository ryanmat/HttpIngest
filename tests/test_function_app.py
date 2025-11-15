# ABOUTME: Tests for function_app.py endpoints including /api/process
# ABOUTME: Tests circuit breaker, async processing, and concurrent request handling

import pytest
import json
import asyncio
import concurrent.futures
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone

from src.function_app import (
    CircuitBreaker,
    CircuitBreakerState,
    process_batch_async,
    db_circuit_breaker
)
from src.data_processor import BatchProcessingStats


class TestCircuitBreaker:
    """Tests for CircuitBreaker class."""

    def test_circuit_breaker_initial_state(self):
        """Test circuit breaker starts in closed state."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

        assert cb.can_attempt() is True
        assert cb.state.is_open is False
        assert cb.state.failures == 0

    def test_circuit_breaker_opens_after_threshold(self):
        """Test circuit breaker opens after failure threshold."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)

        # Record failures
        cb.record_failure()
        assert cb.can_attempt() is True  # Still closed

        cb.record_failure()
        assert cb.can_attempt() is True  # Still closed

        cb.record_failure()
        assert cb.can_attempt() is False  # Now open
        assert cb.state.is_open is True

    def test_circuit_breaker_half_open_after_timeout(self):
        """Test circuit breaker enters half-open state after recovery timeout."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)  # 1 second for testing

        # Open circuit
        cb.record_failure()
        cb.record_failure()

        # Should not allow attempt immediately after opening
        assert cb.can_attempt() is False

        # Wait for recovery timeout
        import time
        time.sleep(1.1)

        # Should allow one attempt (half-open)
        assert cb.can_attempt() is True

    def test_circuit_breaker_closes_after_success_threshold(self):
        """Test circuit breaker closes after success threshold in half-open state."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0, success_threshold=2)

        # Open circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state.is_open is True

        # Record successes
        cb.record_success()
        assert cb.state.is_open is True  # Still open

        cb.record_success()
        assert cb.state.is_open is False  # Now closed

    def test_circuit_breaker_get_state(self):
        """Test circuit breaker state reporting."""
        cb = CircuitBreaker(failure_threshold=3)

        state = cb.get_state()
        assert state['is_open'] is False
        assert state['failures'] == 0
        assert state['success_count'] == 0

        cb.record_failure()
        state = cb.get_state()
        assert state['failures'] == 1


class TestProcessBatchAsync:
    """Tests for async batch processing."""

    @pytest.mark.asyncio
    async def test_process_batch_async_success(self, db_connection, sample_otlp_cpu_metrics, clean_normalized_tables):
        """Test async batch processing with successful records."""
        # Insert test records
        with db_connection.cursor() as cur:
            cur.execute(
                "INSERT INTO lm_metrics (payload) VALUES (%s), (%s)",
                (json.dumps(sample_otlp_cpu_metrics), json.dumps(sample_otlp_cpu_metrics))
            )
            db_connection.commit()

        # Don't mock - let it use real get_db_connection (creates its own connection)
        stats = await process_batch_async(batch_size=10, continue_on_error=True)

        assert stats.total_records >= 2
        assert stats.successful >= 2
        assert stats.failed == 0
        assert stats.metric_data_created >= 2

    @pytest.mark.asyncio
    async def test_process_batch_async_with_errors(self, db_connection, sample_otlp_cpu_metrics, clean_normalized_tables):
        """Test async batch processing handles errors gracefully."""
        # Insert valid and invalid records
        with db_connection.cursor() as cur:
            cur.execute(
                "INSERT INTO lm_metrics (payload) VALUES (%s), (%s)",
                (json.dumps(sample_otlp_cpu_metrics), json.dumps({"invalid": "payload"}))
            )
            db_connection.commit()

        # Don't mock - let it use real get_db_connection
        stats = await process_batch_async(batch_size=10, continue_on_error=True)

        assert stats.total_records >= 2
        assert stats.successful >= 1
        assert stats.failed >= 1

    @pytest.mark.asyncio
    async def test_process_batch_async_empty(self, db_connection, clean_normalized_tables):
        """Test async batch processing with no records."""
        # Don't mock - let it use real get_db_connection
        stats = await process_batch_async(batch_size=10, continue_on_error=True)

        assert stats.total_records == 0
        assert stats.successful == 0
        assert stats.failed == 0


class TestProcessEndpoint:
    """Tests for /api/process endpoint."""

    def test_process_endpoint_requires_post(self):
        """Test that /api/process endpoint requires POST method."""
        # This test is for documentation - Azure Functions handles this
        # The route is defined with methods=["POST"]
        pass

    def test_process_endpoint_batch_size_validation(self):
        """Test batch_size parameter validation."""
        # Test invalid batch_size
        # This would be tested via HTTP requests in integration tests
        pass

    def test_process_endpoint_circuit_breaker_blocks_when_open(self):
        """Test that circuit breaker blocks requests when open."""
        # Open the circuit breaker
        for _ in range(5):
            db_circuit_breaker.record_failure()

        assert db_circuit_breaker.can_attempt() is False

        # Reset for other tests
        db_circuit_breaker.state.is_open = False
        db_circuit_breaker.state.failures = 0


class TestHealthEndpoint:
    """Tests for enhanced /api/health endpoint."""

    def test_health_endpoint_includes_processing_stats(self, db_connection, clean_normalized_tables):
        """Test that health endpoint includes processing statistics."""
        # Insert some test data
        with db_connection.cursor() as cur:
            cur.execute("INSERT INTO lm_metrics (payload) VALUES (%s)", (json.dumps({"test": "data"}),))
            db_connection.commit()

        # In a real test, we would call the health endpoint
        # For now, we verify the database queries work
        with db_connection.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM lm_metrics")
            count = cur.fetchone()[0]
            assert count >= 1

    def test_health_endpoint_includes_circuit_breaker_state(self):
        """Test that health endpoint includes circuit breaker state."""
        state = db_circuit_breaker.get_state()
        assert 'is_open' in state
        assert 'failures' in state


class TestConcurrentProcessing:
    """Tests for concurrent request handling."""

    @pytest.mark.asyncio
    async def test_concurrent_batch_processing(self, db_connection, sample_otlp_cpu_metrics, clean_normalized_tables):
        """Test that concurrent batch processing doesn't cause data loss or duplication."""
        # Insert test records
        with db_connection.cursor() as cur:
            for _ in range(10):
                cur.execute(
                    "INSERT INTO lm_metrics (payload) VALUES (%s)",
                    (json.dumps(sample_otlp_cpu_metrics),)
                )
            db_connection.commit()

        # Run concurrent batch processing (each creates its own connection)
        # Note: Due to transaction isolation, concurrent tasks may not see all records
        tasks = [
            process_batch_async(batch_size=10, continue_on_error=True),
            process_batch_async(batch_size=10, continue_on_error=True)
        ]
        results = await asyncio.gather(*tasks)

        # Verify no duplicate processing in database (the critical test)
        with db_connection.cursor() as cur:
            # Check that exactly 10 records were successfully processed (no duplicates)
            cur.execute("SELECT COUNT(*) FROM processing_status WHERE status = 'success'")
            success_count = cur.fetchone()[0]
            assert success_count == 10, f"Expected 10 unique successful records, got {success_count}"

            # Verify each record ID appears at most once (no duplicate processing_status entries)
            cur.execute("SELECT lm_metrics_id, COUNT(*) FROM processing_status GROUP BY lm_metrics_id HAVING COUNT(*) > 1")
            duplicates = cur.fetchall()
            assert len(duplicates) == 0, f"Found duplicate processing for records: {duplicates}"

        # Note: total_processed may be > success_count because both tasks may attempt
        # to process the same records, but only one will succeed due to unique constraints

    def test_concurrent_http_requests_simulation(self, db_connection, sample_otlp_cpu_metrics, clean_normalized_tables):
        """Test simulated concurrent HTTP requests to /api/process."""
        # Insert test records
        with db_connection.cursor() as cur:
            for _ in range(20):
                cur.execute(
                    "INSERT INTO lm_metrics (payload) VALUES (%s)",
                    (json.dumps(sample_otlp_cpu_metrics),)
                )
            db_connection.commit()

        def process_request():
            """Simulate a single HTTP request."""
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                # Don't mock - let it use real get_db_connection
                result = loop.run_until_complete(
                    process_batch_async(batch_size=10, continue_on_error=True)
                )
                return result
            finally:
                loop.close()

        # Run concurrent requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(process_request) for _ in range(3)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]

        # Verify all records processed
        total_successful = sum(r.successful for r in results)
        assert total_successful >= 20


class TestDatabaseFailureRecovery:
    """Tests for database failure handling and recovery."""

    def test_circuit_breaker_prevents_cascade_failures(self):
        """Test that circuit breaker prevents cascade failures."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=60)

        # Simulate failures
        for i in range(3):
            cb.record_failure()

        # Circuit should be open
        assert cb.can_attempt() is False

        # Subsequent attempts should be blocked without hitting database
        for _ in range(10):
            assert cb.can_attempt() is False

        # Failure count should not increase
        assert cb.state.failures == 3

    def test_circuit_breaker_recovery_after_success(self):
        """Test circuit breaker recovery after successful operations."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0, success_threshold=2)

        # Open circuit
        cb.record_failure()
        cb.record_failure()
        assert cb.state.is_open is True

        # Simulate successful recovery
        cb.record_success()
        cb.record_success()

        # Circuit should be closed
        assert cb.state.is_open is False
        assert cb.can_attempt() is True
