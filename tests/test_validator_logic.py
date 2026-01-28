
import pytest
from validator.app import _compute_backoff, validate_event, ValidationResult

def test_compute_backoff_default_exponential():
    # Assuming default env vars: RETRY_STRATEGY='exponential', RETRY_INITIAL_INTERVAL=1.0
    # attempt 1 -> 1.0 * 2^0 = 1.0
    # attempt 2 -> 1.0 * 2^1 = 2.0
    # attempt 3 -> 1.0 * 2^2 = 4.0
    assert _compute_backoff(1) == 1.0
    assert _compute_backoff(2) == 2.0
    assert _compute_backoff(3) == 4.0

def test_validate_event_invalid_uuid():
    event = {
        "event_id": "not-a-uuid",
        "timestamp": "2025-01-01T12:00:00Z",
        "region": "norte",
        "source": "security.incident",
        "schema_version": "1.0",
        "correlation_id": "123",
        "payload": {}
    }
    result = validate_event(event, "security.incident")
    assert not result.ok
    assert "invalid uuid" in result.error

def test_validate_event_invalid_timestamp_format():
    event = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2025/01/01",
        "region": "norte",
        "source": "security.incident",
        "schema_version": "1.0",
        "correlation_id": "123",
        "payload": {}
    }
    result = validate_event(event, "security.incident")
    assert not result.ok
    assert "invalid timestamp" in result.error

def test_validate_event_no_timezone():
    event = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2025-01-01T12:00:00", # No Z or offset
        "region": "norte",
        "source": "security.incident",
        "schema_version": "1.0",
        "correlation_id": "123",
        "payload": {}
    }
    result = validate_event(event, "security.incident")
    assert not result.ok
    assert "timezone" in result.error

def test_validate_event_mismatched_source():
    event = {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "timestamp": "2025-01-01T12:00:00Z",
        "region": "norte",
        "source": "migration.case",
        "schema_version": "1.0",
        "correlation_id": "123",
        "payload": {}
    }
    result = validate_event(event, "security.incident")
    assert not result.ok
    assert "does not match stream" in result.error
