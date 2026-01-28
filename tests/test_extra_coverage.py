
import pytest
from unittest.mock import MagicMock, patch
from publisher.app import _total_inflight, _setup_random
# We need ensure_groups from somewhere, it's duplicated in services. 
# Let's test one of them, e.g. validator.app.ensure_groups
from validator.app import ensure_groups, validate_event, ValidationResult

def test_publisher_total_inflight_error():
    client = MagicMock()
    # xlen raises error
    from redis import RedisError
    client.xlen.side_effect = RedisError("Fail")
    
    # Should catch exception and continue (return 0 or partial)
    total = _total_inflight(client)
    assert total == 0

def test_publisher_setup_random():
    with patch("publisher.app.SEED", "123"):
        with patch("random.seed") as mock_seed:
            _setup_random()
            mock_seed.assert_called_with(123)

    with patch("publisher.app.SEED", "abc"):
        with patch("random.seed") as mock_seed:
            _setup_random()
            mock_seed.assert_called_with("abc")

def test_validator_ensure_groups_creates():
    client = MagicMock()
    # Mock xgroup_create to work
    # Mock xinfo_groups to return empty or raise error (if groups don't exist)
    # The code usually does: try xgroup_create except ResponseError("BUSYGROUP")
    
    # invalid case: successful create
    ensure_groups(client)
    assert client.xgroup_create.call_count >= 1

def test_validator_ensure_groups_already_exists():
    client = MagicMock()
    # Raise BUSYGROUP error
    from redis.exceptions import ResponseError
    client.xgroup_create.side_effect = ResponseError("BUSYGROUP Consumer Group name already exists")
    
    # Should not crash
    ensure_groups(client)

@patch("validator.app.SCHEMAS", {})   
def test_validate_event_no_schema():
    res = validate_event({
        "event_id": "00000000-0000-0000-0000-000000000000",
        "timestamp": "2025-01-01T00:00:00Z",
        "region": "norte",
        "source": "unknown-stream",
        "schema_version": "1.0",
        "correlation_id": "123",
        "payload": {}
    }, "unknown-stream")
    assert res.ok is True
