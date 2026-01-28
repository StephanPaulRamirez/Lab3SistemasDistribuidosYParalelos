
import pytest
from unittest.mock import MagicMock, patch
import json
from validator.app import process_pending_and_new, main

# Mock constants to avoid env var issues
@patch("validator.app.INPUT_STREAMS", ["stream1"])
@patch("validator.app.CONSUMER_GROUP", "group1")
@patch("validator.app.CONSUMER_NAME", "consumer1")
def test_process_batch_reads_new_messages():
    client = MagicMock()
    # Mock xpending_range to return empty (no pending)
    client.xpending_range.return_value = []
    
    # Mock xreadgroup to return 1 message
    client.xreadgroup.return_value = [
        ("stream1", [
            ("id-1", {"data": json.dumps({"event_id": "e1", "source": "stream1", "timestamp": "2025-01-01T00:00:00Z", "region": "x", "schema_version": "1.0", "correlation_id": "c", "payload": {}})})
        ])
    ]
    
    # Mock validate_event to return True
    with patch("validator.app.validate_event") as mock_validate:
        mock_validate.return_value.ok = True
        
        # Break loop with exception
        client.xreadgroup.side_effect = [
            [
                ("stream1", [
                    ("id-1", {"data": json.dumps({"event_id": "e1", "source": "stream1", "timestamp": "2025-01-01T00:00:00Z", "region": "x", "schema_version": "1.0", "correlation_id": "c", "payload": {}})})
                ])
            ],
            Exception("BreakLoop")
        ]

        try:
            process_pending_and_new(client)
        except Exception:
            pass
        
        # We can only verify side effects like ack
        assert client.xack.call_count == 1

@patch("validator.app.INPUT_STREAMS", ["stream1"])
def test_process_batch_handles_invalid_json():
    client = MagicMock()
    client.xpending_range.return_value = []
    # Invalid JSON in data
    client.xreadgroup.side_effect = [
        [
            ("stream1", [("id-bad", {"data": "not-json"})])
        ],
        Exception("BreakLoop")
    ]
    
    try:
        process_pending_and_new(client)
    except Exception:
        pass
    
    # ensure it goes to deadletter
    assert client.xadd.call_count == 1
    assert "deadletter" in client.xadd.call_args[0][0]

@patch("validator.app.process_pending_and_new")
@patch("validator.app.ensure_groups")
@patch("validator.app._load_schemas")
@patch("redis.Redis")
def test_main_initializes_and_loops(mock_redis_cls, mock_load, mock_ensure, mock_loop):
    # Mock loop to raise exception to break infinite loop
    mock_loop.side_effect = Exception("BreakLoop")
    
    try:
        main()
    except Exception as e:
        assert str(e) == "BreakLoop"
    
    assert mock_redis_cls.call_count == 1
    assert mock_load.call_count == 1
    assert mock_ensure.call_count == 1
    assert mock_loop.call_count == 1
