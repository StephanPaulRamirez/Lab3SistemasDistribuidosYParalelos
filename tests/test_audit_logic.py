
import pytest
from unittest.mock import MagicMock, patch, call
import json
from audit.app import process_events, process_metrics, ensure_groups
import redis

@patch("audit.app.db_connect")
def test_process_events_persists(mock_db_connect):
    client = MagicMock()
    # Mock redis response: [ (stream, [ (msg_id, fields) ]) ]
    client.xreadgroup.return_value = [
        ("validated.events", [
            ("id-1", {"data": json.dumps({
                "event_id": "e1",
                "timestamp": "2025-01-01T10:00:00Z",
                "region": "norte",
                "source": "src",
                "payload": {"foo": "bar"}
            })})
        ])
    ]
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db_connect.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    process_events(client)
    
    assert mock_cursor.execute.call_count == 1
    # Check insert
    args, _ = mock_cursor.execute.call_args
    assert "INSERT OR IGNORE INTO input_events" in args[0]
    params = args[1]
    assert params[0] == "e1"
    assert params[4] == '{"foo": "bar"}' # payload json
    
    assert client.xack.call_count == 1

@patch("audit.app.db_connect")
def test_process_metrics_persists_and_links(mock_db_connect):
    client = MagicMock()
    client.xreadgroup.return_value = [
        ("metrics.daily", [
            ("id-2", {"data": json.dumps({
                "date": "2025-01-01",
                "region": "norte",
                "metrics": {"count": 10}
            })})
        ])
    ]
    
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db_connect.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    mock_cursor.lastrowid = 100 # metric_id

    process_metrics(client)
    
    # 2 executes: INSERT metric, INSERT link
    assert mock_cursor.execute.call_count == 2
    
    # First call: Insert metric
    args1, _ = mock_cursor.execute.call_args_list[0]
    assert "INSERT INTO output_metrics" in args1[0]
    assert args1[1][0] == "2025-01-01"
    
    # Second call: Link
    args2, _ = mock_cursor.execute.call_args_list[1]
    assert "INSERT INTO event_metric_link" in args2[0]
    # Check params for link: metric_id=100 from lastrowid
    # The query uses a SELECT so we check if params are (metric_id, date, region)
    link_params = args2[1]
    assert link_params[0] == 100 # metric_id
    assert link_params[1] == "2025-01-01"
    assert link_params[2] == "norte"

    assert client.xack.call_count == 1

@patch("audit.app.db_connect")
def test_process_events_invalid_json(mock_db):
    client = MagicMock()
    client.xreadgroup.return_value = [
        ("validated.events", [
            ("id-1", {"data": "invalid-json"})
        ])
    ]
    
    # Capture stdout/stderr or just ensure no crash and log call
    with patch("audit.app.log_json") as mock_log:
        process_events(client)
        assert mock_log.call_count == 1
        assert client.xack.call_count == 0 # Should NOT ack if we want to retry or deadletter? 
        # Code actually catches Exception and logs "error_persisting_event", does NOT ack?
        # Let's check code: 
        # except Exception as exc: 
        #   log_json("error", ...)
        # It swallows exception. It does NOT ack. So message remains pending.
        
@patch("audit.app.db_connect")
def test_process_events_db_error(mock_db_connect):
    client = MagicMock()
    client.xreadgroup.return_value = [
        ("validated.events", [("id-1", {"data": "{}"})])
    ]
    
    mock_conn = MagicMock()
    mock_db_connect.return_value.__enter__.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("DB Down")
    
    with patch("audit.app.log_json") as mock_log:
        process_events(client)
        assert mock_log.call_count == 1
        # log_json("error", "error_persisting_event", ...)
        # args[0] is level, args[1] is message
        args, _ = mock_log.call_args
        assert args[1] == "error_persisting_event"

@patch("audit.app.db_connect")
def test_process_metrics_invalid_json(mock_db):
    client = MagicMock()
    client.xreadgroup.return_value = [
        ("metrics.daily", [("id-1", {"data": "bad"})])
    ]
    
    with patch("audit.app.log_json") as mock_log:
        process_metrics(client)
        assert mock_log.call_count == 1

@patch("audit.app.db_connect")
def test_process_metrics_db_error(mock_db_connect):
    client = MagicMock()
    client.xreadgroup.return_value = [
        ("metrics.daily", [("id-1", {"data": "{}"})])
    ]
    mock_conn = MagicMock()
    mock_db_connect.return_value.__enter__.return_value = mock_conn
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.execute.side_effect = Exception("DB Down")
    
    with patch("audit.app.log_json") as mock_log:
        process_metrics(client)
        assert mock_log.call_count == 1

def test_ensure_groups_success():
    client = MagicMock()
    ensure_groups(client)
    assert client.xgroup_create.call_count == 2
    
def test_ensure_groups_busy_ignored():
    client = MagicMock()
    # Raise BUSYGROUP error
    client.xgroup_create.side_effect = redis.ResponseError("BUSYGROUP Consumer Group name already exists")
    
    with patch("audit.app.log_json") as mock_log:
        ensure_groups(client)
        # Should catch and log info
        assert mock_log.call_count == 2
        assert client.xgroup_create.call_count == 2

def test_ensure_groups_other_error():
    client = MagicMock()
    client.xgroup_create.side_effect = redis.ResponseError("OTHER ERROR")
    
    with pytest.raises(redis.ResponseError):
        ensure_groups(client)

