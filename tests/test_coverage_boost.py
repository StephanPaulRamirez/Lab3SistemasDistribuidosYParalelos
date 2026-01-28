
import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from metrics_api.app import app
from publisher.app import main as publisher_main

client = TestClient(app)

@patch("metrics_api.app.get_connection")
def test_get_metric_events_found(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    mock_cursor.execute.return_value.fetchall.return_value = [
        ("id1", "2025-01-01T00:00:00+00:00", "norte", "src", '{"foo":"bar"}')
    ]
    
    resp = client.get("/metrics/1/events")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["event_id"] == "id1"
    assert data[0]["payload"]["foo"] == "bar"

@patch("metrics_api.app.get_connection")
def test_get_metric_events_not_found(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    mock_cursor.execute.return_value.fetchall.return_value = []
    
    resp = client.get("/metrics/999/events")
    assert resp.status_code == 404

@patch("publisher.app.redis.Redis")
@patch("time.sleep")
def test_publisher_main_loop(mock_sleep, mock_redis_cls):
    # Tests that publisher main loop runs and produces events
    # We need to break the loop
    mock_client = MagicMock()
    mock_redis_cls.return_value = mock_client
    
    mock_sleep.side_effect = Exception("Break")
    
    try:
        publisher_main()
    except Exception as e:
        assert str(e) == "Break"
        
    # Check that it called xadd at least once (since it runs immediately)
    # Actually publisher main loops and calls publish_event
    # It should call xadd
    assert mock_client.xadd.call_count >= 1
