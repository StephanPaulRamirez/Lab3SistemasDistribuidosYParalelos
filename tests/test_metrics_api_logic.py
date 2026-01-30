import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
import json
from metrics_api.app import app

client = TestClient(app)

def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}

@patch("metrics_api.app.get_connection")
def test_get_metrics_found(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock DB rows: metric_id, date, region, metrics_json
    mock_cursor.execute.return_value.fetchall.return_value = [
        (1, "2025-01-01", "norte", '{"count": 10}')
    ]
    
    resp = client.get("/metrics?date=2025-01-01")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["region"] == "norte"
    assert data[0]["metrics"]["count"] == 10

@patch("metrics_api.app.get_connection")
def test_get_metrics_not_found(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    mock_cursor.execute.return_value.fetchall.return_value = []
    
    resp = client.get("/metrics?date=2025-01-01")
    assert resp.status_code == 404

@patch("metrics_api.app.get_connection")
def test_dashboard_html(mock_get_conn):
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Dashboard" in resp.text

def test_get_system_stats():
    resp = client.get("/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "uptime_seconds" in data
    assert "metrics" in data
    assert "throughput_rps" in data["metrics"]

@patch("metrics_api.app.get_connection")
def test_exportar_metrics_html(mock_get_conn):
    mock_conn = MagicMock()
    # mock context manager
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    
    # Mock return for query
    # date, region, metrics_json
    mock_conn.execute.return_value.fetchall.return_value = [
        ("2025-01-01", "norte", json.dumps({
            "security.incident": {"count": 5, "by_severity": {"high": 5}},
            "migration.case": {"count": 2, "by_status": {"open": 2}},
            "survey.victimization": {"count": 100, "reported_rate": 0.3}
        }))
    ]
    
    resp = client.get("/exportar-html?date=2025-01-01")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "Reporte Operativo" in resp.text
    assert "norte" in resp.text

@patch("metrics_api.app.get_connection")
def test_exportar_metrics_html_no_data(mock_get_conn):
    mock_conn = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.execute.return_value.fetchall.return_value = []
    
    resp = client.get("/exportar-html?date=2025-01-01")
    assert resp.status_code == 200
    assert "No hay datos" in resp.text

@patch("metrics_api.app.get_connection")
def test_get_metric_events(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    # event_id, timestamp, region, source, payload_json
    mock_cursor.execute.return_value.fetchall.return_value = [
        ("e1", "2025-01-01T10:00:00Z", "norte", "src1", "{}")
    ]
    
    resp = client.get("/metrics/1/events")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["event_id"] == "e1"

@patch("metrics_api.app.get_connection")
def test_get_metric_events_not_found(mock_get_conn):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_conn.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    
    mock_cursor.execute.return_value.fetchall.return_value = []
    
    resp = client.get("/metrics/999/events")
    assert resp.status_code == 404

@patch("metrics_api.app.get_redis_connection")
def test_get_alerts(mock_get_redis):
    mock_redis = MagicMock()
    mock_get_redis.return_value = mock_redis
    
    # Mock xrevrange
    # list of (msg_id, fields_dict)
    payload = {
        "alert_id": "a1",
        "timestamp": "2025-01-01T12:00:00Z",
        "region": "norte",
        "anomaly_type": "spike",
        "severity": "high",
        "description": "desc",
        "detection_method": "method"
    }
    
    mock_redis.xrevrange.return_value = [
        ("1-0", {"payload": json.dumps(payload)})
    ]
    
    resp = client.get("/alerts")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["alerts"]) == 1
    assert data["alerts"][0]["alert_id"] == "a1"

@patch("metrics_api.app.get_redis_connection")
def test_get_alerts_filtered(mock_get_redis):
    mock_redis = MagicMock()
    mock_get_redis.return_value = mock_redis
    
    payload1 = {"region": "norte", "severity": "high"}
    payload2 = {"region": "sur", "severity": "low"}
    
    mock_redis.xrevrange.return_value = [
        ("1-0", {"payload": json.dumps(payload1)}),
        ("2-0", {"payload": json.dumps(payload2)})
    ]
    
    # Filter by region
    resp = client.get("/alerts?region=norte")
    data = resp.json()
    assert len(data["alerts"]) == 1
    assert data["alerts"][0]["region"] == "norte"
    
    # Filter by severity
    resp = client.get("/alerts?severity=low")
    data = resp.json()
    assert len(data["alerts"]) == 1
    assert data["alerts"][0]["severity"] == "low"
    
@patch("metrics_api.app.get_redis_connection")
def test_get_alerts_error(mock_get_redis):
    mock_get_redis.side_effect = Exception("Redis Down")
    resp = client.get("/alerts")
    assert resp.status_code == 503