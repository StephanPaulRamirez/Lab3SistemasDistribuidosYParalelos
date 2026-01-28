
import pytest
from unittest.mock import MagicMock
from datetime import datetime, timezone
import json
from aggregator.app import _compute_backoff, parse_event, update_metrics, flush_metrics, EventContext

def test_compute_backoff_default():
    # Default is exponential
    assert _compute_backoff(1) == 1.0
    assert _compute_backoff(2) == 2.0
    assert _compute_backoff(3) == 4.0

def test_parse_event_valid():
    raw = json.dumps({
        "event_id": "e1",
        "timestamp": "2025-01-01T10:00:00Z",
        "region": "norte",
        "source": "src"
    })
    ctx = parse_event(raw)
    assert ctx.date == "2025-01-01"
    assert ctx.region == "norte"
    assert ctx.event["event_id"] == "e1"

def test_parse_event_missing_timestamp():
    raw = json.dumps({"event_id": "e1"})
    with pytest.raises(ValueError, match="missing timestamp"):
        parse_event(raw)

def test_update_metrics_security():
    state = {("2025-01-01", "norte"): {}}
    ctx = EventContext(
        event={
            "source": "security.incident",
            "payload": {"severity": "high", "crime_type": "theft"}
        },
        date="2025-01-01",
        region="norte"
    )
    update_metrics(state, ctx)
    
    m = state[("2025-01-01", "norte")]["security.incident"]
    assert m["count"] == 1
    assert m["by_severity"]["high"] == 1
    assert m["by_crime_type"]["theft"] == 1

def test_update_metrics_survey_reported_rate_logic():
    state = {("2025-01-01", "sur"): {}}
    # Event 1: reported=True
    ctx1 = EventContext(
        event={"source": "survey.victimization", "payload": {"reported": True}},
        date="2025-01-01",
        region="sur"
    )
    update_metrics(state, ctx1)
    
    # Event 2: reported=False
    ctx2 = EventContext(
        event={"source": "survey.victimization", "payload": {"reported": False}},
        date="2025-01-01",
        region="sur"
    )
    update_metrics(state, ctx2)

    m = state[("2025-01-01", "sur")]["survey.victimization"]
    assert m["count"] == 2
    assert m["_reported_true"] == 1

def test_flush_metrics_calculates_rate():
    # Setup state manually as if aggregator had populated it
    state = {
        ("2025-01-01", "sur"): {
            "survey.victimization": {
                "count": 4,
                "_reported_true": 3
            },
            "security.incident": {
                "count": 10
            }
        }
    }
    
    client = MagicMock()
    flush_metrics(client, state)
    
    # Verify client.xadd calls
    assert client.xadd.call_count == 1
    args, kwargs = client.xadd.call_args
    stream = args[0]
    data_dict = args[1] # fields is the second positional argument
    
    assert stream == "metrics.daily" 
    # Checking content
    payload_json = data_dict["data"]
    payload = json.loads(payload_json)
    
    assert payload["date"] == "2025-01-01"
    assert payload["region"] == "sur"
    
    metrics = payload["metrics"]
    assert metrics["survey.victimization"]["count"] == 4
    assert metrics["survey.victimization"]["reported_rate"] == 0.75
    assert "_reported_true" not in metrics["survey.victimization"]
    
    assert metrics["security.incident"]["count"] == 10
