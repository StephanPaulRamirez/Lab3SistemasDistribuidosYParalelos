from datetime import datetime, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from aggregator.app import EventContext, update_metrics  # type: ignore[attr-defined]


def _ctx(source: str, payload: dict) -> EventContext:
    event = {
        "event_id": "e-1",
        "timestamp": datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc).isoformat(),
        "region": "norte",
        "source": source,
        "schema_version": "1.0",
        "correlation_id": "corr-test",
        "payload": payload,
    }
    return EventContext(event=event, date="2025-01-15", region="norte")


def test_security_incident_aggregates_by_severity_and_type():
    state = {("2025-01-15", "norte"): {}}
    ctx = _ctx(
        "security.incident",
        {
            "crime_type": "theft",
            "severity": "medium",
            "location": {"latitude": 0, "longitude": 0},
            "reported_by": "citizen",
        },
    )
    update_metrics(state, ctx)

    metrics = state[("2025-01-15", "norte")]["security.incident"]
    assert metrics["count"] == 1
    assert metrics["by_severity"]["medium"] == 1
    assert metrics["by_crime_type"]["theft"] == 1


def test_migration_case_aggregates_by_status():
    state = {("2025-01-15", "norte"): {}}
    ctx = _ctx(
        "migration.case",
        {
            "case_id": "mig-1",
            "case_type": "asylum",
            "status": "approved",
            "origin_country": "country-x",
            "application_date": "2025-01-01",
        },
    )
    update_metrics(state, ctx)

    metrics = state[("2025-01-15", "norte")]["migration.case"]
    assert metrics["count"] == 1
    assert metrics["by_status"]["approved"] == 1


def test_survey_victimization_counts_reported_true():
    state = {("2025-01-15", "norte"): {}}
    ctx = _ctx(
        "survey.victimization",
        {
            "survey_id": "s-1",
            "respondent_age": 30,
            "victimization_type": "property_crime",
            "incident_date": "2025-01-10",
            "reported": True,
        },
    )
    update_metrics(state, ctx)

    metrics = state[("2025-01-15", "norte")]["survey.victimization"]
    assert metrics["count"] == 1
    assert metrics["_reported_true"] == 1
