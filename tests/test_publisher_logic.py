
import pytest
from publisher.app import build_security_incident, build_survey_victimization, build_migration_case

def test_build_security_incident_structure():
    event = build_security_incident()
    assert event["source"] == "security.incident"
    assert "event_id" in event
    assert "timestamp" in event
    assert "region" in event
    assert "payload" in event
    
    payload = event["payload"]
    assert "crime_type" in payload
    assert "severity" in payload
    assert "location" in payload

def test_build_survey_victimization_structure():
    event = build_survey_victimization()
    assert event["source"] == "survey.victimization"
    payload = event["payload"]
    assert "survey_id" in payload
    assert "respondent_age" in payload
    assert "reported" in payload

def test_build_migration_case_structure():
    event = build_migration_case()
    assert event["source"] == "migration.case"
    payload = event["payload"]
    assert "case_id" in payload
    assert "status" in payload
    assert "origin_country" in payload
