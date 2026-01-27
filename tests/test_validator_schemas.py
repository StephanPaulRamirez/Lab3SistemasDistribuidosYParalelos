import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from validator.app import validate_event, ValidationResult, SCHEMA_PATH, _load_schemas, SCHEMAS  # type: ignore[attr-defined]


def setup_module(module):  # noqa: D401
    """Ensure schemas are loaded once for all tests."""
    # Limpiar y recargar para evitar fugas entre ejecuciones
    SCHEMAS.clear()
    _load_schemas()


def _load_example(name: str) -> dict:
    path = Path("schemas") / f"{name}.schema.json"
    raw = path.read_text(encoding="utf-8")
    return json.loads(raw)["example"] if "example" in json.loads(raw) else {}


def test_security_incident_valid():
    from publisher.app import build_security_incident  # lazy import

    event = build_security_incident()
    result: ValidationResult = validate_event(event, "security.incident")
    assert result.ok, result.error


def test_survey_victimization_valid():
    from publisher.app import build_survey_victimization

    event = build_survey_victimization()
    result: ValidationResult = validate_event(event, "survey.victimization")
    assert result.ok, result.error


def test_migration_case_valid():
    from publisher.app import build_migration_case

    event = build_migration_case()
    result: ValidationResult = validate_event(event, "migration.case")
    assert result.ok, result.error


def test_missing_required_field_fails():
    from publisher.app import build_security_incident

    event = build_security_incident()
    del event["event_id"]
    result = validate_event(event, "security.incident")
    assert not result.ok
    assert "missing field event_id" in (result.error or "")
