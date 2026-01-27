
import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import redis
from jsonschema import ValidationError, validate


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", "/schemas"))

INPUT_STREAMS = [
	"security.incident",
	"survey.victimization",
	"migration.case",
]

VALIDATED_STREAM = os.getenv("VALIDATED_STREAM", "validated.events")
DEADLETTER_STREAM = os.getenv("DEADLETTER_VALIDATION_STREAM", "deadletter.validation")
CONSUMER_GROUP = os.getenv("VALIDATOR_GROUP", "validator")
CONSUMER_NAME = os.getenv("VALIDATOR_NAME", "validator-1")

# Retry / backoff configuration (RF3)
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_INITIAL_INTERVAL = float(os.getenv("RETRY_INITIAL_INTERVAL", "1.0"))
RETRY_STRATEGY = os.getenv("RETRY_STRATEGY", "exponential").lower()

# Observability configuration
METRICS_LOG_INTERVAL = int(os.getenv("VALIDATOR_METRICS_LOG_INTERVAL", "15"))


SCHEMAS: Dict[str, Dict[str, Any]] = {}


@dataclass
class ValidationResult:
	ok: bool
	error: str | None = None


def log_json(level: str, message: str, **extra: Any) -> None:
	payload = {
		"ts": datetime.now(timezone.utc).isoformat(),
		"service": "validator",
		"level": level,
		"message": message,
		**extra,
	}
	print(json.dumps(payload, ensure_ascii=False))


def _compute_backoff(attempt: int) -> float:
	if RETRY_STRATEGY == "fixed":
		return RETRY_INITIAL_INTERVAL
	if RETRY_STRATEGY == "linear":
		return RETRY_INITIAL_INTERVAL * attempt
	return RETRY_INITIAL_INTERVAL * (2 ** (attempt - 1))


def _load_schemas() -> None:
	"""Carga los JSON Schemas desde el directorio configurado.

	Se espera encontrar archivos:
	- security.incident.schema.json
	- survey.victimization.schema.json
	- migration.case.schema.json
	"""
	global SCHEMAS
	for name in INPUT_STREAMS:
		filename = f"{name}.schema.json"
		path = SCHEMA_PATH / filename
		try:
			with path.open("r", encoding="utf-8") as fh:
				SCHEMAS[name] = json.load(fh)
			log_json("info", "schema_loaded", stream=name, path=str(path))
		except FileNotFoundError:
			log_json("error", "schema_not_found", stream=name, path=str(path))
		except json.JSONDecodeError as exc:
			log_json("error", "schema_invalid_json", stream=name, path=str(path), error=str(exc))


def ensure_groups(client: redis.Redis) -> None:
	for stream in INPUT_STREAMS:
		try:
			client.xgroup_create(stream, CONSUMER_GROUP, id="0-0", mkstream=True)
			log_json("info", "created_consumer_group", stream=stream, group=CONSUMER_GROUP)
		except redis.ResponseError as exc:  # group already exists
			if "BUSYGROUP" not in str(exc):
				raise
			log_json("info", "consumer_group_exists", stream=stream, group=CONSUMER_GROUP)


def validate_event(event: Dict[str, Any], stream: str) -> ValidationResult:
	required_fields = [
		"event_id",
		"timestamp",
		"region",
		"source",
		"schema_version",
		"correlation_id",
		"payload",
	]
	for field in required_fields:
		if field not in event:
			return ValidationResult(False, f"missing field {field}")

	if event["source"] != stream:
		return ValidationResult(False, f"source {event['source']} does not match stream {stream}")

	# Validar formato básico de UUID v4 y timestamp ISO-8601 con timezone (UTC)
	try:
		uuid.UUID(event["event_id"])
	except (ValueError, TypeError):  # noqa: TRY002
		return ValidationResult(False, "invalid uuid in event_id")

	try:
		ts = datetime.fromisoformat(event["timestamp"])
	except (ValueError, TypeError):  # noqa: TRY002
		return ValidationResult(False, "invalid timestamp format (expected ISO-8601)")
	if ts.tzinfo is None:
		return ValidationResult(False, "timestamp must include timezone (UTC)")

	# Validación de esquema completa usando JSON Schema
	schema = SCHEMAS.get(stream)
	if schema is not None:
		try:
			validate(instance=event, schema=schema)
		except ValidationError as exc:
			return ValidationResult(False, f"schema validation error: {exc.message}")

	# Validaciones mínimas específicas por tipo de evento
	payload = event.get("payload", {})
	if stream == "security.incident":
		for f in ["crime_type", "severity", "location", "reported_by"]:
			if f not in payload:
				return ValidationResult(False, f"missing payload.{f}")
	elif stream == "survey.victimization":
		for f in ["survey_id", "respondent_age", "victimization_type", "incident_date", "reported"]:
			if f not in payload:
				return ValidationResult(False, f"missing payload.{f}")
	elif stream == "migration.case":
		for f in ["case_id", "case_type", "status", "origin_country", "application_date"]:
			if f not in payload:
				return ValidationResult(False, f"missing payload.{f}")

	return ValidationResult(True)


def _process_single_message(client: redis.Redis, stream: str, msg_id: str, raw: str) -> None:
	try:
		event = json.loads(raw)
	except json.JSONDecodeError as exc:
		_handle_invalid(client, stream, msg_id, raw, f"invalid json: {exc}")
		return

	result = validate_event(event, stream)
	if not result.ok:
		_handle_invalid(client, stream, msg_id, raw, result.error or "unknown error")
		return

	client.xadd(VALIDATED_STREAM, {"data": json.dumps(event)})
	client.xack(stream, CONSUMER_GROUP, msg_id)
	log_json("info", "event_validated", stream=stream, event_id=event.get("event_id"))


def _process_with_retry(client: redis.Redis, stream: str, msg_id: str, raw: str) -> Tuple[bool, int]:
	for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
		try:
			_process_single_message(client, stream, msg_id, raw)
			return True, attempt
		except Exception as exc:  # noqa: BLE001
			if attempt >= RETRY_MAX_ATTEMPTS:
				return False, attempt
			backoff = _compute_backoff(attempt)
			log_json(
				"warning",
				"validation_retry",
				stream=stream,
				msg_id=msg_id,
				attempt=attempt,
				max_attempts=RETRY_MAX_ATTEMPTS,
				backoff_seconds=backoff,
				error=str(exc),
			)
			time.sleep(backoff)

	return False, RETRY_MAX_ATTEMPTS


def _process_pending(client: redis.Redis) -> Tuple[int, int]:
	processed = 0
	errors = 0
	for stream in INPUT_STREAMS:
		while True:
			pending = client.xpending_range(stream, CONSUMER_GROUP, "-", "+", count=50)
			if not pending:
				break

			msg_ids = [entry[0] for entry in pending]
			claimed = client.xclaim(stream, CONSUMER_GROUP, CONSUMER_NAME, min_idle_time=0, message_ids=msg_ids)
			for msg_id, fields in claimed:
				raw = fields.get("data") or "{}"
				ok, _ = _process_with_retry(client, stream, msg_id, raw)
				if ok:
					processed += 1
				else:
					# Errores permanentes se envían a deadletter directamente
					_handle_invalid(client, stream, msg_id, raw, "max_retries_exceeded")
					errors += 1

	return processed, errors


def _stream_lag(client: redis.Redis) -> Dict[str, Dict[str, int]]:
	result: Dict[str, Dict[str, int]] = {}
	for stream in INPUT_STREAMS:
		length = client.xlen(stream)
		try:
			pending_summary = client.xpending(stream, CONSUMER_GROUP)
			pending_count = pending_summary["pending"] if isinstance(pending_summary, dict) else pending_summary[0]
		except Exception:  # noqa: BLE001
			pending_count = 0
		result[stream] = {"stream_length": length, "pending": pending_count}
	return result


def process_pending_and_new(client: redis.Redis) -> None:
	streams = {s: ">" for s in INPUT_STREAMS}
	last_metrics_log = time.time()
	processed_events = 0
	error_events = 0

	while True:
		pending_ok, pending_err = _process_pending(client)
		processed_events += pending_ok
		error_events += pending_err

		resp = client.xreadgroup(
			groupname=CONSUMER_GROUP,
			consumername=CONSUMER_NAME,
			streams=streams,
			count=10,
			block=5000,
		)
		if resp:
			for stream, messages in resp:
				for msg_id, fields in messages:
					raw = fields.get("data") or "{}"
					ok, _ = _process_with_retry(client, stream, msg_id, raw)
					if ok:
						processed_events += 1
					else:
						_handle_invalid(client, stream, msg_id, raw, "max_retries_exceeded")
						error_events += 1

		now = time.time()
		if now - last_metrics_log >= METRICS_LOG_INTERVAL:
			lag = _stream_lag(client)
			log_json(
				"info",
				"validator_runtime_metrics",
				processed_events=processed_events,
				error_events=error_events,
				lag=lag,
			)
			processed_events = 0
			error_events = 0
			last_metrics_log = now


def _handle_invalid(client: redis.Redis, stream: str, msg_id: str, raw: str, error: str) -> None:
	payload = {
		"stream": stream,
		"original_id": msg_id,
		"error": error,
		"raw": raw,
	}
	client.xadd(DEADLETTER_STREAM, {"data": json.dumps(payload)})
	client.xack(stream, CONSUMER_GROUP, msg_id)
	log_json("error", "event_invalid", stream=stream, msg_id=msg_id, error=error)


def main() -> None:
	client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
	log_json("info", "validator_started", redis_host=REDIS_HOST, redis_port=REDIS_PORT)
	_load_schemas()
	ensure_groups(client)

	while True:
		try:
			process_pending_and_new(client)
		except redis.ConnectionError as exc:
			log_json("error", "redis_connection_error", error=str(exc))
			time.sleep(5)


if __name__ == "__main__":
	main()

