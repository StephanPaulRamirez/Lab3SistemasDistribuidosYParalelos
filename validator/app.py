
import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

INPUT_STREAMS = [
	"security.incident",
	"survey.victimization",
	"migration.case",
]

VALIDATED_STREAM = os.getenv("VALIDATED_STREAM", "validated.events")
DEADLETTER_STREAM = os.getenv("DEADLETTER_VALIDATION_STREAM", "deadletter.validation")
CONSUMER_GROUP = os.getenv("VALIDATOR_GROUP", "validator")
CONSUMER_NAME = os.getenv("VALIDATOR_NAME", "validator-1")


@dataclass
class ValidationResult:
	ok: bool
	error: str | None = None


def ensure_groups(client: redis.Redis) -> None:
	for stream in INPUT_STREAMS:
		try:
			client.xgroup_create(stream, CONSUMER_GROUP, id="0-0", mkstream=True)
			print(f"Created consumer group {CONSUMER_GROUP} for {stream}")
		except redis.ResponseError as exc:  # group already exists
			if "BUSYGROUP" not in str(exc):
				raise


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


def process_pending_and_new(client: redis.Redis) -> None:
	streams = {s: ">" for s in INPUT_STREAMS}

	while True:
		resp = client.xreadgroup(
			groupname=CONSUMER_GROUP,
			consumername=CONSUMER_NAME,
			streams=streams,
			count=10,
			block=5000,
		)
		if not resp:
			continue

		for stream, messages in resp:
			for msg_id, fields in messages:
				raw = fields.get("data") or "{}"
				try:
					event = json.loads(raw)
				except json.JSONDecodeError as exc:
					_handle_invalid(client, stream, msg_id, raw, f"invalid json: {exc}")
					continue

				result = validate_event(event, stream)
				if not result.ok:
					_handle_invalid(client, stream, msg_id, raw, result.error or "unknown error")
					continue

				client.xadd(VALIDATED_STREAM, {"data": json.dumps(event)})
				client.xack(stream, CONSUMER_GROUP, msg_id)
				print(f"Validated event {event.get('event_id')} from {stream}")


def _handle_invalid(client: redis.Redis, stream: str, msg_id: str, raw: str, error: str) -> None:
	payload = {
		"stream": stream,
		"original_id": msg_id,
		"error": error,
		"raw": raw,
	}
	client.xadd(DEADLETTER_STREAM, {"data": json.dumps(payload)})
	client.xack(stream, CONSUMER_GROUP, msg_id)
	print(f"Sent invalid event from {stream}:{msg_id} to {DEADLETTER_STREAM}: {error}")


def main() -> None:
	client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
	print(f"Validator connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
	ensure_groups(client)

	while True:
		try:
			process_pending_and_new(client)
		except redis.ConnectionError as exc:
			print(f"Redis connection error: {exc}, retrying in 5s")
			time.sleep(5)


if __name__ == "__main__":
	main()

