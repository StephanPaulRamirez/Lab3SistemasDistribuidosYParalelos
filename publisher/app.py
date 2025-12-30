
import json
import os
import random
import string
import time
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, Iterable, List

import redis


class Mode(str, Enum):
	NORMAL = "normal"
	BURST = "burst"
	DUPLICATES = "duplicates"
	OUT_OF_ORDER = "out_of_order"


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

EVENT_RATE = float(os.getenv("EVENT_RATE", "5"))  # eventos por segundo en modo normal
BURST_FACTOR = int(os.getenv("BURST_FACTOR", "5"))  # multiplicador en modo burst
MODE = Mode(os.getenv("MODE", Mode.NORMAL))
SEED = os.getenv("SEED")


def _setup_random() -> None:
	if SEED is not None:
		try:
			random.seed(int(SEED))
		except ValueError:
			random.seed(SEED)


def _random_uuid() -> str:
	# UUID v4 sintético (no estrictamente RFC, pero suficiente para el laboratorio)
	hex_digits = [random.choice("0123456789abcdef") for _ in range(32)]
	hex_digits[12] = "4"  # versión
	hex_digits[16] = random.choice("89ab")  # variante
	parts = [
		"".join(hex_digits[0:8]),
		"".join(hex_digits[8:12]),
		"".join(hex_digits[12:16]),
		"".join(hex_digits[16:20]),
		"".join(hex_digits[20:32]),
	]
	return "-".join(parts)


REGIONS = ["norte", "sur", "centro", "este", "oeste"]
CRIME_TYPES = ["theft", "assault", "burglary", "other"]
SEVERITIES = ["low", "medium", "high"]
VICT_TYPES = ["property_crime", "violent_crime", "other"]
MIG_CASE_TYPES = ["asylum", "work", "family", "other"]
MIG_STATUS = ["pending", "approved", "rejected"]


def _now_utc_iso() -> str:
	return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _random_region() -> str:
	return random.choice(REGIONS)


def _random_string(prefix: str, length: int = 6) -> str:
	tail = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))
	return f"{prefix}-{tail}"


def build_security_incident() -> Dict[str, Any]:
	return {
		"event_id": _random_uuid(),
		"timestamp": _now_utc_iso(),
		"region": _random_region(),
		"source": "security.incident",
		"schema_version": "1.0",
		"correlation_id": _random_string("corr"),
		"payload": {
			"crime_type": random.choice(CRIME_TYPES),
			"severity": random.choice(SEVERITIES),
			"location": {
				"latitude": round(random.uniform(-90, 90), 4),
				"longitude": round(random.uniform(-180, 180), 4),
			},
			"reported_by": random.choice(["citizen", "police", "other"]),
		},
	}


def build_survey_victimization() -> Dict[str, Any]:
	return {
		"event_id": _random_uuid(),
		"timestamp": _now_utc_iso(),
		"region": _random_region(),
		"source": "survey.victimization",
		"schema_version": "1.0",
		"correlation_id": _random_string("corr"),
		"payload": {
			"survey_id": _random_string("survey"),
			"respondent_age": random.randint(18, 80),
			"victimization_type": random.choice(VICT_TYPES),
			"incident_date": datetime.now(timezone.utc).date().isoformat(),
			"reported": random.choice([True, False]),
		},
	}


def build_migration_case() -> Dict[str, Any]:
	return {
		"event_id": _random_uuid(),
		"timestamp": _now_utc_iso(),
		"region": _random_region(),
		"source": "migration.case",
		"schema_version": "1.0",
		"correlation_id": _random_string("corr"),
		"payload": {
			"case_id": _random_string("mig"),
			"case_type": random.choice(MIG_CASE_TYPES),
			"status": random.choice(MIG_STATUS),
			"origin_country": "country-x",
			"application_date": datetime.now(timezone.utc).date().isoformat(),
		},
	}


def generate_events() -> Iterable[Dict[str, Any]]:
	builders: List[Any] = [
		("security.incident", build_security_incident),
		("survey.victimization", build_survey_victimization),
		("migration.case", build_migration_case),
	]
	while True:
		topic, builder = random.choice(builders)
		event = builder()
		yield topic, event


def main() -> None:
	_setup_random()
	client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

	print(f"Publisher connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
	print(f"Mode={MODE}, rate={EVENT_RATE} ev/s, burst_factor={BURST_FACTOR}, seed={SEED}")

	base_sleep = 1.0 / max(EVENT_RATE, 0.1)
	duplicate_buffer: List[Dict[str, Any]] = []

	for topic, event in generate_events():
		# Modo out-of-order: a veces retrocedemos el timestamp unos minutos
		if MODE == Mode.OUT_OF_ORDER and random.random() < 0.3:
			past_minutes = random.randint(1, 60)
			dt = datetime.now(timezone.utc) - timedelta(minutes=past_minutes)  # type: ignore[name-defined]
			event["timestamp"] = dt.replace(microsecond=0).isoformat()

		data = json.dumps(event)
		client.xadd(topic, {"data": data})

		print(f"Published to {topic}: {event['event_id']}")

		# Modo duplicates: guardamos algunos eventos para re-enviarlos
		if MODE == Mode.DUPLICATES and random.random() < 0.3:
			duplicate_buffer.append((topic, event))

		# Re-publicar duplicados de vez en cuando
		if MODE == Mode.DUPLICATES and duplicate_buffer and random.random() < 0.2:
			dup_topic, dup_event = random.choice(duplicate_buffer)
			client.xadd(dup_topic, {"data": json.dumps(dup_event)})
			print(f"Republished duplicate to {dup_topic}: {dup_event['event_id']}")

		# Control de ritmo / burst
		sleep_time = base_sleep
		if MODE == Mode.BURST and random.random() < 0.5:
			sleep_time /= max(BURST_FACTOR, 1)

		time.sleep(sleep_time)


if __name__ == "__main__":
	main()

