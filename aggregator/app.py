
import json
import os
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

INPUT_STREAM = os.getenv("VALIDATED_STREAM", "validated.events")
METRICS_STREAM = os.getenv("METRICS_STREAM", "metrics.daily")
DEADLETTER_STREAM = os.getenv("DEADLETTER_PROCESSING_STREAM", "deadletter.processing")
CONSUMER_GROUP = os.getenv("AGGREGATOR_GROUP", "aggregator")
CONSUMER_NAME = os.getenv("AGGREGATOR_NAME", "aggregator-1")
AGGREGATOR_START_ID = os.getenv("AGGREGATOR_START_ID", "0-0")

DEDUP_SET_KEY = os.getenv("DEDUP_SET_KEY", "aggregator:processed:event_ids")
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "86400"))

FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "30"))

# Retry / backoff configuration (RF3)
RETRY_MAX_ATTEMPTS = int(os.getenv("RETRY_MAX_ATTEMPTS", "3"))
RETRY_INITIAL_INTERVAL = float(os.getenv("RETRY_INITIAL_INTERVAL", "1.0"))
RETRY_STRATEGY = os.getenv("RETRY_STRATEGY", "exponential").lower()

# Observability configuration
METRICS_LOG_INTERVAL = int(os.getenv("AGGREGATOR_METRICS_LOG_INTERVAL", "15"))


@dataclass
class EventContext:
	event: Dict[str, Any]
	date: str
	region: str


def log_json(level: str, message: str, **extra: Any) -> None:
	payload = {
		"ts": datetime.now(timezone.utc).isoformat(),
		"service": "aggregator",
		"level": level,
		"message": message,
		**extra,
	}
	print(json.dumps(payload, ensure_ascii=False))


def ensure_group(client: redis.Redis) -> None:
	try:
		client.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id=AGGREGATOR_START_ID, mkstream=True)
		log_json("info", "created_consumer_group", stream=INPUT_STREAM, group=CONSUMER_GROUP, start_id=AGGREGATOR_START_ID)
	except redis.ResponseError as exc:
		if "BUSYGROUP" not in str(exc):
			raise
		log_json("info", "consumer_group_exists", stream=INPUT_STREAM, group=CONSUMER_GROUP)


def _compute_backoff(attempt: int) -> float:
	if RETRY_STRATEGY == "fixed":
		return RETRY_INITIAL_INTERVAL
	if RETRY_STRATEGY == "linear":
		return RETRY_INITIAL_INTERVAL * attempt
	# default: exponential
	return RETRY_INITIAL_INTERVAL * (2 ** (attempt - 1))


def parse_event(raw: str) -> EventContext:
	event = json.loads(raw)
	ts = event.get("timestamp")
	if not ts:
		raise ValueError("missing timestamp")
	dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
	date = dt.date().isoformat()
	region = event.get("region", "unknown")
	return EventContext(event=event, date=date, region=region)


def is_duplicate(client: redis.Redis, event_id: str) -> bool:
	# Usamos un conjunto en Redis para deduplicación con TTL
	added = client.sadd(DEDUP_SET_KEY, event_id)
	client.expire(DEDUP_SET_KEY, DEDUP_TTL_SECONDS)
	return added == 0


def _process_single_message(client: redis.Redis, metrics_state: Dict[Tuple[str, str], Dict[str, Any]], msg_id: str, raw: str) -> None:
	ctx = parse_event(raw)
	event_id = ctx.event.get("event_id")
	if not event_id:
		raise ValueError("missing event_id")

	if is_duplicate(client, event_id):
		log_json("debug", "duplicate_event_skipped", event_id=event_id)
		client.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
		return

	update_metrics(metrics_state, ctx)
	client.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)


def _process_with_retry(client: redis.Redis, metrics_state: Dict[Tuple[str, str], Dict[str, Any]], msg_id: str, raw: str) -> Tuple[bool, int]:
	"""Procesa un mensaje individual aplicando política de retries.

	Devuelve (ok, intentos_realizados).
	"""
	for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
		try:
			_process_single_message(client, metrics_state, msg_id, raw)
			return True, attempt
		except Exception as exc:  # noqa: BLE001
			if attempt >= RETRY_MAX_ATTEMPTS:
				return False, attempt
			backoff = _compute_backoff(attempt)
			log_json(
				"warning",
				"processing_retry",
				msg_id=msg_id,
				attempt=attempt,
				max_attempts=RETRY_MAX_ATTEMPTS,
				backoff_seconds=backoff,
				error=str(exc),
			)
			time.sleep(backoff)

	return False, RETRY_MAX_ATTEMPTS


def _process_pending(client: redis.Redis, metrics_state: Dict[Tuple[str, str], Dict[str, Any]]) -> Tuple[int, int]:
	"""Procesa mensajes pendientes (pendientes tras reinicios) usando XCLAIM.

	Devuelve (procesados_ok, errores).
	"""
	processed = 0
	errors = 0
	while True:
		pending = client.xpending_range(INPUT_STREAM, CONSUMER_GROUP, "-", "+", count=50)
		if not pending:
			break

		msg_ids = [entry[0] for entry in pending]
		claimed = client.xclaim(INPUT_STREAM, CONSUMER_GROUP, CONSUMER_NAME, min_idle_time=0, message_ids=msg_ids)
		for msg_id, fields in claimed:
			raw = fields.get("data") or "{}"
			ok, _ = _process_with_retry(client, metrics_state, msg_id, raw)
			if ok:
				processed += 1
			else:
				errors += 1

	return processed, errors


def _stream_lag(client: redis.Redis) -> Dict[str, Any]:
	length = client.xlen(INPUT_STREAM)
	try:
		pending_summary = client.xpending(INPUT_STREAM, CONSUMER_GROUP)
		pending_count = pending_summary["pending"] if isinstance(pending_summary, dict) else pending_summary[0]
	except Exception:  # noqa: BLE001
		pending_count = 0

	return {"stream_length": length, "pending": pending_count}


def aggregate_loop(client: redis.Redis) -> None:
	metrics_state: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(lambda: defaultdict(dict))
	last_flush = time.time()
	last_metrics_log = time.time()
	processed_events = 0
	error_events = 0

	while True:
		# Primero, intentar procesar mensajes pendientes (post‑reinicio)
		pending_ok, pending_err = _process_pending(client, metrics_state)
		processed_events += pending_ok
		error_events += pending_err

		resp = client.xreadgroup(
			groupname=CONSUMER_GROUP,
			consumername=CONSUMER_NAME,
			streams={INPUT_STREAM: ">"},
			count=50,
			block=5000,
		)

		now = time.time()
		if resp:
			for _, messages in resp:
				for msg_id, fields in messages:
					raw = fields.get("data") or "{}"
					ok, _attempts = _process_with_retry(client, metrics_state, msg_id, raw)
					if ok:
						processed_events += 1
					else:
						# En caso de fallo tras retries, movemos a deadletter
						handle_processing_error(client, msg_id, raw, "max_retries_exceeded")
						client.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
						error_events += 1

		if now - last_flush >= FLUSH_INTERVAL_SECONDS and metrics_state:
			flush_metrics(client, metrics_state)
			metrics_state.clear()
			last_flush = now

		if now - last_metrics_log >= METRICS_LOG_INTERVAL:
			lag = _stream_lag(client)
			log_json(
				"info",
				"aggregator_runtime_metrics",
				processed_events=processed_events,
				error_events=error_events,
				stream_length=lag["stream_length"],
				pending=lag["pending"],
			)
			processed_events = 0
			error_events = 0
			last_metrics_log = now


def update_metrics(state: Dict[Tuple[str, str], Dict[str, Any]], ctx: EventContext) -> None:
	key = (ctx.date, ctx.region)
	metrics = state[key]

	source = ctx.event.get("source")
	if source not in metrics:
		metrics[source] = {"count": 0}
	metrics[source]["count"] += 1

	payload = ctx.event.get("payload", {})
	if source == "security.incident":
		by_severity = metrics[source].setdefault("by_severity", {})
		severity = payload.get("severity") or "unknown"
		by_severity[severity] = by_severity.get(severity, 0) + 1

		by_type = metrics[source].setdefault("by_crime_type", {})
		crime_type = payload.get("crime_type") or "other"
		by_type[crime_type] = by_type.get(crime_type, 0) + 1

	elif source == "survey.victimization":
		# reported_rate = reported_true / total
		reported_true = metrics[source].get("_reported_true", 0)
		if payload.get("reported") is True:
			reported_true += 1
		metrics[source]["_reported_true"] = reported_true
		# reported_rate se calcula en flush_metrics a partir de estos contadores.

	elif source == "migration.case":
		by_status = metrics[source].setdefault("by_status", {})
		status = payload.get("status") or "unknown"
		by_status[status] = by_status.get(status, 0) + 1


def flush_metrics(client: redis.Redis, state: Dict[Tuple[str, str], Dict[str, Any]]) -> None:
	for (date, region), metrics in state.items():
		# Normalizamos métricas antes de publicarlas (ej. reported_rate)
		final_metrics: Dict[str, Any] = {}
		for source, data in metrics.items():
			entry = dict(data)
			if source == "survey.victimization":
				count = entry.get("count", 0)
				reported_true = entry.pop("_reported_true", 0)
				entry["reported_rate"] = (reported_true / count) if count > 0 else 0.0
			final_metrics[source] = entry

		payload = {
			"date": date,
			"region": region,
			"metrics": final_metrics,
		}
		client.xadd(METRICS_STREAM, {"data": json.dumps(payload)})
		log_json("info", "metrics_published", date=date, region=region, metrics_keys=list(metrics.keys()))


def handle_processing_error(client: redis.Redis, msg_id: str, raw: str, error: str) -> None:
	payload = {
		"original_id": msg_id,
		"error": error,
		"raw": raw,
	}
	client.xadd(DEADLETTER_STREAM, {"data": json.dumps(payload)})
	log_json("error", "processing_error_deadletter", msg_id=msg_id, error=error)


def main() -> None:
	client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
	log_json("info", "aggregator_started", redis_host=REDIS_HOST, redis_port=REDIS_PORT)
	ensure_group(client)

	while True:
		try:
			aggregate_loop(client)
		except redis.ConnectionError as exc:
			log_json("error", "redis_connection_error", error=str(exc))
			time.sleep(5)


if __name__ == "__main__":
	main()

