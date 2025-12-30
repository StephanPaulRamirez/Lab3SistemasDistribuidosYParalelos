
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

DEDUP_SET_KEY = os.getenv("DEDUP_SET_KEY", "aggregator:processed:event_ids")
DEDUP_TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "86400"))

FLUSH_INTERVAL_SECONDS = int(os.getenv("FLUSH_INTERVAL_SECONDS", "30"))


@dataclass
class EventContext:
	event: Dict[str, Any]
	date: str
	region: str


def ensure_group(client: redis.Redis) -> None:
	try:
		client.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0-0", mkstream=True)
		print(f"Created consumer group {CONSUMER_GROUP} for {INPUT_STREAM}")
	except redis.ResponseError as exc:
		if "BUSYGROUP" not in str(exc):
			raise


def parse_event(raw: str) -> EventContext:
	event = json.loads(raw)
	ts = event.get("timestamp")
	dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
	date = dt.date().isoformat()
	region = event.get("region", "unknown")
	return EventContext(event=event, date=date, region=region)


def is_duplicate(client: redis.Redis, event_id: str) -> bool:
	# Usamos un conjunto en Redis para deduplicación con TTL
	added = client.sadd(DEDUP_SET_KEY, event_id)
	client.expire(DEDUP_SET_KEY, DEDUP_TTL_SECONDS)
	return added == 0


def aggregate_loop(client: redis.Redis) -> None:
	metrics_state: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(lambda: defaultdict(dict))
	last_flush = time.time()

	while True:
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
					try:
						ctx = parse_event(raw)
						event_id = ctx.event.get("event_id")
						if not event_id:
							raise ValueError("missing event_id")

						if is_duplicate(client, event_id):
							print(f"Duplicate event {event_id} skipped")
							client.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
							continue

						update_metrics(metrics_state, ctx)
						client.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
					except Exception as exc:  # noqa: BLE001
						handle_processing_error(client, msg_id, raw, str(exc))

		if now - last_flush >= FLUSH_INTERVAL_SECONDS and metrics_state:
			flush_metrics(client, metrics_state)
			metrics_state.clear()
			last_flush = now


def update_metrics(state: Dict[Tuple[str, str], Dict[str, Any]], ctx: EventContext) -> None:
	key = (ctx.date, ctx.region)
	metrics = state[key]

	source = ctx.event.get("source")
	if source not in metrics:
		metrics[source] = {"count": 0}
	metrics[source]["count"] += 1


def flush_metrics(client: redis.Redis, state: Dict[Tuple[str, str], Dict[str, Any]]) -> None:
	for (date, region), metrics in state.items():
		payload = {
			"date": date,
			"region": region,
			"metrics": metrics,
		}
		client.xadd(METRICS_STREAM, {"data": json.dumps(payload)})
		print(f"Published metrics for {date} {region} -> {METRICS_STREAM}")


def handle_processing_error(client: redis.Redis, msg_id: str, raw: str, error: str) -> None:
	payload = {
		"original_id": msg_id,
		"error": error,
		"raw": raw,
	}
	client.xadd(DEADLETTER_STREAM, {"data": json.dumps(payload)})
	client.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
	print(f"Sent processing error to {DEADLETTER_STREAM}: {error}")


def main() -> None:
	client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
	print(f"Aggregator connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
	ensure_group(client)

	while True:
		try:
			aggregate_loop(client)
		except redis.ConnectionError as exc:
			print(f"Redis connection error: {exc}, retrying in 5s")
			time.sleep(5)


if __name__ == "__main__":
	main()

