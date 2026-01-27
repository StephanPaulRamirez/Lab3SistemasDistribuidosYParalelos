
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

VALIDATED_STREAM = os.getenv("VALIDATED_STREAM", "validated.events")
METRICS_STREAM = os.getenv("METRICS_STREAM", "metrics.daily")

EVENTS_GROUP = os.getenv("AUDIT_EVENTS_GROUP", "audit-events")
EVENTS_CONSUMER = os.getenv("AUDIT_EVENTS_NAME", "audit-events-1")

METRICS_GROUP = os.getenv("AUDIT_METRICS_GROUP", "audit-metrics")
METRICS_CONSUMER = os.getenv("AUDIT_METRICS_NAME", "audit-metrics-1")

DB_PATH = os.getenv("DB_PATH", "/data/audit.db")

# Observabilidad básica
AUDIT_METRICS_LOG_INTERVAL = int(os.getenv("AUDIT_METRICS_LOG_INTERVAL", "30"))


def init_db() -> None:
	with sqlite3.connect(DB_PATH) as conn:
		cur = conn.cursor()
		cur.execute(
			"""
			CREATE TABLE IF NOT EXISTS input_events (
				event_id TEXT PRIMARY KEY,
				timestamp TEXT NOT NULL,
				region TEXT NOT NULL,
				source TEXT NOT NULL,
				payload_json TEXT NOT NULL
			)
			"""
		)
		cur.execute(
			"""
			CREATE TABLE IF NOT EXISTS output_metrics (
				metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
				date TEXT NOT NULL,
				region TEXT NOT NULL,
				metrics_json TEXT NOT NULL
			)
			"""
		)
		cur.execute(
			"""
			CREATE TABLE IF NOT EXISTS event_metric_link (
				event_id TEXT NOT NULL,
				metric_id INTEGER NOT NULL,
				FOREIGN KEY(event_id) REFERENCES input_events(event_id),
				FOREIGN KEY(metric_id) REFERENCES output_metrics(metric_id)
			)
			"""
		)
		conn.commit()


def log_json(level: str, message: str, **extra: Any) -> None:
	payload = {
		"ts": datetime.now(timezone.utc).isoformat(),
		"service": "audit",
		"level": level,
		"message": message,
		**extra,
	}
	print(json.dumps(payload, ensure_ascii=False))


@contextmanager
def db_connect():
	conn = sqlite3.connect(DB_PATH)
	try:
		yield conn
	finally:
		conn.close()


def ensure_groups(client: redis.Redis) -> None:
	for stream, group in [
		(VALIDATED_STREAM, EVENTS_GROUP),
		(METRICS_STREAM, METRICS_GROUP),
	]:
		try:
			client.xgroup_create(stream, group, id="0-0", mkstream=True)
			log_json("info", "created_consumer_group", stream=stream, group=group)
		except redis.ResponseError as exc:
			if "BUSYGROUP" not in str(exc):
				raise
			log_json("info", "consumer_group_exists", stream=stream, group=group)


def process_events(client: redis.Redis) -> None:
	resp = client.xreadgroup(
		groupname=EVENTS_GROUP,
		consumername=EVENTS_CONSUMER,
		streams={VALIDATED_STREAM: ">"},
		count=50,
		block=1000,
	)
	if not resp:
		return

	with db_connect() as conn:
		cur = conn.cursor()
		for _, messages in resp:
			for msg_id, fields in messages:
				raw = fields.get("data") or "{}"
				try:
					event = json.loads(raw)
					cur.execute(
						"""
						INSERT OR IGNORE INTO input_events (event_id, timestamp, region, source, payload_json)
						VALUES (?, ?, ?, ?, ?)
						""",
						(
							event.get("event_id"),
							event.get("timestamp"),
							event.get("region"),
							event.get("source"),
							json.dumps(event.get("payload", {})),
						),
					)
					client.xack(VALIDATED_STREAM, EVENTS_GROUP, msg_id)
				except Exception as exc:  # noqa: BLE001
					log_json("error", "error_persisting_event", msg_id=msg_id, error=str(exc))
		conn.commit()


def process_metrics(client: redis.Redis) -> None:
	resp = client.xreadgroup(
		groupname=METRICS_GROUP,
		consumername=METRICS_CONSUMER,
		streams={METRICS_STREAM: ">"},
		count=50,
		block=1000,
	)
	if not resp:
		return

	with db_connect() as conn:
		cur = conn.cursor()
		for _, messages in resp:
			for msg_id, fields in messages:
				raw = fields.get("data") or "{}"
				try:
					metric = json.loads(raw)
					cur.execute(
						"""
						INSERT INTO output_metrics (date, region, metrics_json)
						VALUES (?, ?, ?)
						""",
						(
							metric.get("date"),
							metric.get("region"),
							json.dumps(metric.get("metrics", {})),
						),
					)
					metric_id = cur.lastrowid
					# Ligamos todos los eventos de ese día/región a la métrica agregada.
					cur.execute(
						"""
						INSERT INTO event_metric_link (event_id, metric_id)
						SELECT event_id, ? FROM input_events
						WHERE date(timestamp) = ? AND region = ?
						""",
						(metric_id, metric.get("date"), metric.get("region")),
					)

					client.xack(METRICS_STREAM, METRICS_GROUP, msg_id)
				except Exception as exc:  # noqa: BLE001
					log_json("error", "error_persisting_metric", msg_id=msg_id, error=str(exc))
		conn.commit()


def main() -> None:
	init_db()
	client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
	log_json("info", "audit_started", redis_host=REDIS_HOST, redis_port=REDIS_PORT, db_path=DB_PATH)
	ensure_groups(client)

	last_metrics_log = time.time()
	events_persisted = 0
	metrics_persisted = 0

	while True:
		try:
			before_events = events_persisted
			before_metrics = metrics_persisted
			process_events(client)
			process_metrics(client)
			# Como `process_events` y `process_metrics` no devuelven contadores,
			# usamos tamaño de tablas como estimación simple.
			with db_connect() as conn:
				cur = conn.cursor()
				events_persisted = cur.execute("SELECT COUNT(*) FROM input_events").fetchone()[0]
				metrics_persisted = cur.execute("SELECT COUNT(*) FROM output_metrics").fetchone()[0]

			now = time.time()
			if now - last_metrics_log >= AUDIT_METRICS_LOG_INTERVAL:
				log_json(
					"info",
					"audit_runtime_metrics",
					input_events_total=events_persisted,
					output_metrics_total=metrics_persisted,
				)
				last_metrics_log = now
		except redis.ConnectionError as exc:
			log_json("error", "redis_connection_error", error=str(exc))
			time.sleep(5)


if __name__ == "__main__":
	main()

