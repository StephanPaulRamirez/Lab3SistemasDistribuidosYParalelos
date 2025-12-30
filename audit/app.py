
import json
import os
import sqlite3
import time
from contextlib import contextmanager
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
			print(f"Created consumer group {group} for {stream}")
		except redis.ResponseError as exc:
			if "BUSYGROUP" not in str(exc):
				raise


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
					print(f"Error persisting input event {msg_id}: {exc}")
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

					# De momento no ligamos explícitamente event_ids -> metric_id
					# pero la tabla event_metric_link está lista para extender.

					client.xack(METRICS_STREAM, METRICS_GROUP, msg_id)
				except Exception as exc:  # noqa: BLE001
					print(f"Error persisting metric {msg_id}: {exc}")
		conn.commit()


def main() -> None:
	init_db()
	client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
	print(f"Audit service connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
	print(f"Using SQLite DB at {DB_PATH}")
	ensure_groups(client)

	while True:
		try:
			process_events(client)
			process_metrics(client)
		except redis.ConnectionError as exc:
			print(f"Redis connection error: {exc}, retrying in 5s")
			time.sleep(5)


if __name__ == "__main__":
	main()

