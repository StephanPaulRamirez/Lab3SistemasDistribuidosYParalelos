
import os
import sqlite3
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse


DB_PATH = os.getenv("DB_PATH", "/data/audit.db")

app = FastAPI(title="Metrics API", version="1.0.0")


def get_connection() -> sqlite3.Connection:
	return sqlite3.connect(DB_PATH)


@app.get("/health")
def health() -> Dict[str, str]:
	return {"status": "ok"}


@app.get("/metrics")
def get_metrics(
	date: str = Query(..., description="Fecha en formato YYYY-MM-DD"),
	region: str | None = Query(None, description="Región opcional"),
) -> Any:
	query = "SELECT date, region, metrics_json FROM output_metrics WHERE date = ?"
	params: List[Any] = [date]
	if region is not None:
		query += " AND region = ?"
		params.append(region)

	with get_connection() as conn:
		cur = conn.cursor()
		rows = cur.execute(query, params).fetchall()

	if not rows:
		raise HTTPException(status_code=404, detail="No hay métricas para los parámetros dados")

	result = []
	for row_date, row_region, metrics_json in rows:
		result.append(
			{
				"date": row_date,
				"region": row_region,
				"metrics": __import__("json").loads(metrics_json),
			}
		)

	return JSONResponse(content=result)


if __name__ == "__main__":
	import uvicorn

	uvicorn.run(app, host="0.0.0.0", port=8000)

