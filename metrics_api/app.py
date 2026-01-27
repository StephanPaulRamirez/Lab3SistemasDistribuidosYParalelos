
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse


DB_PATH = os.getenv("DB_PATH", "/data/audit.db")

app = FastAPI(title="Metrics API", version="1.0.0")


def log_json(level: str, message: str, **extra: Any) -> None:
	payload = {
		"ts": datetime.now(timezone.utc).isoformat(),
		"service": "metrics-api",
		"level": level,
		"message": message,
		**extra,
	}
	# FastAPI ya maneja logging propio, pero para consistencia usamos stdout JSON.
	print(__import__("json").dumps(payload, ensure_ascii=False))


def get_connection() -> sqlite3.Connection:
	return sqlite3.connect(DB_PATH)


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
	"""Frontend mínimo para visualizar métricas agregadas.

	Permite seleccionar fecha y región opcional, consulta /metrics y muestra
	los resultados en una tabla simple.
	"""
	return """<!DOCTYPE html>
<html lang='es'>
	<head>
		<meta charset='utf-8' />
		<title>Dashboard de Métricas</title>
		<style>
			body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem; background: #f5f5f5; }
			h1 { margin-bottom: 0.5rem; }
			.card { background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); max-width: 1100px; }
			label { display: block; margin-top: 0.5rem; font-weight: 600; }
			input, select { padding: 0.35rem 0.5rem; margin-top: 0.25rem; }
			button { margin-top: 0.75rem; padding: 0.4rem 0.9rem; border: none; border-radius: 4px; background: #2563eb; color: white; cursor: pointer; font-weight: 600; }
			button:disabled { opacity: 0.6; cursor: default; }
			table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
			th, td { border: 1px solid #ddd; padding: 0.4rem 0.5rem; text-align: left; font-size: 0.9rem; }
			th { background: #f3f4f6; }
			.error { color: #b91c1c; margin-top: 0.5rem; }
			.muted { color: #6b7280; font-size: 0.85rem; }
			code { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; }
			#charts { margin-top: 1.5rem; }
			#charts h2 { margin-bottom: 0.5rem; }
			.chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 1.25rem; }
			.chart-card { background: #f9fafb; border-radius: 8px; padding: 0.75rem 1rem; border: 1px solid #e5e7eb; }
			.chart-card canvas { max-height: 260px; }
		</style>
		<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
	</head>
	<body>
		<div class="card">
			<h1>Dashboard de Métricas</h1>
			<p class="muted">Consulta las métricas diarias agregadas generadas por el Aggregator.</p>
			<form id="metrics-form">
				<label>
					Fecha (YYYY-MM-DD)
					<input type="date" id="date" required />
				</label>
				<label>
					Región (opcional)
					<select id="region">
						<option value="">Todas</option>
						<option value="norte">norte</option>
						<option value="sur">sur</option>
						<option value="centro">centro</option>
						<option value="este">este</option>
						<option value="oeste">oeste</option>
					</select>
				</label>
				<button type="submit" id="submit-btn">Consultar</button>
				<div id="error" class="error"></div>
			</form>

			<div id="charts" style="display:none;">
				<h2>Visualizaciones</h2>
				<p class="muted">Gráficos construidos a partir de las métricas agregadas.</p>
				<div class="chart-grid">
					<div class="chart-card">
						<h3>Delitos por región</h3>
						<canvas id="securityChart"></canvas>
					</div>
					<div class="chart-card">
						<h3>Casos de migración por región</h3>
						<canvas id="migrationChart"></canvas>
					</div>
					<div class="chart-card">
						<h3>Tasa de reporte de victimización (%)</h3>
						<canvas id="surveyChart"></canvas>
					</div>
				</div>
			</div>

			<div id="results"></div>
		</div>

		<script>
			const form = document.getElementById('metrics-form');
			const dateInput = document.getElementById('date');
			const regionInput = document.getElementById('region');
			const resultsDiv = document.getElementById('results');
			const errorDiv = document.getElementById('error');
			const submitBtn = document.getElementById('submit-btn');
			const chartsSection = document.getElementById('charts');
			let securityChart = null;
			let migrationChart = null;
			let surveyChart = null;

			// Prefijar fecha actual en el input
			const today = new Date().toISOString().slice(0, 10);
			dateInput.value = today;

			form.addEventListener('submit', async (ev) => {
				ev.preventDefault();
				errorDiv.textContent = '';
				resultsDiv.innerHTML = '';

				const date = dateInput.value;
				const region = regionInput.value;
				if (!date) {
					errorDiv.textContent = 'Debes seleccionar una fecha.';
					return;
				}

				submitBtn.disabled = true;
				try {
					const params = new URLSearchParams({ date });
					if (region) params.append('region', region);
					const resp = await fetch('/metrics?' + params.toString());
					if (!resp.ok) {
						const data = await resp.json().catch(() => null);
						const msg = data?.detail || `Error HTTP ${resp.status}`;
						throw new Error(msg);
					}
					const data = await resp.json();
					renderResults(data);
				} catch (err) {
					errorDiv.textContent = err.message || 'Error al consultar métricas';
				} finally {
					submitBtn.disabled = false;
				}
			});

			function renderResults(items) {
				if (!items || items.length === 0) {
					resultsDiv.innerHTML = '<p class="muted">No hay métricas para los filtros dados.</p>';
					if (chartsSection) chartsSection.style.display = 'none';
					return;
				}

				let html = '<table><thead><tr><th>Fecha</th><th>Región</th><th>Métricas (JSON)</th></tr></thead><tbody>';
				for (const item of items) {
					html += `<tr><td>${item.date}</td><td>${item.region}</td><td><code>${JSON.stringify(item.metrics)}</code></td></tr>`;
				}
				html += '</tbody></table>';
				resultsDiv.innerHTML = html;
				renderCharts(items);
			}

			function renderCharts(items) {
				if (!chartsSection) return;
				if (!items || items.length === 0) {
					chartsSection.style.display = 'none';
					return;
				}

				const byRegion = {};
				for (const item of items) {
					byRegion[item.region] = item.metrics || {};
				}

				const regions = Object.keys(byRegion);
				if (regions.length === 0) {
					chartsSection.style.display = 'none';
					return;
				}

				const securityCounts = [];
				const migrationCounts = [];
				const surveyRates = [];

				for (const region of regions) {
					const m = byRegion[region] || {};
					const sec = m['security.incident'] || {};
					const mig = m['migration.case'] || {};
					const surv = m['survey.victimization'] || {};
					securityCounts.push(sec.count || 0);
					migrationCounts.push(mig.count || 0);
					surveyRates.push(((surv.reported_rate || 0) * 100).toFixed(1));
				}

				chartsSection.style.display = 'block';

				if (securityChart) securityChart.destroy();
				if (migrationChart) migrationChart.destroy();
				if (surveyChart) surveyChart.destroy();

				const secCtx = document.getElementById('securityChart').getContext('2d');
				securityChart = new Chart(secCtx, {
					type: 'bar',
					data: {
						labels: regions,
						datasets: [{
							label: 'Eventos de delitos',
							data: securityCounts,
							backgroundColor: 'rgba(37, 99, 235, 0.7)'
						}]
					},
					options: {
						responsive: true,
						plugins: { legend: { display: true } },
						scales: { y: { beginAtZero: true } }
					}
				});

				const migCtx = document.getElementById('migrationChart').getContext('2d');
				migrationChart = new Chart(migCtx, {
					type: 'bar',
					data: {
						labels: regions,
						datasets: [{
							label: 'Casos de migración',
							data: migrationCounts,
							backgroundColor: 'rgba(16, 185, 129, 0.7)'
						}]
					},
					options: {
						responsive: true,
						plugins: { legend: { display: true } },
						scales: { y: { beginAtZero: true } }
					}
				});

				const survCtx = document.getElementById('surveyChart').getContext('2d');
				surveyChart = new Chart(survCtx, {
					type: 'bar',
					data: {
						labels: regions,
						datasets: [{
							label: 'Tasa de reporte (%)',
							data: surveyRates,
							backgroundColor: 'rgba(249, 115, 22, 0.8)'
						}]
					},
					options: {
						responsive: true,
						plugins: { legend: { display: true } },
						scales: { y: { beginAtZero: true, max: 100 } }
					}
				});
			}
		</script>
	</body>
</html>"""
@app.get("/health")
def health() -> Dict[str, str]:
	log_json("info", "health_check")
	return {"status": "ok"}


@app.get("/metrics")
def get_metrics(
	date: str = Query(..., description="Fecha en formato YYYY-MM-DD"),
	region: str | None = Query(None, description="Región opcional"),
) -> Any:
	query = "SELECT metric_id, date, region, metrics_json FROM output_metrics WHERE date = ?"
	params: List[Any] = [date]
	if region is not None:
		query += " AND region = ?"
		params.append(region)

	with get_connection() as conn:
		cur = conn.cursor()
		rows = cur.execute(query, params).fetchall()

	if not rows:
		log_json("warning", "metrics_not_found", date=date, region=region)
		raise HTTPException(status_code=404, detail="No hay métricas para los parámetros dados")

	result = []
	for metric_id, row_date, row_region, metrics_json in rows:
		result.append(
			{
				"metric_id": metric_id,
				"date": row_date,
				"region": row_region,
				"metrics": __import__("json").loads(metrics_json),
			}
		)

	log_json("info", "metrics_queried", date=date, region=region, count=len(result))
	return JSONResponse(content=result)


@app.get("/metrics/{metric_id}/events")
def get_metric_events(metric_id: int) -> Any:
	"""Devuelve los eventos de entrada que contribuyeron a una métrica específica.

	Consulta la tabla event_metric_link unida a input_events.
	"""
	query = """
	SELECT e.event_id, e.timestamp, e.region, e.source, e.payload_json
	FROM event_metric_link l
	JOIN input_events e ON e.event_id = l.event_id
	WHERE l.metric_id = ?
	"""
	with get_connection() as conn:
		cur = conn.cursor()
		rows = cur.execute(query, [metric_id]).fetchall()

	if not rows:
		raise HTTPException(status_code=404, detail="No hay eventos asociados a la métrica indicada")

	events: List[Dict[str, Any]] = []
	for event_id, ts, region, source, payload_json in rows:
		events.append(
			{
				"event_id": event_id,
				"timestamp": ts,
				"region": region,
				"source": source,
				"payload": __import__("json").loads(payload_json),
			}
		)

	log_json("info", "metric_events_queried", metric_id=metric_id, count=len(events))
	return JSONResponse(content=events)


if __name__ == "__main__":
	import uvicorn

	uvicorn.run(app, host="0.0.0.0", port=8000)

