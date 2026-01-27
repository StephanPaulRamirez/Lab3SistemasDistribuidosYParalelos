import os
import sqlite3
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

# --- Imports necesarios para el monitoreo ---
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

DB_PATH = os.getenv("DB_PATH", "/data/audit.db")

app = FastAPI(title="Metrics API", version="1.0.0")

# --- Estado global para guardar las métricas en memoria ---
app_state = {
    "total_requests": 0,
    "total_errors": 0,
    "total_latency_ms": 0.0,
    "start_time": time.time()
}

def log_json(level: str, message: str, **extra: Any) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "metrics-api",
        "level": level,
        "message": message,
        **extra,
    }
    print(json.dumps(payload, ensure_ascii=False))

def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)

# --- Middleware de Monitoreo (Throughput, Lag, Error Rate) ---
class MonitoringMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        
        try:
            response = await call_next(request)
            status_code = response.status_code
        except Exception as e:
            status_code = 500
            app_state["total_errors"] += 1
            log_json("error", "unhandled_exception", error=str(e))
            raise e
        finally:
            process_time_ms = (time.perf_counter() - start_time) * 1000
            
            # Actualizar contadores globales
            app_state["total_requests"] += 1
            app_state["total_latency_ms"] += process_time_ms
            if status_code >= 400:
                app_state["total_errors"] += 1

            # Loguear métrica individual
            log_json(
                "info", 
                "request_processed", 
                method=request.method,
                path=request.url.path,
                status=status_code,
                lag_ms=round(process_time_ms, 2)
            )

        return response

app.add_middleware(MonitoringMiddleware)

# --- Frontend Actualizado ---
@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    """Frontend que incluye métricas de negocio y estado del sistema en vivo."""
    return """<!DOCTYPE html>
<html lang='es'>
    <head>
        <meta charset='utf-8' />
        <title>Dashboard de Métricas</title>
        <style>
            body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem; background: #f5f5f5; }
            h1 { margin-bottom: 0.5rem; margin-top: 0; }
            h2 { margin-top: 0; }
            
            .card { background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); max-width: 1100px; margin-bottom: 2rem; }
            
            /* --- ESTILOS NUEVOS: MONITORING DASHBOARD --- */
            .stats-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-top: 1rem; }
            .stat-box { background: #f8fafc; border: 1px solid #e2e8f0; padding: 1rem; border-radius: 6px; text-align: center; }
            .stat-value { font-size: 1.5rem; font-weight: bold; color: #0f172a; display: block; }
            .stat-label { font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
            @keyframes blink { 0% { opacity: 1; } 50% { opacity: 0.4; } 100% { opacity: 1; } }

            /* --- ESTILOS EXISTENTES --- */
            label { display: block; margin-top: 0.5rem; font-weight: 600; }
            input, select { padding: 0.35rem 0.5rem; margin-top: 0.25rem; }
            button { margin-top: 0.75rem; padding: 0.4rem 0.9rem; border: none; border-radius: 4px; background: #2563eb; color: white; cursor: pointer; font-weight: 600; }
            button:disabled { opacity: 0.6; cursor: default; }
            table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
            th, td { border: 1px solid #ddd; padding: 0.4rem 0.5rem; text-align: left; font-size: 0.9rem; }
            th { background: #f3f4f6; }
            .error { color: #b91c1c; margin-top: 0.5rem; }
            .muted { color: #6b7280; font-size: 0.85rem; }
            
            /* Gráficos */
            #charts { margin-top: 1.5rem; }
            .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 1.25rem; }
            .chart-card { background: #f9fafb; border-radius: 8px; padding: 0.75rem 1rem; border: 1px solid #e5e7eb; }
            .chart-card canvas { max-height: 260px; }
            
            /* Badges tabla */
            .badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.75rem; margin-right: 4px; }
            .bg-blue { background: #dbeafe; color: #1e40af; }
            .bg-orange { background: #ffedd5; color: #9a3412; }
            .metric-cell ul { list-style: none; padding: 0; margin: 4px 0; }
            .metric-cell li { margin-bottom: 2px; }
        </style>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    </head>
    <body>
        
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <h2>Estado del Sistema</h2>
                <small class="muted">Actualizando cada 2s</small>
            </div>
            <div class="stats-container">
                <div class="stat-box">
                    <span class="stat-value" id="stat-throughput">0.0</span>
                    <span class="stat-label">Throughput (Req/s)</span>
                </div>
                <div class="stat-box">
                    <span class="stat-value" id="stat-error">0%</span>
                    <span class="stat-label">Error Rate</span>
                </div>
                <div class="stat-box">
                    <span class="stat-value" id="stat-lag">0 ms</span>
                    <span class="stat-label">Lag Promedio</span>
                </div>
                <div class="stat-box">
                    <span class="stat-value" id="stat-total">0</span>
                    <span class="stat-label">Total Peticiones</span>
                </div>
            </div>
        </div>

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
                <h3 style="margin-top:1.5rem;">Visualizaciones</h3>
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
            // --- NUEVO: Lógica del Monitoreo en Tiempo Real ---
            async function fetchSystemStats() {
                try {
                    const resp = await fetch('/stats');
                    if(resp.ok) {
                        const data = await resp.json();
                        const m = data.metrics;
                        // Actualizar DOM
                        document.getElementById('stat-throughput').textContent = m.throughput_rps.toFixed(2);
                        document.getElementById('stat-error').textContent = m.error_rate_percent + '%';
                        document.getElementById('stat-lag').textContent = m.avg_lag_ms + ' ms';
                        document.getElementById('stat-total').textContent = m.total_requests;
                    }
                } catch(e) {
                    console.error("Error fetching stats", e);
                }
            }
            // Iniciar intervalo cada 2 segundos
            fetchSystemStats();
            setInterval(fetchSystemStats, 2000);

            // --- EXISTENTE: Lógica de Métricas de Negocio ---
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

                // Generación de tabla
                let html = '<table><thead><tr><th>Fecha</th><th>Región</th><th>Seguridad</th><th>Migración</th><th>Victimización</th></tr></thead><tbody>';
                
                for (const item of items) {
                    const m = item.metrics || {};
                    const sec = m['security.incident'] || { count: 0, by_severity: {} };
                    const mig = m['migration.case'] || { count: 0, by_status: {} };
                    const surv = m['survey.victimization'] || { count: 0, reported_rate: 0 };

                    html += `<tr>
                        <td><strong>${item.date}</strong></td>
                        <td><span class="badge bg-blue">${item.region.toUpperCase()}</span></td>
                        <td class="metric-cell">
                            <strong>Total: ${sec.count}</strong>
                            <ul>
                                ${Object.entries(sec.by_severity || {}).map(([k, v]) => `<li><small>${k}: ${v}</small></li>`).join('')}
                            </ul>
                        </td>
                        <td class="metric-cell">
                            <strong>Total: ${mig.count}</strong>
                            <ul>
                                ${Object.entries(mig.by_status || {}).map(([k, v]) => `<li><small>${k}: ${v}</small></li>`).join('')}
                            </ul>
                        </td>
                        <td class="metric-cell">
                            <strong>Muestra: ${surv.count}</strong><br>
                            <span class="badge bg-orange">Tasa: ${(surv.reported_rate * 100).toFixed(1)}%</span>
                        </td>
                    </tr>`;
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

                const totalsByRegion = {};
                for (const item of items) {
                    const reg = item.region;
                    if (!totalsByRegion[reg]) {
                        totalsByRegion[reg] = { sec: 0, mig: 0, survRate: 0 };
                    }
                    
                    const m = item.metrics || {};
                    totalsByRegion[reg].sec += (m['security.incident']?.count || 0);
                    totalsByRegion[reg].mig += (m['migration.case']?.count || 0);
                    
                    if (m['survey.victimization']) {
                        totalsByRegion[reg].survRate = (m['survey.victimization'].reported_rate || 0) * 100;
                    }
                }

                const regions = Object.keys(totalsByRegion);
                if (regions.length === 0) {
                    chartsSection.style.display = 'none';
                    return;
                }

                const securityCounts = regions.map(r => totalsByRegion[r].sec);
                const migrationCounts = regions.map(r => totalsByRegion[r].mig);
                const surveyRates = regions.map(r => totalsByRegion[r].survRate.toFixed(1));

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
                    options: { responsive: true, scales: { y: { beginAtZero: true } } }
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
                    options: { responsive: true, scales: { y: { beginAtZero: true } } }
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
                    options: { responsive: true, scales: { y: { beginAtZero: true, max: 100 } } }
                });
            }
        </script>
    </body>
</html>"""

@app.get("/stats")
def get_system_stats() -> Dict[str, Any]:
    uptime_seconds = time.time() - app_state["start_time"]
    total = app_state["total_requests"]
    
    throughput_rps = total / uptime_seconds if uptime_seconds > 0 else 0
    error_rate = (app_state["total_errors"] / total) if total > 0 else 0
    avg_lag = (app_state["total_latency_ms"] / total) if total > 0 else 0

    return {
        "uptime_seconds": round(uptime_seconds, 2),
        "metrics": {
            "throughput_rps": round(throughput_rps, 4),
            "error_rate_percent": round(error_rate * 100, 2),
            "avg_lag_ms": round(avg_lag, 2),
            "total_requests": total
        }
    }

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
                "metrics": json.loads(metrics_json),
            }
        )

    log_json("info", "metrics_queried", date=date, region=region, count=len(result))
    return JSONResponse(content=result)


@app.get("/metrics/{metric_id}/events")
def get_metric_events(metric_id: int) -> Any:
    """Devuelve los eventos de entrada que contribuyeron a una métrica específica."""
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
                "payload": json.loads(payload_json),
            }
        )

    log_json("info", "metric_events_queried", metric_id=metric_id, count=len(events))
    return JSONResponse(content=events)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)