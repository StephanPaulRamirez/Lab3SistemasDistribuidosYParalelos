import os
import sqlite3
import time
import json
from datetime import datetime, timezone
from typing import Any, Dict, List

# --- Imports necesarios para el monitoreo y API ---
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

# --- NUEVO IMPORT PARA LA EXPORTACIÓN ---
import pandas as pd

DB_PATH = os.getenv("DB_PATH", "audit.db") # Asegurado para local

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

# --- Middleware de Monitoreo ---
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

@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    """Frontend que incluye métricas de negocio y estado del sistema en vivo."""
    return """<!DOCTYPE html>
<html lang='es'>
    <head>
        <meta charset='utf-8' />
        <title>Dashboard de Métricas</title>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jspdf/2.5.1/jspdf.umd.min.js"></script>
        
        <style>
            body { font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem; background: #f5f5f5; }
            h1 { margin-bottom: 0.5rem; margin-top: 0; }
            h2 { margin-top: 0; }
            
            .card { background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); max-width: 1100px; margin-bottom: 2rem; }
            
            .stats-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-top: 1rem; }
            .stat-box { background: #f8fafc; border: 1px solid #e2e8f0; padding: 1rem; border-radius: 6px; text-align: center; }
            .stat-value { font-size: 1.5rem; font-weight: bold; color: #0f172a; display: block; }
            .stat-label { font-size: 0.85rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }

            label { display: block; margin-top: 0.5rem; font-weight: 600; }
            input, select { padding: 0.35rem 0.5rem; margin-top: 0.25rem; }
            
            button { margin-top: 0.75rem; padding: 0.4rem 0.9rem; border: none; border-radius: 4px; background: #2563eb; color: white; cursor: pointer; font-weight: 600; margin-right: 10px; }
            button:disabled { opacity: 0.6; cursor: default; }
            
            /* Botones específicos */
            .btn-pdf { background: #dc2626; } 
            .btn-html { background: #16a34a; } /* Verde para el nuevo botón */
            
            table { border-collapse: collapse; width: 100%; margin-top: 1rem; }
            th, td { border: 1px solid #ddd; padding: 0.4rem 0.5rem; text-align: left; font-size: 0.9rem; }
            th { background: #f3f4f6; }
            .error { color: #b91c1c; margin-top: 0.5rem; }
            .muted { color: #6b7280; font-size: 0.85rem; }
            
            #charts { margin-top: 1.5rem; }
            .chart-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 1.25rem; }
            .chart-card { background: #f9fafb; border-radius: 8px; padding: 0.75rem 1rem; border: 1px solid #e5e7eb; }
            .chart-card canvas { max-height: 260px; }
            
            .badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.75rem; margin-right: 4px; }
            .bg-blue { background: #dbeafe; color: #1e40af; }
            .bg-orange { background: #ffedd5; color: #9a3412; }
            .metric-cell ul { list-style: none; padding: 0; margin: 4px 0; }
            .metric-cell li { margin-bottom: 2px; }
            
            #report-container { padding: 10px; background: white; }
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
                <div style="margin-top: 10px;">
                    <button type="submit" id="submit-btn">Consultar</button>
                    <button type="button" id="export-pdf-btn" class="btn-pdf" style="display:none;" onclick="downloadPDF()">Imprimir PDF (Vista)</button>
                    <button type="button" id="export-html-btn" class="btn-html" style="display:none;">Exportar Todo (HTML)</button>
                </div>
                <div id="error" class="error"></div>
            </form>

            <div id="report-container">
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
        </div>

        <script>
            // --- NUEVO: Manejo del botón HTML ---
            document.getElementById('export-html-btn').addEventListener('click', () => {
                const date = document.getElementById('date').value;
                const region = document.getElementById('region').value;
                
                if(!date) {
                    alert("Selecciona una fecha primero");
                    return;
                }
                
                // Construir URL con params
                let url = `/exportar-html?date=${date}`;
                if(region) url += `&region=${region}`;
                
                // Forzar descarga abriendo en nueva pestaña
                window.location.href = url;
            });

            // --- PDF Function ---
            async function downloadPDF() {
                const { jsPDF } = window.jspdf;
                const element = document.getElementById('report-container');
                const btn = document.getElementById('export-pdf-btn');
                
                const originalText = btn.innerText;
                btn.innerText = "Generando...";
                btn.disabled = true;

                try {
                    const canvas = await html2canvas(element, { scale: 2 });
                    const imgData = canvas.toDataURL('image/png');
                    const pdf = new jsPDF('p', 'mm', 'a4');
                    const pdfWidth = pdf.internal.pageSize.getWidth();
                    
                    const imgProps = pdf.getImageProperties(imgData);
                    const imgHeight = (imgProps.height * pdfWidth) / imgProps.width;
                    
                    pdf.text("Reporte de Métricas - " + document.getElementById('date').value, 10, 10);
                    pdf.addImage(imgData, 'PNG', 0, 15, pdfWidth, imgHeight);
                    pdf.save("reporte_metricas.pdf");
                } catch (err) {
                    console.error("Error:", err);
                    alert("Error al generar el PDF");
                } finally {
                    btn.innerText = originalText;
                    btn.disabled = false;
                }
            }

            // --- Stats Fetching ---
            async function fetchSystemStats() {
                try {
                    const resp = await fetch('/stats');
                    if(resp.ok) {
                        const data = await resp.json();
                        const m = data.metrics;
                        document.getElementById('stat-throughput').textContent = m.throughput_rps.toFixed(2);
                        document.getElementById('stat-error').textContent = m.error_rate_percent + '%';
                        document.getElementById('stat-lag').textContent = m.avg_lag_ms + ' ms';
                        document.getElementById('stat-total').textContent = m.total_requests;
                    }
                } catch(e) { console.error(e); }
            }
            fetchSystemStats();
            setInterval(fetchSystemStats, 2000);

            // --- Business Metrics Logic ---
            const form = document.getElementById('metrics-form');
            const dateInput = document.getElementById('date');
            const regionInput = document.getElementById('region');
            const resultsDiv = document.getElementById('results');
            const errorDiv = document.getElementById('error');
            const submitBtn = document.getElementById('submit-btn');
            const exportPdfBtn = document.getElementById('export-pdf-btn');
            const exportHtmlBtn = document.getElementById('export-html-btn'); 
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
                exportPdfBtn.style.display = 'none';
                exportHtmlBtn.style.display = 'none';

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
                        throw new Error(`Error HTTP ${resp.status}`);
                    }
                    const data = await resp.json();
                    renderResults(data);
                    
                    if (data && data.length > 0) {
                        exportPdfBtn.style.display = 'inline-block';
                        exportHtmlBtn.style.display = 'inline-block'; // Mostrar botón nuevo
                    }

                } catch (err) {
                    errorDiv.textContent = err.message || 'Error al consultar';
                } finally {
                    submitBtn.disabled = false;
                }
            });

            function renderResults(items) {
                if (!items || items.length === 0) {
                    resultsDiv.innerHTML = '<p class="muted">No hay métricas.</p>';
                    if (chartsSection) chartsSection.style.display = 'none';
                    return;
                }
                let html = '<table><thead><tr><th>Fecha</th><th>Región</th><th>Seguridad</th><th>Migración</th><th>Victimización</th></tr></thead><tbody>';
                for (const item of items) {
                    const m = item.metrics || {};
                    const sec = m['security.incident'] || { count: 0, by_severity: {} };
                    const mig = m['migration.case'] || { count: 0, by_status: {} };
                    const surv = m['survey.victimization'] || { count: 0, reported_rate: 0 };
                    html += `<tr>
                        <td><strong>${item.date}</strong></td>
                        <td><span class="badge bg-blue">${item.region.toUpperCase()}</span></td>
                        <td class="metric-cell"><strong>Total: ${sec.count}</strong><ul>${Object.entries(sec.by_severity || {}).map(([k, v]) => `<li><small>${k}: ${v}</small></li>`).join('')}</ul></td>
                        <td class="metric-cell"><strong>Total: ${mig.count}</strong><ul>${Object.entries(mig.by_status || {}).map(([k, v]) => `<li><small>${k}: ${v}</small></li>`).join('')}</ul></td>
                        <td class="metric-cell"><strong>Muestra: ${surv.count}</strong><br><span class="badge bg-orange">Tasa: ${(surv.reported_rate * 100).toFixed(1)}%</span></td>
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
                    if (!totalsByRegion[reg]) totalsByRegion[reg] = { sec: 0, mig: 0, survRate: 0 };
                    const m = item.metrics || {};
                    totalsByRegion[reg].sec += (m['security.incident']?.count || 0);
                    totalsByRegion[reg].mig += (m['migration.case']?.count || 0);
                    if (m['survey.victimization']) totalsByRegion[reg].survRate = (m['survey.victimization'].reported_rate || 0) * 100;
                }
                const regions = Object.keys(totalsByRegion);
                const securityCounts = regions.map(r => totalsByRegion[r].sec);
                const migrationCounts = regions.map(r => totalsByRegion[r].mig);
                const surveyRates = regions.map(r => totalsByRegion[r].survRate.toFixed(1));

                chartsSection.style.display = 'block';
                if (securityChart) securityChart.destroy();
                if (migrationChart) migrationChart.destroy();
                if (surveyChart) surveyChart.destroy();

                securityChart = new Chart(document.getElementById('securityChart').getContext('2d'), {
                    type: 'bar',
                    data: { labels: regions, datasets: [{ label: 'Delitos', data: securityCounts, backgroundColor: 'rgba(37, 99, 235, 0.7)' }] },
                    options: { responsive: true, scales: { y: { beginAtZero: true } } }
                });
                migrationChart = new Chart(document.getElementById('migrationChart').getContext('2d'), {
                    type: 'bar',
                    data: { labels: regions, datasets: [{ label: 'Migración', data: migrationCounts, backgroundColor: 'rgba(16, 185, 129, 0.7)' }] },
                    options: { responsive: true, scales: { y: { beginAtZero: true } } }
                });

                // --- MODIFICADO: Gráfico de Victimización ahora es LINEA (igual que el export) ---
                surveyChart = new Chart(document.getElementById('surveyChart').getContext('2d'), {
                    type: 'line', // Cambiado a LINE
                    data: { 
                        labels: regions, 
                        datasets: [{ 
                            label: 'Tasa Reporte (%)', 
                            data: surveyRates, 
                            borderColor: 'rgba(249, 115, 22, 1)',      // Naranja solido
                            backgroundColor: 'rgba(249, 115, 22, 0.2)', // Naranja transparente relleno
                            fill: true // Relleno activado
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

@app.get("/exportar-html", response_class=HTMLResponse)
def exportar_metrics_html(
    date: str = Query(..., description="Fecha obligatoria"),
    region: str | None = Query(None, description="Región opcional")
):
    """
    Genera un reporte HTML completo que incluye:
    1. Gráficos visuales (usando Chart.js incrustado).
    2. Tabla de datos detallada.
    """
    # 1. Obtener datos de la BD
    query = "SELECT date, region, metrics_json FROM output_metrics WHERE date = ?"
    params = [date]
    if region:
        query += " AND region = ?"
        params.append(region)
    
    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    
    if not rows:
        return HTMLResponse("<h1>No hay datos para exportar con esos filtros.</h1>")

    # 2. Procesar datos para la Tabla y para los Gráficos simultáneamente
    table_rows = ""
    
    # Acumuladores para los gráficos
    # Estructura: {'norte': {'sec': 10, 'mig': 5, 'vic_rate_sum': 0.5, 'vic_count': 1}, ...}
    agg_data = {}

    for row_date, row_region, metrics_str in rows:
        m = json.loads(metrics_str)
        
        # --- Lógica de Tabla ---
        sec = m.get('security.incident', {'count': 0, 'by_severity': {}})
        mig = m.get('migration.case', {'count': 0, 'by_status': {}})
        surv = m.get('survey.victimization', {'count': 0, 'reported_rate': 0})
        
        sec_details = "".join([f"<li>{k}: {v}</li>" for k, v in sec.get('by_severity', {}).items()])
        mig_details = "".join([f"<li>{k}: {v}</li>" for k, v in mig.get('by_status', {}).items()])
        rate_pct = round(surv.get('reported_rate', 0) * 100, 1)

        table_rows += f"""
        <tr>
            <td><strong>{row_date}</strong></td>
            <td><span class="badge bg-blue">{row_region.upper()}</span></td>
            <td class="metric-cell"><strong>Total: {sec['count']}</strong><ul class="detail-list">{sec_details}</ul></td>
            <td class="metric-cell"><strong>Total: {mig['count']}</strong><ul class="detail-list">{mig_details}</ul></td>
            <td class="metric-cell"><strong>Muestra: {surv['count']}</strong><br><span class="badge bg-orange">Tasa: {rate_pct}%</span></td>
        </tr>
        """

        # --- Lógica de Agregación para Gráficos ---
        if row_region not in agg_data:
            agg_data[row_region] = {'sec': 0, 'mig': 0, 'vic_rate': []}
        
        agg_data[row_region]['sec'] += sec['count']
        agg_data[row_region]['mig'] += mig['count']
        if surv['count'] > 0:
            agg_data[row_region]['vic_rate'].append(surv['reported_rate'])

    # 3. Preparar listas para Chart.js (JSON serializado)
    chart_labels = list(agg_data.keys())
    chart_sec_data = [d['sec'] for d in agg_data.values()]
    chart_mig_data = [d['mig'] for d in agg_data.values()]
    
    # Calcular promedio de tasa para el gráfico
    chart_vic_data = []
    for d in agg_data.values():
        rates = d['vic_rate']
        avg = (sum(rates) / len(rates) * 100) if rates else 0
        chart_vic_data.append(round(avg, 1))

    # 4. Construir el HTML final con JS inyectado
    # Nota: Usamos json.dumps para pasar las listas de Python a JS de forma segura
    full_html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="utf-8">
        <title>Reporte Gráfico - {date}</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{ font-family: system-ui, sans-serif; margin: 30px; background: #f0f2f5; }}
            .container {{ max-width: 1200px; margin: 0 auto; }}
            
            .card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
            h2 {{ text-align: center; color: #1e293b; }}
            
            /* Grid de gráficos */
            .charts-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 30px; }}
            .chart-box {{ background: #fff; padding: 15px; border: 1px solid #e2e8f0; border-radius: 8px; }}
            
            /* Tabla */
            table {{ width: 100%; border-collapse: collapse; }}
            th {{ background: #f8fafc; padding: 12px; text-align: left; border-bottom: 2px solid #e2e8f0; }}
            td {{ padding: 12px; border-bottom: 1px solid #e2e8f0; }}
            .badge {{ padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 0.75rem; }}
            .bg-blue {{ background: #dbeafe; color: #1e40af; }}
            .bg-orange {{ background: #ffedd5; color: #9a3412; }}
            ul.detail-list {{ list-style: none; padding: 0; margin: 0; font-size: 0.85rem; color: #64748b; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>Reporte Operativo del {date}</h2>
            
            <div class="charts-grid">
                <div class="card chart-box">
                    <canvas id="chartSec"></canvas>
                </div>
                <div class="card chart-box">
                    <canvas id="chartMig"></canvas>
                </div>
                <div class="card chart-box">
                    <canvas id="chartVic"></canvas>
                </div>
            </div>

            <div class="card">
                <h3>Detalle de Registros</h3>
                <table>
                    <thead>
                        <tr><th>Fecha</th><th>Región</th><th>Seguridad</th><th>Migración</th><th>Victimización</th></tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            </div>
        </div>

        <script>
            // Datos inyectados desde Python
            const labels = {json.dumps(chart_labels)};
            const secData = {json.dumps(chart_sec_data)};
            const migData = {json.dumps(chart_mig_data)};
            const vicData = {json.dumps(chart_vic_data)};

            // Configuración común
            const commonOptions = {{ responsive: true, plugins: {{ legend: {{ position: 'top' }} }} }};

            // 1. Gráfico Seguridad
            new Chart(document.getElementById('chartSec'), {{
                type: 'bar',
                data: {{
                    labels: labels,
                    datasets: [{{ label: 'Incidentes de Seguridad', data: secData, backgroundColor: 'rgba(59, 130, 246, 0.7)' }}]
                }},
                options: commonOptions
            }});

            // 2. Gráfico Migración
            new Chart(document.getElementById('chartMig'), {{
                type: 'bar',
                data: {{
                    labels: labels,
                    datasets: [{{ label: 'Casos de Migración', data: migData, backgroundColor: 'rgba(16, 185, 129, 0.7)' }}]
                }},
                options: commonOptions
            }});

            // 3. Gráfico Victimización
            new Chart(document.getElementById('chartVic'), {{
                type: 'line',
                data: {{
                    labels: labels,
                    datasets: [{{ label: 'Tasa Victimización (%)', data: vicData, borderColor: 'rgba(249, 115, 22, 1)', backgroundColor: 'rgba(249, 115, 22, 0.2)', fill: true }}]
                }},
                options: {{ ...commonOptions, scales: {{ y: {{ beginAtZero: true, max: 100 }} }} }}
            }});
        </script>
    </body>
    </html>
    """
    
    filename = f"reporte_grafico_{date}.html"
    return HTMLResponse(
        content=full_html,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

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