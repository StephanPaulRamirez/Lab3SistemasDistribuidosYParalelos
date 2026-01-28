"""
Anomaly Detector Service - BONUS (+10%)

Este servicio detecta anomalías en las métricas agregadas y publica alertas
en el tópico 'alerts.anomaly'.

Estrategias de detección implementadas:
1. Detección por umbral estadístico (Z-score)
2. Cambios bruscos entre ventanas temporales
3. Reglas de negocio personalizadas

El detector consume del stream 'metrics.daily' y publica en 'alerts.anomaly'.
"""

import json
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import statistics

import redis


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

INPUT_STREAM = os.getenv("METRICS_STREAM", "metrics.daily")
OUTPUT_STREAM = os.getenv("ALERTS_STREAM", "alerts.anomaly")
CONSUMER_GROUP = os.getenv("ANOMALY_GROUP", "anomaly-detector")
CONSUMER_NAME = os.getenv("ANOMALY_NAME", "anomaly-detector-1")

# Configuración de detección
ANOMALY_ZSCORE_THRESHOLD = float(os.getenv("ANOMALY_ZSCORE_THRESHOLD", "2.5"))
ANOMALY_WINDOW_SIZE = int(os.getenv("ANOMALY_WINDOW_SIZE", "20"))
ANOMALY_SPIKE_MULTIPLIER = float(os.getenv("ANOMALY_SPIKE_MULTIPLIER", "3.0"))

# Umbrales de reglas de negocio
THRESHOLD_HIGH_CRIME_COUNT = int(os.getenv("THRESHOLD_HIGH_CRIME_COUNT", "100"))
THRESHOLD_HIGH_SEVERITY = float(os.getenv("THRESHOLD_HIGH_SEVERITY", "0.3"))

# Observability
METRICS_LOG_INTERVAL = int(os.getenv("ANOMALY_METRICS_LOG_INTERVAL", "30"))


@dataclass
class AnomalyAlert:
    """Representa una alerta de anomalía detectada"""
    alert_id: str
    timestamp: str
    region: str
    anomaly_type: str
    severity: str
    description: str
    metric_date: str
    affected_metrics: Dict[str, Any]
    detection_method: str


class MetricsHistory:
    """Mantiene historial de métricas por región para análisis estadístico"""
    
    def __init__(self, window_size: int = 20):
        self.window_size = window_size
        # region -> metric_name -> deque of values
        self.history: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(lambda: deque(maxlen=window_size)))
    
    def add_metric(self, region: str, metric_name: str, value: float) -> None:
        """Agrega un valor de métrica al historial"""
        self.history[region][metric_name].append(value)
    
    def get_stats(self, region: str, metric_name: str) -> Optional[Dict[str, float]]:
        """Calcula estadísticas básicas para una métrica"""
        values = list(self.history[region][metric_name])
        if len(values) < 2:
            return None
        
        return {
            "mean": statistics.mean(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0,
            "min": min(values),
            "max": max(values),
            "count": len(values)
        }
    
    def calculate_zscore(self, region: str, metric_name: str, value: float) -> Optional[float]:
        """Calcula el z-score para un valor dado"""
        stats = self.get_stats(region, metric_name)
        if not stats or stats["stdev"] == 0:
            return None
        
        return (value - stats["mean"]) / stats["stdev"]


def log_json(level: str, message: str, **extra: Any) -> None:
    """Genera logs estructurados en formato JSON"""
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "service": "anomaly-detector",
        "level": level,
        "message": message,
        **extra,
    }
    print(json.dumps(payload), flush=True)


def generate_alert_id() -> str:
    """Genera un ID único para la alerta"""
    import uuid
    return f"alert-{uuid.uuid4()}"


def detect_zscore_anomaly(
    region: str,
    metric_name: str,
    value: float,
    history: MetricsHistory,
    threshold: float
) -> Optional[AnomalyAlert]:
    """
    Detecta anomalías usando el método de Z-score.
    Si el valor está más allá de threshold desviaciones estándar, es una anomalía.
    """
    zscore = history.calculate_zscore(region, metric_name, value)
    if zscore is None or abs(zscore) < threshold:
        return None
    
    severity = "critical" if abs(zscore) > threshold + 1 else "high"
    
    return AnomalyAlert(
        alert_id=generate_alert_id(),
        timestamp=datetime.now(timezone.utc).isoformat(),
        region=region,
        anomaly_type="statistical_outlier",
        severity=severity,
        description=f"Valor anómalo detectado: {metric_name}={value:.2f} (z-score={zscore:.2f})",
        metric_date="",  # Se llenará después
        affected_metrics={metric_name: value},
        detection_method=f"zscore (threshold={threshold})"
    )


def detect_spike_anomaly(
    region: str,
    metric_name: str,
    value: float,
    history: MetricsHistory,
    multiplier: float
) -> Optional[AnomalyAlert]:
    """
    Detecta picos bruscos: si el valor actual es X veces mayor que el promedio histórico.
    """
    stats = history.get_stats(region, metric_name)
    if not stats or stats["mean"] == 0:
        return None
    
    ratio = value / stats["mean"]
    if ratio < multiplier:
        return None
    
    return AnomalyAlert(
        alert_id=generate_alert_id(),
        timestamp=datetime.now(timezone.utc).isoformat(),
        region=region,
        anomaly_type="sudden_spike",
        severity="high",
        description=f"Pico inusual detectado: {metric_name}={value:.2f} es {ratio:.1f}x el promedio histórico ({stats['mean']:.2f})",
        metric_date="",
        affected_metrics={metric_name: value},
        detection_method=f"spike_detection (multiplier={multiplier})"
    )


def detect_business_rule_anomalies(region: str, metrics: Dict[str, Any]) -> List[AnomalyAlert]:
    """
    Detecta anomalías usando reglas de negocio específicas del dominio.
    
    Reglas implementadas:
    - Alto número de delitos en una región
    - Alta proporción de delitos severos
    - Baja tasa de reportes en victimización (posible subregistro)
    """
    alerts = []
    
    # Regla 1: Alto número de delitos
    if "security.incident" in metrics:
        incident_count = metrics["security.incident"].get("count", 0)
        if incident_count > THRESHOLD_HIGH_CRIME_COUNT:
            alerts.append(AnomalyAlert(
                alert_id=generate_alert_id(),
                timestamp=datetime.now(timezone.utc).isoformat(),
                region=region,
                anomaly_type="high_crime_rate",
                severity="high",
                description=f"Número excepcionalmente alto de delitos: {incident_count} incidentes reportados",
                metric_date="",
                affected_metrics={"incident_count": incident_count},
                detection_method="business_rule (high_crime_threshold)"
            ))
    
    # Regla 2: Alta proporción de delitos severos
    if "security.incident" in metrics:
        by_severity = metrics["security.incident"].get("by_severity", {})
        total = sum(by_severity.values())
        if total > 0:
            high_severity_ratio = by_severity.get("high", 0) / total
            if high_severity_ratio > THRESHOLD_HIGH_SEVERITY:
                alerts.append(AnomalyAlert(
                    alert_id=generate_alert_id(),
                    timestamp=datetime.now(timezone.utc).isoformat(),
                    region=region,
                    anomaly_type="high_severity_crimes",
                    severity="critical",
                    description=f"Alta proporción de delitos severos: {high_severity_ratio:.1%} de gravedad 'high'",
                    metric_date="",
                    affected_metrics={"high_severity_ratio": high_severity_ratio},
                    detection_method="business_rule (severity_threshold)"
                ))
    
    # Regla 3: Baja tasa de reportes (posible subregistro)
    if "survey.victimization" in metrics:
        reported_rate = metrics["survey.victimization"].get("reported_rate", 1.0)
        if reported_rate < 0.3:  # Menos del 30% reporta
            alerts.append(AnomalyAlert(
                alert_id=generate_alert_id(),
                timestamp=datetime.now(timezone.utc).isoformat(),
                region=region,
                anomaly_type="low_report_rate",
                severity="medium",
                description=f"Tasa de reporte inusualmente baja: {reported_rate:.1%} de víctimas reportan",
                metric_date="",
                affected_metrics={"reported_rate": reported_rate},
                detection_method="business_rule (report_threshold)"
            ))
    
    return alerts


def analyze_metrics(metric_event: Dict[str, Any], history: MetricsHistory) -> List[AnomalyAlert]:
    """
    Analiza un evento de métricas y detecta todas las anomalías posibles.
    """
    alerts = []
    
    date = metric_event.get("date", "")
    region = metric_event.get("region", "unknown")
    metrics = metric_event.get("metrics", {})
    
    # 1. Detección estadística (Z-score) para conteos principales
    for source in ["security.incident", "survey.victimization", "migration.case"]:
        if source in metrics:
            count = metrics[source].get("count", 0)
            metric_name = f"{source}.count"
            
            # Detectar usando Z-score
            alert = detect_zscore_anomaly(
                region, metric_name, count, history, ANOMALY_ZSCORE_THRESHOLD
            )
            if alert:
                alert.metric_date = date
                alerts.append(alert)
            
            # Detectar picos bruscos
            alert = detect_spike_anomaly(
                region, metric_name, count, history, ANOMALY_SPIKE_MULTIPLIER
            )
            if alert:
                alert.metric_date = date
                alerts.append(alert)
            
            # Agregar al historial
            history.add_metric(region, metric_name, count)
    
    # 2. Detección por reglas de negocio
    business_alerts = detect_business_rule_anomalies(region, metrics)
    for alert in business_alerts:
        alert.metric_date = date
    alerts.extend(business_alerts)
    
    return alerts


def publish_alert(r: redis.Redis, alert: AnomalyAlert) -> None:
    """Publica una alerta en el stream de alertas"""
    alert_data = {
        "alert_id": alert.alert_id,
        "timestamp": alert.timestamp,
        "region": alert.region,
        "anomaly_type": alert.anomaly_type,
        "severity": alert.severity,
        "description": alert.description,
        "metric_date": alert.metric_date,
        "affected_metrics": json.dumps(alert.affected_metrics),
        "detection_method": alert.detection_method,
    }
    
    r.xadd(OUTPUT_STREAM, {"payload": json.dumps(alert_data)})
    
    log_json(
        "warning",
        f"Anomalía detectada: {alert.anomaly_type}",
        alert_id=alert.alert_id,
        region=alert.region,
        severity=alert.severity,
        description=alert.description,
        detection_method=alert.detection_method
    )


def main() -> None:
    """Loop principal del detector de anomalías"""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Crear consumer group
    try:
        r.xgroup_create(INPUT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        log_json("info", f"Consumer group '{CONSUMER_GROUP}' creado")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise
        log_json("info", f"Consumer group '{CONSUMER_GROUP}' ya existe")
    
    history = MetricsHistory(window_size=ANOMALY_WINDOW_SIZE)
    
    log_json(
        "info",
        "Anomaly Detector iniciado",
        input_stream=INPUT_STREAM,
        output_stream=OUTPUT_STREAM,
        zscore_threshold=ANOMALY_ZSCORE_THRESHOLD,
        window_size=ANOMALY_WINDOW_SIZE,
        spike_multiplier=ANOMALY_SPIKE_MULTIPLIER
    )
    
    # Métricas de observabilidad
    total_metrics_processed = 0
    total_anomalies_detected = 0
    last_metrics_log = time.time()
    anomalies_by_type = defaultdict(int)
    
    while True:
        try:
            # Leer eventos del stream
            messages = r.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={INPUT_STREAM: ">"},
                count=10,
                block=1000
            )
            
            for stream_name, stream_messages in messages:
                for msg_id, msg_data in stream_messages:
                    try:
                        payload = json.loads(msg_data.get("payload", "{}"))
                        
                        # Analizar métricas y detectar anomalías
                        alerts = analyze_metrics(payload, history)
                        
                        # Publicar alertas detectadas
                        for alert in alerts:
                            publish_alert(r, alert)
                            total_anomalies_detected += 1
                            anomalies_by_type[alert.anomaly_type] += 1
                        
                        total_metrics_processed += 1
                        
                        # ACK del mensaje
                        r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
                        
                    except Exception as e:
                        log_json(
                            "error",
                            f"Error procesando mensaje {msg_id}",
                            error=str(e),
                            msg_id=msg_id
                        )
                        # ACK para evitar reprocesar mensajes con errores persistentes
                        r.xack(INPUT_STREAM, CONSUMER_GROUP, msg_id)
            
            # Log de métricas periódico
            now = time.time()
            if now - last_metrics_log >= METRICS_LOG_INTERVAL:
                log_json(
                    "info",
                    "Métricas del detector",
                    metrics_processed=total_metrics_processed,
                    anomalies_detected=total_anomalies_detected,
                    anomalies_by_type=dict(anomalies_by_type),
                    detection_rate=f"{(total_anomalies_detected/total_metrics_processed*100):.2f}%" if total_metrics_processed > 0 else "0%"
                )
                last_metrics_log = now
        
        except KeyboardInterrupt:
            log_json("info", "Detector detenido por usuario")
            break
        except Exception as e:
            log_json("error", f"Error en loop principal: {e}", error=str(e))
            time.sleep(5)


if __name__ == "__main__":
    main()
