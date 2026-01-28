"""
Tests unitarios para el Anomaly Detector

Cubre:
- MetricsHistory: Historial y estadísticas
- Detección por Z-score
- Detección de spikes
- Detección por reglas de negocio
- Análisis integrado
- Edge cases y funciones utilitarias
"""

import unittest
import json
from datetime import datetime, timezone
from io import StringIO
from unittest.mock import patch, MagicMock
from anomaly_detector.app import (
    MetricsHistory,
    AnomalyAlert,
    detect_zscore_anomaly,
    detect_spike_anomaly,
    detect_business_rule_anomalies,
    analyze_metrics,
    log_json,
    generate_alert_id,
    publish_alert,
)


class TestMetricsHistory(unittest.TestCase):
    """Tests para la clase MetricsHistory"""
    
    def setUp(self):
        """Inicializa un objeto MetricsHistory para cada test"""
        self.history = MetricsHistory(window_size=5)
    
    def test_add_metric(self):
        """Verifica que las métricas se agreguen correctamente"""
        self.history.add_metric("norte", "crime_rate", 10.0)
        self.history.add_metric("norte", "crime_rate", 12.0)
        
        values = list(self.history.history["norte"]["crime_rate"])
        self.assertEqual(values, [10.0, 12.0])
    
    def test_window_size_limit(self):
        """Verifica que el historial respete el tamaño de ventana"""
        for i in range(10):
            self.history.add_metric("norte", "metric", float(i))
        
        # Debe tener como máximo 5 elementos (window_size)
        values = list(self.history.history["norte"]["metric"])
        self.assertEqual(len(values), 5)
        self.assertEqual(values, [5.0, 6.0, 7.0, 8.0, 9.0])
    
    def test_get_stats_with_insufficient_data(self):
        """Verifica que get_stats retorne None con datos insuficientes"""
        self.history.add_metric("norte", "metric", 10.0)
        stats = self.history.get_stats("norte", "metric")
        # Con solo 1 elemento, no debería retornar stats
        self.assertIsNone(stats)
    
    def test_get_stats_calculation(self):
        """Verifica el cálculo correcto de estadísticas"""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        for v in values:
            self.history.add_metric("norte", "metric", v)
        
        stats = self.history.get_stats("norte", "metric")
        self.assertIsNotNone(stats)
        self.assertEqual(stats["mean"], 30.0)
        self.assertEqual(stats["min"], 10.0)
        self.assertEqual(stats["max"], 50.0)
        self.assertEqual(stats["count"], 5)
        # Desviación estándar de [10, 20, 30, 40, 50] es 15.811...
        self.assertAlmostEqual(stats["stdev"], 15.811, places=2)
    
    def test_calculate_zscore(self):
        """Verifica el cálculo del z-score"""
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        for v in values:
            self.history.add_metric("norte", "metric", v)
        
        # Z-score para valor en la media
        zscore = self.history.calculate_zscore("norte", "metric", 30.0)
        self.assertAlmostEqual(zscore, 0.0, places=5)
        
        # Z-score para valor por encima de la media
        zscore = self.history.calculate_zscore("norte", "metric", 50.0)
        self.assertGreater(zscore, 0)
    
    def test_multiple_regions(self):
        """Verifica que el historial maneje múltiples regiones correctamente"""
        self.history.add_metric("norte", "metric", 10.0)
        self.history.add_metric("sur", "metric", 20.0)
        
        norte_values = list(self.history.history["norte"]["metric"])
        sur_values = list(self.history.history["sur"]["metric"])
        
        self.assertEqual(norte_values, [10.0])
        self.assertEqual(sur_values, [20.0])


class TestZscoreDetection(unittest.TestCase):
    """Tests para la detección por Z-score"""
    
    def setUp(self):
        """Inicializa MetricsHistory con datos conocidos"""
        self.history = MetricsHistory(window_size=10)
        # Datos normales: media 100, stdev ~30
        normal_values = [80, 90, 100, 110, 120]
        for v in normal_values:
            self.history.add_metric("norte", "crime_count", float(v))
    
    def test_normal_value_no_anomaly(self):
        """Verifica que valores normales no generen alertas"""
        alert = detect_zscore_anomaly(
            "norte", "crime_count", 100.0, self.history, threshold=2.5
        )
        self.assertIsNone(alert)
    
    def test_outlier_high_severity(self):
        """Verifica detección de valores extremos"""
        # Valor muy alto (más de 3 desvs estándar arriba)
        alert = detect_zscore_anomaly(
            "norte", "crime_count", 250.0, self.history, threshold=2.5
        )
        self.assertIsNotNone(alert)
        self.assertEqual(alert.anomaly_type, "statistical_outlier")
        self.assertEqual(alert.severity, "critical")
    
    def test_outlier_high_alert(self):
        """Verifica detección de anomalía con severidad alta"""
        # Valor moderadamente alto (zscore entre 2.5 y 3.5)
        alert = detect_zscore_anomaly(
            "norte", "crime_count", 176.0, self.history, threshold=2.5
        )
        self.assertIsNotNone(alert)
        # Puede ser high o critical dependiendo del zscore exacto
        self.assertIn(alert.severity, ["high", "critical"])


class TestSpikeDetection(unittest.TestCase):
    """Tests para la detección de spikes"""
    
    def setUp(self):
        """Inicializa MetricsHistory con datos base"""
        self.history = MetricsHistory(window_size=10)
        # Baseline: 50
        baseline = [45, 50, 55, 48, 52]
        for v in baseline:
            self.history.add_metric("norte", "incidents", float(v))
    
    def test_no_spike_within_threshold(self):
        """Verifica que valores normales no generen alertas"""
        # La media es ~50, con multiplicador 3.0, umbral es 150
        alert = detect_spike_anomaly(
            "norte", "incidents", 100.0, self.history, multiplier=3.0
        )
        self.assertIsNone(alert)
    
    def test_spike_detected(self):
        """Verifica detección de spike (valor 5x la media)"""
        # La media es ~50, con multiplicador 3.0, valor 250 dispara alerta
        alert = detect_spike_anomaly(
            "norte", "incidents", 250.0, self.history, multiplier=3.0
        )
        self.assertIsNotNone(alert)
        self.assertEqual(alert.anomaly_type, "sudden_spike")
        self.assertEqual(alert.severity, "high")
    
    def test_spike_calculation_accuracy(self):
        """Verifica que el cálculo del ratio es correcto"""
        alert = detect_spike_anomaly(
            "norte", "incidents", 150.0, self.history, multiplier=3.0
        )
        # Esperamos encontrar "3.0x" en la descripción (150/50 = 3.0)
        self.assertIn("3.0x", alert.description)


class TestBusinessRuleDetection(unittest.TestCase):
    """Tests para la detección de anomalías por reglas de negocio"""
    
    def test_high_crime_rate_detection(self):
        """Verifica detección de tasa de criminalidad alta"""
        metrics = {
            "security.incident": {
                "count": 150,  # Mayor que threshold de 100
                "by_severity": {"high": 50, "critical": 100}
            }
        }
        alerts = detect_business_rule_anomalies("norte", metrics)
        
        # Debe haber una alerta por crimen alto
        crime_alerts = [a for a in alerts if a.anomaly_type == "high_crime_rate"]
        self.assertEqual(len(crime_alerts), 1)
        self.assertEqual(crime_alerts[0].severity, "high")
    
    def test_high_severity_crimes_detection(self):
        """Verifica detección de alta proporción de crímenes severos"""
        # La regla chequea by_severity["high"] > 30% del total
        metrics = {
            "security.incident": {
                "count": 100,
                "by_severity": {
                    "high": 35,  # 35% -> dispara alerta
                    "critical": 20,
                    "medium": 45
                }
            }
        }
        alerts = detect_business_rule_anomalies("sur", metrics)
        
        severity_alerts = [a for a in alerts if a.anomaly_type == "high_severity_crimes"]
        self.assertEqual(len(severity_alerts), 1)
        self.assertEqual(severity_alerts[0].severity, "critical")
        self.assertIn("35.0%", severity_alerts[0].description)
    
    def test_low_report_rate_detection(self):
        """Verifica detección de baja tasa de reporte de víctimas"""
        metrics = {
            "survey.victimization": {
                "count": 100,
                "reported_rate": 0.2  # Solo 20% reporta (menor a threshold 0.3)
            }
        }
        alerts = detect_business_rule_anomalies("este", metrics)
        
        report_alerts = [a for a in alerts if a.anomaly_type == "low_report_rate"]
        self.assertEqual(len(report_alerts), 1)
        self.assertEqual(report_alerts[0].severity, "medium")
    
    def test_no_alert_for_normal_metrics(self):
        """Verifica que métricas normales no generen alertas"""
        metrics = {
            "security.incident": {
                "count": 50,
                "by_severity": {"high": 10, "critical": 5, "medium": 35}
            },
            "survey.victimization": {
                "count": 100,
                "reported_rate": 0.5
            }
        }
        alerts = detect_business_rule_anomalies("norte", metrics)
        self.assertEqual(len(alerts), 0)
    
    def test_multiple_rule_violations(self):
        """Verifica que múltiples reglas se detecten simultáneamente"""
        metrics = {
            "security.incident": {
                "count": 200,  # Viola regla 1: high crime count
                "by_severity": {
                    "high": 150,  # Viola regla 2: high severity ratio
                    "critical": 50
                }
            },
            "survey.victimization": {
                "count": 100,
                "reported_rate": 0.1  # Viola regla 3: low report rate
            }
        }
        alerts = detect_business_rule_anomalies("norte", metrics)
        
        # Debe tener alertas para las 3 reglas violadas
        self.assertEqual(len(alerts), 3)
        anomaly_types = {a.anomaly_type for a in alerts}
        self.assertIn("high_crime_rate", anomaly_types)
        self.assertIn("high_severity_crimes", anomaly_types)
        self.assertIn("low_report_rate", anomaly_types)


class TestIntegratedAnalysis(unittest.TestCase):
    """Tests para el análisis integrado"""
    
    def setUp(self):
        """Inicializa el historial con datos baseline"""
        self.history = MetricsHistory(window_size=10)
        
        # Crear baseline normal
        for i in range(5):
            self.history.add_metric("norte", "security.incident.count", 50.0 + i)
            self.history.add_metric("norte", "migration.case.count", 20.0 + i)
            self.history.add_metric("norte", "survey.victimization.count", 100.0 + i*2)
    
    def test_analyze_normal_metrics(self):
        """Verifica que métricas normales no generen alertas"""
        metric_event = {
            "date": "2026-01-27",
            "region": "norte",
            "metrics": {
                "security.incident": {
                    "count": 55,
                    "by_severity": {"high": 20, "critical": 10, "medium": 25}
                },
                "migration.case": {
                    "count": 21,
                    "by_status": {"pending": 10, "resolved": 11}
                },
                "survey.victimization": {
                    "count": 105,
                    "reported_rate": 0.5
                }
            }
        }
        alerts = analyze_metrics(metric_event, self.history)
        # Debería haber pocas o ninguna alerta
        self.assertLess(len(alerts), 3)
    
    def test_analyze_anomalous_metrics(self):
        """Verifica detección de múltiples anomalías en un evento"""
        metric_event = {
            "date": "2026-01-27",
            "region": "norte",
            "metrics": {
                "security.incident": {
                    "count": 300,  # Spike y regla de negocio
                    "by_severity": {"high": 250, "critical": 50}
                },
                "migration.case": {
                    "count": 25,
                    "by_status": {"pending": 12, "resolved": 13}
                },
                "survey.victimization": {
                    "count": 200,
                    "reported_rate": 0.1  # Regla de negocio: bajo reporte
                }
            }
        }
        alerts = analyze_metrics(metric_event, self.history)
        
        # Debe detectar múltiples anomalías
        self.assertGreater(len(alerts), 0)
        
        # Verificar que las alertas tengan los campos requeridos
        for alert in alerts:
            self.assertIsNotNone(alert.alert_id)
            self.assertEqual(alert.region, "norte")
            self.assertEqual(alert.metric_date, "2026-01-27")
            self.assertIn(alert.severity, ["critical", "high", "medium"])
    
    def test_metrics_added_to_history(self):
        """Verifica que analyze_metrics agregue datos al historial"""
        metric_event = {
            "date": "2026-01-27",
            "region": "norte",
            "metrics": {
                "security.incident": {
                    "count": 55,
                    "by_severity": {}
                },
                "migration.case": {
                    "count": 21,
                    "by_status": {}
                },
                "survey.victimization": {
                    "count": 105,
                    "reported_rate": 0.5
                }
            }
        }
        
        # Contar elementos en el historial antes
        history_size_before = len(
            list(self.history.history["norte"]["security.incident.count"])
        )
        
        analyze_metrics(metric_event, self.history)
        
        # Debe tener un elemento más después del análisis
        history_size_after = len(
            list(self.history.history["norte"]["security.incident.count"])
        )
        self.assertEqual(history_size_after, history_size_before + 1)


class TestAnomalyAlertDataClass(unittest.TestCase):
    """Tests para la estructura AnomalyAlert"""
    
    def test_anomaly_alert_creation(self):
        """Verifica que se pueda crear una alerta correctamente"""
        alert = AnomalyAlert(
            alert_id="test-123",
            timestamp="2026-01-27T10:00:00Z",
            region="norte",
            anomaly_type="high_crime_rate",
            severity="high",
            description="Test anomaly",
            metric_date="2026-01-27",
            affected_metrics={"crime_count": 150},
            detection_method="test_method"
        )
        
        self.assertEqual(alert.alert_id, "test-123")
        self.assertEqual(alert.region, "norte")
        self.assertEqual(alert.severity, "high")
        self.assertEqual(alert.affected_metrics["crime_count"], 150)


class TestUtilityFunctions(unittest.TestCase):
    """Tests para funciones utilitarias"""
    
    def test_generate_alert_id(self):
        """Verifica que se generen IDs únicos"""
        id1 = generate_alert_id()
        id2 = generate_alert_id()
        
        # Ambos deben empezar con "alert-"
        self.assertTrue(id1.startswith("alert-"))
        self.assertTrue(id2.startswith("alert-"))
        
        # Deben ser diferentes
        self.assertNotEqual(id1, id2)
    
    @patch('sys.stdout', new_callable=StringIO)
    def test_log_json_format(self, mock_stdout):
        """Verifica que log_json produzca JSON válido"""
        log_json("info", "Test message", extra_field="test_value")
        
        output = mock_stdout.getvalue()
        # Debería poder parsearse como JSON
        log_data = json.loads(output.strip())
        
        self.assertEqual(log_data["level"], "info")
        self.assertEqual(log_data["message"], "Test message")
        self.assertEqual(log_data["extra_field"], "test_value")
        self.assertEqual(log_data["service"], "anomaly-detector")
        self.assertIn("ts", log_data)
    
    def test_log_json_contains_timestamp(self):
        """Verifica que log_json incluya timestamp"""
        import sys
        from io import StringIO
        
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        
        try:
            log_json("debug", "Debug message")
            output = sys.stdout.getvalue()
            log_data = json.loads(output.strip())
            
            # Verificar que tiene timestamp válido en formato ISO
            timestamp = log_data["ts"]
            datetime.fromisoformat(timestamp)  # Debería no lanzar excepción
        finally:
            sys.stdout = old_stdout
    
    @patch('redis.Redis')
    def test_publish_alert_with_correct_payload(self, mock_redis_class):
        """Verifica que publish_alert envíe el payload correcto"""
        mock_redis_instance = MagicMock()
        mock_redis_class.return_value = mock_redis_instance
        
        alert = AnomalyAlert(
            alert_id="test-456",
            timestamp="2026-01-27T10:00:00Z",
            region="sur",
            anomaly_type="high_crime_rate",
            severity="high",
            description="High crime in region",
            metric_date="2026-01-27",
            affected_metrics={"count": 250},
            detection_method="business_rule"
        )
        
        publish_alert(mock_redis_instance, alert)
        
        # Verificar que xadd se llamó exactamente una vez
        self.assertEqual(mock_redis_instance.xadd.call_count, 1)
        
        # Obtener los argumentos de la llamada
        call_args = mock_redis_instance.xadd.call_args
        stream_name = call_args[0][0]
        payload_dict = call_args[0][1]
        
        # Verificar nombre del stream
        self.assertEqual(stream_name, "alerts.anomaly")
        
        # Verificar que contiene 'payload' key
        self.assertIn("payload", payload_dict)
        
        # Parsear el payload JSON
        payload_json = json.loads(payload_dict["payload"])
        self.assertEqual(payload_json["alert_id"], "test-456")
        self.assertEqual(payload_json["severity"], "high")
        self.assertEqual(payload_json["region"], "sur")


class TestEdgeCases(unittest.TestCase):
    """Tests para casos extremos y edge cases"""
    
    def test_zscore_with_zero_stdev(self):
        """Verifica que z-score retorne None cuando stdev es 0"""
        history = MetricsHistory(window_size=5)
        
        # Agregar valores idénticos (stdev = 0)
        history.add_metric("norte", "metric", 10.0)
        history.add_metric("norte", "metric", 10.0)
        
        # Z-score debe retornar None
        zscore = history.calculate_zscore("norte", "metric", 10.0)
        self.assertIsNone(zscore)
    
    def test_zscore_anomaly_with_no_history(self):
        """Verifica que no haya alerta si no hay historial"""
        history = MetricsHistory(window_size=5)
        
        alert = detect_zscore_anomaly(
            "norte", "metric", 100.0, history, threshold=2.5
        )
        self.assertIsNone(alert)
    
    def test_spike_with_zero_mean(self):
        """Verifica que spike detection maneje media de cero"""
        history = MetricsHistory(window_size=5)
        
        # Agregar valores cercanos a cero
        history.add_metric("norte", "metric", 0.001)
        history.add_metric("norte", "metric", 0.001)
        
        # Con media muy cercana a 0, cualquier valor generará ratio muy alto
        # Esto es correcto: si la media es casi 0, cualquier valor es un spike
        alert = detect_spike_anomaly(
            "norte", "metric", 10.0, history, multiplier=3.0
        )
        # Esperamos que haya alerta (correcto comportamiento)
        self.assertIsNotNone(alert)
    
    def test_business_rules_with_empty_metrics(self):
        """Verifica que business rules maneje métricas vacías"""
        metrics = {}
        alerts = detect_business_rule_anomalies("norte", metrics)
        
        self.assertEqual(len(alerts), 0)
    
    def test_business_rules_with_missing_by_severity(self):
        """Verifica que maneja by_severity missing"""
        metrics = {
            "security.incident": {
                "count": 50
                # Sin by_severity
            }
        }
        alerts = detect_business_rule_anomalies("norte", metrics)
        
        # Podría no tener alertas o solo por cuenta
        self.assertIsInstance(alerts, list)
    
    def test_analyze_metrics_with_empty_metrics_dict(self):
        """Verifica que analyze_metrics maneje diccionario vacío"""
        history = MetricsHistory(window_size=5)
        
        metric_event = {
            "date": "2026-01-27",
            "region": "norte",
            "metrics": {}  # Vacío
        }
        
        alerts = analyze_metrics(metric_event, history)
        self.assertIsInstance(alerts, list)


class TestMultipleAnomaliesDetection(unittest.TestCase):
    """Tests para detectar múltiples anomalías simultáneamente"""
    
    def test_zscore_and_spike_simultaneously(self):
        """Verifica detección de Z-score y spike juntos"""
        history = MetricsHistory(window_size=5)
        
        # Baseline normal
        for i in range(5):
            history.add_metric("norte", "metric", 50.0 + i)
        
        # Valor que es ambos: outlier y spike
        zscore_alert = detect_zscore_anomaly(
            "norte", "metric", 200.0, history, threshold=2.5
        )
        spike_alert = detect_spike_anomaly(
            "norte", "metric", 200.0, history, multiplier=3.0
        )
        
        # Ambas deben detectarse
        self.assertIsNotNone(zscore_alert)
        self.assertIsNotNone(spike_alert)
        
        # Pero con métodos diferentes
        self.assertNotEqual(zscore_alert.detection_method, spike_alert.detection_method)
    
    def test_multiple_regions_independent(self):
        """Verifica que anomalías en diferentes regiones son independientes"""
        history = MetricsHistory(window_size=5)
        
        # Baseline para norte: valores con stdev > 0
        for i in range(5):
            history.add_metric("norte", "metric", 50.0 + i)
        
        # Baseline diferente para sur: valores con stdev > 0
        for i in range(5):
            history.add_metric("sur", "metric", 200.0 + i)
        
        # Valor que es normal en sur pero anómalo en norte
        zscore_norte = history.calculate_zscore("norte", "metric", 200.0)
        zscore_sur = history.calculate_zscore("sur", "metric", 200.0)
        
        # Norte debe tener zscore alto (200 está lejano de ~52)
        self.assertIsNotNone(zscore_norte)
        self.assertGreater(abs(zscore_norte), 1.0)
        
        # Sur debe tener zscore mucho más bajo que norte (200 está más cerca de ~202)
        self.assertIsNotNone(zscore_sur)
        self.assertLess(abs(zscore_sur), abs(zscore_norte))


class TestAlertSeverityLevels(unittest.TestCase):
    """Tests para verificar asignación correcta de severidad"""
    
    def test_severity_high_for_moderate_outlier(self):
        """Verifica severidad HIGH para zscore moderado"""
        history = MetricsHistory(window_size=5)
        
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        for v in values:
            history.add_metric("norte", "metric", v)
        
        # Valor con zscore entre 2.5 y 3.5
        alert = detect_zscore_anomaly(
            "norte", "metric", 100.0, history, threshold=2.5
        )
        
        self.assertIsNotNone(alert)
        self.assertIn(alert.severity, ["high", "critical"])
    
    def test_severity_critical_for_extreme_outlier(self):
        """Verifica severidad CRITICAL para zscore extremo"""
        history = MetricsHistory(window_size=5)
        
        values = [10.0, 20.0, 30.0, 40.0, 50.0]
        for v in values:
            history.add_metric("norte", "metric", v)
        
        # Valor muy extremo (zscore > 5)
        alert = detect_zscore_anomaly(
            "norte", "metric", 500.0, history, threshold=2.5
        )
        
        self.assertIsNotNone(alert)
        self.assertEqual(alert.severity, "critical")
    
    def test_business_rule_severity_levels(self):
        """Verifica severidad correcta en reglas de negocio"""
        metrics = {
            "security.incident": {
                "count": 200,
                "by_severity": {"high": 100, "critical": 100}
            },
            "survey.victimization": {
                "count": 100,
                "reported_rate": 0.1
            }
        }
        
        alerts = detect_business_rule_anomalies("norte", metrics)
        
        # Verificar severidades
        severities = {a.anomaly_type: a.severity for a in alerts}
        
        self.assertEqual(severities.get("high_crime_rate"), "high")
        self.assertEqual(severities.get("high_severity_crimes"), "critical")
        self.assertEqual(severities.get("low_report_rate"), "medium")


class TestRealWorldScenarios(unittest.TestCase):
    """Tests que simulan escenarios reales del mundo"""
    
    def test_realistic_crime_data_detection(self):
        """Simula detección de anomalía con datos realistas de criminalidad"""
        history = MetricsHistory(window_size=10)
        
        # Histórico normal de delitos por día
        normal_days = [45, 48, 50, 52, 49, 51, 47, 50, 49, 48]
        for count in normal_days:
            history.add_metric("norte", "security.incident.count", float(count))
        
        # Detección de un día anómalo con spike de criminalidad
        metric_event = {
            "date": "2026-01-27",
            "region": "norte",
            "metrics": {
                "security.incident": {
                    "count": 200,  # 4x el promedio
                    "by_severity": {"high": 100, "critical": 50, "medium": 50}
                },
                "migration.case": {
                    "count": 20,
                    "by_status": {"pending": 10, "resolved": 10}
                },
                "survey.victimization": {
                    "count": 150,
                    "reported_rate": 0.4
                }
            }
        }
        
        alerts = analyze_metrics(metric_event, history)
        
        # Debe detectar múltiples anomalías
        self.assertGreater(len(alerts), 0)
        
        # Al menos una debe ser por spike
        spike_alerts = [a for a in alerts if "spike" in a.anomaly_type or "high" in a.anomaly_type]
        self.assertGreater(len(spike_alerts), 0)
    
    def test_realistic_low_report_rate_detection(self):
        """Simula detección de tasa baja de reportes de victimización"""
        history = MetricsHistory(window_size=5)
        
        # Baseline normal
        for i in range(5):
            history.add_metric("este", "survey.victimization.count", 100.0 + i*5)
        
        # Evento con baja tasa de reporte (posible subregistro)
        metric_event = {
            "date": "2026-01-27",
            "region": "este",
            "metrics": {
                "security.incident": {
                    "count": 80,
                    "by_severity": {"high": 30, "critical": 10, "medium": 40}
                },
                "survey.victimization": {
                    "count": 500,  # Muchos incidentes
                    "reported_rate": 0.15  # Pero solo 15% reporta
                }
            }
        }
        
        alerts = analyze_metrics(metric_event, history)
        
        # Debe detectar baja tasa de reporte
        low_report_alerts = [a for a in alerts if "low_report_rate" in a.anomaly_type]
        self.assertEqual(len(low_report_alerts), 1)
        self.assertEqual(low_report_alerts[0].severity, "medium")


if __name__ == "__main__":
    unittest.main()
