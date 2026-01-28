#!/usr/bin/env python3
"""
Script para inyectar métricas anómalas en Redis y disparar detecciones.
Prueba diferentes tipos de anomalías.
"""
import json
import redis
import sys
from datetime import datetime

# Configuración
REDIS_HOST = "localhost"
REDIS_PORT = 6379
METRICS_STREAM = "metrics.daily"

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def inject_high_crime_anomaly():
    """Inyecta métrica con tasa de criminalidad muy alta (dispara business rule)"""
    print("📍 Inyectando anomalía: TASA DE CRIMEN ALTA (>100 incidentes)")
    
    metric = {
        "date": datetime.utcnow().isoformat(),
        "region": "norte",
        "metrics": {
            "security.incident": {
                "count": 250,  # Mucho mayor que el threshold de 100
                "by_severity": {
                    "high": 150,
                    "critical": 50,
                    "medium": 50
                }
            },
            "migration.case": {
                "count": 30,
                "by_status": {"pending": 15, "resolved": 15}
            },
            "survey.victimization": {
                "count": 200,
                "reported_rate": 0.5
            }
        }
    }
    
    r.xadd(METRICS_STREAM, {"payload": json.dumps(metric)})
    print("✅ Inyectada\n")

def inject_high_severity_anomaly():
    """Inyecta métrica con proporción alta de crímenes severos"""
    print("📍 Inyectando anomalía: PROPORCIÓN ALTA DE CRÍMENES SEVEROS (>30%)")
    
    metric = {
        "date": datetime.utcnow().isoformat(),
        "region": "sur",
        "metrics": {
            "security.incident": {
                "count": 120,
                "by_severity": {
                    "critical": 80,  # 80 de 120 = 66.7% -> ANOMALÍA
                    "high": 30,
                    "medium": 10
                }
            },
            "migration.case": {
                "count": 20,
                "by_status": {"pending": 10, "resolved": 10}
            },
            "survey.victimization": {
                "count": 150,
                "reported_rate": 0.6
            }
        }
    }
    
    r.xadd(METRICS_STREAM, {"payload": json.dumps(metric)})
    print("✅ Inyectada\n")

def inject_low_report_anomaly():
    """Inyecta métrica con tasa de reporte muy baja"""
    print("📍 Inyectando anomalía: TASA DE REPORTE BAJA (<30%)")
    
    metric = {
        "date": datetime.utcnow().isoformat(),
        "region": "este",
        "metrics": {
            "security.incident": {
                "count": 80,
                "by_severity": {
                    "high": 40,
                    "critical": 20,
                    "medium": 20
                }
            },
            "migration.case": {
                "count": 25,
                "by_status": {"pending": 12, "resolved": 13}
            },
            "survey.victimization": {
                "count": 200,
                "reported_rate": 0.15  # Solo 15% -> ANOMALÍA
            }
        }
    }
    
    r.xadd(METRICS_STREAM, {"payload": json.dumps(metric)})
    print("✅ Inyectada\n")

def inject_spike_anomaly():
    """Inyecta métrica con spike abrupto (10x el promedio)"""
    print("📍 Inyectando anomalía: SPIKE ABRUPTO (10x aumento)")
    
    metric = {
        "date": datetime.utcnow().isoformat(),
        "region": "centro",
        "metrics": {
            "security.incident": {
                "count": 500,  # Spike muy alto
                "by_severity": {
                    "high": 250,
                    "critical": 150,
                    "medium": 100
                }
            },
            "migration.case": {
                "count": 50,
                "by_status": {"pending": 25, "resolved": 25}
            },
            "survey.victimization": {
                "count": 300,
                "reported_rate": 0.5
            }
        }
    }
    
    r.xadd(METRICS_STREAM, {"payload": json.dumps(metric)})
    print("✅ Inyectada\n")

def inject_normal_baseline():
    """Inyecta métricas normales para establecer baseline"""
    print("📍 Inyectando 10 métricas normales para baseline...")
    
    regions = ["norte", "sur", "centro", "este", "oeste"]
    
    for i in range(10):
        for region in regions:
            metric = {
                "date": datetime.utcnow().isoformat(),
                "region": region,
                "metrics": {
                    "security.incident": {
                        "count": 50 + i*2,
                        "by_severity": {
                            "high": 20,
                            "critical": 10,
                            "medium": 20 + i*2
                        }
                    },
                    "migration.case": {
                        "count": 15 + i,
                        "by_status": {"pending": 7, "resolved": 8 + i}
                    },
                    "survey.victimization": {
                        "count": 100 + i*5,
                        "reported_rate": 0.5 + (i * 0.01)
                    }
                }
            }
            r.xadd(METRICS_STREAM, {"payload": json.dumps(metric)})
    
    print("✅ Baseline establecido\n")

def main():
    """Menú principal"""
    print("=" * 60)
    print("🔍 Script para inyectar anomalías y probar el detector")
    print("=" * 60)
    print("\nOpciones:")
    print("1. Inyectar métrica con TASA DE CRIMEN ALTA")
    print("2. Inyectar métrica con PROPORCIÓN ALTA DE CRÍMENES SEVEROS")
    print("3. Inyectar métrica con TASA DE REPORTE BAJA")
    print("4. Inyectar métrica con SPIKE ABRUPTO")
    print("5. Establecer BASELINE (10 métricas normales)")
    print("6. Inyectar TODAS las anomalías a la vez")
    print("0. Salir")
    
    while True:
        try:
            choice = input("\n▶ Selecciona opción (0-6): ").strip()
            
            if choice == "0":
                print("👋 Saliendo...")
                sys.exit(0)
            elif choice == "1":
                inject_high_crime_anomaly()
            elif choice == "2":
                inject_high_severity_anomaly()
            elif choice == "3":
                inject_low_report_anomaly()
            elif choice == "4":
                inject_spike_anomaly()
            elif choice == "5":
                inject_normal_baseline()
            elif choice == "6":
                print("\n🚀 Inyectando TODAS las anomalías...\n")
                inject_high_crime_anomaly()
                inject_high_severity_anomaly()
                inject_low_report_anomaly()
                inject_spike_anomaly()
                print("✨ ¡Todas inyectadas! Espera 5 segundos y revisa el dashboard.")
            else:
                print("❌ Opción no válida")
                
        except KeyboardInterrupt:
            print("\n👋 Saliendo...")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    try:
        r.ping()
        print("✅ Conectado a Redis\n")
        main()
    except redis.ConnectionError:
        print("❌ No se puede conectar a Redis en localhost:6379")
        print("   Asegúrate de que los servicios Docker están corriendo:")
        print("   $ docker-compose up -d")
        sys.exit(1)
