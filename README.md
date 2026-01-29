
Event-Driven Lab – Python + Redis Streams
=========================================

Este repositorio implementa la arquitectura mínima del **Laboratorio 3: Publish & Subscribe** usando **Redis Streams** como broker pub-sub y una serie de microservicios en Python.

Servicios
---------

Todos los servicios se ejecutan en contenedores separados y se orquestan con `docker compose`:

- **redis**: Broker pub-sub basado en Redis 7 (Streams).
- **publisher**: Generador de eventos sintéticos para los tres tópicos de entrada, con soporte de modos *normal*, *burst*, *duplicates* y *out_of_order* y lógica básica de *backpressure* sobre los streams de entrada.
- **validator**: Validador de esquema; separa eventos válidos e inválidos, con política de reintentos configurables y manejo de mensajes pendientes tras reinicios.
- **aggregator**: Deduplicación y agregación diaria por región; publica métricas, aplica política de *retries* con *backoff* y procesa mensajes pendientes para mantener la semántica *at-least-once*.
- **audit**: Persistencia de eventos y métricas en SQLite para trazabilidad.
- **metrics-api**: API HTTP (FastAPI) para consultar las métricas agregadas y dashboard web mínimo para visualizarlas.

Tópicos / Streams en Redis
--------------------------

Se modelan los tópicos del enunciado como **Redis Streams**:

- `security.incident` – Eventos de delitos.
- `survey.victimization` – Eventos de encuestas de victimización.
- `migration.case` – Eventos de casos de migración.
- `validated.events` – Stream interno con eventos ya validados (input del Aggregator y Audit).
- `metrics.daily` – Métricas agregadas diarias por región (output del Aggregator, input de Audit / Metrics API).
- `deadletter.validation` – Eventos que fallaron validación de esquema.
- `deadletter.processing` – Eventos que fallaron en el procesamiento/aggregación.

Cada consumidor usa **consumer groups** de Redis para soportar paralelismo y semántica *at-least-once*.

Flujo de los Servicios
----------------------

1. **Publisher** (`publisher/app.py`)
	 - Conecta a Redis (`REDIS_HOST=redis`, `REDIS_PORT=6379`).
	 - Genera eventos sintéticos con el esquema:
		 - `event_id`, `timestamp` (UTC ISO‑8601), `region`, `source`, `schema_version`, `correlation_id`, `payload`.
	 - Publica eventos en los streams:
		 - `security.incident`
		 - `survey.victimization`
		 - `migration.case`
	 - Soporta varios modos configurables por variables de entorno:
		 - `MODE=normal` – tasa constante.
		 - `MODE=burst` – picos de eventos (reduce el `sleep` usando `BURST_FACTOR`).
		 - `MODE=duplicates` – re‑publica algunos eventos con el mismo `event_id` para probar deduplicación.
		 - `MODE=out_of_order` – inyecta timestamps fuera de orden (en el pasado).
	 - Otras variables:
		 - `EVENT_RATE` – eventos/segundo en modo normal (por defecto 5).
		 - `BURST_FACTOR` – multiplicador de tasa en modo `burst`.
		 - `SEED` – semilla para `random` (reproducibilidad de la demo).

2. **Validator** (`validator/app.py`)
	 - Consumer group `validator` sobre los streams:
		 - `security.incident`, `survey.victimization`, `migration.case`.
	 - Para cada mensaje:
		 - Deserializa el JSON.
		 - Valida campos obligatorios y forma básica del `payload` según `source`.
		 - Verifica que `source` coincida con el stream de origen.
	 - Eventos válidos:
		 - Se publican en `validated.events`.
	 - Eventos inválidos:
		 - Se envían a `deadletter.validation` con información del error y el mensaje original.
	 - Usa `XACK` solo después de procesar, cumpliendo semántica *at-least-once*.

3. **Aggregator** (`aggregator/app.py`)
	 - Consumer group `aggregator` sobre `validated.events` (grupo configurable por `AGGREGATOR_GROUP` / `AGGREGATOR_NAME`).
	 - **Deduplicación**:
		 - Usa un conjunto en Redis (`DEDUP_SET_KEY`, por defecto `aggregator:processed:event_ids`) con TTL (`DEDUP_TTL_SECONDS`, por defecto 86400 s) para evitar procesar dos veces el mismo `event_id`.
	 - **Agregación diaria por región**:
		 - Convierte `timestamp` a fecha UTC (`YYYY‑MM‑DD`).
		 - Agrupa por `(date, region)` y cuenta eventos por `source` (`security.incident`, `survey.victimization`, `migration.case`).
	 - **Publicación de métricas**:
		 - Cada `FLUSH_INTERVAL_SECONDS` (por defecto 30s) publica un snapshot en `metrics.daily` con la forma:
			 ```json
			 {
				 "date": "2025-12-29",
				 "region": "norte",
				 "metrics": {
					 "security.incident": {"count": 150},
					 "survey.victimization": {"count": 200},
					 "migration.case": {"count": 75}
				 }
			 }
			 ```
	 - **Retries y manejo de fallas**:
		 - Aplica una política de reintentos con *backoff* (estrategia `RETRY_STRATEGY` = `exponential`/`linear`/`fixed`, intentos `RETRY_MAX_ATTEMPTS`, intervalo inicial `RETRY_INITIAL_INTERVAL`).
		 - Mensajes que exceden el número máximo de reintentos terminan en `deadletter.processing` con información del error.
	 - **Mensajes pendientes / replay básico**:
		 - Al iniciar, procesa primero los mensajes pendientes del consumer group usando `XPENDING`/`XCLAIM` antes de consumir nuevos, reforzando la semántica *at-least-once*.
		 - La variable `AGGREGATOR_START_ID` permite crear un consumer group alternativo que lea desde un ID específico del stream para hacer *replay* (ver script `scripts/replay.sh`).

4. **Audit / Trazabilidad** (`audit/app.py`)
	 - Usa una base **SQLite** en el volumen compartido `/data/audit.db`.
	 - Consumer groups:
		 - `audit-events` sobre `validated.events`.
		 - `audit-metrics` sobre `metrics.daily`.
	 - Esquema mínimo:
		 - `input_events(event_id, timestamp, region, source, payload_json)`.
		 - `output_metrics(metric_id AUTOINCREMENT, date, region, metrics_json)`.
		 - `event_metric_link(event_id, metric_id)` (preparado para trazar qué eventos contribuyeron a qué métricas).
	 - Persiste todos los eventos validados y las métricas publicadas para consultas posteriores.

5. **Metrics API / Dashboard** (`metrics_api/app.py`)
	 - API REST construida con **FastAPI**.
	 - Usa la misma base SQLite `/data/audit.db` que el servicio de Audit.
	 - Endpoints principales:
		 - `GET /` – dashboard web mínimo que permite seleccionar fecha y región y visualizar las métricas en una tabla HTML.
		 - `GET /health` – respuesta `{ "status": "ok" }` si la API está viva.
		 - `GET /metrics?date=YYYY-MM-DD[&region=...]` – consulta la tabla `output_metrics` y devuelve lista de objetos con `date`, `region` y `metrics` (el mismo formato que publica el Aggregator).
	 - Toda la API emite logs estructurados JSON para facilitar la observabilidad.

Ejecución rápida
----------------

Requisitos previos:

- Docker y Docker Compose instalados (Docker Desktop en Windows).

### Opción 1: stack normal de laboratorio

Desde la raíz del proyecto:

```bash
docker compose up --build
```

Esto levanta todos los servicios con el publisher en modo **normal** (`EVENT_RATE=5`).

Una vez que el sistema esté arriba:

- Dashboard en el navegador: <http://localhost:8000/>
- Salud de la API: `curl http://localhost:8000/health`
- Consulta de métricas por fecha y región:

```bash
curl "http://localhost:8000/metrics?date=2025-12-29"
curl "http://localhost:8000/metrics?date=2025-12-29&region=norte"
```

### Opción 2: scripts de demo

En el directorio `scripts/` se incluyen utilidades para los distintos modos del enunciado (pueden ejecutarse desde WSL / Linux):

- `scripts/run_load.sh`
	- Levanta todo el stack con el publisher en modo **normal**.
- `scripts/run_burst.sh`
	- Reinicia el stack con el publisher en modo **burst** (mayor `EVENT_RATE` y `BURST_FACTOR`) para probar *backpressure* y *retries*.
- `scripts/run_chaos.sh`
	- Asume el stack corriendo en otra terminal.
	- Mata y reinicia `validator` y `aggregator`, y reinicia Redis para demostrar tolerancia a fallas y procesamiento de mensajes pendientes.
- `scripts/replay.sh [START_ID]`
	- Crea un aggregator en modo **replay** (nuevo consumer group) que vuelve a procesar `validated.events` desde un ID específico (por defecto `0-0`).
	- Útil para demostrar *re-procesamiento* por tiempo / offset.

- `scripts/make_demo.sh`
	- Orquesta una demo completa (~8 minutos) encadenando carga normal, burst y caos.
	- Pensado para ejecutarse desde WSL / Linux:

		```bash
		./scripts/make_demo.sh
		```

- `scripts/make_demo.ps1`
	- Versión equivalente para Windows/PowerShell.
	- Desde la raíz del repo:

		```powershell
		powershell -ExecutionPolicy Bypass -File .\scripts\make_demo.ps1
		```

Observabilidad y métricas
-------------------------

- Todos los servicios (`publisher`, `validator`, `aggregator`, `audit`, `metrics-api`) emiten **logs estructurados JSON** por stdout, con campos como `service`, `level`, `message`, contadores y lag.
- Se exponen métricas básicas vía logs:
	- Throughput aproximado (eventos procesados por ventana de tiempo en publisher/validator/aggregator).
	- `error_events` y conteo de mensajes enviados a deadletter.
	- `stream_length` y `pending` para estimar lag/backlog de los streams de entrada.

Tests y CI (RT3, RT7)
----------------------

- Para ejecutar los tests localmente (requiere Python 3.11):

	- Crear y activar un entorno virtual si se desea.
	- Instalar dependencias mínimas para tests:

		```bash
		pip install -r publisher/requirements.txt -r validator/requirements.txt -r aggregator/requirements.txt -r audit/requirements.txt -r metrics_api/requirements.txt pytest
		```

	- Ejecutar:

		```bash
		pytest -q
		```

	- Los tests cubren:
		- Validación de eventos contra los JSON Schemas usando el servicio validator.
		- Agregación de métricas en el servicio aggregator (by_severity, by_crime_type, by_status, reported_rate/_reported_true).

- CI/CD mínimo (GitHub Actions):

	- Se incluye un workflow en `.github/workflows/ci.yml` que:
		- Se ejecuta en cada push y pull request.
		- Instala dependencias y corre `pytest`.
		- Valida la configuración de `docker-compose.yml` con `docker compose config`.
	- Esto cubre el requisito de tener un pipeline de integración continua básico (RT3).

Notas y extensiones
-------------------

- El sistema demuestra el flujo end‑to‑end: generación → validación → agregación → auditoría → dashboard.
- Para trabajo futuro se podría añadir el tópico adicional `alerts.anomaly` para detección de anomalías y completar trazabilidad `event_metric_link`.


GUION  - SISTEMAS DISTRIBUIDOS LAB 3
===============================================

INTRODUCCIÓN (30 seg)
----------------------
"Buenos días. Vamos a presentar una arquitectura orientada a eventos usando Redis Streams y Python.
El sistema procesa reportes de seguridad simulados, los valida, agrega y visualiza en tiempo real."

-------------------------------------------------------------------------------
PASO 1: INGESTIÓN Y FLUJO NORMAL (1.5 min)
Objetivo: Demostrar que el sistema levanta y procesa flujo constante.

Narrativa:
"Iniciamos el stack. Tenemos un 'Publisher' generando eventos de delitos, victimización y migración.
Estos pasan a Redis y son consumidos por nuestros microservicios."

Comandos:
1.  docker compose up --build -d
    (Esperar a que levanten los contenedores)

2.  docker compose logs -f publisher

Qué observar:
- Verás logs tipo: `{"level": "info", "message": "event_published", ...}`
- Explicar: "Aquí vemos al Publisher inyectando eventos a una tasa de 5/seg."

(Ctrl+C para salir de logs)

-------------------------------------------------------------------------------
PASO 2: VALIDACIÓN Y DEADLETTER (1 min)
Objetivo: Probar que el sistema detecta y aísla datos corruptos (Schema Validation).

Narrativa:
"El 'Validator' asegura la calidad del dato. Vamos a inyectar basura manualmente en Redis
para ver cómo el sistema la rechaza y la envía a una cola de Deadletter, protegiendo el flujo principal."

Comandos:
1.  En Terminal A:
    docker compose logs -f validator

2.  En Terminal B (Acción de sabotaje):
    docker compose exec redis redis-cli XADD security.incident * data "INVALID_JSON_CONTENT"

Qué observar (Terminal A):
- Buscar log rojo/warning: `Validation error` o `Message sent to deadletter`.
- Explicar: "El validador detectó el JSON inválido y lo movió a 'deadletter.validation' sin detenerse."

-------------------------------------------------------------------------------
PASO 3: AGREGACIÓN Y VISUALIZACIÓN (1.5 min)
Objetivo: Ver la transformación de datos en información de negocio (Métricas).

Narrativa:
"Los eventos válidos llegan al 'Aggregator', que calcula contadores diarios por región.
Estas métricas se exponen vía API REST."

Comandos:
1.  docker compose logs -f aggregator
    (Esperar log: `metrics_flushed`, ocurre cada 30 seg)

2.  curl "http://localhost:8000/metrics?date=$(Get-Date -Format 'yyyy-MM-dd')"

Qué observar:
- En logs: "metrics_flushed" muestra que se guardó el snapshot.
- En curl: Un JSON con conteos (`security.incident: 150`, etc.).
- (Opcional) Mostrar rápidamente el dashboard en http://localhost:8000/

-------------------------------------------------------------------------------
PASO 4: RESILIENCIA - FALLA INDUCIDA (1.5 min)
Objetivo: Demostrar que si un servicio muere, no se pierden datos (At-least-once).

Narrativa:
"¿Qué pasa si el Validador crashea? Redis guarda los mensajes en el Stream.
Al reiniciar, el servicio debe retomar exactamente donde se quedó."

Comandos:
1.  docker compose kill validator
    (Explicar: "Servicio caído. Los eventos se acumulan en Redis, no se pierden.")

2.  docker compose start validator

3.  docker compose logs -f validator

Qué observar:
- Al arrancar, verás logs procesando mensajes viejos rápidamente (Catch-up).
- Explicar: "El validador recuperó los mensajes pendientes gracias a los Consumer Groups de Redis."

-------------------------------------------------------------------------------
PASO 5: TOLERANCIA A FALLOS DE BROKER (1 min)
Objetivo: Mostrar recuperación ante caída de infraestructura crítica.

Narrativa:
"Vamos más allá: reiniciamos el propio Broker (Redis). Los servicios intentarán reconectar automáticamente."

Comandos:
1.  docker compose restart redis

2.  docker compose logs -f aggregator

Qué observar:
- Logs de `ConnectionError` seguidos de `Connected to Redis`.
- Explicar: "El sistema implementa lógica de reconexión y backoff exponencial."

-------------------------------------------------------------------------------
PASO 6: REPLAY - REPROCESAMIENTO HISTÓRICO (1.5 min)
Objetivo: La "máquina del tiempo". Recalcular métricas desde cero.

Narrativa:
"Finalmente, necesitamos recalcular métricas de hoy por un cambio en la lógica.
Lanzamos un Aggregator efímero que lee el stream desde el principio ('0-0')."

Comandos:
1.  powershell -ExecutionPolicy Bypass -File .\scripts\replay.ps1 -StartId "0-0"

Qué observar:
- Logs frenéticos procesando miles de eventos antiguos.
- Explicar: "Estamos procesando todo el historial sin afectar al consumidor principal que sigue en tiempo real."

-------------------------------------------------------------------------------
CIERRE
"Con esto demostramos: Ingestión, Calidad de Datos, Observabilidad y Resiliencia."

Limpieza:
docker compose down
