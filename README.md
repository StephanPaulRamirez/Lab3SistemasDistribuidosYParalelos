
Event-Driven Lab – Python + Redis Streams
=========================================

Este repositorio implementa la arquitectura mínima del **Laboratorio 3: Publish & Subscribe** usando **Redis Streams** como broker pub-sub y una serie de microservicios en Python.

Servicios
---------

Todos los servicios se ejecutan en contenedores separados y se orquestan con `docker compose`:

- **redis**: Broker pub-sub basado en Redis 7 (Streams).
- **publisher**: Generador de eventos sintéticos para los tres tópicos de entrada.
- **validator**: Validador de esquema; separa eventos válidos e inválidos.
- **aggregator**: Deduplicación y agregación diaria por región; publica métricas.
- **audit**: Persistencia de eventos y métricas en SQLite para trazabilidad.
- **metrics-api**: API HTTP (FastAPI) para consultar las métricas agregadas.

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
	 - Consumer group `aggregator` sobre `validated.events`.
	 - **Deduplicación**:
		 - Usa un conjunto en Redis (`aggregator:processed:event_ids`) con TTL (`DEDUP_TTL_SECONDS`, por defecto 86400 s) para evitar procesar dos veces el mismo `event_id`.
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
	 - Errores de procesamiento se envían a `deadletter.processing`.

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
		 - `GET /health`
			 - Respuesta: `{ "status": "ok" }` si la API está viva.
		 - `GET /metrics?date=YYYY-MM-DD[&region=...]`
			 - Consulta la tabla `output_metrics`.
			 - Filtros:
				 - `date` (obligatorio).
				 - `region` (opcional).
			 - Respuesta: lista de objetos con `date`, `region` y `metrics` (el mismo formato que publica el Aggregator).

Ejecución
---------

Requisitos previos:

- Docker y Docker Compose instalados (Docker Desktop en Windows).

Pasos:

1. Desde la raíz del proyecto, construir y levantar todos los servicios:

	 ```bash
	 docker compose up --build
	 ```

2. Verificar que los servicios estén corriendo: deberías ver logs de `publisher`, `validator`, `aggregator`, `audit` y `metrics-api` procesando eventos.

3. Probar la API de métricas (mientras `docker compose up` sigue corriendo):

	 - Salud:

		 ```bash
		 curl http://localhost:8000/health
		 ```

	 - Métricas para una fecha dada (ejemplo con la fecha actual):

		 ```bash
		 curl "http://localhost:8000/metrics?date=2025-12-29"
		 ```

	 - Métricas filtradas por región:

		 ```bash
		 curl "http://localhost:8000/metrics?date=2025-12-29&region=norte"
		 ```

Notas y Extensiones
-------------------

- El sistema ya demuestra el flujo end‑to‑end: generación → validación → agregación → auditoría → consulta.
- La configuración actual se centra en los requisitos mínimos; se puede extender para:
	- Implementar scripts de demo (`run_load`, `run_burst`, `run_chaos`, `replay`).
	- Añadir lógica explícita de *retries* y *backpressure* sobre los consumidores.
	- Implementar el tópico adicional `alerts.anomaly` para detección de anomalías.

