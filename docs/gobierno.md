# Gobierno preliminar · entrega 2

Controles, metadatos, linaje, responsabilidades, acceso y evidencias del pipeline tal como está implementado el 16/11/2026. Es la versión preliminar que pide la consigna en 6.2; se completa en la entrega final.

## Controles

| Control | Dónde | Qué hace si falla |
|---|---|---|
| Landing intacta | `landing.verify`, al inicio de `make pipeline` | Compara cada archivo contra el SHA-256 de `data/landing_manifest.csv`; si algo cambió, el pipeline no sigue |
| Filas que no parsean | Bronze batch y foreachBatch del streaming | La fila va a quarantine con la regla `parse_error` y el texto original |
| Clave duplicada en la carga | Bronze batch | Se queda la primera fila; la otra va a quarantine (`clave_duplicada`) |
| `event_id` nulo o repetido | Silver | Quarantine (`event_id_nulo`, `event_id_duplicado`) |
| `cost_usd_increment` menor a -0,01 | Silver | Quarantine (`costo_negativo`). Entre -0,01 y 0 se acepta como redondeo y se marca `dq_cost_rounding` |
| `value` no numérico | Silver | Quarantine (`value_no_numerico`); el texto original queda en `value_raw` |
| Recurso inexistente o inconsistente | Silver | Quarantine (`recurso_inexistente`, `recurso_inconsistente`) |
| `unit` faltante con `value` presente | Silver | Se completa desde `metric` y se marca `dq_unit_filled` |
| Subtotal negativo en facturación | Silver | Quarantine (`subtotal_negativo`), D-10 |
| Tipo de cambio de USD distinto de 1 | Silver | Se fija en 1, el original queda en `exchange_rate_to_usd_raw` y se marca `dq_fx_forced` |
| NPS fuera de -100 a 100 | Silver | Pasa a nulo con un flag (`dq_nps_out_of_range`); la fila se conserva. Lo mismo va a aplicar a CSAT fuera de 1 a 5 cuando se sume Silver de tickets (entrega final) |
| Conteo por tabla | Cada paso | Filas que entran = filas que siguen + filas en quarantine; queda en el registro de la corrida |
| Reconciliación de costo | Gold | El costo de Silver para las fechas recalculadas tiene que coincidir con el de Gold (diferencia menor a 0,01 USD) |

Si un control de conteo o reconciliación falla, el paso termina con código de error y el registro de la corrida lo muestra como `"ok": false`.

## Metadatos

- **Manifiesto de Landing:** `data/landing_manifest.csv`, con los 127 archivos, su tamaño y su SHA-256.
- **Esquemas:** `src/cpa/schemas.py`, uno por fuente, con su clave natural. Ninguna lectura infiere tipos.
- **Registro por corrida:** `evidence/runs/<run_id>_<paso>.json`, con filas por tabla y zona, filas en quarantine por regla y el resultado de cada control.
- **Métricas del streaming:** `evidence/02_entrega/streaming_progreso.csv`, con filas por lote, filas en estado, filas descartadas por watermark, duplicados descartados y duración del lote.
- **Configuración:** `config/settings.toml`, con rutas, umbrales y parámetros. Ningún valor de negocio queda escrito en el código.

## Diccionario de datos

Columnas técnicas, presentes desde Bronze:

| Columna | Significado |
|---|---|
| `ingest_ts` | Momento en que la fila entró a Bronze. En eventos, es el mismo para todo el micro-lote |
| `ingest_date` | Fecha de `ingest_ts`; partición de Bronze |
| `batch_id` | Micro-lote del streaming que trajo el evento (solo eventos) |
| `source_file` | Archivo de Landing de donde salió la fila |
| `run_id` | Corrida que escribió la fila en esa zona |

**Silver `usage_events`** (partición `event_date`): un evento de uso válido y deduplicado.

| Columna | Tipo | Significado |
|---|---|---|
| `event_id` | string | Clave natural, única en Silver |
| `timestamp`, `event_date` | timestamp, date | Momento del evento (UTC) y su fecha |
| `org_id`, `resource_id`, `service`, `region` | string | Organización, recurso, servicio y región, normalizados en minúsculas |
| `metric`, `value`, `unit` | string, double, string | Métrica (`requests`, `cpu_hours`, `storage_gb_hours`), su valor numérico y su unidad |
| `value_raw` | string | `value` tal como llegó |
| `cost_usd_increment` | double | Costo incremental en USD |
| `schema_version` | int | 1 o 2 |
| `carbon_kg`, `genai_tokens` | double, bigint | Solo en v2; nulos en v1 ("no se midió", no cero) |
| `plan_tier`, `industry` | string | De `customers_orgs` |
| `is_late` | boolean | El evento es anterior en más de 1 hora al máximo visto en los micro-lotes previos |
| `max_ts_previous_batches` | timestamp | Ese máximo, para poder auditar `is_late` |
| `dq_unit_filled`, `dq_cost_rounding` | boolean | Flags de calidad |

**Silver `billing_monthly`** (partición `month`): una factura válida, con el tipo de cambio corregido (D-10).

| Columna | Tipo | Significado |
|---|---|---|
| `invoice_id`, `org_id`, `month` | string, string, date | Factura, organización y mes |
| `subtotal`, `credits`, `taxes`, `currency` | double, string | Importes en la moneda original; `credits` nulo pasa a 0 |
| `exchange_rate_to_usd`, `exchange_rate_to_usd_raw` | double | Tipo de cambio aplicado (1 en USD) y el original |
| `subtotal_usd`, `credits_usd`, `taxes_usd`, `revenue_usd` | double | Importes en USD; revenue = (subtotal − créditos + impuestos) × tipo de cambio |
| `dq_fx_forced`, `dq_credits_filled` | boolean | Flags de calidad |

**Gold `org_daily_usage_by_service`** (partición `usage_date`): grano organización × día × servicio.

| Columna | Tipo | Significado |
|---|---|---|
| `org_id`, `usage_date`, `service` | string, date, string | Grano |
| `daily_cost_usd` | double | Suma de `cost_usd_increment` |
| `requests`, `cpu_hours`, `storage_gb_hours` | double | Suma de `value` por métrica |
| `genai_tokens`, `carbon_kg` | bigint, double | Sumas; nulas si el día solo tiene eventos v1 |
| `events`, `late_events` | bigint | Eventos del día y cuántos llegaron tarde |
| `plan_tier`, `industry` | string | Dimensión de la organización |
| `computed_at` | timestamp | Cuándo se recalculó la fila |

**Gold `cost_anomaly_mart`** (partición `usage_date`): mismo grano, con el score de D-11.

| Columna | Tipo | Significado |
|---|---|---|
| `daily_cost_usd`, `baseline_daily_cost_usd` | double | Costo del día y costo típico de esa organización y servicio (mediana) |
| `anomaly_score` | double | z robusto: 0,6745 × (log(1 + costo) − mediana) / MAD; nulo si hay menos de 14 días de historia |
| `is_anomaly`, `z_threshold` | boolean, double | Score mayor al umbral, y el umbral usado |
| `history_days`, `median_log_cost`, `mad_log_cost` | bigint, double | Insumos del score, para auditarlo |

**Quarantine** (`datalake/quarantine/<fuente>/`): `source`, `dq_rule`, `dq_reason`, `record` (la fila original en JSON), `run_id`, `quarantined_at` y la partición de la fuente.

**Cassandra:** las dos tablas de `cql/schema.cql` repiten las columnas del mart, con `loaded_at` como momento de carga (D-14).

## Linaje

```
Landing (archivo)            source_file
  → Bronze (fila tipada)     + ingest_ts, ingest_date, batch_id, run_id
  → Silver (fila conformada) conserva source_file, ingest_ts, batch_id; run_id de Silver
  → Gold (agregado)          run_id y computed_at del recálculo
  → Cassandra                run_id del mart y loaded_at
```

Desde cualquier fila de Silver se vuelve al archivo y al micro-lote de origen. Desde Gold se llega a la corrida que la calculó, y su registro en `evidence/runs/` dice cuántas filas entraron y cuántas fueron a quarantine. Cada fila de quarantine guarda el registro original completo.

## Responsabilidades

| Rol | Qué decide o sostiene |
|---|---|
| Dueño del dato (FinOps) | Las reglas de negocio de facturación y costo: D-10, D-11, D-12 |
| Ingeniería de datos, capa batch | Bronze y Silver de maestros, reglas de calidad, quarantine |
| Ingeniería de datos, capa speed | Streaming, checkpoints, idempotencia, capa speed |
| Ingeniería de datos, serving | Modelo de Cassandra, carga y consultas |
| Calidad y gobierno | Revisar quarantine en cada corrida, mantener este documento y el diccionario |

En el grupo, cada integrante cubre más de un rol (diseño v1, sección 11).

## Acceso y seguridad

- **Datos personales:** `users.email` no pasa de Bronze; en Silver queda `email_hash` (SHA-256), que sirve para cruzar pero no identifica. Los 85 recursos etiquetados `pii:true` quedan marcados en Silver (`is_pii`). Gold no tiene columnas personales.
- **Credenciales:** van en `.env`, que no se versiona (`.env.example` sin valores). Cassandra local no tiene usuario porque solo escucha en `127.0.0.1`. En un entorno compartido iría `PasswordAuthenticator` con un usuario de solo lectura para consumo y otro de escritura para la carga.
- **Acceso por zona (propuesta):** Landing y Bronze solo para ingeniería; Silver para ingeniería y analistas; Gold y Cassandra para los usuarios de negocio.

## Evidencias

Todo lo anterior se puede verificar en `evidence/02_entrega/` (conteos, quarantine por regla, consultas, experimentos e idempotencia) y en los registros de `evidence/runs/`.
