# Registro de decisiones

Cada decisión tiene estado, contexto, alternativas y consecuencias. Las abiertas se cierran en la entrega indicada. Las evidencias citadas están en `evidence/01_exploracion/` y en `notebooks/01_exploracion_landing.ipynb`.

| ID | Decisión | Estado | Fecha |
|---|---|---|---|
| D-01 | Patrón Lambda | Tomada | 28/09/2026 |
| D-02 | PySpark 3.5.9, Java 17 local y Java 11 en Colab | Tomada | 28/09/2026 |
| D-03 | Landing inmutable con manifiesto SHA-256 | Tomada | 28/09/2026 |
| D-04 | Esquemas explícitos y cast con fallback | Tomada | 28/09/2026 |
| D-05 | Micro-lotes de 10 archivos | Tomada | 28/09/2026 |
| D-06 | Watermark sobre `ingest_ts`; los tardíos no se descartan | Tomada | 28/09/2026 |
| D-07 | Particionado por zona | Tomada | 28/09/2026 |
| D-08 | Parquet sin Delta Lake | Tomada, se revisa en E2 | 28/09/2026 |
| D-09 | Serving en AstraDB o Cassandra local | Abierta | Entrega 2 |
| D-10 | Tipo de cambio en facturas USD y subtotales negativos | Abierta | Entrega 2 |
| D-11 | Criterio de spikes de costo | Abierta | Entrega 2 |

---

## D-01 · Patrón Lambda

Contexto. Los maestros, la facturación, las encuestas y los tickets cambian una vez por día o por mes. Los eventos de uso llegan de forma continua y FinOps necesita el costo del día.

Decisión. Capa batch para todo lo que no son eventos y para el recálculo diario de los marts. Capa speed con Structured Streaming solo para `usage_events_stream`.

Alternativas. Kappa: obliga a tratar como stream una facturación mensual de 240 filas y, con eventos desordenados (D-06), recalcular por fecha exige re-stream completo o un watermark que cubra todo el período. Híbrido: no resuelve nada que Lambda no resuelva acá, y suma complejidad.

Consecuencias. Hay dos caminos de código. Se mitiga con funciones de transformación compartidas entre el batch y el `foreachBatch` del streaming.

## D-02 · PySpark 3.5.9, Java 17 local y Java 11 en Colab

Contexto. La consigna pide que corra en Colab o en un entorno equivalente. Colab trae Java 11. Spark 4.x exige Java 17 o 21. El conector de Spark para Cassandra está publicado para Spark 3.5.

Decisión. PySpark 3.5.9 fijado en `requirements.txt`. En local, Java 17 de Homebrew en `JAVA_HOME`. En Colab, el Java 11 que ya trae.

Alternativas. Spark 4.x: obliga a instalar otro Java en Colab en cada sesión y complica el conector de Cassandra en la entrega 2.

Consecuencias. No se usan funciones exclusivas de Spark 4. `dropDuplicatesWithinWatermark`, que es la que hace falta para D-06, existe desde 3.5.0.

## D-03 · Landing inmutable con manifiesto SHA-256

Contexto. La consigna prohíbe modificar Landing, pero no dice cómo detectar si alguien lo hizo.

Decisión. `make landing` extrae solo `datalake/landing/` del zip, deja los archivos en solo lectura y los compara contra `data/landing_manifest.csv`. Si hay una diferencia, termina con error.

Consecuencias. Landing se puede borrar y regenerar con un comando. El manifiesto se versiona en el repo y es la referencia.

## D-04 · Esquemas explícitos y cast con fallback

Contexto. `value` llega como número en unos eventos y como texto en otros. `tags_json` escapa las comillas duplicándolas.

Decisión. Ninguna lectura usa `inferSchema`. `value` se lee como texto y se castea con `cast_with_fallback`, que conserva el original en `value_raw` y marca los fallos. Los CSV se leen con `escape='"'`. Todos los esquemas llevan `_corrupt_record`.

Evidencia. Declarar `value` como double marca como corruptas 1.309 filas y deja 2.186 nulos donde hay 877. Sin `escape='"'`, 211 de 400 filas de `resources` quedan corruptas.

## D-05 · Micro-lotes de 10 archivos

Contexto. Hay 120 archivos de 360 eventos.

Decisión. `maxFilesPerTrigger = 10`: 12 micro-lotes de 3.600 eventos.

Alternativas. 1 archivo por lote: 120 lotes, más archivos de salida y más overhead por lote. Sin límite: un solo lote, que no ejercita el streaming.

Consecuencias. Se ajusta con el volumen real. Está en `config/settings.toml`.

## D-06 · Watermark sobre `ingest_ts`; los tardíos no se descartan

Contexto. Cada archivo de eventos trae eventos de los 60 días del período, así que los eventos no llegan en orden de fecha.

Evidencia (prueba con Structured Streaming, notebook 01):

| Configuración | Filas escritas de 43.200 | Descartadas |
|---|---|---|
| `withWatermark("timestamp", "1 hour")` + dedupe | 7.228 | 35.972 |
| `withWatermark("timestamp", "60 days")` + dedupe | 43.200 | 0, con las 43.200 claves en estado |
| `withWatermark("ingest_ts", "1 hour")` + dedupe | 43.200 | 0 |
| Lo mismo, re-ejecutado con el mismo checkpoint | 0 nuevas | 0 |

Decisión. La deduplicación en streaming usa `dropDuplicatesWithinWatermark(["event_id"])` con el watermark sobre `ingest_ts` y un retraso de una hora. Cubre el duplicado típico, que es el mismo archivo entregado dos veces, con estado acotado. Ningún evento se descarta por su fecha: el `foreachBatch` recalcula en Silver y Gold los días que tocó cada lote. Se marca como tardío el evento cuya fecha es más de una hora anterior a la máxima vista, para medirlo.

Alternativas. Watermark de 60 días sobre la fecha del evento: no pierde datos, pero el estado guarda todo el período, y el número sale de este dataset y no de una regla de negocio. Watermark corto sobre la fecha del evento: pierde el 83%.

Consecuencias. Un duplicado que llegue con más de una hora de diferencia pasa la capa speed; lo descarta la regla de unicidad de `event_id` en Silver. Con datos desordenados, cada lote toca casi todas las fechas y el recálculo por lote es casi completo. Con datos reales, que llegan mayormente en orden, tocaría uno o dos días.

## D-07 · Particionado por zona

Evidencia (notebook 01). Escribir Bronze de eventos particionado por fecha del evento deja 3.600 archivos de 12 eventos (5,4 KB cada uno). Por fecha de ingesta deja 60 archivos de 720 eventos.

Decisión.

| Tabla | Partición |
|---|---|
| Bronze `usage_events` | `ingest_date` |
| Silver `usage_events` | `event_date` |
| Gold diarios | fecha del grano |
| `billing_monthly` | `month` |
| Resto de los maestros | sin partición |

`spark.sql.shuffle.partitions = 8` y `repartition` por la columna de partición antes de escribir Silver y Gold.

Consecuencias. Con más volumen conviene sumar `service` como segunda partición en Silver.

## D-08 · Parquet sin Delta Lake

Contexto. La consigna pide Parquet. Delta Lake daría `MERGE` y haría más simples los upserts.

Decisión. Parquet. La idempotencia se logra con checkpoints, claves naturales, sobrescritura dinámica de particiones (`partitionOverwriteMode=dynamic`) y upserts en Cassandra, que escribe por clave primaria.

Se revisa en la entrega 2 si la sobrescritura de particiones resulta insuficiente.

## D-09 · Serving en AstraDB o Cassandra local (abierta)

Opciones. AstraDB en plan gratuito: funciona desde Colab sin instalar nada, depende de la red y de credenciales. Cassandra en Docker: no depende de la red, no corre en Colab.

Criterio. Que el profesor pueda reproducir la demo. Plan alternativo: la otra opción, más capturas y salidas guardadas en `evidence/`.

## D-10 · Tipo de cambio en facturas USD y subtotales negativos (abierta)

Contexto. Las 160 facturas en USD tienen `exchange_rate_to_usd` entre 0,85 y 1,12, no 1. Forzar 1 cambia el total facturado en 108 USD sobre 164.185 (0,07%). Hay 13 facturas con subtotal negativo.

Se consulta en el foro antes de la entrega 2. Mientras tanto se aplica el tipo de cambio como viene y se marcan las dos situaciones.

## D-11 · Criterio de spikes de costo (abierta)

Evidencia (notebook 01). z robusto (MAD) mayor a 3,5 sobre el costo marca 12.448 eventos (29%). El mismo criterio sobre `log(1 + costo)` marca 52. Costo mayor a 5 veces el p99 del servicio marca 75.

Criterio preliminar: z robusto sobre el logaritmo, por servicio. El umbral se fija en la entrega 2.
