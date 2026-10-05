# Registro de decisiones

Cada decisión tiene estado, contexto, alternativas y consecuencias. Las abiertas se cierran en la entrega indicada. Las evidencias citadas están en `evidence/01_exploracion/` y en `notebooks/01_exploracion_landing.ipynb`.

| ID | Decisión | Estado | Fecha |
|---|---|---|---|
| D-01 | Patrón Lambda | Tomada | 28/09/2026 |
| D-02 | PySpark 4.0.4, Java 17 local y Java 21 en Colab | Tomada | 04/10/2026 |
| D-03 | Landing inmutable con manifiesto SHA-256 | Tomada | 28/09/2026 |
| D-04 | Esquemas explícitos y cast con fallback | Tomada | 28/09/2026 |
| D-05 | Micro-lotes de 10 archivos | Tomada | 28/09/2026 |
| D-06 | Watermark sobre `ingest_ts`; los tardíos no se descartan | Tomada | 28/09/2026 |
| D-07 | Particionado por zona | Tomada | 04/10/2026 |
| D-08 | Parquet sin Delta Lake | Tomada | 04/10/2026 |
| D-09 | Serving en AstraDB o Cassandra local | Abierta | Entrega 2 |
| D-10 | Facturación: tipo de cambio en USD, subtotales negativos y revenue | Tomada | 04/10/2026 |
| D-11 | Criterio de spikes de costo | Abierta | Entrega 2 |
| D-12 | Costo estimado de GenAI | Tomada | 04/10/2026 |

---

## D-01 · Patrón Lambda

Contexto. Los maestros, la facturación, las encuestas y los tickets cambian una vez por día o por mes. Los eventos de uso llegan de forma continua y FinOps necesita el costo del día.

Decisión. Capa batch para todo lo que no son eventos y para el recálculo diario de los marts. Capa speed con Structured Streaming solo para `usage_events_stream`.

Alternativas. Kappa: obliga a tratar como stream una facturación mensual de 240 filas y, con eventos desordenados (D-06), recalcular por fecha exige re-stream completo o un watermark que cubra todo el período. Híbrido: no resuelve nada que Lambda no resuelva acá, y suma complejidad.

Consecuencias. Hay dos caminos de código. Se mitiga con funciones de transformación compartidas entre el batch y el `foreachBatch` del streaming.

## D-02 · PySpark 4.0.4, Java 17 local y Java 21 en Colab

Contexto. La consigna pide que corra en Colab o en un entorno equivalente validado. La imagen de Colab a octubre de 2026 trae Ubuntu 24.04, Python 3.13, Java 21 y PySpark 4.0.4 ya instalado (según el repositorio público `googlecolab/backend-info`). Spark 4.0 corre sobre Java 17 o 21 y Python 3.9 a 3.13.

La primera versión de esta decisión (28/09) fijaba PySpark 3.5.9 suponiendo que Colab traía Java 11. Ya no es así: Spark 3.5 soporta Java 8, 11 y 17 y Python hasta 3.11, así que en el Colab actual quedaba fuera de lo soportado.

Decisión. PySpark 4.0.4 fijado en `requirements.txt`, la misma versión que trae Colab. En local, Java 17 de Homebrew en `JAVA_HOME`. En Colab no hay que instalar nada.

Consecuencias.

- Spark 4 trae el modo ANSI activado: un cast inválido corta el job con error en vez de devolver nulo. `cast_with_fallback` usa `try_cast` por eso (D-04).
- `dropDuplicatesWithinWatermark`, que hace falta para D-06, sigue disponible.
- El conector de Spark para Cassandra no tiene versión publicada para Spark 4 (la última es la 3.5.1). La carga a Cassandra va con el driver de Python (D-09).

## D-03 · Landing inmutable con manifiesto SHA-256

Contexto. La consigna prohíbe modificar Landing, pero no dice cómo detectar si alguien lo hizo.

Decisión. `make landing` extrae solo `datalake/landing/` del zip, deja los archivos en solo lectura y los compara contra `data/landing_manifest.csv`. Si hay una diferencia, termina con error.

Consecuencias. Landing se puede borrar y regenerar con un comando. El manifiesto se versiona en el repo y es la referencia.

## D-04 · Esquemas explícitos y cast con fallback

Contexto. `value` llega como número en unos eventos y como texto en otros. `tags_json` escapa las comillas duplicándolas.

Decisión. Ninguna lectura usa `inferSchema`. `value` se lee como texto y se castea con `cast_with_fallback`, que usa `try_cast`, conserva el original en `value_raw` y marca los fallos. Los CSV se leen con `escape='"'`. Todos los esquemas llevan `_corrupt_record`.

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

Evidencia (notebook 01). Escribir Bronze de eventos particionado por fecha del evento deja 3.600 archivos de 12 eventos (5,6 KB cada uno). Por fecha de ingesta deja 60 archivos de 720 eventos. Son cifras de una máquina de 8 núcleos: cada tarea escribe un archivo por carpeta y la cantidad de tareas por micro-lote depende de los núcleos. En Colab, con 2 núcleos, quedan 1.440 archivos contra 24. La proporción, 60 a 1, no cambia.

Decisión.

| Tabla | Partición |
|---|---|
| Bronze `usage_events` | `ingest_date` |
| Silver `usage_events` | `event_date` |
| Gold diarios | fecha del grano |
| `billing_monthly` | `month` |
| Bronze del resto de los maestros y fuentes batch | `ingest_date`: una foto completa por carga diaria |
| Silver del resto de los maestros y fuentes batch | sin partición: la versión vigente, entre 80 y 1.500 filas |

`spark.sql.shuffle.partitions = 8` y `repartition` por la columna de partición antes de escribir Silver y Gold.

La primera versión (28/09) dejaba los maestros sin partición por su tamaño. Se corrigió porque la consigna pide ingestarlos a Parquet particionado (6.2), y la foto diaria además permite reprocesar un día pisando solo su partición y reconstruir la historia si una consulta necesita SCD.

Consecuencias. Con más volumen conviene sumar `service` como segunda partición en Silver de eventos.

## D-08 · Parquet sin Delta Lake

Contexto. Delta Lake es una capa sobre Parquet: los datos siguen en archivos Parquet y un log de transacciones agrega `MERGE` (upsert por clave), transacciones y versiones. Con `MERGE`, un reproceso no puede duplicar filas.

Decisión. Parquet puro en todas las zonas.

Por qué.

- La consigna pide Parquet como almacenamiento intermedio (2.2 y 4.2) y no menciona Delta.
- Delta solo se nombró en clase como formato avanzado; no se vio, y el docente pidió no sumar tecnologías que no se dieron.
- La idempotencia no necesita `MERGE` en este diseño: checkpoints del streaming, claves naturales, sobrescritura dinámica de particiones (`partitionOverwriteMode=dynamic`) y upserts en Cassandra, que escribe por clave primaria. Cada reproceso recalcula particiones completas, así que pisar la partición alcanza.
- Suma una dependencia (`delta-spark`) que habría que instalar en Colab.

## D-09 · Serving en AstraDB o Cassandra local (abierta)

Opciones. AstraDB en plan gratuito: funciona desde Colab sin instalar nada, depende de la red y de credenciales. Cassandra en Docker: no depende de la red, no corre en Colab.

Criterio. Que el profesor pueda reproducir la demo. Plan alternativo: la otra opción, más capturas y salidas guardadas en `evidence/`.

Con cualquiera de las dos, la carga desde Spark va con el driver de Python de Cassandra dentro de `foreachBatch` (o desde el driver de Spark para los marts batch), porque el conector de Spark para Cassandra no está publicado para Spark 4 (D-02). La consigna permite las dos vías (4.4) y los marts de Gold son chicos.

## D-10 · Facturación: tipo de cambio en USD, subtotales negativos y revenue

Evidencia: notebook 01, sección 11, y `evidence/01_exploracion/revenue_escenarios.csv`.

Tipo de cambio en USD. Las 160 facturas en USD traen `exchange_rate_to_usd` entre 0,85 y 1,12. Un dólar vale un dólar, así que en Silver el tipo de cambio de USD se fija en 1, el original queda en `exchange_rate_to_usd_raw` y un flag marca la fila. Con todas las facturas, el total cambia en 108 USD.

Subtotales negativos. Hay 13. En las 240 facturas el impuesto es el 21% del subtotal en valor absoluto, y en estas 13 es positivo. Una nota de crédito tendría el impuesto en negativo, y los créditos ya tienen su propia columna. Se toman como error: van a quarantine con su regla y no entran al revenue.

Revenue. (subtotal − créditos + impuestos) × tipo de cambio, con `credits` nulo tomado como 0. Total con las dos reglas: 172.252,22 USD sobre 227 facturas.

Lo que nos llama la atención. 50 de las 80 organizaciones cambian de moneda entre un mes y otro, y los montos en ARS son del mismo orden que en USD (mediana de 816 pesos contra 655 dólares). Al convertir, cada factura en pesos queda en uno o dos dólares, y el revenue de esas organizaciones salta de un mes a otro por la moneda y no por el consumo. Se aplica la conversión como viene, porque la consigna pide revenue normalizado a USD, y el mart conserva la moneda original y el tipo de cambio para poder revisarlo.

## D-11 · Criterio de spikes de costo (abierta)

Evidencia (notebook 01). z robusto (MAD) mayor a 3,5 sobre el costo marca 12.448 eventos (29%). El mismo criterio sobre `log(1 + costo)` marca 52. Costo mayor a 5 veces el p99 del servicio marca 75.

Criterio preliminar: z robusto sobre el logaritmo, por servicio. El umbral se fija en la entrega 2.

## D-12 · Costo estimado de GenAI

Contexto. La consulta 5 pide tokens de GenAI y "costo estimado" por día, sin decir de dónde sale el costo. El `cost_usd_increment` de cada evento corresponde a su métrica (requests, horas de CPU o almacenamiento) y no a los tokens: dividido por los tokens va de 0 a 3,23 USD por token, y en total da 8.746 USD por millón de tokens. Los eventos no traen precio por token ni separan tokens de entrada y salida.

Decisión. `genai_tokens_by_org_date` publica los dos costos:

- `cost_usd`: suma de `cost_usd_increment` de los eventos de genai del día. Es el costo que registró la plataforma para el servicio.
- `estimated_token_cost_usd`: tokens × `genai.price_usd_per_million_tokens` de `config/settings.toml`, hoy 2 USD por millón.

Precio de referencia. Precios por millón de tokens de entrada y de salida a octubre de 2026, de modelos de gama media (ni los más caros ni los más baratos de cada proveedor):

| Modelo | Entrada | Salida | Mezcla 3:1 |
|---|---|---|---|
| Gemini 3.8 Flash (precio vigente hasta el 31/12/2026) | 0,75 | 3,75 | 1,50 |
| GPT-5.4 mini | 0,75 | 4,50 | 1,69 |
| Claude Haiku 4.5 | 1,00 | 5,00 | 2,00 |
| Claude Sonnet 5.5 | 2,00 | 10,00 | 4,00 |

La mezcla supone 3 tokens de entrada por cada uno de salida, que es lo habitual en chat y búsqueda sobre documentos. Se toman 2 USD por millón, el valor de un modelo de gama media. Con los 2.545.481 tokens del dataset da 5,09 USD, contra 22.263,35 USD de `cost_usd_increment` en los mismos eventos.

Consecuencias. El precio es un supuesto y se cambia en la configuración sin tocar código. Las dos columnas responden la consulta 5 con cualquiera de las dos lecturas de "costo estimado".
