# Cloud Provider Analytics

Proyecto integrador de Big Data (72.80), ITBA, 2.º cuatrimestre 2026. Prof. Diego Mosquera. Grupo 9: Nicolás Martín Amarilla Díaz, Francisco Cattaneo, Juan Manuel Rilo y Gonzalo Ruiz Camauer.

Pipeline de datos para un proveedor de nube: ingesta batch y streaming de ocho fuentes, Data Lake Landing / Bronze / Silver / Gold en Parquet y marts servidos en Cassandra para FinOps, Soporte y Producto. Stack: PySpark 4.0, Structured Streaming, Parquet y Cassandra 5.0.

## Estado

| Entrega | Fecha límite | Qué incluye | Estado |
|---|---|---|---|
| 1 · Diseño y fundación | 05/10/2026 18:30 | [Documento de diseño v1](docs/diseno_v1.md), [decisiones](DECISIONS.md), repositorio inicial y [exploración de datos](notebooks/01_exploracion_landing.ipynb) | Entregada, tag `entrega-1` |
| 2 · Implementación técnica | 16/11/2026 18:30 | Pipeline Landing → Bronze → Silver → Gold → Cassandra, batch y streaming, calidad, anomalías, idempotencia. Ver [informe de la entrega 2](docs/entrega_2.md) | Entregada, tag `entrega-2` |
| Final · MVP y defensa | 07/12/2026 21:30 | Pipeline completo, 5 consultas, documentación, video | Pendiente |

Lo que se suba después de la fecha límite de una entrega va en commits que empiezan con `feedback:`.

## Dónde está cada artefacto de la entrega 2

| Artefacto | Ubicación |
|---|---|
| Informe: qué se implementó, experimentos y checklist 9.2 | [`docs/entrega_2.md`](docs/entrega_2.md) |
| Diagrama de arquitectura v2 (lo implementado) | [`docs/img/arquitectura_v2.svg`](docs/img/arquitectura_v2.svg) |
| Código del pipeline | [`src/cpa/`](src/cpa/): `bronze`, `streaming`, `silver`, `gold`, `serving`, `pipeline` |
| Scripts CQL | [`cql/schema.cql`](cql/schema.cql) y [`cql/queries.cql`](cql/queries.cql) |
| Registro de decisiones | [`DECISIONS.md`](DECISIONS.md) |
| Gobierno, diccionario de datos y linaje | [`docs/gobierno.md`](docs/gobierno.md) |
| Evidencias de ejecución | [`evidence/02_entrega/`](evidence/02_entrega/) y un registro por corrida en [`evidence/runs/`](evidence/runs/) |
| Backlog para el MVP final | [`docs/backlog.md`](docs/backlog.md) |
| Plan de correcciones de la entrega 1 | [`docs/plan_de_correcciones.md`](docs/plan_de_correcciones.md) |

Los artefactos de la entrega 1 siguen en el [tag `entrega-1`](https://github.com/gonrc/cloud-provider-analytics/tree/entrega-1).

## Quickstart

### 1. Entorno

Requisitos: Python 3.11 o posterior, Java 17 y [uv](https://docs.astral.sh/uv/). En macOS con Homebrew:

```bash
brew install openjdk@17
export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home
```

```bash
git clone https://github.com/gonrc/cloud-provider-analytics.git
cd cloud-provider-analytics
make setup      # crea .venv e instala dependencias, incluido el driver de Cassandra
make test       # 10 tests de calidad, facturación, casting, tardíos y Landing
```

### 2. Cassandra (serving)

Cassandra 5.0 corre sobre Java 11 o 17. La forma más liviana es el binario oficial de Apache, sin instalar nada más:

```bash
mkdir -p ~/opt && cd ~/opt
curl -LO https://dlcdn.apache.org/cassandra/5.0.9/apache-cassandra-5.0.9-bin.tar.gz
tar -xzf apache-cassandra-5.0.9-bin.tar.gz
MAX_HEAP_SIZE=1G HEAP_NEWSIZE=256M ~/opt/apache-cassandra-5.0.9/bin/cassandra
```

Con Docker es equivalente: `docker run -d --name cassandra -p 9042:9042 cassandra:5.0`. El pipeline busca Cassandra en `127.0.0.1:9042` (se cambia en `config/settings.toml`). `make cqlsh` abre la consola.

### 3. Pipeline

```bash
make pipeline       # Landing → Bronze → Silver → streaming → Gold → Cassandra, unos 4 minutos
make experimentos   # los cinco experimentos del informe, unos 2 minutos
make idempotencia   # re-ejecuta el pipeline dos veces y compara conteos, unos 10 minutos
```

Salida esperada de `make pipeline`: 12 micro-lotes de 3.600 eventos, `controles fallidos: ninguno` y la salida de las dos consultas CQL en `evidence/02_entrega/consultas_cql.txt`. Si Cassandra no responde, el pipeline lo avisa y corre igual hasta Gold.

Cada paso también se puede correr suelto: `make bronze`, `make silver`, `make stream`, `make gold`, `make serve`. `make reset` borra Bronze, Silver, Gold, quarantine y checkpoints, y deja Landing.

### En Google Colab

Colab ya trae PySpark 4.0.4 y Java 21, así que no hay que instalar nada. Hay dos notebooks que se abren directo desde GitHub:

- [02_pipeline_entrega2.ipynb en Colab](https://colab.research.google.com/github/gonrc/cloud-provider-analytics/blob/main/notebooks/02_pipeline_entrega2.ipynb): corre el pipeline de la entrega 2 sin serving, porque Colab no tiene Cassandra, y muestra conteos, controles, micro-lotes, quarantine y Gold. Tarda unos 7 minutos.
- [01_exploracion_landing.ipynb en Colab](https://colab.research.google.com/github/gonrc/cloud-provider-analytics/blob/main/notebooks/01_exploracion_landing.ipynb): la exploración de la entrega 1.

## Estructura

```
├── config/settings.toml   rutas y parámetros; credenciales en .env (ver .env.example)
├── data/raw/              zip del dataset de la cátedra
├── data/landing_manifest.csv   SHA-256 de cada archivo de Landing
├── src/cpa/               Landing, Bronze, streaming, Silver, Gold, serving y el pipeline
├── cql/                   keyspace, tablas y consultas de Cassandra
├── scripts/               experimentos, prueba de idempotencia y armado de PDF
├── notebooks/             exploración (entrega 1) y pipeline en Colab (entrega 2)
├── tests/
├── evidence/              salidas de cada entrega y registro por corrida
├── docs/                  diseño, informe de la entrega 2, gobierno, backlog
├── DECISIONS.md
└── datalake/              generado, no se versiona
```

## Convenciones

- Código, tablas y columnas en inglés y en `snake_case`, con los nombres de la fuente. Documentación en castellano.
- Ninguna lectura usa `inferSchema`: los esquemas están en `src/cpa/schemas.py`.
- Landing no se modifica. Se regenera desde el zip y se verifica por SHA-256.
- Rutas y parámetros en `config/settings.toml`. Credenciales solo en `.env`, que no se versiona.
- Columnas técnicas: `ingest_ts`, `ingest_date`, `source_file`, `run_id` (y `batch_id` en eventos). Columnas de calidad: prefijo `dq_`.
- Toda escritura es idempotente: overwrite dinámico de particiones en el lake y upsert por clave primaria en Cassandra.

## Limitaciones conocidas

- En Colab el pipeline corre sin serving. La demo de Cassandra es local.
- El estado de la deduplicación en streaming está acotado a una hora de llegadas. Una corrida completa dura unos minutos, así que dentro de una corrida no vence nada (ver [informe](docs/entrega_2.md), experimento E1).
- Gold tiene por ahora los marts `org_daily_usage_by_service` y `cost_anomaly_mart`. Los demás marts y las consultas 3 a 5 son de la entrega final ([backlog](docs/backlog.md)).
