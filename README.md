# Cloud Provider Analytics

Proyecto integrador de Big Data (72.80), ITBA, 2.º cuatrimestre 2026. Prof. Diego Mosquera. Entrega individual de Gonzalo Ruiz Camauer.

Pipeline de datos para un proveedor de nube: ingesta batch y streaming de ocho fuentes, Data Lake Landing / Bronze / Silver / Gold en Parquet y marts servidos en Cassandra para FinOps, Soporte y Producto. Stack: PySpark 3.5, Structured Streaming, Parquet y Cassandra/AstraDB.

## Estado

| Entrega | Fecha límite | Qué incluye | Estado |
|---|---|---|---|
| 1 · Diseño y fundación | 28/09/2026 18:30 | [Documento de diseño v1](docs/diseno_v1.md), [decisiones](DECISIONS.md), repositorio inicial y [exploración de datos](notebooks/01_exploracion_landing.ipynb) | En curso |
| 2 · Implementación técnica | 16/11/2026 18:30 | Bronze, Silver y Gold ejecutables, calidad, Cassandra, idempotencia | Pendiente |
| Final · MVP y defensa | 07/12/2026 21:30 | Pipeline completo, 5 consultas, documentación, video | Pendiente |

Lo que se suba después de la fecha límite de una entrega va en commits que empiezan con `feedback:`.

## Dónde está cada artefacto de la entrega 1

| Artefacto | Ubicación |
|---|---|
| Documento de diseño | [`docs/diseno_v1.md`](docs/diseno_v1.md) (también en PDF: [`docs/diseno_v1.pdf`](docs/diseno_v1.pdf)) |
| Diagrama de arquitectura v1 | [Sección 4 del documento](docs/diseno_v1.md#4-arquitectura-v1) |
| Matriz requisito-componente | [Sección 6](docs/diseno_v1.md#6-matriz-requisito-componente) |
| Plan inicial: supuestos, riesgos, esfuerzo, próximos pasos | [Secciones 10 a 13](docs/diseno_v1.md#10-supuestos-riesgos-y-decisiones-abiertas) |
| Registro de decisiones | [`DECISIONS.md`](DECISIONS.md) |
| Evidencia de exploración | [`notebooks/01_exploracion_landing.ipynb`](notebooks/01_exploracion_landing.ipynb) y [`evidence/01_exploracion/`](evidence/01_exploracion/) |
| Plan de correcciones | [`docs/plan_de_correcciones.md`](docs/plan_de_correcciones.md), se completa después del feedback |

## Cómo correrlo

### En Google Colab

Abrir el notebook desde GitHub: [01_exploracion_landing.ipynb en Colab](https://colab.research.google.com/github/gonrc/cloud-provider-analytics/blob/main/notebooks/01_exploracion_landing.ipynb). La primera celda instala PySpark 3.5.9, clona el repo y extrae los datos. Colab ya trae Java 11, que Spark 3.5 soporta.

### En local (macOS o Linux)

Requisitos: Python 3.11, Java 17 (o 11) y [uv](https://docs.astral.sh/uv/). En macOS con Homebrew:

```bash
brew install openjdk@17
export JAVA_HOME=$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home
```

```bash
git clone https://github.com/gonrc/cloud-provider-analytics.git
cd cloud-provider-analytics
make setup      # crea .venv e instala dependencias
make landing    # extrae Landing del zip y la verifica contra el manifiesto
make explore    # ejecuta el notebook de exploración y regenera evidence/01_exploracion/
make test       # tests de casting y del control de Landing
make clean      # borra datalake/; se regenera con make landing
```

Salida esperada de `make landing`: `Landing coincide con el manifiesto`. `make explore` tarda menos de un minuto en una notebook, partiendo de cero.

## Estructura

```
├── config/settings.toml   rutas y parámetros; credenciales en .env (ver .env.example)
├── data/raw/              zip del dataset de la cátedra
├── data/landing_manifest.csv   SHA-256 de cada archivo de Landing
├── src/cpa/               config, sesión de Spark, esquemas, cast con fallback, Landing
├── notebooks/             exploración y prácticas
├── tests/
├── evidence/              salidas de cada entrega
├── docs/                  documento de diseño, plan de correcciones
├── DECISIONS.md
└── datalake/              generado, no se versiona
```

## Convenciones

- Código, tablas y columnas en inglés y en `snake_case`, con los nombres de la fuente. Documentación en castellano.
- Ninguna lectura usa `inferSchema`: los esquemas están en `src/cpa/schemas.py`.
- Landing no se modifica. Se regenera desde el zip y se verifica por SHA-256.
- Rutas y parámetros en `config/settings.toml`. Credenciales solo en `.env`, que no se versiona.
- Columnas técnicas: `ingest_ts`, `source_file`, `run_id`. Columnas de calidad: prefijo `dq_`.

## Limitaciones conocidas

- Por ahora solo existe Landing y la exploración. Bronze, Silver, Gold y Cassandra son de la entrega 2.
- Las decisiones D-09, D-10 y D-11 de `DECISIONS.md` están abiertas.
