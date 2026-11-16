"""Ingesta batch: fuentes CSV de Landing a Bronze en Parquet.

Bronze tiene el mismo grano que la fuente, tipos explícitos (los esquemas de
schemas.py) y columnas técnicas: ingest_ts, ingest_date, source_file y run_id.
No corrige nada de negocio; solo separa a quarantine las filas que no parsean y
los duplicados exactos de clave natural dentro de la misma carga.

Particiones (D-07): billing_monthly por `month`, que es su unidad de carga; el
resto por `ingest_date`, una foto completa por día. Volver a correr el mismo día
pisa solo esa partición, así que la carga es idempotente.

Uso: python -m cpa.bronze
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cpa import quality
from cpa.config import load_settings
from cpa.runlog import Run
from cpa.schemas import SOURCES, read_landing
from cpa.spark import get_spark

BATCH_SOURCES = [name for name, (_, fmt, _, _) in SOURCES.items() if fmt == "csv"]
PARTITION = {"billing_monthly": "month"}


def partition_of(name: str) -> str:
    return PARTITION.get(name, "ingest_date")


def with_technical_columns(df: DataFrame, run: Run) -> DataFrame:
    return (
        df.withColumn("ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.lit(run.ingest_date).cast("date"))
        .withColumn("run_id", F.lit(run.run_id))
    )


def ingest_source(spark: SparkSession, cfg: dict, name: str, run: Run) -> None:
    paths = cfg["paths"]
    key = SOURCES[name][3]
    part = partition_of(name)

    # cache antes de filtrar por _corrupt_record: Spark no permite consultar solo esa
    # columna sobre el archivo crudo.
    raw = read_landing(spark, paths["landing"], name).withColumn("source_file", F.col("_metadata.file_name")).cache()
    rows_in = raw.count()

    parsed, corrupt = quality.split(raw, F.col("_corrupt_record").isNotNull(), "parse_error", "la fila no respeta el esquema de la fuente")
    parsed = with_technical_columns(parsed.drop("_corrupt_record"), run)

    # Duplicado exacto de clave natural dentro de la misma carga: se queda la primera fila.
    w = Window.partitionBy(*key).orderBy(F.monotonically_increasing_id())
    ranked = parsed.withColumn("_rn", F.row_number().over(w))
    ok, dupes = quality.split(ranked, F.col("_rn") > 1, "clave_duplicada", f"{'+'.join(key)} repetido en la misma carga")
    ok, dupes = ok.drop("_rn"), dupes.drop("_rn")

    ok = ok.cache()
    rows_ok = ok.count()
    (ok.repartition(part).write.mode("overwrite").partitionBy(part).parquet(f"{paths['bronze']}/{name}"))

    failed = (
        quality.to_quarantine(with_technical_columns(corrupt, run), name, run.run_id, "ingest_date")
        .unionByName(quality.to_quarantine(dupes, name, run.run_id, "ingest_date"))
        .cache()
    )
    rows_q = failed.count()
    if rows_q:
        quality.write_quarantine(failed, paths["quarantine"], name, "ingest_date")

    run.count(f"bronze.{name}", landing=rows_in, bronze=rows_ok, quarantine=rows_q, partition=part)
    run.check(f"bronze.{name}.conteo", rows_in == rows_ok + rows_q, f"{rows_in} = {rows_ok} + {rows_q}")
    for df in (raw, ok, failed):
        df.unpersist()


def main() -> int:
    cfg = load_settings()
    spark = get_spark(cfg)
    run = Run("bronze_batch", cfg["paths"]["evidence"])
    for name in BATCH_SOURCES:
        ingest_source(spark, cfg, name, run)
        c = run.counts[f"bronze.{name}"]
        print(f"bronze {name:18s} landing={c['landing']:>5} bronze={c['bronze']:>5} quarantine={c['quarantine']:>3} part={c['partition']}")
    print("registro:", run.save())
    spark.stop()
    return 0 if all(c["ok"] for c in run.checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
