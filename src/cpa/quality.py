"""Reglas de calidad y escritura de quarantine.

Una regla separa un DataFrame en lo que pasa y lo que no. Lo que no pasa va a
quarantine con la regla, el motivo, el registro original en JSON y el run_id,
para poder revisarlo y reprocesarlo. Nada se descarta en silencio: por fuente,
filas que entran = filas que siguen + filas en quarantine.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

QUARANTINE_COLUMNS = ["source", "dq_rule", "dq_reason", "record", "run_id", "quarantined_at"]


def split(df: DataFrame, bad: Column, rule: str, reason: str) -> tuple[DataFrame, DataFrame]:
    """Devuelve (filas que pasan, filas que fallan con dq_rule y dq_reason)."""
    flagged = df.withColumn("_dq_bad", F.coalesce(bad, F.lit(False)))
    good = flagged.filter(~F.col("_dq_bad")).drop("_dq_bad")
    failed = (
        flagged.filter(F.col("_dq_bad"))
        .drop("_dq_bad")
        .withColumn("dq_rule", F.lit(rule))
        .withColumn("dq_reason", F.lit(reason))
    )
    return good, failed


def to_quarantine(failed: DataFrame, source: str, run_id: str, partition_col: str) -> DataFrame:
    """Lleva filas fallidas al formato común de quarantine, conservando la columna de partición."""
    original = [c for c in failed.columns if c not in ("dq_rule", "dq_reason")]
    return failed.select(
        F.lit(source).alias("source"),
        "dq_rule",
        "dq_reason",
        F.to_json(F.struct(*[F.col(c) for c in original])).alias("record"),
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("quarantined_at"),
        F.col(partition_col),
    )


def write_quarantine(rows: DataFrame, quarantine_dir: str, source: str, partition_col: str) -> None:
    """Escribe quarantine particionado; con overwrite dinámico, reprocesar no duplica."""
    (
        rows.repartition(partition_col)
        .write.mode("overwrite")
        .partitionBy(partition_col)
        .parquet(f"{quarantine_dir}/{source}")
    )
