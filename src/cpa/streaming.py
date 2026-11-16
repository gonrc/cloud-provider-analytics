"""Capa speed: eventos de uso de Landing a Bronze con Structured Streaming.

- Esquema explícito (USAGE_EVENTS) y `maxFilesPerTrigger` = 10: 12 micro-lotes (D-05).
- Watermark de 1 hora sobre `ingest_ts` y `dropDuplicatesWithinWatermark(["event_id"])`
  (D-06): acota el estado de la deduplicación sin descartar eventos por su fecha.
- Checkpoint propio de la query: re-ejecutar no vuelve a leer archivos ya procesados.
- foreachBatch idempotente: cada lote se escribe en su partición
  `ingest_date=.../batch_id=N` con overwrite dinámico. Si Spark repite el lote N
  después de una falla (offsets/N sin commits/N), lo pisa en lugar de duplicarlo.
- Después de escribir Bronze, el lote recalcula Silver y Gold de las fechas de evento
  que tocó, y si Cassandra está disponible hace upsert de esas fechas. Así el
  evento tardío corrige su día en lugar de perderse.

Uso: python -m cpa.streaming [--sin-speed] [--sin-serving]
"""
import argparse
import csv
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cpa import gold, quality, silver
from cpa.casting import cast_with_fallback
from cpa.config import load_settings
from cpa.runlog import Run
from cpa.schemas import USAGE_EVENTS
from cpa.spark import get_spark


def build_stream(spark, cfg: dict) -> DataFrame:
    s = cfg["streaming"]
    stream = (
        spark.readStream.schema(USAGE_EVENTS)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("maxFilesPerTrigger", s["max_files_per_trigger"])
        .json(f"{cfg['paths']['landing']}/usage_events_stream")
        .withColumn("source_file", F.col("_metadata.file_name"))
        .withColumn("ingest_ts", F.current_timestamp())
        .withWatermark("ingest_ts", s["late_threshold"])
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    return cast_with_fallback(stream, "value", "double")


class BatchWriter:
    """foreachBatch: Bronze idempotente por lote + recálculo de las fechas tocadas."""

    def __init__(self, spark, cfg: dict, run: Run, speed: bool, serving_on: bool):
        self.spark, self.cfg, self.run = spark, cfg, run
        self.speed, self.serving_on = speed, serving_on
        self.max_ts_seen = None
        self.rows = []

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        paths = self.cfg["paths"]
        b = (
            batch.withColumn("batch_id", F.lit(batch_id))
            .withColumn("ingest_date", F.to_date("ingest_ts"))
            .withColumn("run_id", F.lit(self.run.run_id))
            .cache()
        )
        ok, corrupt = quality.split(b, F.col("_corrupt_record").isNotNull(), "parse_error", "el evento no respeta el esquema")
        ok = ok.drop("_corrupt_record")
        (ok.repartition("ingest_date").write.mode("overwrite")
           .partitionBy("ingest_date", "batch_id").parquet(f"{paths['bronze']}/usage_events"))
        n_bad = corrupt.count()
        if n_bad:
            quality.write_quarantine(quality.to_quarantine(corrupt, "usage_events", self.run.run_id, "ingest_date"),
                                     paths["quarantine"], "usage_events_bronze", "ingest_date")

        # Las fechas se formatean en Spark (UTC): collect() las convertiría a la hora local de la máquina.
        utc = "yyyy-MM-dd HH:mm:ss'Z'"
        stats = ok.agg(F.count("*").alias("n"), F.date_format(F.min("timestamp"), utc).alias("lo"),
                       F.date_format(F.max("timestamp"), utc).alias("hi"), F.max("timestamp").alias("hi_ts"),
                       F.countDistinct(F.to_date("timestamp")).alias("dias")).first()
        dates = [r[0] for r in ok.select(F.to_date("timestamp")).distinct().collect()]
        late = 0
        if self.max_ts_seen is not None:
            late = ok.filter(F.col("timestamp") < F.lit(self.max_ts_seen) - F.expr(f"INTERVAL {self.cfg['streaming']['late_threshold']}")).count()
        self.max_ts_seen = max(filter(None, [self.max_ts_seen, stats["hi_ts"]]), default=None)
        t_bronze = time.time() - t0

        if self.speed and dates:
            silver.build_events(self.spark, self.cfg, self.run, dates).unpersist()
            gold.build_daily(self.spark, self.cfg, self.run, dates)
            if self.serving_on:
                from cpa import serving
                serving.load_daily(self.spark, self.cfg, self.run, dates)
        self.rows.append({
            "batch_id": batch_id, "filas_bronze": stats["n"], "filas_quarantine": n_bad,
            "primer_evento": stats["lo"], "ultimo_evento": stats["hi"], "fechas_tocadas": stats["dias"],
            "tardios_vs_lotes_previos": late, "seg_bronze": round(t_bronze, 1),
            "seg_total": round(time.time() - t0, 1),
        })
        b.unpersist()
        print(f"lote {batch_id}: {stats['n']} eventos, {stats['dias']} fechas tocadas, {late} tardíos, {self.rows[-1]['seg_total']} s")


def run_stream(spark, cfg: dict, run: Run, speed: bool = True, serving_on: bool = True, checkpoint_name: str = "bronze_usage_events"):
    writer = BatchWriter(spark, cfg, run, speed, serving_on)
    q = (
        build_stream(spark, cfg).writeStream.foreachBatch(writer)
        .option("checkpointLocation", f"{cfg['paths']['checkpoints']}/{checkpoint_name}")
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    progress = []
    for p in q.recentProgress:
        ops = (p.get("stateOperators") or [{}])[0]
        progress.append({
            "batch_id": p["batchId"], "numInputRows": p["numInputRows"],
            "processedRowsPerSecond": round(p.get("processedRowsPerSecond") or 0, 1),
            "triggerExecution_ms": p.get("durationMs", {}).get("triggerExecution"),
            "watermark": (p.get("eventTime") or {}).get("watermark"),
            "estado_filas": ops.get("numRowsTotal"), "descartadas_por_watermark": ops.get("numRowsDroppedByWatermark"),
            "duplicados_descartados": (ops.get("customMetrics") or {}).get("numDroppedDuplicateRows"),
        })
    return writer.rows, progress


def _write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-speed", action="store_true", help="solo Bronze, sin recalcular Silver y Gold por lote")
    ap.add_argument("--sin-serving", action="store_true", help="no cargar Cassandra desde la capa speed")
    args = ap.parse_args()
    cfg = load_settings()
    spark = get_spark(cfg)
    run = Run("streaming", cfg["paths"]["evidence"])
    serving_on = not args.sin_serving
    if serving_on:
        from cpa import serving
        serving_on = serving.available(cfg)
        if not serving_on:
            print("Cassandra no responde: la capa speed recalcula Silver y Gold pero no carga serving")
    rows, progress = run_stream(spark, cfg, run, speed=not args.sin_speed, serving_on=serving_on)
    ev = Path(cfg["paths"]["evidence"]) / "02_entrega"
    if rows:
        _write_csv(rows, ev / "streaming_lotes.csv")
        _write_csv(progress, ev / "streaming_progreso.csv")
    run.count("streaming", lotes=len(rows), eventos_bronze=sum(r["filas_bronze"] for r in rows),
              quarantine=sum(r["filas_quarantine"] for r in rows), tardios=sum(r["tardios_vs_lotes_previos"] for r in rows))
    print("lotes procesados:", len(rows), "| registro:", run.save())
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
