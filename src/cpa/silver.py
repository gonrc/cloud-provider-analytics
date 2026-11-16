"""Silver: datos conformados a partir de Bronze.

Eventos de uso (reglas de la sección 7.5 del diseño):
- Deduplicación por event_id: se queda la primera ingesta; las demás van a quarantine.
- event_id no nulo, costo >= min_cost_usd, value numérico y recurso existente; lo que
  falla va a quarantine con la regla.
- unit faltante se completa desde metric (cada métrica tiene una sola unidad) y se marca.
- carbon_kg y genai_tokens quedan nulos en v1: "no se midió" no es "fue cero".
- Evento tardío (D-06): su timestamp es anterior, en más del umbral, al máximo visto en
  los micro-lotes previos. No se descarta; se marca para medirlo.
- Enriquecimiento con plan_tier e industry de customers_orgs (broadcast).

Maestros: customers_orgs, resources y users toman la última foto de Bronze;
billing_monthly aplica D-10 (tipo de cambio de USD en 1, subtotales negativos a
quarantine, revenue en USD).

Cada tabla se escribe con overwrite dinámico de sus particiones, así que recalcular
un conjunto de fechas pisa exactamente esas fechas.

Uso: python -m cpa.silver
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cpa import quality
from cpa.config import load_settings
from cpa.runlog import Run
from cpa.spark import get_spark

UNIT_BY_METRIC = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}


def _latest_snapshot(df: DataFrame) -> DataFrame:
    last = df.agg(F.max("ingest_date")).first()[0]
    return df.filter(F.col("ingest_date") == F.lit(last))


def _norm(col: str):
    return F.lower(F.trim(F.col(col)))


def _write(df: DataFrame, path: str, partition: str | None) -> None:
    if partition:
        df.repartition(partition).write.mode("overwrite").partitionBy(partition).parquet(path)
    else:
        df.coalesce(1).write.mode("overwrite").parquet(path)


# ---------------------------------------------------------------- maestros

def build_customers(spark: SparkSession, cfg: dict, run: Run) -> DataFrame:
    b = _latest_snapshot(spark.read.parquet(f"{cfg['paths']['bronze']}/customers_orgs"))
    out = (
        b.withColumn("dq_nps_out_of_range", F.col("nps_score").isNotNull() & ~F.col("nps_score").between(-100, 100))
        .withColumn("nps_score", F.when(F.col("dq_nps_out_of_range"), None).otherwise(F.col("nps_score")))
        .withColumn("plan_tier", _norm("plan_tier"))
        .withColumn("industry", _norm("industry"))
        .withColumn("hq_region", _norm("hq_region"))
        .withColumn("dq_plan_mismatch", (F.col("plan_tier") == "enterprise") != F.col("is_enterprise"))
        .withColumn("run_id", F.lit(run.run_id))
    )
    _write(out, f"{cfg['paths']['silver']}/customers_orgs", None)
    run.count("silver.customers_orgs", rows=out.count(), nps_fuera_de_rango=out.filter("dq_nps_out_of_range").count(),
              plan_inconsistente=out.filter("dq_plan_mismatch").count())
    return out


def build_resources(spark: SparkSession, cfg: dict, run: Run) -> DataFrame:
    b = _latest_snapshot(spark.read.parquet(f"{cfg['paths']['bronze']}/resources"))
    out = (
        b.withColumn("service", _norm("service"))
        .withColumn("region", _norm("region"))
        .withColumn("tags", F.coalesce(F.from_json("tags_json", "array<string>"), F.array().cast("array<string>")))
        .withColumn("is_pii", F.array_contains("tags", "pii:true"))
        .drop("tags_json")
        .withColumn("run_id", F.lit(run.run_id))
    )
    _write(out, f"{cfg['paths']['silver']}/resources", None)
    run.count("silver.resources", rows=out.count(), pii=out.filter("is_pii").count())
    return out


def build_users(spark: SparkSession, cfg: dict, run: Run) -> DataFrame:
    b = _latest_snapshot(spark.read.parquet(f"{cfg['paths']['bronze']}/users"))
    out = (
        # El email es dato personal: en Silver queda solo su hash, que sigue sirviendo para cruzar.
        b.withColumn("email_hash", F.sha2(F.lower(F.trim("email")), 256))
        .drop("email")
        .withColumn("dq_login_before_created", F.col("last_login") < F.col("created_at"))
        .withColumn("run_id", F.lit(run.run_id))
    )
    _write(out, f"{cfg['paths']['silver']}/users", None)
    run.count("silver.users", rows=out.count(), login_anterior_al_alta=out.filter("dq_login_before_created").count())
    return out


def build_billing(spark: SparkSession, cfg: dict, run: Run) -> DataFrame:
    b = spark.read.parquet(f"{cfg['paths']['bronze']}/billing_monthly")
    w = Window.partitionBy("invoice_id").orderBy(F.desc("ingest_ts"))
    b = b.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    rows_in = b.count()

    # D-10: el impuesto es positivo en los subtotales negativos, así que no son notas de crédito.
    ok, failed = quality.split(b, F.col("subtotal") < 0, "subtotal_negativo",
                               "subtotal negativo con impuesto positivo: no es nota de crédito")
    out = (
        ok.withColumn("currency", F.upper(F.trim("currency")))
        .withColumn("exchange_rate_to_usd_raw", F.col("exchange_rate_to_usd"))
        .withColumn("dq_fx_forced", (F.col("currency") == "USD") & (F.abs(F.col("exchange_rate_to_usd") - 1) > 1e-9))
        .withColumn("exchange_rate_to_usd", F.when(F.col("currency") == "USD", F.lit(1.0)).otherwise(F.col("exchange_rate_to_usd")))
        .withColumn("dq_credits_filled", F.col("credits").isNull())
        .withColumn("credits", F.coalesce("credits", F.lit(0.0)))
        .withColumn("subtotal_usd", F.round(F.col("subtotal") * F.col("exchange_rate_to_usd"), 4))
        .withColumn("credits_usd", F.round(F.col("credits") * F.col("exchange_rate_to_usd"), 4))
        .withColumn("taxes_usd", F.round(F.col("taxes") * F.col("exchange_rate_to_usd"), 4))
        .withColumn("revenue_usd", F.round((F.col("subtotal") - F.col("credits") + F.col("taxes")) * F.col("exchange_rate_to_usd"), 4))
        .withColumn("run_id", F.lit(run.run_id))
    )
    _write(out, f"{cfg['paths']['silver']}/billing_monthly", "month")
    q = quality.to_quarantine(failed, "billing_monthly", run.run_id, "month")
    rows_q = q.count()
    if rows_q:
        quality.write_quarantine(q, cfg["paths"]["quarantine"], "billing_monthly_silver", "month")
    rows_ok = out.count()
    run.count("silver.billing_monthly", bronze=rows_in, silver=rows_ok, quarantine=rows_q,
              fx_forzado=out.filter("dq_fx_forced").count(), creditos_completados=out.filter("dq_credits_filled").count())
    run.check("silver.billing_monthly.conteo", rows_in == rows_ok + rows_q, f"{rows_in} = {rows_ok} + {rows_q}")
    return out


def build_masters(spark: SparkSession, cfg: dict, run: Run) -> None:
    build_customers(spark, cfg, run)
    build_resources(spark, cfg, run)
    build_users(spark, cfg, run)
    build_billing(spark, cfg, run)


# ---------------------------------------------------------------- eventos

def _late_flags(b: DataFrame, threshold: str) -> DataFrame:
    """Máximo timestamp visto en los micro-lotes anteriores a cada lote."""
    per_batch = b.groupBy("ingest_ts", "batch_id").agg(F.max("timestamp").alias("_batch_max"))
    w = Window.orderBy("ingest_ts", "batch_id").rowsBetween(Window.unboundedPreceding, -1)
    prev = per_batch.withColumn("max_ts_previous_batches", F.max("_batch_max").over(w)).drop("_batch_max")
    return (
        b.join(F.broadcast(prev), ["ingest_ts", "batch_id"], "left")
        .withColumn("is_late", F.coalesce(F.col("timestamp") < F.col("max_ts_previous_batches") - F.expr(f"INTERVAL {threshold}"), F.lit(False)))
    )


def build_events(spark: SparkSession, cfg: dict, run: Run, dates: list | None = None) -> DataFrame:
    """Recalcula Silver de eventos. Con `dates`, solo esas fechas de evento (capa speed)."""
    paths = cfg["paths"]
    b = spark.read.parquet(f"{paths['bronze']}/usage_events").withColumn("event_date", F.to_date("timestamp"))
    b = _late_flags(b, cfg["streaming"]["late_threshold"])
    if dates is not None:
        b = b.filter(F.col("event_date").isin(dates))
    b = b.cache()
    rows_in = b.count()

    failed = []

    def rule(df, bad, name, reason):
        ok, ko = quality.split(df, bad, name, reason)
        failed.append(ko)
        return ok

    s = rule(b, F.col("event_id").isNull(), "event_id_nulo", "evento sin event_id")
    w = Window.partitionBy("event_id").orderBy("ingest_ts", "batch_id", "source_file")
    s = s.withColumn("_rn", F.row_number().over(w))
    s = rule(s, F.col("_rn") > 1, "event_id_duplicado", "el mismo event_id ya llegó en una ingesta anterior").drop("_rn")
    failed[-1] = failed[-1].drop("_rn")
    s = rule(s, F.col("cost_usd_increment") < F.lit(cfg["quality"]["min_cost_usd"]), "costo_negativo",
             f"cost_usd_increment menor a {cfg['quality']['min_cost_usd']}")
    s = rule(s, F.col("value_cast_failed"), "value_no_numerico", "value no se pudo convertir a número")

    res = spark.read.parquet(f"{paths['silver']}/resources").select("resource_id", F.col("service").alias("_res_service"), F.col("org_id").alias("_res_org"))
    s = s.withColumn("service", _norm("service")).withColumn("region", _norm("region")).join(F.broadcast(res), "resource_id", "left")
    s = rule(s, F.col("_res_service").isNull(), "recurso_inexistente", "resource_id no existe en resources")
    s = rule(s, (F.col("_res_service") != F.col("service")) | (F.col("_res_org") != F.col("org_id")), "recurso_inconsistente",
             "el servicio o la organización del evento no coinciden con los del recurso")
    s = s.drop("_res_service", "_res_org")

    unit_map = F.create_map(*[F.lit(x) for kv in UNIT_BY_METRIC.items() for x in kv])
    cust = spark.read.parquet(f"{paths['silver']}/customers_orgs").select("org_id", "plan_tier", "industry")
    out = (
        s.withColumn("dq_unit_filled", F.col("unit").isNull() & F.col("value").isNotNull())
        .withColumn("unit", F.coalesce(F.col("unit"), unit_map[F.col("metric")]))
        .withColumn("dq_cost_rounding", F.col("cost_usd_increment") < 0)
        .join(F.broadcast(cust), "org_id", "left")
        .withColumn("run_id", F.lit(run.run_id))
        .select("event_id", "timestamp", "event_date", "org_id", "resource_id", "service", "region", "metric",
                "value", "value_raw", "unit", "cost_usd_increment", "schema_version", "carbon_kg", "genai_tokens",
                "plan_tier", "industry", "is_late", "max_ts_previous_batches", "dq_unit_filled", "dq_cost_rounding",
                "ingest_ts", "ingest_date", "batch_id", "source_file", "run_id")
    ).cache()
    rows_ok = out.count()
    _write(out, f"{paths['silver']}/usage_events", "event_date")

    q = None
    for ko in failed:
        part = quality.to_quarantine(ko, "usage_events", run.run_id, "event_date")
        q = part if q is None else q.unionByName(part)
    q = q.cache()
    rows_q = q.count()
    if rows_q:
        quality.write_quarantine(q, paths["quarantine"], "usage_events", "event_date")
    by_rule = {r["dq_rule"]: r["count"] for r in q.groupBy("dq_rule").count().collect()}

    run.count("silver.usage_events", bronze=rows_in, silver=rows_ok, quarantine=rows_q, quarantine_por_regla=by_rule,
              tardios=out.filter("is_late").count(), unit_completada=out.filter("dq_unit_filled").count(),
              fechas=out.select("event_date").distinct().count())
    run.check("silver.usage_events.conteo", rows_in == rows_ok + rows_q, f"{rows_in} = {rows_ok} + {rows_q}")
    b.unpersist()
    q.unpersist()
    return out


def main() -> int:
    cfg = load_settings()
    spark = get_spark(cfg)
    run = Run("silver", cfg["paths"]["evidence"])
    build_masters(spark, cfg, run)
    out = build_events(spark, cfg, run)
    out.unpersist()
    for table, c in run.counts.items():
        print(table, c)
    print("registro:", run.save())
    spark.stop()
    return 0 if all(c["ok"] for c in run.checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
