"""Gold: marts para consumo.

- org_daily_usage_by_service (FinOps): grano organización × día × servicio, con
  costo, requests, horas de CPU, GB-hora, tokens de GenAI, carbono, eventos y
  eventos tardíos. Es la agregación de la sección 9 del diseño (map, shuffle,
  reduce) con el join replicado a customers_orgs.
- cost_anomaly_mart (analítica, D-11): z robusto (MAD) sobre log(1 + costo diario)
  contra la historia de la misma organización y servicio. Se recalcula entero,
  porque el score de un día depende de todos los demás.

Control de reconciliación: el costo de Silver para las fechas recalculadas tiene
que coincidir con el de Gold (diferencia menor a 0,01 USD).

Uso: python -m cpa.gold
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from cpa.config import load_settings
from cpa.runlog import Run
from cpa.spark import get_spark


def _metric(name: str):
    return F.sum(F.when(F.col("metric") == name, F.col("value"))).alias(name)


def build_daily(spark: SparkSession, cfg: dict, run: Run, dates: list | None = None) -> DataFrame:
    paths = cfg["paths"]
    s = spark.read.parquet(f"{paths['silver']}/usage_events")
    if dates is not None:
        s = s.filter(F.col("event_date").isin(dates))
    cust = spark.read.parquet(f"{paths['silver']}/customers_orgs").select("org_id", "plan_tier", "industry")
    daily = (
        s.groupBy("org_id", F.col("event_date").alias("usage_date"), "service")
        .agg(
            F.round(F.sum("cost_usd_increment"), 4).alias("daily_cost_usd"),
            _metric("requests"), _metric("cpu_hours"), _metric("storage_gb_hours"),
            F.sum("genai_tokens").alias("genai_tokens"),
            F.round(F.sum("carbon_kg"), 6).alias("carbon_kg"),
            F.count("*").alias("events"),
            F.sum(F.col("is_late").cast("int")).alias("late_events"),
        )
        .join(F.broadcast(cust), "org_id", "left")
        .withColumn("run_id", F.lit(run.run_id))
        .withColumn("computed_at", F.current_timestamp())
    ).cache()
    (daily.repartition("usage_date").write.mode("overwrite").partitionBy("usage_date")
          .parquet(f"{paths['gold']}/finops/org_daily_usage_by_service"))

    cost_silver = s.agg(F.sum("cost_usd_increment")).first()[0] or 0.0
    cost_gold = daily.agg(F.sum("daily_cost_usd")).first()[0] or 0.0
    rows = daily.count()
    run.count("gold.org_daily_usage_by_service", filas=rows, costo_silver=round(cost_silver, 2), costo_gold=round(cost_gold, 2),
              fechas=len(dates) if dates is not None else daily.select("usage_date").distinct().count())
    run.check("gold.org_daily_usage_by_service.reconciliacion", abs(cost_silver - cost_gold) < 0.01,
              f"Silver {cost_silver:,.2f} USD vs Gold {cost_gold:,.2f} USD")
    daily.unpersist()
    return daily


def build_anomalies(spark: SparkSession, cfg: dict, run: Run) -> DataFrame:
    paths, a = cfg["paths"], cfg["anomaly"]
    d = spark.read.parquet(f"{paths['gold']}/finops/org_daily_usage_by_service").select(
        "org_id", "usage_date", "service", "daily_cost_usd", "events", "plan_tier", "industry")
    d = d.withColumn("log_cost", F.log1p(F.greatest(F.col("daily_cost_usd"), F.lit(0.0))))
    key = Window.partitionBy("org_id", "service")
    d = d.withColumn("history_days", F.count("*").over(key))
    med = d.groupBy("org_id", "service").agg(F.percentile_approx("log_cost", 0.5, 10000).alias("median_log_cost"))
    d = d.join(med, ["org_id", "service"]).withColumn("abs_dev", F.abs(F.col("log_cost") - F.col("median_log_cost")))
    mad = d.groupBy("org_id", "service").agg(F.percentile_approx("abs_dev", 0.5, 10000).alias("mad_log_cost"))
    out = (
        d.join(mad, ["org_id", "service"])
        .withColumn("anomaly_score", F.when((F.col("history_days") >= a["min_days"]) & (F.col("mad_log_cost") > 0),
                                            F.round(0.6745 * (F.col("log_cost") - F.col("median_log_cost")) / F.col("mad_log_cost"), 3)))
        .withColumn("is_anomaly", F.coalesce(F.col("anomaly_score") > a["z_threshold"], F.lit(False)))
        .withColumn("baseline_daily_cost_usd", F.round(F.expm1("median_log_cost"), 4))
        .withColumn("z_threshold", F.lit(float(a["z_threshold"])))
        .withColumn("run_id", F.lit(run.run_id))
        .withColumn("computed_at", F.current_timestamp())
        .drop("log_cost", "abs_dev")
    ).cache()
    (out.repartition(1).write.mode("overwrite").partitionBy("usage_date")
        .parquet(f"{paths['gold']}/finops/cost_anomaly_mart"))
    run.count("gold.cost_anomaly_mart", filas=out.count(), con_score=out.filter("anomaly_score is not null").count(),
              anomalias=out.filter("is_anomaly").count(), umbral=a["z_threshold"])
    out.unpersist()
    return out


def main() -> int:
    cfg = load_settings()
    spark = get_spark(cfg)
    run = Run("gold", cfg["paths"]["evidence"])
    build_daily(spark, cfg, run)
    build_anomalies(spark, cfg, run)
    for table, c in run.counts.items():
        print(table, c)
    for name, c in run.checks.items():
        print(name, "OK" if c["ok"] else "FALLA", c["detail"])
    print("registro:", run.save())
    spark.stop()
    return 0 if all(c["ok"] for c in run.checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
