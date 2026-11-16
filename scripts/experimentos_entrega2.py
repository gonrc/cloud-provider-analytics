"""Experimentos de la entrega 2: cada uno pone a prueba una decisión con datos.

Requiere haber corrido el pipeline (make pipeline). Escribe sus tablas en
evidence/02_entrega/ y trabaja en datalake/_experimentos/, que se puede borrar.

  E1 Duplicados: un archivo reenviado, dentro y fuera de la ventana del watermark.
  E2 El patrón de clase (watermark sobre event time + ventana diaria + append)
     contra nuestro Gold.
  E3 Sensibilidad del umbral de anomalías.
  E4 Archivos y tamaños por zona del Data Lake.
  E5 Serving contra el lake: las consultas de Cassandra dan lo mismo que Gold.

Uso: python scripts/experimentos_entrega2.py [E1 E2 ...]
"""
import copy
import csv
import os
import shutil
import sys
from pathlib import Path

from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from cpa import gold, serving, silver, streaming  # noqa: E402
from cpa.config import load_settings  # noqa: E402
from cpa.runlog import Run  # noqa: E402
from cpa.schemas import USAGE_EVENTS  # noqa: E402
from cpa.spark import get_spark  # noqa: E402

CFG = load_settings()
P = CFG["paths"]
EV = Path(P["evidence"]) / "02_entrega"
EXP = Path(P["landing"]).parent / "_experimentos"


def save(rows: list[dict], name: str) -> None:
    EV.mkdir(parents=True, exist_ok=True)
    with (EV / name).open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    print(f"\n{name}")
    for r in rows:
        print("  ", r)


def cfg_for(base: Path, late_threshold: str) -> dict:
    cfg = copy.deepcopy(CFG)
    cfg["paths"].update(landing=str(base / "landing"), bronze=str(base / "bronze"), silver=str(base / "silver"),
                        quarantine=str(base / "quarantine"), checkpoints=str(base / "chk"))
    cfg["streaming"]["late_threshold"] = late_threshold
    return cfg


def e1_duplicados(spark) -> None:
    """Hipótesis: un archivo reenviado se descarta en streaming si llega dentro de la ventana
    del watermark (1 hora de llegada), y si llega más tarde lo atrapa la regla de unicidad de Silver."""
    rows = []
    for escenario, delay in (("reenvío dentro de la ventana (1 hour)", "1 hour"),
                             ("reenvío fuera de la ventana (simulado con 1 second)", "1 second")):
        base = EXP / ("dup_" + delay.replace(" ", ""))
        shutil.rmtree(base, ignore_errors=True)
        src = Path(P["landing"]) / "usage_events_stream"
        dst = base / "landing" / "usage_events_stream"
        dst.mkdir(parents=True)
        for f in sorted(src.glob("*.jsonl")):
            shutil.copy(f, dst / f.name)
        # El reenvío es el archivo más nuevo: Structured Streaming lo toma en el último lote.
        shutil.copy(src / "events_part_0000.jsonl", dst / "events_part_0000_reenvio.jsonl")
        cfg = cfg_for(base, delay)
        # Silver necesita los maestros: se copian los de la corrida real.
        for t in ("resources", "customers_orgs"):
            shutil.copytree(Path(P["silver"]) / t, base / "silver" / t)
        run = Run("exp_duplicados", P["evidence"])
        lotes, progreso = streaming.run_stream(spark, cfg, run, speed=False, serving_on=False, checkpoint_name="dup")
        descartados = sum((p["duplicados_descartados"] or 0) for p in progreso)
        bronze_rows = spark.read.parquet(cfg["paths"]["bronze"] + "/usage_events").count()
        silver.build_events(spark, cfg, run).unpersist()
        q = run.counts["silver.usage_events"]["quarantine_por_regla"].get("event_id_duplicado", 0)
        rows.append({"escenario": escenario, "eventos_en_landing": 43200 + 360, "lotes": len([l for l in lotes if l["filas_bronze"]]),
                     "descartados_por_dedupe_streaming": descartados, "filas_en_bronze": bronze_rows,
                     "duplicados_en_quarantine_silver": q, "filas_en_silver": run.counts["silver.usage_events"]["silver"]})
    save(rows, "exp_e1_duplicados.csv")


def e2_patron_de_clase(spark) -> None:
    """Hipótesis: con watermark de 1 hora sobre event time, ventana diaria y append, como en
    AndesShop, el Gold en streaming pierde casi todo, porque cada archivo trae los 60 días."""
    real_cost = spark.read.parquet(P["silver"] + "/usage_events").agg(F.sum("cost_usd_increment")).first()[0]
    real_rows = spark.read.parquet(P["gold"] + "/finops/org_daily_usage_by_service").count()
    rows = []
    for delay in ("1 hour", "7 days"):
        base = EXP / ("canon_" + delay.replace(" ", ""))
        shutil.rmtree(base, ignore_errors=True)
        agg = (
            spark.readStream.schema(USAGE_EVENTS).option("maxFilesPerTrigger", CFG["streaming"]["max_files_per_trigger"])
            .json(P["landing"] + "/usage_events_stream")
            .filter(F.col("cost_usd_increment") >= CFG["quality"]["min_cost_usd"])
            .withWatermark("timestamp", delay)
            .groupBy(F.window("timestamp", "1 day"), "org_id", "service")
            .agg(F.sum("cost_usd_increment").alias("daily_cost_usd"), F.count("*").alias("events"))
        )
        q = (agg.writeStream.format("parquet").outputMode("append")
             .option("path", str(base / "out")).option("checkpointLocation", str(base / "chk"))
             .trigger(availableNow=True).start())
        q.awaitTermination()
        dropped = sum(((p.get("stateOperators") or [{}])[0].get("numRowsDroppedByWatermark") or 0) for p in q.recentProgress)
        out = spark.read.parquet(str(base / "out"))
        n, cost, ev = out.count(), out.agg(F.sum("daily_cost_usd")).first()[0] or 0.0, out.agg(F.sum("events")).first()[0] or 0
        rows.append({"configuracion": f"watermark {delay} sobre timestamp + ventana de 1 día + append",
                     "filas_gold": n, "filas_gold_lambda": real_rows, "eventos_contados": ev, "eventos_validos": 42989,
                     "pct_eventos_perdidos": round(100 * (1 - ev / 42989), 1),
                     "costo_usd": round(cost, 2), "costo_usd_lambda": round(real_cost, 2),
                     "descartados_por_watermark": dropped})
    save(rows, "exp_e2_patron_de_clase.csv")


def e3_umbral_anomalias(spark) -> None:
    """Hipótesis: con historia propia por organización y servicio, el umbral de 3,5 marca
    pocos días y el resultado no es muy sensible a moverlo medio punto."""
    a = spark.read.parquet(P["gold"] + "/finops/cost_anomaly_mart")
    total = a.count()
    rows = []
    for t in (2.5, 3.0, 3.5, 4.0, 5.0):
        n = a.filter(F.col("anomaly_score") > t).count()
        rows.append({"umbral_z": t, "dias_marcados": n, "pct_del_total": round(100 * n / total, 2),
                     "organizaciones": a.filter(F.col("anomaly_score") > t).select("org_id").distinct().count()})
    # Alternativa: línea base por servicio entre todas las organizaciones, sin historia propia.
    d = a.withColumn("lc", F.log1p(F.greatest("daily_cost_usd", F.lit(0.0))))
    med = d.groupBy("service").agg(F.percentile_approx("lc", 0.5, 10000).alias("m"))
    d = d.join(med, "service").withColumn("dev", F.abs(F.col("lc") - F.col("m")))
    mad = d.groupBy("service").agg(F.percentile_approx("dev", 0.5, 10000).alias("mad"))
    d = d.join(mad, "service").withColumn("z", 0.6745 * (F.col("lc") - F.col("m")) / F.col("mad"))
    n_cross = d.filter("z > 3.5").count()
    both = d.filter("z > 3.5 and is_anomaly").count()
    rows.append({"umbral_z": "3.5 con línea base por servicio (todas las organizaciones)", "dias_marcados": n_cross,
                 "pct_del_total": round(100 * n_cross / total, 2), "organizaciones": f"coinciden con la elegida: {both}"})
    save(rows, "exp_e3_umbral_anomalias.csv")
    top = (a.filter("is_anomaly").orderBy(F.desc("anomaly_score"))
           .select("org_id", "usage_date", "service", "daily_cost_usd", "baseline_daily_cost_usd", "anomaly_score", "history_days")
           .limit(10).toPandas())
    top.to_csv(EV / "anomalias_top10.csv", index=False)


def e4_archivos_por_zona(spark) -> None:
    rows = []
    for zone in ("bronze", "silver", "gold", "quarantine"):
        root = Path(P[zone])
        tables = sorted({p.parent for p in root.rglob("*.parquet")})
        by_table = {}
        for f in root.rglob("*.parquet"):
            rel = f.relative_to(root).parts
            table = "/".join(x for x in rel[:-1] if "=" not in x)
            t = by_table.setdefault(table, {"archivos": 0, "bytes": 0, "particiones": set()})
            t["archivos"] += 1
            t["bytes"] += f.stat().st_size
            t["particiones"].add(f.parent)
        for table, t in sorted(by_table.items()):
            rows.append({"zona": zone, "tabla": table, "particiones": len(t["particiones"]), "archivos": t["archivos"],
                         "mb": round(t["bytes"] / 1e6, 2), "kb_por_archivo": round(t["bytes"] / t["archivos"] / 1024, 1)})
        _ = tables
    save(rows, "exp_e4_archivos_por_zona.csv")


def e5_serving_vs_lake(spark) -> None:
    """Las dos consultas de Cassandra tienen que dar lo mismo que Gold en el lake."""
    from cassandra.cluster import Cluster
    org = "org_53lc58dr"
    g = spark.read.parquet(P["gold"] + "/finops/org_daily_usage_by_service").filter(F.col("org_id") == org)
    q1_lake = g.filter(F.col("usage_date").between("2025-08-25", "2025-08-31"))
    q2_lake = {r["service"]: r["c"] for r in g.filter(F.col("usage_date") >= "2025-08-18").groupBy("service")
               .agg(F.sum("daily_cost_usd").alias("c")).collect()}
    c = Cluster([CFG["serving"]["host"]], port=CFG["serving"]["port"])
    s = c.connect(CFG["serving"]["keyspace"])
    q1 = list(s.execute("SELECT service, daily_cost_usd FROM org_daily_usage_by_service WHERE org_id = %s "
                        "AND usage_date >= '2025-08-25' AND usage_date <= '2025-08-31'", (org,)))
    q2 = {r.service: r.cost for r in s.execute(
        "SELECT service, SUM(daily_cost_usd) AS cost FROM org_service_daily_cost WHERE org_id = %s AND service IN "
        "('analytics','compute','database','genai','networking','storage') AND usage_date >= '2025-08-18' GROUP BY service", (org,))}
    c.shutdown()
    rows = [{"consulta": "1: rango de fechas", "filas_cassandra": len(q1), "filas_lake": q1_lake.count(),
             "costo_cassandra": round(sum(r.daily_cost_usd for r in q1), 4),
             "costo_lake": round(q1_lake.agg(F.sum("daily_cost_usd")).first()[0], 4)}]
    for svc in sorted(q2_lake):
        rows.append({"consulta": f"2: {svc}", "filas_cassandra": 1 if svc in q2 else 0, "filas_lake": 1,
                     "costo_cassandra": round(q2.get(svc, 0), 4), "costo_lake": round(q2_lake[svc], 4)})
    save(rows, "exp_e5_serving_vs_lake.csv")


EXPERIMENTS = {"E1": e1_duplicados, "E2": e2_patron_de_clase, "E3": e3_umbral_anomalias,
               "E4": e4_archivos_por_zona, "E5": e5_serving_vs_lake}

if __name__ == "__main__":
    wanted = sys.argv[1:] or list(EXPERIMENTS)
    spark = get_spark(CFG)
    for k in wanted:
        print(f"\n=== {k}: {EXPERIMENTS[k].__doc__.strip().splitlines()[0] if EXPERIMENTS[k].__doc__ else k}")
        EXPERIMENTS[k](spark)
    spark.stop()
    _ = (os, gold, serving)
