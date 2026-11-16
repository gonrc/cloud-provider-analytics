"""Prueba de idempotencia (consigna 6.2): conteos antes y después de re-ejecutar.

Escenarios:
  1. Re-ejecutar el pipeline completo con el mismo checkpoint.
  2. Re-ejecutarlo después de borrar el checkpoint del streaming. Es el error que la
     clase marca como causa de duplicados; acá se mide qué pasa con este diseño.

Por tabla compara filas de Bronze, Silver, Gold, quarantine y Cassandra, y el costo
total de Gold. Deja evidence/02_entrega/idempotencia.csv.

Requiere Cassandra levantada. Uso: python scripts/idempotencia.py
"""
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from cpa.config import load_settings  # noqa: E402
from cpa.spark import get_spark  # noqa: E402

CFG = load_settings()
P = CFG["paths"]


def tables() -> list[tuple[str, str]]:
    out = []
    for zone in ("bronze", "silver", "gold", "quarantine"):
        root = Path(P[zone])
        for d in sorted({f.parent for f in root.rglob("*.parquet")}):
            parts = [x for x in d.relative_to(root).parts if "=" not in x]
            out.append((zone, "/".join(parts)))
    return sorted(set(out))


def snapshot() -> dict:
    from cassandra.cluster import Cluster
    spark = get_spark(CFG)
    counts = {}
    for zone, t in tables():
        df = spark.read.parquet(f"{P[zone]}/{t}")
        counts[f"{zone}/{t}"] = df.count()
    from pyspark.sql import functions as F
    counts["gold/costo_total_usd"] = round(spark.read.parquet(f"{P['gold']}/finops/org_daily_usage_by_service")
                                           .agg(F.sum("daily_cost_usd")).first()[0], 2)
    counts["silver/usage_events event_id distintos"] = (spark.read.parquet(f"{P['silver']}/usage_events")
                                                       .select("event_id").distinct().count())
    spark.stop()
    c = Cluster([CFG["serving"]["host"]], port=CFG["serving"]["port"])
    s = c.connect(CFG["serving"]["keyspace"])
    for t in ("org_daily_usage_by_service", "org_service_daily_cost"):
        counts[f"cassandra/{t}"] = s.execute(f"SELECT COUNT(*) FROM {t}").one()[0]
    c.shutdown()
    return counts


def run_pipeline() -> dict:
    res = subprocess.run([sys.executable, "-m", "cpa.pipeline"], cwd=ROOT, capture_output=True, text=True,
                         env={**__import__("os").environ, "PYTHONPATH": str(ROOT / "src")})
    if res.returncode != 0:
        print(res.stdout[-3000:], res.stderr[-3000:])
        raise SystemExit("el pipeline falló")
    log = sorted((Path(P["evidence"]) / "runs").glob("*_pipeline.json"))[-1]
    return json.loads(log.read_text())["counts"].get("streaming", {})


def main() -> int:
    antes = snapshot()
    s1 = run_pipeline()
    despues1 = snapshot()
    shutil.rmtree(Path(P["checkpoints"]) / "bronze_usage_events")
    s2 = run_pipeline()
    despues2 = snapshot()

    rows = []
    for k in sorted(set(antes) | set(despues1) | set(despues2)):
        a, b, c = antes.get(k), despues1.get(k), despues2.get(k)
        rows.append({"tabla": k, "antes": a, "reejecucion_mismo_checkpoint": b, "reejecucion_sin_checkpoint": c,
                     "igual": a == b == c})
    rows.append({"tabla": "streaming: micro-lotes ejecutados (incluye el lote vacío con que Spark cierra)", "antes": "",
                 "reejecucion_mismo_checkpoint": s1.get("lotes"), "reejecucion_sin_checkpoint": s2.get("lotes"), "igual": ""})
    out = Path(P["evidence"]) / "02_entrega" / "idempotencia.csv"
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    for r in rows:
        print(r)
    distintas = [r["tabla"] for r in rows if r["igual"] is False]
    print("tablas con diferencias:", distintas or "ninguna")
    return 1 if distintas else 0


if __name__ == "__main__":
    raise SystemExit(main())
