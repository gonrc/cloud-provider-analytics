"""Serving: carga de Gold en Cassandra y consultas.

La carga va con el driver de Python y no con el conector de Spark, que no tiene
versión para Spark 4 (D-02, D-09). La consigna permite las dos vías (4.4). Gold
es chico (unas 11.000 filas), así que se lee a pandas y se inserta con sentencias
preparadas en paralelo.

Cassandra escribe por clave primaria: insertar dos veces la misma fila la pisa.
Por eso volver a cargar es idempotente y no hace falta borrar antes.

Uso:
  python -m cpa.serving             crea keyspace y tablas, carga Gold completo
  python -m cpa.serving --consultas además corre cql/queries.cql con cqlsh y guarda la salida
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from cpa.config import ROOT, load_settings
from cpa.runlog import Run

DAILY_COLS = ["org_id", "usage_date", "service", "daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours",
              "genai_tokens", "carbon_kg", "events", "late_events", "plan_tier", "industry", "run_id"]


def _cluster(cfg: dict):
    from cassandra.cluster import Cluster
    s = cfg["serving"]
    return Cluster([s["host"]], port=s["port"], connect_timeout=5)


def available(cfg: dict) -> bool:
    try:
        c = _cluster(cfg)
        c.connect().execute("SELECT release_version FROM system.local")
        c.shutdown()
        return True
    except Exception:
        return False


def ensure_schema(session) -> None:
    text = (ROOT / "cql" / "schema.cql").read_text()
    body = "\n".join(line for line in text.splitlines() if not line.strip().startswith("--"))
    for stmt in filter(None, (s.strip() for s in body.split(";"))):
        session.execute(stmt)


def _none(v):
    # pandas trae NaN para los nulos de columnas numéricas; Cassandra los quiere como null.
    return None if v is None or (isinstance(v, float) and v != v) else v


def load_daily(spark, cfg: dict, run: Run, dates: list | None = None) -> int:
    from cassandra.concurrent import execute_concurrent_with_args

    gold = spark.read.parquet(f"{cfg['paths']['gold']}/finops/org_daily_usage_by_service")
    if dates is not None:
        gold = gold.filter(F.col("usage_date").isin(dates))
    pdf = gold.select(*DAILY_COLS).toPandas()
    loaded_at = datetime.now(timezone.utc)
    ks = cfg["serving"]["keyspace"]

    cluster = _cluster(cfg)
    session = cluster.connect()
    ensure_schema(session)
    ins_daily = session.prepare(
        f"INSERT INTO {ks}.org_daily_usage_by_service ({', '.join(DAILY_COLS)}, loaded_at) "
        f"VALUES ({', '.join('?' * (len(DAILY_COLS) + 1))})")
    ins_cost = session.prepare(
        f"INSERT INTO {ks}.org_service_daily_cost (org_id, service, usage_date, daily_cost_usd, run_id, loaded_at) "
        "VALUES (?, ?, ?, ?, ?, ?)")

    rows_daily, rows_cost = [], []
    for r in pdf.itertuples(index=False):
        d = r._asdict()
        vals = [_none(d[c]) for c in DAILY_COLS]
        for i, c in enumerate(DAILY_COLS):
            if c in ("genai_tokens", "events", "late_events") and vals[i] is not None:
                vals[i] = int(vals[i])
        rows_daily.append(vals + [loaded_at])
        rows_cost.append([d["org_id"], d["service"], d["usage_date"], _none(d["daily_cost_usd"]), d["run_id"], loaded_at])

    t0 = time.time()
    errors = 0
    for stmt, rows in ((ins_daily, rows_daily), (ins_cost, rows_cost)):
        for ok, _ in execute_concurrent_with_args(session, stmt, rows, concurrency=64, raise_on_first_error=False):
            errors += 0 if ok else 1
    secs = round(time.time() - t0, 1)
    total = session.execute(f"SELECT COUNT(*) FROM {ks}.org_daily_usage_by_service").one()[0]
    cluster.shutdown()
    run.count("serving.org_daily_usage_by_service", filas_gold=len(pdf), filas_cassandra=total, errores=errors, segundos=secs,
              fechas=len(dates) if dates is not None else None)
    run.check("serving.carga_sin_errores", errors == 0, f"{len(pdf)} filas por tabla, {errors} errores")
    return len(pdf)


def run_queries(cfg: dict, out_path: Path) -> str:
    """Corre cql/queries.cql con cqlsh y guarda la salida tal cual, como captura de resultados."""
    cqlsh = os.environ.get("CQLSH", str(Path.home() / "opt" / "apache-cassandra-5.0.9" / "bin" / "cqlsh.py"))
    cmd = [sys.executable, cqlsh, cfg["serving"]["host"], str(cfg["serving"]["port"]), "-f", str(ROOT / "cql" / "queries.cql")]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    text = res.stdout + (("\n" + res.stderr) if res.stderr.strip() else "")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header = f"-- Salida de cqlsh -f cql/queries.cql, {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n"
    out_path.write_text(header + text)
    return text


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--consultas", action="store_true", help="correr cql/queries.cql y guardar la salida")
    args = ap.parse_args()
    cfg = load_settings()
    if not available(cfg):
        print(f"Cassandra no responde en {cfg['serving']['host']}:{cfg['serving']['port']}. Ver README, sección Serving.")
        return 1
    from cpa.spark import get_spark
    spark = get_spark(cfg)
    run = Run("serving", cfg["paths"]["evidence"])
    n = load_daily(spark, cfg, run)
    print("filas cargadas por tabla:", n, run.counts)
    spark.stop()
    if args.consultas:
        out = Path(cfg["paths"]["evidence"]) / "02_entrega" / "consultas_cql.txt"
        print(run_queries(cfg, out))
        print("salida guardada en", out)
    print("registro:", run.save())
    return 0 if all(c["ok"] for c in run.checks.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
