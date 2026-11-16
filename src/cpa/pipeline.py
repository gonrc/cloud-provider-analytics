"""Pipeline completo, en el orden de la arquitectura Lambda.

1. Landing: extrae y verifica contra el manifiesto. Si un archivo cambió, no sigue.
2. Batch → Bronze: las siete fuentes CSV.
3. Silver de maestros: customers_orgs, resources, users y billing_monthly.
4. Capa speed: streaming de eventos a Bronze; cada micro-lote recalcula Silver y Gold
   de las fechas que tocó y, si Cassandra responde, las carga.
5. Capa batch: recalcula Silver de eventos y Gold completos (la cifra definitiva) y
   el mart de anomalías.
6. Serving: carga Gold completo en Cassandra y corre las consultas.

Cada paso deja su registro en evidence/runs/. Todo es idempotente: correrlo dos
veces deja los mismos conteos (ver `make idempotencia`).

Uso: python -m cpa.pipeline [--sin-serving]
"""
import argparse
import time
from pathlib import Path

from cpa import bronze, gold, serving, silver, streaming
from cpa.config import load_settings
from cpa.landing import extract, verify
from cpa.runlog import Run
from cpa.spark import get_spark


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-serving", action="store_true", help="no usar Cassandra (por ejemplo en Colab)")
    args = ap.parse_args()
    cfg = load_settings()
    paths = cfg["paths"]
    t0 = time.time()

    extract(paths["raw_zip"], paths["landing"])
    problems = verify(paths["landing"], paths["manifest"])
    if problems:
        print("Landing no coincide con el manifiesto:", *problems, sep="\n  ")
        return 1

    serving_on = not args.sin_serving and serving.available(cfg)
    if not args.sin_serving and not serving_on:
        print(f"Cassandra no responde en {cfg['serving']['host']}:{cfg['serving']['port']}: se corre sin serving")

    spark = get_spark(cfg)
    run = Run("pipeline", paths["evidence"])

    for name in bronze.BATCH_SOURCES:
        bronze.ingest_source(spark, cfg, name, run)
    print(f"[{time.time() - t0:5.0f} s] bronze batch listo")

    silver.build_masters(spark, cfg, run)
    print(f"[{time.time() - t0:5.0f} s] silver de maestros listo")

    rows, progress = streaming.run_stream(spark, cfg, run, speed=True, serving_on=serving_on)
    ev = Path(paths["evidence"]) / "02_entrega"
    if rows:
        streaming._write_csv(rows, ev / "streaming_lotes.csv")
        streaming._write_csv(progress, ev / "streaming_progreso.csv")
    run.count("streaming", lotes=len(rows), eventos_bronze=sum(r["filas_bronze"] for r in rows))
    print(f"[{time.time() - t0:5.0f} s] capa speed lista: {len(rows)} micro-lotes")

    silver.build_events(spark, cfg, run).unpersist()
    gold.build_daily(spark, cfg, run)
    gold.build_anomalies(spark, cfg, run)
    print(f"[{time.time() - t0:5.0f} s] capa batch lista")

    if serving_on:
        serving.load_daily(spark, cfg, run)
        print(f"[{time.time() - t0:5.0f} s] serving cargado")
    spark.stop()
    if serving_on:
        serving.run_queries(cfg, ev / "consultas_cql.txt")

    failed = {k: v for k, v in run.checks.items() if not v["ok"]}
    print("registro:", run.save())
    print(f"listo en {time.time() - t0:.0f} s; controles fallidos: {failed or 'ninguno'}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
