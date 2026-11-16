from pyspark.sql import functions as F

from cpa import quality
from cpa.silver import _late_flags


def test_split_no_pierde_filas(spark):
    df = spark.createDataFrame([("a", 1.0), ("b", -5.0), ("c", None)], "event_id string, cost double")
    ok, ko = quality.split(df, F.col("cost") < -0.01, "costo_negativo", "costo menor a -0,01")
    assert ok.count() + ko.count() == df.count()
    assert [r.event_id for r in ko.collect()] == ["b"]
    # Un nulo en la condición no manda la fila a quarantine: la regla solo atrapa lo que falla.
    assert sorted(r.event_id for r in ok.collect()) == ["a", "c"]
    assert ko.first().dq_rule == "costo_negativo"


def test_quarantine_conserva_el_registro_original(spark):
    df = spark.createDataFrame([("b", -5.0, "2025-08-01")], "event_id string, cost double, event_date string")
    _, ko = quality.split(df, F.col("cost") < 0, "costo_negativo", "x")
    q = quality.to_quarantine(ko, "usage_events", "run-1", "event_date").first()
    assert q.source == "usage_events" and q.run_id == "run-1" and q.event_date == "2025-08-01"
    assert '"event_id":"b"' in q.record and '"cost":-5.0' in q.record


def test_tardio_se_mide_contra_lotes_anteriores(spark):
    rows = [
        # lote 0 llega primero y su máximo es el 31/08
        ("e1", "2025-08-31 10:00:00", "2026-01-01 00:00:00", 0),
        ("e2", "2025-07-03 10:00:00", "2026-01-01 00:00:00", 0),
        # lote 1: un evento muy anterior (tardío) y otro dentro de la hora
        ("e3", "2025-08-01 00:00:00", "2026-01-01 00:01:00", 1),
        ("e4", "2025-08-31 09:30:00", "2026-01-01 00:01:00", 1),
    ]
    df = (spark.createDataFrame(rows, "event_id string, ts string, its string, batch_id int")
          .select("event_id", F.to_timestamp("ts").alias("timestamp"), F.to_timestamp("its").alias("ingest_ts"), "batch_id"))
    late = {r.event_id: r.is_late for r in _late_flags(df, "1 hour").collect()}
    # En el primer lote no hay máximo previo: nada es tardío, aunque venga desordenado.
    assert late == {"e1": False, "e2": False, "e3": True, "e4": False}
