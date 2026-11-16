from datetime import date, datetime

from cpa.runlog import Run
from cpa.silver import build_billing


def test_reglas_de_facturacion_d10(spark, tmp_path):
    rows = [
        # USD con tipo de cambio distinto de 1: se fuerza a 1
        ("i1", "o1", date(2025, 6, 1), 100.0, None, 21.0, "USD", 0.9),
        # ARS: se convierte como viene; créditos aplicados
        ("i2", "o1", date(2025, 7, 1), 1000.0, 100.0, 210.0, "ARS", 0.0015),
        # subtotal negativo con impuesto positivo: no es nota de crédito, va a quarantine
        ("i3", "o2", date(2025, 6, 1), -50.0, None, 10.5, "USD", 1.0),
    ]
    bronze = spark.createDataFrame(
        [r + (datetime(2026, 11, 16), date(2026, 11, 16), "billing_monthly.csv", "r0") for r in rows],
        "invoice_id string, org_id string, month date, subtotal double, credits double, taxes double, currency string, "
        "exchange_rate_to_usd double, ingest_ts timestamp, ingest_date date, source_file string, run_id string")
    paths = {k: str(tmp_path / k) for k in ("bronze", "silver", "quarantine", "evidence")}
    bronze.write.partitionBy("month").parquet(paths["bronze"] + "/billing_monthly")

    run = Run("test", paths["evidence"])
    out = {r.invoice_id: r for r in build_billing(spark, {"paths": paths}, run).collect()}

    assert set(out) == {"i1", "i2"}
    assert out["i1"].exchange_rate_to_usd == 1.0 and out["i1"].exchange_rate_to_usd_raw == 0.9 and out["i1"].dq_fx_forced
    assert out["i1"].revenue_usd == 121.0
    assert out["i2"].revenue_usd == round((1000 - 100 + 210) * 0.0015, 4) and not out["i2"].dq_fx_forced
    assert run.counts["silver.billing_monthly"]["quarantine"] == 1
    assert run.checks["silver.billing_monthly.conteo"]["ok"]
