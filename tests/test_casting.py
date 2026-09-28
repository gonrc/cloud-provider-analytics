from cpa.casting import cast_with_fallback


def test_numero_como_texto_se_castea(spark):
    df = spark.createDataFrame([("8.9427",), ("95.0",)], ["value"])
    out = cast_with_fallback(df, "value", "double").collect()
    assert [r.value for r in out] == [8.9427, 95.0]
    assert not any(r.value_cast_failed for r in out)


def test_texto_invalido_queda_nulo_y_marcado(spark):
    df = spark.createDataFrame([("abc",)], ["value"])
    row = cast_with_fallback(df, "value", "double").first()
    assert row.value is None
    assert row.value_raw == "abc"
    assert row.value_cast_failed


def test_nulo_no_cuenta_como_fallo(spark):
    df = spark.createDataFrame([(None,), ("",)], "value string")
    out = cast_with_fallback(df, "value", "double").collect()
    assert not any(r.value_cast_failed for r in out)
