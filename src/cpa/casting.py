"""Cast con fallback controlado."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def cast_with_fallback(df: DataFrame, column: str, dtype: str) -> DataFrame:
    """Castea `column` a `dtype` sin perder el valor original.

    Deja tres columnas:
      <column>              el valor casteado, nulo si no se pudo convertir
      <column>_raw          el texto tal como llegó
      <column>_cast_failed  True si había un valor y el cast lo dejó nulo

    Una fila con cast fallido no se descarta acá: la regla de calidad decide si
    va a quarantine.

    Usa try_cast y no cast: Spark 4 trae el modo ANSI activado y un cast
    inválido corta el job con error en vez de devolver nulo.
    """
    raw = f"{column}_raw"
    return (
        df.withColumn(raw, F.trim(F.col(column).cast("string")))
        .withColumn(column, F.col(raw).try_cast(dtype))
        .withColumn(
            f"{column}_cast_failed",
            F.col(raw).isNotNull() & (F.col(raw) != "") & F.col(column).isNull(),
        )
    )
