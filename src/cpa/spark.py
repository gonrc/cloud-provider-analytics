"""SparkSession única para notebooks, jobs y tests."""
from pyspark.sql import SparkSession


def get_spark(cfg: dict) -> SparkSession:
    s = cfg["spark"]
    spark = (
        SparkSession.builder.appName(s["app_name"])
        .master(s["master"])
        .config("spark.sql.shuffle.partitions", s["shuffle_partitions"])
        # Los timestamps de los eventos vienen en UTC ("Z"). Fijar la zona evita
        # que un event_date cambie de día según la máquina donde corre el job.
        .config("spark.sql.session.timeZone", s["timezone"])
        # Con mode("overwrite") y partitionBy, Spark reemplaza solo las particiones que
        # trae el DataFrame y no la tabla entera. Es lo que hace idempotente cada
        # escritura: volver a correr un día o un lote pisa exactamente lo mismo.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
