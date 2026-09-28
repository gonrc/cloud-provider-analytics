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
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
