import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .config("spark.sql.shuffle.partitions", 2)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    yield s
    s.stop()
