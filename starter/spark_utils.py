from pathlib import Path
from pyspark.sql import SparkSession

def spark_session(app_name: str, with_delta: bool = False):
    builder = SparkSession.builder.master("local[*]").appName(app_name)
    if with_delta:
        builder = (builder
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        try:
            from delta import configure_spark_with_delta_pip
            return configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError as exc:
            raise RuntimeError("Install requirements.txt or run in a Fabric Spark notebook.") from exc
    return builder.getOrCreate()

def project_root() -> Path:
    return Path(__file__).resolve().parents[1]
