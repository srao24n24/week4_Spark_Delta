"""Day 6 capstone. Implement functions; do not place the entire pipeline at module scope."""
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from spark_utils import project_root, spark_session

def ingest_bronze(spark, input_dir, bronze_dir, batch_id):
    # TODO: explicit schemas; add ingestion timestamp, source_file and batch_id; append raw Delta.
    raise NotImplementedError

def build_silver(spark, bronze_dir, silver_dir):
    # TODO: type/standardize/deduplicate/validate and MERGE all four entities by business key.
    raise NotImplementedError

def build_gold(spark, silver_dir, gold_dir):
    # TODO: build gold_sales_detail and gold_monthly_category; exclude Cancelled orders.
    raise NotImplementedError

def reconcile(spark, bronze_dir, silver_dir, gold_dir):
    # TODO: counts, duplicate keys, orphan checks, null checks and financial totals; return a DataFrame.
    raise NotImplementedError

if __name__ == "__main__":
    spark=spark_session("week4-capstone", with_delta=True); root=project_root(); work=root/"work/day6"
    # TODO: run initial batch, save evidence; run incremental batch; run it again to prove idempotency.
    spark.stop()
