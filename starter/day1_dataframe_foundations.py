from pyspark.sql import functions as F, types as T
from spark_utils import project_root, spark_session
spark=spark_session("day1-foundations")
root=project_root(); src=root/"provided_data/day1/orders_sample.csv"
schema=T.StructType([T.StructField("order_id",T.IntegerType(),False),T.StructField("customer_id",T.IntegerType(),False),T.StructField("order_date",T.DateType(),True),T.StructField("order_status",T.StringType(),True),T.StructField("sales_channel",T.StringType(),True)])
orders=(spark.read.option("header",True).schema(schema).csv(str(src)))
output = root / "work/day1/orders_selected"

# --- Lab 1.2 ---
# TODO 1: print schema, show 10 rows, and count rows.
orders.printSchema()
orders.show(10, truncate = False)
print(f"Row count: {orders.count()}")
print(f"# of Partitions: {orders.rdd.getNumPartitions()}")

# --- Lab 1.3 ---
# TODO 2: filter non-cancelled Web/Mobile orders after 2026-01-10.
selected_orders = orders.filter((F.col("order_status") != "Cancelled") & F.col("sales_channel").isin("Web", "Mobile") & (F.col("order_date") >= F.lit("2026-01-10").cast("date")))
selected_orders.show()

# --- Lab 1.4 & 1.5 ---
selected_orders = (selected_orders.select("order_id", "customer_id", "order_date", "order_status", "sales_channel")
    .filter(F.col("order_id").isNotNull()).withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    .withColumn("status_group", F.when(F.col("order_status").isin("Completed", "Shipped"), "FULFILLED").otherwise("OTHER"))
)
 
print("----- Explain Before -----")
selected_orders.explain("formatted")
 
print("----- Action -----")
print("Selected rows:", selected_orders.count())
selected_orders.show(5, truncate = False)
 
print("----- Explain After -----")
selected_orders.explain("formatted")

# --- Lab 1.6 ---
# TODO 5: write the selected result as Parquet to work/day1/orders_selected.
selected_orders.write.mode("overwrite").parquet(str(output))
readback = spark.read.parquet(str(output))
readback_count = readback.count()
print("Written:", selected_orders.count(), "Read back:", readback_count)
assert readback_count == selected_orders.count(), "read-back count mismatch"

spark.stop()
