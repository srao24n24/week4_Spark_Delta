import shutil
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from spark_utils import project_root, spark_session
spark=spark_session("day5-delta", with_delta=True); root=project_root(); src=root/"provided_data/day5"; table=root/"work/day5/delta/customers"

# --- Lab 5.1 ---
# TODO: write customers_initial as Delta and inspect _delta_log/history.
shutil.rmtree(table, ignore_errors=True)
cus_initial = spark.read.option("header", True).option("inferSchema", True).csv(str(src/"customers_initial.csv"))
cus_initial.write.format("delta").mode("overwrite").save(str(table))

DeltaTable.forPath(spark, str(table)).history().select("version", "operation", "operationMetrics").show(truncate = False)
print("rows:", spark.read.format("delta").load(str(table)).count())

"""
The table folder holds one Parquet data file and a _delta_log folder with the entry, 00000000000000000000.json. The history shows one commit, 
version 0, a WRITE with 12 rows. The log entry records the operation (commitInfo), the table schema (metaData), the protocol versions (protocol) 
and the data file that belongs to the table (add). The log, not the Parquet file, is what defines the table.
"""

# --- Lab 5.2 ---
# TODO: prove schema enforcement by attempting one incompatible append and capturing the error.
cus_changes = spark.read.option("header", True).option("inferSchema", True).csv(str(src/"customers_changes.csv"))

try:
    cus_changes.write.format("delta").mode("append").save(str(table))
    print("Append succeeded NOT RIGHT !!!!!!!!!!!!")
except Exception as e:
    print("Append rejected GOOOOOOD :", type(e).__name__)
    print(str(e))

"""
I tried to append customers_changes.csv to the Delta table. The file has an extra column, source_system, that the table schema doesn't have. 
Delta rejected the write with a schema mismatch error instead of changing the table. This is schema enforcement: on every write, Delta checks 
the incoming schema against the schema stored in the transaction log, and refuses writes that don't match unless schema evolution is explicitly 
enabled.
"""

# --- Lab 5.3 ---
cus_changes.limit(0).write.format("delta").mode("append").option("mergeSchema", "true").save(str(table))

evolved = spark.read.format("delta").load(str(table))
evolved.printSchema()
print("rows:", evolved.count())

DeltaTable.forPath(spark, str(table)).history().select("version", "operation", "operationParameters", "operationMetrics").show(truncate = False)

"""
customers_changes.csv has the extra column called source_system, that the target table doesn't have. In 5.2, Delta rejected the append because 
of this. This column is a deliberate change, so we must evolve the schema and to do that, I enabled evolution for one write only, using the 
mergeSchema option on an empty append, instead of turning on session-wide auto-merge which is not recommened in the reading. That way schema 
enforcement stays active for every other write. The table now has eight columns and still has 12 rows, and the existing rows have null in 
source_system. History shows the change as version 1.
"""

# --- Lab 5.4 ---
# TODO: MERGE customers_changes: update matched rows, insert unmatched rows; handle source_system evolution intentionally.
target = DeltaTable.forPath(spark, str(table))
target.alias("target").merge(cus_changes.alias("cus_changes"), "target.customer_id = cus_changes.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

current = spark.read.format("delta").load(str(table))
print("rows:", current.count())
current.filter(F.col("customer_id").isin(3, 7, 13, 14)).orderBy("customer_id").show(truncate = False)
target.history().select("version", "operation", "operationMetrics").show(truncate = False)

# --- Lab 5.5 ---
# TODO: query the original version, compare it with the current version, then show history.
version0 = spark.read.format("delta").option("versionAsOf", 0).load(str(table))
current = spark.read.format("delta").load(str(table))
print(f"version 0 rows: {version0.count()}\ncurrent version rows: {current.count()}")

(version0.alias("v0").join(current.alias("curr"), "customer_id")
   .filter((F.col("v0.email") != F.col("curr.email")) | (F.col("v0.city") != F.col("curr.city")) | (F.col("v0.segment") != F.col("curr.segment")))
   .select("customer_id", F.col("v0.email").alias("old_email"), F.col("curr.email").alias("new_email"), F.col("v0.city").alias("old_city"), F.col("curr.city").alias("new_city"),
           F.col("v0.segment").alias("old_segment"), F.col("curr.segment").alias("new_segment")).orderBy("customer_id").show(truncate = False))

DeltaTable.forPath(spark, str(table)).history().select("version", "operation", "operationMetrics").show(truncate = False)

"""
I used time travel to read version 0 and compared it with the current Delta table. Version 0 had 12 rows, while the current version had 14 rows. 
Customers 3 and 7 had updated information, and customers 13 and 14 were inserted. The history showed the initial write, schema evolution, and 
MERGE operations. This demonstrates how Delta Lake allows me to access previous versions and track changes over time.
"""

# --- Lab 5.6 ---
# TODO: rerun MERGE and prove row count and values are unchanged.
before = spark.read.format("delta").option("versionAsOf", 2).load(str(table))

DeltaTable.forPath(spark, str(table)).alias("target").merge(cus_changes.alias("cus_changes"), "target.customer_id = cus_changes.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

after = spark.read.format("delta").load(str(table))

before_only = before.exceptAll(after)
after_only = after.exceptAll(before)

print(f"rows before: {before.count()}\nrows after: {after.count()}")
print(f"different rows: {before_only.count() + after_only.count()}")

# --- Lab 5.7 ---
"""
Delta Lake supports ACID transactions, which help keep data reliable. Atomicity means a change either completes fully or does not happen. Consistency keeps the table data valid. Isolation helps manage transactions running at the same time, and Durability means committed changes are saved.
Schema enforcement rejects incoming data when its schema does not match the target table. Schema evolution allows intentional schema changes, such as adding a new column using mergeSchema.
Optimistic concurrency control allows multiple operations to work on a table but checks for conflicts before saving changes. If a conflicting change occurs, a transaction may fail instead of silently overwriting another change.
Time travel lets us read previous versions of a Delta table. However, old versions are only available while the required transaction logs and data files are retained. Running VACUUM can delete old data files, making some previous versions unavailable.
"""


spark.stop()
