from pyspark import StorageLevel
from pyspark.sql import functions as F
from spark_utils import project_root, spark_session
spark=spark_session("day4-performance"); root=project_root(); src=root/"provided_data/day4"
facts=spark.read.option("header",True).option("inferSchema",True).csv(str(src/"order_items_50k.csv"))
products=spark.read.option("header",True).option("inferSchema",True).csv(str(src/"products_small.csv"))

# --- Lab 4.1 ---
# TODO: inspect partitions; compare repartition(8) and coalesce(2).
print("Input partitions:", facts.rdd.getNumPartitions())
print("repartition(8):", facts.repartition(8).rdd.getNumPartitions())
print("coalesce(2):", facts.coalesce(2).rdd.getNumPartitions())

"""
A shuffle happens when Spark has to move data between partitions to reorganize it. For example, if Spark has data 
in 2 partitions but needs to move some rows into different partitions, the rows have to be shuffled around, which 
can be expensive. repartition() creates the number of partitions we ask for by redistributing the data, so 
repartition(8) changes the data from 1 partition to 8 partitions and performs a shuffle. This is useful when we 
want more parallelism or want to spread the data more evenly, such as before a large join or aggregation. coalesce() 
is different because it combines existing partitions without doing a full shuffle. For example, if we have 8 
partitions and use coalesce(2), Spark can combine them into 2 partitions. However, in this lab the input has only 1 
partition, so coalesce(2) stays at 1 because coalesce cannot increase the number of partitions. A simple way to 
think about it is that repartition() is like moving students into new classrooms to spread them out, while coalesce() 
is like combining classrooms when we need fewer of them.
"""

# --- Lab 4.2 ---
# TODO: explain a normal join and a broadcast(products) join; identify the physical join operators.
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
norm_join = facts.join(products, "product_id", "inner")
norm_join.explain("formatted")

print(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")
brod_join = facts.join(F.broadcast(products), "product_id", "inner")
brod_join.explain("formatted")

"""
In the normal product join, Spark used a SortMergeJoin. Both tables had to be shuffled using product_id, shown by the 
Exchange steps in the plan, and then sorted before Spark could join them. This is more work because Spark has to move 
and organize the data. When broadcast(products) was used, Spark used a BroadcastHashJoin instead. Since the products 
table only has 15 rows, Spark can copy this small table to each executor and keep it in memory. The large order_items 
table does not need to be shuffled or sorted, so Spark can simply look up the matching product_id in the small 
broadcast table. 
"""

# --- Lab 4.3 ---
# TODO: persist one reused aggregation, trigger materialization, reuse it twice, then unpersist.
category_agg = (facts.join(F.broadcast(products), "product_id", "inner").groupBy("category").agg(F.sum("quantity").alias("units"),
                F.sum(F.col("quantity") * F.col("unit_price")).alias("revenue"),  F.count("order_item_id").alias("order_items")))

print("Cache lifecycle evidence:")
print(f"Before persist MEMORY_AND_DISK, is_cached --> {category_agg.is_cached}")

category_agg.persist(StorageLevel.MEMORY_AND_DISK)
print(f"After persist MEMORY_AND_DISK, is_cached --> {category_agg.is_cached}")

print(f"Materializing it using the action count() --> rows = {category_agg.count()}")
print(f"After action count(), is_cached --> {category_agg.is_cached}")

category_agg.orderBy(F.desc("revenue")).show(truncate = False)
category_agg.filter(F.col("units") >= 10000 ).show(truncate = False)

category_agg.unpersist()
print(f"After unpersist, is_cached --> {category_agg.is_cached}")

# --- Lab 4.4 ---
# TODO: quantify key skew and propose two mitigations. Do not claim timings from local mode prove cluster speed.
total_rows = facts.count()
skew_table = (facts.groupBy("product_id").count().withColumn("pct_of_rows", F.round(F.col("count") / F.lit(total_rows) * 100, 2))
              .orderBy(F.desc("count")))

skew_table.show(truncate = False)
hottest = skew_table.first()
print(f"Hottest key is product_id {hottest["product_id"]} with {hottest["count"]} rows which is {hottest["pct_of_rows"]}% of all rows")

"""
Product 1 is the hottest key because it has many more rows than the other products. This can cause data skew because when 
Spark groups or joins data by product_id, rows with the same key may be sent to the same partition. Product 1 could make 
one partition much larger than the others, causing one task to do much more work and slowing down the overall job.
"""

# --- Lab 4.5 ---
"""
Mitigation 1
I would use a broadcast join because products_small only has 15 rows. Broadcasting the small table allows Spark to send 
it to the executors instead of shuffling the 50,000-row facts table by product_id. This can help avoid the skew problem 
seen with product 1. However, broadcasting would not be a good choice if the table being broadcast is large, because it 
would require more memory on the executors.

Mitigation 2
Another option is Adaptive Query Execution (AQE). AQE uses runtime statistics to re-optimize the query plan while the 
query is running. This can help Spark make better decisions when the actual data distribution is different from what 
was expected. However, AQE would not necessarily solve every skew problem, especially for operations such as a skewed 
aggregation where the same hot key still needs to be processed together.
"""
spark.stop()
