from pyspark.sql import functions as F, types as T, Window
from spark_utils import project_root, spark_session
spark=spark_session("day3-analytics"); root=project_root(); src=root/"provided_data/day3"; out=root/"work/day3"

# --- Lab 3.1 ---
# TODO: read four tables using explicit schemas.
customer_schema = T.StructType([T.StructField("customer_id",T.IntegerType(),True),T.StructField("customer_name",T.StringType(),True),T.StructField("email",T.StringType(),True),T.StructField("city",T.StringType(),True),T.StructField("state",T.StringType(),True),T.StructField("signup_date",T.DateType(),True),T.StructField("segment",T.StringType(),True)])
product_schema = T.StructType([T.StructField("product_id",T.IntegerType(),True),T.StructField("product_name",T.StringType(),True),T.StructField("category",T.StringType(),True),T.StructField("brand",T.StringType(),True),T.StructField("cost_price",T.DecimalType(10,2),True),T.StructField("list_price",T.DecimalType(10,2),True)])
order_schema = T.StructType([T.StructField("order_id",T.IntegerType(),True),T.StructField("customer_id",T.IntegerType(),True),T.StructField("order_date",T.DateType(),True),T.StructField("order_status",T.StringType(),True),T.StructField("sales_channel",T.StringType(),True)])
item_schema = T.StructType([T.StructField("order_item_id",T.IntegerType(),True),T.StructField("order_id",T.IntegerType(),True),T.StructField("product_id",T.IntegerType(),True),T.StructField("quantity",T.IntegerType(),True),T.StructField("unit_price",T.DecimalType(10,2),True),T.StructField("discount_pct",T.DecimalType(5,2),True)])
 
customers = spark.read.option("header", True).schema(customer_schema).csv(f"{src}/customers.csv")
products = spark.read.option("header", True).schema(product_schema).csv(f"{src}/products.csv")
orders = spark.read.option("header", True).schema(order_schema).csv(f"{src}/orders.csv")
items = spark.read.option("header", True).schema(item_schema).csv(f"{src}/order_items.csv")

# TODO: join order items -> orders -> customers -> products and exclude Cancelled orders.
joined = (items.join(orders, "order_id", "inner").join(customers, "customer_id", "inner").join(products, "product_id", "inner"))
print(f"Number of rows in items: {items.count()}\nNumber of rows in joined: {joined.count()}")  

uncancelled = joined.filter(F.col("order_status") != "Cancelled")
print(f"not cancelled rows: {uncancelled.count()}") 
print(f"Columns of joined not cancelled table: {uncancelled.columns}")

# --- Lab 3.2 ---
# TODO: derive gross_sales, discount_amount, net_sales and profit.
gold_sales_detail = (uncancelled.withColumn("order_month", F.date_format("order_date", "yyyy-MM")).withColumn("gross_sales", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,4)"))
                    .withColumn("discount_amount", (F.col("gross_sales") * F.col("discount_pct") / 100).cast("decimal(18,4)")).withColumn("net_sales", (F.col("gross_sales") - F.col("discount_amount")).cast("decimal(18,4)"))
                    .withColumn("profit", (F.col("net_sales") - (F.col("quantity") * F.col("cost_price"))).cast("decimal(18,4)")))

gold_sales_detail.agg(F.sum("gross_sales"), F.sum("discount_amount"), F.sum("net_sales"), F.sum("profit")).show(truncate = False)

# --- Labs 3.3 & 3.4 ---
# TODO: build monthly_category_sales and customer_summary.
monthly_category_sales = (gold_sales_detail.select("order_month", "order_id", "category", "net_sales", "profit").groupBy("order_month", "category").agg(F.countDistinct("order_id").alias("num_orders"), F.sum("net_sales").alias("net_sales"), F.sum("profit").alias("profit"))).orderBy("order_month", "category")
monthly_category_sales.show(truncate = False)

customer_summary = (gold_sales_detail.select("customer_id", "customer_name", "order_id", "net_sales").groupBy("customer_id").agg(F.countDistinct("order_id").alias("order_count"), F.sum("net_sales").alias("lifetime_value"))).orderBy("customer_id")
customer_summary.show(truncate = False)

# --- Labs 3.5 & 3.6 ---
# TODO: rank the top 3 products inside each category and calculate monthly growth with LAG.
product_revenue = (gold_sales_detail.groupBy("category", "product_id", "product_name").agg(F.sum("net_sales").alias("revenue")))
product_window = (Window.partitionBy("category").orderBy(F.col("revenue").desc(), F.col("product_id").asc()))
top3_prod = (product_revenue.withColumn("product_rank", F.row_number().over(product_window)).filter(F.col("product_rank") <= 3).orderBy("category", "product_rank"))
top3_prod.show(truncate = False)

monthly_sales = (gold_sales_detail.groupBy("order_month").agg(F.sum("net_sales").alias("monthly_revenue")))
monthly_window = (Window.orderBy("order_month"))
monthly_growth = (monthly_sales.withColumn("previous_month_sales", F.lag("monthly_revenue", 1).over(monthly_window)).withColumn("monthly_growth", ((F.col("monthly_revenue") - F.col("previous_month_sales")) / F.col("previous_month_sales") * 100).cast("decimal(18,4)"))).orderBy("order_month")
monthly_growth.show(truncate = False)

# --- Lab 3.7 ---
# TODO: reproduce monthly_category_sales using Spark SQL temporary views and compare results.
gold_sales_detail.createOrReplaceTempView("gold_sales_detail")

monthly_category_sql = spark.sql("""
    SELECT order_month, category, COUNT(DISTINCT order_id) AS num_orders, SUM(net_sales) AS net_sales, SUM(profit) AS profit FROM gold_sales_detail
    GROUP BY order_month, category
    ORDER BY order_month, category
""")

monthly_category_sql.show(truncate = False)

df_mismatch = monthly_category_sales.exceptAll(monthly_category_sql)
sql_mismatch = monthly_category_sql.exceptAll(monthly_category_sales)

print(f"Dataframe rows missing from SQL: {df_mismatch.count()}")
print(f"SQL rows missing from Dataframe: {sql_mismatch.count()}")

# --- Lab 3.8 ---
gold_sales_detail.write.mode("overwrite").parquet(f"{out}/gold_sales_detail")
monthly_category_sales.write.mode("overwrite").parquet(f"{out}/monthly_category_sales")
customer_summary.write.mode("overwrite").parquet(f"{out}/customer_summary")
top3_prod.write.mode("overwrite").parquet(f"{out}/top3_products")
monthly_growth.write.mode("overwrite").parquet(f"{out}/monthly_growth")

spark.stop()
