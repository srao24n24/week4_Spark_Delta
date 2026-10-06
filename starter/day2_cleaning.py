from pyspark.sql import functions as F, types as T
from spark_utils import project_root, spark_session
spark=spark_session("day2-cleaning"); root=project_root()
src=root/"provided_data/day2"; out=root/"work/day2"

# --- Lab 2.1 ---
customer_schema = T.StructType([T.StructField("customer_id",T.IntegerType(),True),T.StructField("customer_name",T.StringType(),True),T.StructField("email",T.StringType(),True),T.StructField("city",T.StringType(),True),T.StructField("state",T.StringType(),True),T.StructField("signup_date",T.StringType(),True),T.StructField("segment",T.StringType(),True)])
product_schema = T.StructType([T.StructField("product_id",T.IntegerType(),True),T.StructField("product_name",T.StringType(),True),T.StructField("category",T.StringType(),True),T.StructField("brand",T.StringType(),True),T.StructField("cost_price",T.DecimalType(10,2),True),T.StructField("list_price",T.DecimalType(10,2),True)])
order_schema = T.StructType([T.StructField("order_id",T.IntegerType(),True),T.StructField("customer_id",T.IntegerType(),True),T.StructField("order_date",T.StringType(),True),T.StructField("order_status",T.StringType(),True),T.StructField("sales_channel",T.StringType(),True)])
item_schema = T.StructType([T.StructField("order_item_id",T.IntegerType(),True),T.StructField("order_id",T.IntegerType(),True),T.StructField("product_id",T.IntegerType(),True),T.StructField("quantity",T.IntegerType(),True),T.StructField("unit_price",T.DecimalType(10,2),True),T.StructField("discount_pct",T.DecimalType(5,2),True)])

def read_raw(filename, schema):
    return (spark.read.option("header", True).schema(schema).csv(f"{src}/{filename}").withColumn("source_file", F.lit(filename)).withColumn("ingested_at", F.current_timestamp()))

customers_raw = read_raw("customers_raw.csv", customer_schema)
products_raw = read_raw("products_raw.csv", product_schema)
orders_raw = read_raw("orders_raw.csv", order_schema)
items_raw = read_raw("order_items_raw.csv", item_schema)

for name, df in [("customers", customers_raw), ("products", products_raw), ("orders", orders_raw), ("order_items", items_raw)]:
    print(f"{name} raw row count: {df.count()}")

# --- Lab 2.2 ---
customers_clean = (customers_raw
    .withColumn("customer_name", F.trim("customer_name"))
    .withColumn("email", F.lower(F.trim("email")))
    .withColumn("city", F.trim("city"))
    .withColumn("state", F.trim("state"))
    .withColumn("segment", F.initcap(F.trim("segment")))
    .withColumn("signup_date_raw", F.col("signup_date"))
    .withColumn("signup_date", F.expr("try_cast(trim(signup_date) as date)"))
    .dropDuplicates(["customer_id"]))

products_clean = (products_raw
    .withColumn("product_name", F.trim("product_name"))
    .withColumn("category", F.trim("category"))
    .withColumn("brand", F.trim("brand"))
    .dropDuplicates(["product_id"]))

orders_clean = (orders_raw
    .withColumn("order_status", F.when(F.lower(F.trim("order_status")) == "complete", "Completed").otherwise(F.initcap(F.trim("order_status"))))
    .withColumn("sales_channel", F.initcap(F.trim("sales_channel")))
    .withColumn("order_date_raw", F.col("order_date"))
    .withColumn("order_date", F.expr("try_cast(trim(order_date) as date)"))
    .dropDuplicates(["order_id"]))

items_clean = items_raw.dropDuplicates(["order_item_id"])

print("customers:", customers_clean.count(), "| products:", products_clean.count(), "| orders:", orders_clean.count(), "| order_items:", items_clean.count())

# --- Lab 2.3 ---
# TODO: reject invalid email/date, missing required values, quantity <= 0, discount outside 0..100,
#       and customer/product foreign keys not found in the dimensions.
email_regex = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

customers_checked = customers_clean.withColumn("reject_reason",
    F.when(F.col("customer_id").isNull(), "missing_customer_id")
     .when(F.col("customer_name").isNull(), "missing_customer_name")
     .when(F.col("email").isNull(), "missing_email")
     .when(~F.col("email").rlike(email_regex), "invalid_email")
     .when(F.col("signup_date_raw").isNull(), "missing_signup_date")
     .when(F.col("signup_date").isNull(), "invalid_signup_date"))

products_checked = products_clean.withColumn("reject_reason",
    F.when(F.col("product_id").isNull(), "missing_product_id")
     .when(F.col("product_name").isNull(), "missing_product_name")
     .when(F.col("category").isNull(), "missing_category")
     .when(F.col("cost_price").isNull(), "missing_cost_price")
     .when(F.col("list_price").isNull(), "missing_list_price")
     .when(F.col("cost_price") <= 0, "non_positive_cost_price")
     .when(F.col("list_price") <= 0, "non_positive_list_price"))

orders_checked = orders_clean.withColumn("reject_reason",
    F.when(F.col("order_id").isNull(), "missing_order_id")
     .when(F.col("customer_id").isNull(), "missing_customer_id")
     .when(F.col("order_status").isNull(), "missing_order_status")
     .when(F.col("sales_channel").isNull(), "missing_sales_channel")
     .when(F.col("order_date_raw").isNull(), "missing_order_date")
     .when(F.col("order_date").isNull(), "invalid_order_date"))

items_checked = items_clean.withColumn("reject_reason",
    F.when(F.col("order_item_id").isNull(), "missing_order_item_id")
     .when(F.col("order_id").isNull(), "missing_order_id")
     .when(F.col("product_id").isNull(), "missing_product_id")
     .when(F.col("quantity").isNull(), "missing_quantity")
     .when(F.col("unit_price").isNull(), "missing_unit_price")
     .when(F.col("discount_pct").isNull(), "missing_discount_pct")
     .when(F.col("quantity") <= 0, "non_positive_quantity")
     .when((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100), "discount_out_of_range")
     .when(F.col("unit_price") <= 0, "non_positive_unit_price"))

customers_valid = customers_checked.filter("reject_reason IS NULL").drop("reject_reason")
customers_rejects = customers_checked.filter("reject_reason IS NOT NULL")
products_valid = products_checked.filter("reject_reason IS NULL").drop("reject_reason")
products_rejects = products_checked.filter("reject_reason IS NOT NULL")
orders_valid = orders_checked.filter("reject_reason IS NULL").drop("reject_reason")
orders_rejects = orders_checked.filter("reject_reason IS NOT NULL")
items_valid = items_checked.filter("reject_reason IS NULL").drop("reject_reason")
items_rejects = items_checked.filter("reject_reason IS NOT NULL")

for name, v, r in [("customers", customers_valid, customers_rejects), ("products", products_valid, products_rejects), ("orders", orders_valid, orders_rejects), ("order_items", items_valid, items_rejects)]:
    print(f"{name}: valid {v.count()}, rejected {r.count()}")
    r.groupBy("reject_reason").count().show(truncate =  False)

# --- Lab 2.4 ---
# left_semi keeps rows with a matching key.
# left_anti keeps rows without a matching key.

customer_keys = (customers_valid.select(F.col("customer_id").alias("valid_customer_id")).distinct())
product_keys = (products_valid.select(F.col("product_id").alias("valid_product_id")).distinct())

orders_valid_fk = orders_valid.join(customer_keys, orders_valid["customer_id"] == customer_keys["valid_customer_id"], "left_semi")
orders_unknown_customer = (orders_valid.join(customer_keys, orders_valid["customer_id"] == customer_keys["valid_customer_id"], "left_anti").withColumn("reject_reason", F.lit("unknown_customer_id")))

order_keys = (orders_valid_fk.select(F.col("order_id").alias("valid_order_id")).distinct())
items_valid_order = items_valid.join(order_keys, items_valid["order_id"] == order_keys["valid_order_id"], "left_semi")
items_unknown_order = (items_valid.join(order_keys, items_valid["order_id"] == order_keys["valid_order_id"], "left_anti").withColumn("reject_reason", F.lit("unknown_order_id")))

items_valid_product = items_valid_order.join(product_keys, items_valid_order["product_id"] == product_keys["valid_product_id"], "left_semi")
items_unknown_product = (items_valid_order.join(product_keys, items_valid_order["product_id"] == product_keys["valid_product_id"], "left_anti").withColumn("reject_reason", F.lit("unknown_product_id")))

orders_rejects_all = (orders_rejects.unionByName(orders_unknown_customer))
items_rejects_all = (items_rejects.unionByName(items_unknown_order).unionByName(items_unknown_product))

print(f"orders: valid {orders_valid_fk.count()} rejected {orders_rejects_all.count()}")
print(f"order_items: valid {items_valid_product.count()} rejected {items_rejects_all.count()}")

print("Orders rejection reasons:")
orders_rejects_all.groupBy("reject_reason").count().show(truncate = False)
print("Order items rejection reasons:")
items_rejects_all.groupBy("reject_reason").count().show(truncate = False)

# --- Lab 2.5 ---
# TODO: write curated Parquet and rejects with reject_reason.
curated = {"customers": customers_valid.drop("signup_date_raw"), "products": products_valid, "orders": orders_valid_fk.drop("order_date_raw"),"order_items": items_valid_product}
rejects = {"customers": customers_rejects, "products": products_rejects, "orders": orders_rejects_all, "order_items": items_rejects_all}
 
for name, df in curated.items():
    df.write.mode("overwrite").parquet(f"{out}/curated/{name}")
for name, df in rejects.items():
    df.write.mode("overwrite").json(f"{out}/rejects/{name}")

for name, df in curated.items():
    print("curated", name, df.count(), spark.read.parquet(f"{out}/curated/{name}").count())
for name, df in rejects.items():
    print("rejects", name, df.count(), spark.read.schema(df.schema).json(f"{out}/rejects/{name}").count())

# --- Lab 2.6 ---
# TODO: create a one-row quality_summary.
summary = {}
entities = {
    "customers": (customers_raw, customers_clean, customers_valid, customers_rejects),
    "products": (products_raw, products_clean, products_valid, products_rejects),
    "orders": (orders_raw, orders_clean, orders_valid_fk, orders_rejects_all),
    "order_items": (items_raw, items_clean, items_valid_product, items_rejects_all)
}

for name, (raw, clean, valid, rejected) in entities.items():
    raw_num = raw.count()
    clean_num = clean.count()
    valid_num = valid.count()
    rejected_num = rejected.count()

    summary[f"{name}_raw"] = raw_num
    summary[f"{name}_duplicates"] = raw_num - clean_num
    summary[f"{name}_valid"] = valid_num
    summary[f"{name}_rejected"] = rejected_num

quality_summary = spark.createDataFrame([summary])
quality_summary.write.mode("overwrite").parquet(f"{out}/quality_summary")
quality_summary.show(truncate = False)

# --- Lab 2.7 ---
def clean_customers(df):
    return (df
        .withColumn("customer_name", F.trim("customer_name"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("city", F.trim("city"))
        .withColumn("state", F.trim("state"))
        .withColumn("segment", F.initcap(F.trim("segment")))
        .withColumn("signup_date_raw", F.col("signup_date"))
        .withColumn("signup_date", F.expr("try_cast(trim(signup_date) as date)"))
        .dropDuplicates(["customer_id"]))

def clean_products(df):
    return (df
        .withColumn("product_name", F.trim("product_name"))
        .withColumn("category", F.trim("category"))
        .withColumn("brand", F.trim("brand"))
        .dropDuplicates(["product_id"]))

def clean_orders(df):
    return (df
        .withColumn("order_status", F.when(F.lower(F.trim("order_status")) == "complete", "Completed").otherwise(F.initcap(F.trim("order_status"))))
        .withColumn("sales_channel", F.initcap(F.trim("sales_channel")))
        .withColumn("order_date_raw", F.col("order_date"))
        .withColumn("order_date", F.expr("try_cast(trim(order_date) as date)"))
        .dropDuplicates(["order_id"]))

def clean_items(df):
    return df.dropDuplicates(["order_item_id"])

customers_clean = clean_customers(customers_raw)
products_clean = clean_products(products_raw)
orders_clean = clean_orders(orders_raw)
items_clean = clean_items(items_raw)

print("customers:", customers_clean.count(), "| products:", products_clean.count(), "| orders:", orders_clean.count(), "| order_items:", items_clean.count())

def validate_customers(df):
    email_regex = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
    return df.withColumn("reject_reason",
        F.when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("customer_name").isNull(), "missing_customer_name")
        .when(F.col("email").isNull(), "missing_email")
        .when(~F.col("email").rlike(email_regex), "invalid_email")
        .when(F.col("signup_date_raw").isNull(), "missing_signup_date")
        .when(F.col("signup_date").isNull(), "invalid_signup_date"))

def validate_products(df):
    return df.withColumn("reject_reason",
        F.when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("product_name").isNull(), "missing_product_name")
        .when(F.col("category").isNull(), "missing_category")
        .when(F.col("cost_price").isNull(), "missing_cost_price")
        .when(F.col("list_price").isNull(), "missing_list_price")
        .when(F.col("cost_price") <= 0, "non_positive_cost_price")
        .when(F.col("list_price") <= 0, "non_positive_list_price"))

def validate_orders(df):
    return df.withColumn("reject_reason",
        F.when(F.col("order_id").isNull(), "missing_order_id")
        .when(F.col("customer_id").isNull(), "missing_customer_id")
        .when(F.col("order_status").isNull(), "missing_order_status")
        .when(F.col("sales_channel").isNull(), "missing_sales_channel")
        .when(F.col("order_date_raw").isNull(), "missing_order_date")
        .when(F.col("order_date").isNull(), "invalid_order_date"))

def validate_items(df):
    return df.withColumn("reject_reason",
        F.when(F.col("order_item_id").isNull(), "missing_order_item_id")
        .when(F.col("order_id").isNull(), "missing_order_id")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("quantity").isNull(), "missing_quantity")
        .when(F.col("unit_price").isNull(), "missing_unit_price")
        .when(F.col("discount_pct").isNull(), "missing_discount_pct")
        .when(F.col("quantity") <= 0, "non_positive_quantity")
        .when((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100), "discount_out_of_range")
        .when(F.col("unit_price") <= 0, "non_positive_unit_price"))

customers_checked = validate_customers(customers_clean)
products_checked = validate_products(products_clean)
orders_checked = validate_orders(orders_clean)
items_checked = validate_items(items_clean)

def split_valid_and_rejected(df):
    valid = df.filter("reject_reason IS NULL").drop("reject_reason")
    rejected = df.filter("reject_reason IS NOT NULL")
    return valid, rejected

customers_valid, customers_rejects = split_valid_and_rejected(customers_checked)
products_valid, products_rejects = split_valid_and_rejected(products_checked)
orders_valid, orders_rejects = split_valid_and_rejected(orders_checked)
items_valid, items_rejects = split_valid_and_rejected(items_checked)

for name, v, r in [("customers", customers_valid, customers_rejects), ("products", products_valid, products_rejects), ("orders", orders_valid, orders_rejects), ("order_items", items_valid, items_rejects)]:
    print(f"{name}: valid {v.count()}, rejected {r.count()}")
    r.groupBy("reject_reason").count().show(truncate =  False)

# --- Lab 2.4 ---
customer_keys = (customers_valid.select(F.col("customer_id").alias("valid_customer_id")).distinct())
product_keys = (products_valid.select(F.col("product_id").alias("valid_product_id")).distinct())

orders_valid_fk = orders_valid.join(customer_keys, orders_valid["customer_id"] == customer_keys["valid_customer_id"], "left_semi")
orders_unknown_customer = (orders_valid.join(customer_keys, orders_valid["customer_id"] == customer_keys["valid_customer_id"], "left_anti").withColumn("reject_reason", F.lit("unknown_customer_id")))

order_keys = (orders_valid_fk.select(F.col("order_id").alias("valid_order_id")).distinct())
items_valid_order = items_valid.join(order_keys, items_valid["order_id"] == order_keys["valid_order_id"], "left_semi")
items_unknown_order = (items_valid.join(order_keys, items_valid["order_id"] == order_keys["valid_order_id"], "left_anti").withColumn("reject_reason", F.lit("unknown_order_id")))

items_valid_product = items_valid_order.join(product_keys, items_valid_order["product_id"] == product_keys["valid_product_id"], "left_semi")
items_unknown_product = (items_valid_order.join(product_keys, items_valid_order["product_id"] == product_keys["valid_product_id"], "left_anti").withColumn("reject_reason", F.lit("unknown_product_id")))

orders_rejects_all = (orders_rejects.unionByName(orders_unknown_customer))
items_rejects_all = (items_rejects.unionByName(items_unknown_order).unionByName(items_unknown_product))

print(f"orders: valid {orders_valid_fk.count()} rejected {orders_rejects_all.count()}")
print(f"order_items: valid {items_valid_product.count()} rejected {items_rejects_all.count()}")

print("Orders rejection reasons:")
orders_rejects_all.groupBy("reject_reason").count().show(truncate = False)
print("Order items rejection reasons:")
items_rejects_all.groupBy("reject_reason").count().show(truncate = False)

# --- Lab 2.5 ---
# TODO: write curated Parquet and rejects with reject_reason.
curated = {"customers": customers_valid.drop("signup_date_raw"), "products": products_valid, "orders": orders_valid_fk.drop("order_date_raw"),"order_items": items_valid_product}
rejects = {"customers": customers_rejects, "products": products_rejects, "orders": orders_rejects_all, "order_items": items_rejects_all}
 
for name, df in curated.items():
    df.write.mode("overwrite").parquet(f"{out}/curated/{name}")
for name, df in rejects.items():
    df.write.mode("overwrite").json(f"{out}/rejects/{name}")

for name, df in curated.items():
    print("curated", name, df.count(), spark.read.parquet(f"{out}/curated/{name}").count())
for name, df in rejects.items():
    print("rejects", name, df.count(), spark.read.schema(df.schema).json(f"{out}/rejects/{name}").count())

# --- Lab 2.6 ---
# TODO: create a one-row quality_summary.
summary = {}
entities = {
    "customers": (customers_raw, customers_clean, customers_valid, customers_rejects),
    "products": (products_raw, products_clean, products_valid, products_rejects),
    "orders": (orders_raw, orders_clean, orders_valid_fk, orders_rejects_all),
    "order_items": (items_raw, items_clean, items_valid_product, items_rejects_all)
}

for name, (raw, clean, valid, rejected) in entities.items():
    raw_num = raw.count()
    clean_num = clean.count()
    valid_num = valid.count()
    rejected_num = rejected.count()

    summary[f"{name}_raw"] = raw_num
    summary[f"{name}_duplicates"] = raw_num - clean_num
    summary[f"{name}_valid"] = valid_num
    summary[f"{name}_rejected"] = rejected_num

quality_summary = spark.createDataFrame([summary])
quality_summary.write.mode("overwrite").parquet(f"{out}/quality_summary")
quality_summary.show(truncate = False, vertical = True)

spark.stop()
