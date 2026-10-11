"""Day 6 capstone. Implement functions; do not place the entire pipeline at module scope."""
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T, Window
from spark_utils import project_root, spark_session
import shutil

# --- Lab 6.1 ---
def ingest_bronze(spark, input_dir, bronze_dir, batch_id):
    customer_schema = T.StructType([T.StructField("customer_id",T.IntegerType(),True),T.StructField("customer_name",T.StringType(),True),T.StructField("email",T.StringType(),True),T.StructField("city",T.StringType(),True),T.StructField("state",T.StringType(),True),T.StructField("signup_date",T.StringType(),True),T.StructField("segment",T.StringType(),True)])
    product_schema = T.StructType([T.StructField("product_id",T.IntegerType(),True),T.StructField("product_name",T.StringType(),True),T.StructField("category",T.StringType(),True),T.StructField("brand",T.StringType(),True),T.StructField("cost_price",T.DecimalType(10,2),True),T.StructField("list_price",T.DecimalType(10,2),True)])
    order_schema = T.StructType([T.StructField("order_id",T.IntegerType(),True),T.StructField("customer_id",T.IntegerType(),True),T.StructField("order_date",T.StringType(),True),T.StructField("order_status",T.StringType(),True),T.StructField("sales_channel",T.StringType(),True)])
    item_schema = T.StructType([T.StructField("order_item_id",T.IntegerType(),True),T.StructField("order_id",T.IntegerType(),True),T.StructField("product_id",T.IntegerType(),True),T.StructField("quantity",T.IntegerType(),True),T.StructField("unit_price",T.DecimalType(10,2),True),T.StructField("discount_pct",T.DecimalType(5,2),True)])

    def read_raw(filename, schema):
        return (spark.read.option("header", True).schema(schema).csv(f"{input_dir}/{filename}").withColumn("ingested_at", F.current_timestamp())).withColumn("source_file", F.lit(filename)).withColumn("batch_id", F.lit(batch_id))

    customers_raw = read_raw("customers.csv", customer_schema)
    customers_raw.write.format("delta").mode("append").save(str(bronze_dir / "customers"))

    products_raw = read_raw("products.csv", product_schema)
    products_raw.write.format("delta").mode("append").save(str(bronze_dir / "products"))

    orders_raw = read_raw("orders.csv", order_schema)
    orders_raw.write.format("delta").mode("append").save(str(bronze_dir / "orders"))

    items_raw = read_raw("order_items.csv", item_schema)
    items_raw.write.format("delta").mode("append").save(str(bronze_dir / "order_items"))


# --- Lab 6.2 ---
def dedupe(df, key):
    w = Window.partitionBy(key).orderBy(F.col("ingested_at").desc())
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def merge_into(spark, df, path, key):
    path = str(path)
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").save(path)
    else:
        (DeltaTable.forPath(spark, path).alias("t").merge(df.alias("s"), f"t.{key} = s.{key}").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def read_bronze(spark, bronze_dir, name):
    return spark.read.format("delta").load(str(bronze_dir / name))


def build_silver(spark, bronze_dir, silver_dir):
    customers_raw = read_bronze(spark, bronze_dir, "customers")
    products_raw = read_bronze(spark, bronze_dir, "products")
    orders_raw = read_bronze(spark, bronze_dir, "orders")
    items_raw = read_bronze(spark, bronze_dir, "order_items")

    customers_clean = (customers_raw
        .withColumn("customer_name", F.trim("customer_name"))
        .withColumn("email", F.lower(F.trim("email")))
        .withColumn("city", F.trim("city"))
        .withColumn("state", F.trim("state"))
        .withColumn("segment", F.initcap(F.trim("segment")))
        .withColumn("signup_date_raw", F.col("signup_date"))
        .withColumn("signup_date", F.expr("try_cast(trim(signup_date) as date)")))

    products_clean = (products_raw
        .withColumn("product_name", F.trim("product_name"))
        .withColumn("category", F.trim("category"))
        .withColumn("brand", F.trim("brand")))

    orders_clean = (orders_raw
        .withColumn("order_status", F.when(F.lower(F.trim("order_status")) == "complete", "Completed").otherwise(F.initcap(F.trim("order_status"))))
        .withColumn("sales_channel", F.initcap(F.trim("sales_channel")))
        .withColumn("order_date_raw", F.col("order_date"))
        .withColumn("order_date", F.expr("try_cast(trim(order_date) as date)")))

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

    items_checked = items_raw.withColumn("reject_reason",
        F.when(F.col("order_item_id").isNull(), "missing_order_item_id")
        .when(F.col("order_id").isNull(), "missing_order_id")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("quantity").isNull(), "missing_quantity")
        .when(F.col("unit_price").isNull(), "missing_unit_price")
        .when(F.col("discount_pct").isNull(), "missing_discount_pct")
        .when(F.col("quantity") <= 0, "non_positive_quantity")
        .when((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100), "discount_out_of_range")
        .when(F.col("unit_price") <= 0, "non_positive_unit_price"))

    customers_valid = dedupe(customers_checked.filter("reject_reason IS NULL").drop("reject_reason"), "customer_id")
    products_valid = dedupe(products_checked.filter("reject_reason IS NULL").drop("reject_reason"), "product_id")
    orders_valid = dedupe(orders_checked.filter("reject_reason IS NULL").drop("reject_reason"), "order_id")
    items_valid = dedupe(items_checked.filter("reject_reason IS NULL").drop("reject_reason"), "order_item_id")

    customers_rejects = customers_checked.filter("reject_reason IS NOT NULL")
    products_rejects = products_checked.filter("reject_reason IS NOT NULL")
    orders_rejects = orders_checked.filter("reject_reason IS NOT NULL")
    items_rejects = items_checked.filter("reject_reason IS NOT NULL")

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

    merge_into(spark, customers_valid.drop("signup_date_raw"), silver_dir / "customers", "customer_id")
    merge_into(spark, products_valid, silver_dir / "products", "product_id")
    merge_into(spark, orders_valid_fk.drop("order_date_raw"), silver_dir / "orders", "order_id")
    merge_into(spark, items_valid_product, silver_dir / "order_items", "order_item_id")

    for name, rej in [("customers", customers_rejects), ("products", products_rejects), ("orders", orders_rejects_all), ("order_items", items_rejects_all)]:
        rej.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(silver_dir / "rejects" / name))


# --- Lab 6.3 ---
def build_gold(spark, silver_dir, gold_dir):
    customers = (spark.read.format("delta").load(str(silver_dir / "customers")).select("customer_id", "customer_name", "segment", "city", "state"))
    products = (spark.read.format("delta").load(str(silver_dir / "products")).select("product_id", "product_name", "category", "brand", "cost_price"))
    orders = (spark.read.format("delta").load(str(silver_dir / "orders")).select("order_id", "customer_id", "order_date", "order_status", "sales_channel"))
    items = (spark.read.format("delta").load(str(silver_dir / "order_items")).select("order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct"))

    joined = (items.join(orders, "order_id", "inner").join(customers, "customer_id", "inner").join(products, "product_id", "inner"))
    uncancelled = joined.filter(F.col("order_status") != "Cancelled")

    gold_sales_detail = (uncancelled
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
        .withColumn("gross_sales", (F.col("quantity") * F.col("unit_price")).cast("decimal(18,4)"))
        .withColumn("discount_amount", (F.col("gross_sales") * F.col("discount_pct") / 100).cast("decimal(18,4)"))
        .withColumn("net_sales", (F.col("gross_sales") - F.col("discount_amount")).cast("decimal(18,4)"))
        .withColumn("profit", (F.col("net_sales") - (F.col("quantity") * F.col("cost_price"))).cast("decimal(18,4)")))

    gold_monthly_category = (gold_sales_detail.groupBy("order_month", "category").agg(F.countDistinct("order_id").alias("num_orders"), F.sum("quantity").alias("units_sold"), F.sum("gross_sales").alias("gross_sales"), F.sum("net_sales").alias("net_sales"), F.sum("profit").alias("profit")).orderBy("order_month", "category"))

    gold_sales_detail.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(gold_dir / "gold_sales_detail"))
    gold_monthly_category.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(gold_dir / "gold_monthly_category"))


def read_delta(spark, path):
    return spark.read.format("delta").load(str(path))


def add_check(results, check_name, actual, expected):
    status = "PASS" if actual == expected else "FAIL"
    results.append((check_name, str(actual), str(expected), status))


def reconcile(spark, bronze_dir, silver_dir, gold_dir):
    results = []
    keys = {"customers": "customer_id", "products": "product_id",
            "orders": "order_id", "order_items": "order_item_id"}
    required_columns = {"customers": ["customer_name", "email", "signup_date"],
                        "products": ["product_name", "category", "cost_price", "list_price"],
                        "orders": ["customer_id", "order_date", "order_status", "sales_channel"],
                        "order_items": ["order_id", "product_id", "quantity", "unit_price", "discount_pct"]}

    for name, key in keys.items():
        bronze = read_delta(spark, bronze_dir / name)
        silver = read_delta(spark, silver_dir / name)
        rejects = read_delta(spark, silver_dir / "rejects" / name)

        bronze_rows = bronze.count()
        silver_rows = silver.count()
        reject_rows = rejects.count()
        results.append((f"{name}: bronze / silver / rejected rows", f"{bronze_rows} / {silver_rows} / {reject_rows}", "-", "INFO"))

        bronze_keys = bronze.select(key).dropna().distinct().count()
        reject_keys = rejects.select(key).dropna().distinct().count()
        add_check(results, f"{name}: bronze distinct keys = silver rows + rejected keys", silver_rows + reject_keys, bronze_keys)

        add_check(results, f"{name}: duplicate {key} in silver", silver_rows - silver.select(key).distinct().count(), 0)
        add_check(results, f"{name}: null {key} in silver", silver.filter(F.col(key).isNull()).count(), 0)

        for column in required_columns[name]:
            add_check(results, f"{name}: null {column} in silver", silver.filter(F.col(column).isNull()).count(), 0)

    customers = read_delta(spark, silver_dir / "customers")
    products = read_delta(spark, silver_dir / "products")
    orders = read_delta(spark, silver_dir / "orders")
    items = read_delta(spark, silver_dir / "order_items")

    add_check(results, "orders without a customer", orders.join(customers, "customer_id", "left_anti").count(), 0)
    add_check(results, "order_items without an order", items.join(orders, "order_id", "left_anti").count(), 0)
    add_check(results, "order_items without a product", items.join(products, "product_id", "left_anti").count(), 0)

    detail = read_delta(spark, gold_dir / "gold_sales_detail")
    monthly = read_delta(spark, gold_dir / "gold_monthly_category")

    add_check(results, "gold_sales_detail: Cancelled rows", detail.filter(F.col("order_status") == "Cancelled").count(), 0)

    items_small = items.select("order_id", "product_id", "quantity", "unit_price", "discount_pct")
    orders_small = orders.select("order_id", "order_status")
    products_small = products.select("product_id", "cost_price")
    silver_sales = (items_small.join(orders_small, "order_id").filter(F.col("order_status") != "Cancelled").join(products_small, "product_id"))

    add_check(results, "gold_sales_detail rows = silver items in non-cancelled orders", detail.count(), silver_sales.count())

    net = F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100)
    expected = silver_sales.agg(
        F.sum(F.col("quantity") * F.col("unit_price")).cast("decimal(18,4)").alias("gross"),
        F.sum(net).cast("decimal(18,4)").alias("net"),
        F.sum(net - F.col("quantity") * F.col("cost_price")).cast("decimal(18,4)").alias("profit")).collect()[0]
    actual = detail.agg(
        F.sum("gross_sales").alias("gross"),
        F.sum("net_sales").alias("net"),
        F.sum("profit").alias("profit")).collect()[0]
    monthly_totals = monthly.agg(
        F.sum("gross_sales").alias("gross"),
        F.sum("net_sales").alias("net"),
        F.sum("profit").alias("profit")).collect()[0]

    for metric in ["gross", "net", "profit"]:
        add_check(results, f"gold_sales_detail total {metric} = silver recalculation", actual[metric], expected[metric])
        add_check(results, f"gold_monthly_category total {metric} = gold_sales_detail total", monthly_totals[metric], actual[metric])

    report = None
    for check_name, actual_value, expected_value, status in results:
        row = spark.range(1).select(F.lit(check_name).alias("check"), F.lit(actual_value).alias("actual"), F.lit(expected_value).alias("expected"), F.lit(status).alias("status"))
        if report is None:
            report = row
        else:
            report = report.unionByName(row)
    return report

if __name__ == "__main__":
    spark = spark_session("week4-capstone", with_delta=True)
    root = project_root()
    input_root = root / "provided_data/day6_capstone"
    work = root / "work/day6"
    bronze = work / "bronze"
    silver = work / "silver"
    gold = work / "gold"

    shutil.rmtree(work, ignore_errors=True)   # start from empty folders every run

    def print_summary(title):
        print(f"\n===== {title} =====")
        for name in ["customers", "products", "orders", "order_items"]:
            print(f"silver/{name}: {read_delta(spark, silver / name).count()} rows")
        detail = read_delta(spark, gold / "gold_sales_detail")
        print("gold_sales_detail rows:", detail.count())
        detail.agg(F.sum("gross_sales"), F.sum("discount_amount"), F.sum("net_sales"), F.sum("profit")).show(truncate = False)

    def gold_keys():
        detail = read_delta(spark, gold / "gold_sales_detail")
        return sorted(row["order_item_id"] for row in detail.select("order_item_id").collect())

    # Run 1: initial batch
    ingest_bronze(spark, input_root / "initial", bronze, "batch_initial")
    build_silver(spark, bronze, silver)
    build_gold(spark, silver, gold)
    print_summary("RUN 1: initial batch")

    # Run 2: incremental batch
    ingest_bronze(spark, input_root / "incremental", bronze, "batch_incr_01")
    build_silver(spark, bronze, silver)
    build_gold(spark, silver, gold)
    print_summary("RUN 2: incremental batch")
    keys_after_run2 = gold_keys()

    # Run 3: the same incremental batch again
    ingest_bronze(spark, input_root / "incremental", bronze, "batch_incr_01")
    build_silver(spark, bronze, silver)
    build_gold(spark, silver, gold)
    print_summary("RUN 3: same incremental batch again")
    keys_after_run3 = gold_keys()

    print("\nGold keys unchanged between run 2 and run 3:", keys_after_run2 == keys_after_run3)

    read_delta(spark, silver / "orders").filter("order_id = 1011").show(truncate = False)

    read_delta(spark, gold / "gold_monthly_category").show(50, truncate = False)

    report = reconcile(spark, bronze, silver, gold)
    report.show(100, truncate = False)
    print("Checks failed:", report.filter(F.col("status") == "FAIL").count())
    report.coalesce(1).write.mode("overwrite").option("header", True).csv(str(work / "evidence" / "reconciliation"))

    spark.stop()
