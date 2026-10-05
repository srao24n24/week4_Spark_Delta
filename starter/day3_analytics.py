from pyspark.sql import functions as F, Window
from spark_utils import project_root, spark_session
spark=spark_session("day3-analytics"); root=project_root(); src=root/"provided_data/day3"; out=root/"work/day3"
# TODO: read four tables using explicit schemas.
# TODO: join order items -> orders -> customers -> products and exclude Cancelled orders.
# TODO: derive gross_sales, discount_amount, net_sales and profit.
# TODO: build monthly_category_sales and customer_summary.
# TODO: rank the top 3 products inside each category and calculate monthly growth with LAG.
# TODO: reproduce monthly_category_sales using Spark SQL temporary views and compare results.
spark.stop()
