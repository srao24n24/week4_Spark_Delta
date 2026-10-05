from pyspark.sql import functions as F
from spark_utils import project_root, spark_session
spark=spark_session("day4-performance"); root=project_root(); src=root/"provided_data/day4"
facts=spark.read.option("header",True).option("inferSchema",True).csv(str(src/"order_items_50k.csv"))
products=spark.read.option("header",True).option("inferSchema",True).csv(str(src/"products_small.csv"))
# TODO: inspect partitions; compare repartition(8) and coalesce(2).
# TODO: explain a normal join and a broadcast(products) join; identify the physical join operators.
# TODO: persist one reused aggregation, trigger materialization, reuse it twice, then unpersist.
# TODO: quantify key skew and propose two mitigations. Do not claim timings from local mode prove cluster speed.
spark.stop()
