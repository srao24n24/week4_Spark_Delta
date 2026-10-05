import sys
from starter.spark_utils import spark_session
print("Python:", sys.version.split()[0])
spark = spark_session("week4-setup-check")
print("Spark:", spark.version)
print("Rows:", spark.range(5).count())
spark.stop()
print("SETUP_OK")
