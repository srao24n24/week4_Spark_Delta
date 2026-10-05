from pyspark.sql import functions as F, types as T
from spark_utils import project_root, spark_session
spark=spark_session("day2-cleaning"); root=project_root()
src=root/"provided_data/day2"; out=root/"work/day2"
# TODO: define explicit schemas and read all four raw CSV files.
# TODO: trim text, standardize status/channel, parse dates, remove exact/business-key duplicates.
# TODO: reject invalid email/date, missing required values, quantity <= 0, discount outside 0..100,
#       and customer/product foreign keys not found in the dimensions.
# TODO: write curated Parquet and rejects with reject_reason; create a one-row quality_summary.
spark.stop()
