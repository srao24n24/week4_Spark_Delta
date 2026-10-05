from delta.tables import DeltaTable
from pyspark.sql import functions as F
from spark_utils import project_root, spark_session
spark=spark_session("day5-delta", with_delta=True); root=project_root(); src=root/"provided_data/day5"; table=root/"work/day5/delta/customers"
# TODO: write customers_initial as Delta and inspect _delta_log/history.
# TODO: prove schema enforcement by attempting one incompatible append and capturing the error.
# TODO: MERGE customers_changes: update matched rows, insert unmatched rows; handle source_system evolution intentionally.
# TODO: query the original version, compare it with the current version, then show history.
# TODO: rerun MERGE and prove row count and values are unchanged.
spark.stop()
