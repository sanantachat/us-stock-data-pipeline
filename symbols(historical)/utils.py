from pyspark.sql.functions import col, max as spark_max
from datetime import datetime, timedelta


def parquet_exists(spark, path: str) -> bool:
    try:
        spark.read.parquet(path).limit(1)
        return True
    except Exception:
        return False


def get_max_ts_from_parquet(spark, path: str):
    if not parquet_exists(spark, path):
        return None

    df = spark.read.parquet(path)
    row = df.select(spark_max(col("ts")).alias("max_ts")).collect()[0]
    return row["max_ts"]


# ------------------------------------------------------------
# Window logic
# ------------------------------------------------------------
def determine_window(
    load_type: str,
    timeframe: str,
    start_ts: str,
    end_ts: str,
    data_path: str,
    spark,
):
    """
    Decide start/end window for Polygon API ingestion
    """

    # ---------------- FULL LOAD ----------------
    if load_type == "full":
        # If user provides window → use it
        if start_ts and end_ts:
            return start_ts, end_ts

        # Otherwise use default historical backfill
        if timeframe == "1m":
            return "2015-01-01", datetime.utcnow().strftime("%Y-%m-%d")
        return "2000-01-01", datetime.utcnow().strftime("%Y-%m-%d")

    # ---------------- INCREMENTAL LOAD ----------------
    if load_type == "incremental":
        max_ts = get_max_ts_from_parquet(spark, data_path)

        if not max_ts:
            raise ValueError("No existing data found for incremental load")

        start_date = (max_ts - timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = datetime.utcnow().strftime("%Y-%m-%d")
        return start_date, end_date

    raise ValueError("Invalid load_type")