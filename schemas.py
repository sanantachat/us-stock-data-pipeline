from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    DoubleType,
    StringType,
    TimestampType,
)

RAW_SCHEMA = StructType(
    [
        StructField("t", LongType(), True),     # epoch millis
        StructField("o", DoubleType(), True),   # open
        StructField("h", DoubleType(), True),   # high
        StructField("l", DoubleType(), True),   # low
        StructField("c", DoubleType(), True),   # close
        StructField("v", DoubleType(), True),   # volume
        StructField("vw", DoubleType(), True),  # vwap
        StructField("n", DoubleType(), True),   # trades count 
    ]
)

# ============================================================
# AGG / DELTA SCHEMA
# - Last Schema that write into Delta Lake
# - Type must strict + stable
# ============================================================

AGG_SCHEMA = StructType(
    [
        StructField("ts", TimestampType(), False),
        StructField("symbol", StringType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("vwap", DoubleType(), True),
        StructField("trades", LongType(), True),
    ]
)
