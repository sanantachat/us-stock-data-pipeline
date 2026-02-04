from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType
)

RAW_SCHEMA = StructType([
    StructField("t", LongType(), True),   # timestamp in ms
    StructField("o", DoubleType(), True), # open
    StructField("h", DoubleType(), True), # high
    StructField("l", DoubleType(), True), # low
    StructField("c", DoubleType(), True), # close
    StructField("v", DoubleType(), True), # volume
    StructField("vw", DoubleType(), True),# vwap
    StructField("n", DoubleType(), True), # trades
])