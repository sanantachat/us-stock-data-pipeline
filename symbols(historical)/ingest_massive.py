import os
import time
import requests
from urllib.parse import urlparse, parse_qs
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from schemas import RAW_SCHEMA
from polygon_client import build_polygon_path, fetch_all_pages, normalize_polygon_rows
from utils import determine_window, get_max_ts_from_parquet

# ============================================================
# 1. BUILD SYMBOL UNIVERSE
# ============================================================

API_KEY = os.environ["MASSIVE_API_KEY"]
REF_BASE_URL = "https://api.polygon.io/v3/reference/tickers"

REQUEST_DELAY = 12
session = requests.Session()


def fetch_tickers(security_type: str, active_flag: bool):
    start_time = time.time()

    tickers = []
    cursor = None

    while True:
        params = {
            "apiKey": API_KEY,
            "market": "stocks",
            "locale": "us",
            "type": security_type,
            "active": str(active_flag).lower(),
            "limit": 1000,
        }

        if cursor:
            params["cursor"] = cursor

        resp = session.get(REF_BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        tickers.extend([r["ticker"] for r in data.get("results", [])])

        next_url = data.get("next_url")
        if not next_url:
            break

        parsed = urlparse(next_url)
        cursor = parse_qs(parsed.query).get("cursor", [None])[0]

        print(f"[{security_type} | active={active_flag}] {len(tickers)} so far")
        time.sleep(REQUEST_DELAY)

    print(f"Finished {security_type} active={active_flag} in {time.time() - start_time:.2f}s")
    return tickers


print("Building ticker universe...")
t0 = time.time()

CS_listed = fetch_tickers("CS", True)
CS_delisted = fetch_tickers("CS", False)
PFD_listed = fetch_tickers("PFD", True)
PFD_delisted = fetch_tickers("PFD", False)

ordered_symbols = CS_listed + CS_delisted + PFD_listed + PFD_delisted

print(f"Universe build done in {time.time() - t0:.2f}s")
print(f"Total symbols to ingest: {len(ordered_symbols)}")


# ============================================================
# 2. INGEST DATA INTO GCS
# ============================================================

RAW_BUCKET = "gs://raw-massive-symbols-parquet"


def create_spark():
    return SparkSession.builder.appName("Polygon → Parquet Ingestion").getOrCreate()


def ingest_symbols(symbols, timeframe, load_type, start_ts, end_ts):
    spark = create_spark()

    source = "minute" if timeframe == "1m" else "daily"
    parquet_path = f"{RAW_BUCKET}/source={source}"

    print("Determining load window...")
    t_window = time.time()
    start, end = determine_window(load_type, timeframe, start_ts, end_ts, parquet_path, spark)
    print(f"LOAD WINDOW: {start} → {end} ({time.time() - t_window:.2f}s)")

    max_ts = None
    if load_type == "incremental":
        t_max = time.time()
        max_ts = get_max_ts_from_parquet(spark, parquet_path)
        print(f"Existing max ts: {max_ts} ({time.time() - t_max:.2f}s)")

    total_symbols = len(symbols)

    for i, symbol in enumerate(symbols, 1):
        symbol_start = time.time()
        print(f"\n[{i}/{total_symbols}] Fetching {symbol} ...")

        t_api = time.time()
        path = build_polygon_path(symbol, timeframe, start, end)
        rows_raw = fetch_all_pages(path, params={"adjusted": "true", "sort": "asc"})
        print(f"API fetch took {time.time() - t_api:.2f}s")

        if not rows_raw:
            print(f"No data for {symbol}")
            continue

        t_norm = time.time()
        rows = normalize_polygon_rows(rows_raw)
        df_raw = spark.createDataFrame(rows, schema=RAW_SCHEMA)
        print(f"Normalization took {time.time() - t_norm:.2f}s")

        t_transform = time.time()
        df = (
            df_raw.withColumn("symbol", lit(symbol))
                  .withColumn("ts", (col("t") / 1000).cast("timestamp"))
                  .select(
                      "ts", "symbol",
                      col("o").alias("open"),
                      col("h").alias("high"),
                      col("l").alias("low"),
                      col("c").alias("close"),
                      col("v").alias("volume"),
                      col("vw").alias("vwap"),
                      col("n").alias("trades"),
                  )
        )

        if max_ts:
            df = df.filter(col("ts") > lit(max_ts))

        print(f"Transform took {time.time() - t_transform:.2f}s")

        # SAFER EMPTY CHECK
        t_check = time.time()
        if df.limit(1).count() == 0:
            print(f"No new rows for {symbol} ({time.time() - t_check:.2f}s)")
            continue

        print(f"Empty check took {time.time() - t_check:.2f}s")

        # WRITE (NO coalesce)
        t_write = time.time()
        df.write.mode("append").partitionBy("symbol").parquet(parquet_path)
        print(f"Write took {time.time() - t_write:.2f}s")

        print(f"✔ {symbol} done in {time.time() - symbol_start:.2f}s")

    spark.stop()


# ============================================================
# 3. RUN PIPELINE
# ============================================================

if __name__ == "__main__":
    ingest_symbols(
        symbols=ordered_symbols,
        timeframe="1m",      # change to 1d if needed
        load_type="full",    # or incremental
        start_ts=None,
        end_ts=None,
    )