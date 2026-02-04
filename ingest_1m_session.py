import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from schemas import RAW_SCHEMA
from massive_client import build_path, fetch_all_pages
from utils import filter_session

SYMBOLS = [
    "A",
    "AA",
    "AACB",
    "AAL",
    "AAM",
    "AAME",
    "AAMI",
    "AAOI",
    "AAON",
    "AAP",
    "AAPL",
    "AARD",
    "AAT",
    "AAUC",
    "AB",
    "ABAT",
    "ABBV",
    "ABCB",
    "ABCL",
    "ABEO",
    "ABG",
    "ABLV",
    "ABM",
    "ABNB",
    "ABOS",
    "ABP",
    "ABR",
    "ABSI",
    "ABT",
    "ABTC",
    "ABTS",
    "ABUS",
    "ABVC",
    "ABVE",
    "ABX",
    "ACA",
    "ACAD",
    "ACB",
    "ACCL",
    "ACCO",
    "ACCS",
    "ACDC",
    "ACEL",
    "ACET",
    "ACFN",
    "ACGL",
    "ACGLN",
    "ACH",
    "ACHC",
    "ACHR",
    "ACHV",
    "ACI",
    "ACIC",
    "ACIU",
    "ACIW",
    "ACLS",
    "ACLX",
    "ACM",
    "ACMR",
    "ACN",
    "ACNB",
    "ACNT",
    "ACOG",
    "ACON",
    "ACR",
    "ACRE",
    "ACRS",
    "ACRV",
    "ACT",
    "ACTG",
    "ACTU",
    "ACU",
    "ACVA",
    "ACXP",
    "AD",
    "ADAM",
    "ADAMG",
    "ADAMH",
    "ADAMI",
    "ADAML",
    "ADAMO",
    "ADAMZ",
    "ADBE",
    "ADC",
    "ADCT",
    "ADEA",
    "ADGM",
    "ADI",
    "ADIL",
    "ADM",
    "ADMA",
    "ADNT",
    "ADP",
    "ADPT",
    "ADSE",
    "ADSK",
    "ADT",
    "ADTN",
    "ADTX",
    "ADUR",
    "ADUS",
    "ADV",
    "ADVB",
    "AEBI",
    "AEC",
    "AEE",
    "AEG",
    "AEHL",
    "AEHR",
    "AEI",
    "AEIS",
    "AEM",
    "AEMD",
    "AENT",
    "AEO",
    "AEON",
    "AEP",
    "AER",
    "AERT",
    "AES",
    "AESI",
    "AEVA",
    "AEXA",
    "AEYE",
    "AFBI",
    "AFCG",
    "AFG",
    "AFJK",
    "AFL",
    "AFRI",
    "AFRM",
    "AFYA",
    "AG",
    "AGAE",
    "AGCC",
    "AGCO",
    "AGEN",
    "AGH",
    "AGI",
    "AGIG",
    "AGIO",
    "AGL",
    "AGM",
    "AGM.A",
    "AGMH",
    "AGNC",
    "AGNCL",
    "AGNCO",
    "AGO",
    "AGPU",
    "AGRO",
    "AGRZ",
    "AGX",
    "AGYS",
    "AHCO",
    "AHH",
    "AHL",
    "AHMA",
    "AHR",
    "AHT",
    "AI",
    "AIFF",
    "AIFU",
    "AIG",
    "AIHS",
    "AII",
    "AIIA",
    "AIIO",
    "AIM",
    "AIMD",
    "AIN",
    "AIOT",
    "AIP",
    "AIR",
    "AIRE",
    "AIRG",
    "AIRI",
    "AIRJ",
    "AIRO",
    "AIRS",
    "AIRT",
    "AISP",
    "AIT",
    "AIV",
    "AIXC",
    "AIZ",
    "AJG",
    "AKA",
    "AKAM",
    "AKAN",
    "AKBA",
    "AKR",
    "AKTS",
]

BRONZE_BUCKET = "gs://1m-session-delta"
TABLES = {
    "regular": f"{BRONZE_BUCKET}/us_stocks_1m_regular",
    "premarket": f"{BRONZE_BUCKET}/us_stocks_1m_premarket",
    "afterhours": f"{BRONZE_BUCKET}/us_stocks_1m_afterhours",
}


def create_spark():
    return (
        SparkSession.builder.appName("US Stocks 1m Session Ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def normalize_row(r):
    return {
        "t": int(r.get("t")) if r.get("t") is not None else None,
        "o": float(r.get("o")) if r.get("o") is not None else None,
        "h": float(r.get("h")) if r.get("h") is not None else None,
        "l": float(r.get("l")) if r.get("l") is not None else None,
        "c": float(r.get("c")) if r.get("c") is not None else None,
        "v": float(r.get("v")) if r.get("v") is not None else None,
        "vw": float(r.get("vw")) if r.get("vw") is not None else None,
        "n": float(r.get("n")) if r.get("n") is not None else None,
    }


def build_df(spark, rows, symbol):
    clean_rows = [normalize_row(r) for r in rows]

    df = spark.createDataFrame(clean_rows, schema=RAW_SCHEMA)

    df = (
        df.withColumn("symbol", lit(symbol))
        .withColumn("ts", (col("t") / 1000).cast("timestamp"))
        .select(
            "ts",
            "symbol",
            col("o").alias("open"),
            col("h").alias("high"),
            col("l").alias("low"),
            col("c").alias("close"),
            col("v").alias("volume"),
            col("vw").alias("vwap"),
            col("n").alias("trades"),
        )
    )

    return df


def main():
    start = sys.argv[1]
    end = sys.argv[2]

    spark = create_spark()

    for symbol in SYMBOLS:
        print(f"\n=== {symbol} ===")

        rows_reg = fetch_all_pages(build_path(symbol, start, end, "regular"))
        if rows_reg:
            df_reg = build_df(spark, rows_reg, symbol)
            df_reg.write.format("delta").mode("append").partitionBy("symbol").save(
                TABLES["regular"]
            )
            print("regular saved")

        rows_full = fetch_all_pages(build_path(symbol, start, end))
        if rows_full:
            df_full = build_df(spark, rows_full, symbol)

            df_pre = filter_session(df_full, "premarket")
            if not df_pre.rdd.isEmpty():
                df_pre.write.format("delta").mode("append").partitionBy("symbol").save(
                    TABLES["premarket"]
                )
                print("premarket saved")

            df_after = filter_session(df_full, "afterhours")
            if not df_after.rdd.isEmpty():
                df_after.write.format("delta").mode("append").partitionBy(
                    "symbol"
                ).save(TABLES["afterhours"])
                print("afterhours saved")

    spark.stop()


if __name__ == "__main__":
    main()
