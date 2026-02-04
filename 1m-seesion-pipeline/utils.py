from pyspark.sql.functions import col, from_utc_timestamp, hour, minute

def add_et_columns(df):
    df = df.withColumn("ts_et", from_utc_timestamp("ts", "America/New_York"))
    return df.withColumn("hhmm", hour("ts_et") * 100 + minute("ts_et"))

def filter_session(df, session_name):
    df = add_et_columns(df)

    if session_name == "premarket":
        return df.filter((col("hhmm") >= 400) & (col("hhmm") <= 929))

    elif session_name == "regular":
        return df.filter((col("hhmm") >= 930) & (col("hhmm") <= 1600))

    elif session_name == "afterhours":
        return df.filter((col("hhmm") >= 1601) & (col("hhmm") <= 2000))

    return df
