import os
import time
import logging
import requests
from pyspark.sql import SparkSession

BASE_URL = "https://api.massive.com/v2/aggs/ticker"
REQUEST_DELAY = 12
session = requests.Session()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def get_api_key():

    key = os.environ.get("MASSIVE_API_KEY")
    if key:
        return key

    try:
        spark = SparkSession.getActiveSession()
        if spark:
            return spark.conf.get("spark.massive.api.key")
    except Exception:
        pass

    raise RuntimeError("MASSIVE_API_KEY not found")


def build_path(symbol, start, end, session_name=None):
    api_key = get_api_key()  

    url = f"{BASE_URL}/{symbol}/range/1/minute/{start}/{end}"
    params = {
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": api_key,
    }
    if session_name == "regular":
        params["session"] = "regular"
    return url, params


def fetch_all_pages(path_tuple):
    url, params = path_tuple
    rows = []

    while url:
        logging.info(f" Fetching {url}")
        resp = session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        rows.extend(data.get("results", []))
        url = data.get("next_url")
        params = None
        time.sleep(REQUEST_DELAY)

    return rows
