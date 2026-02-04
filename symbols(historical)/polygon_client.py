import os
import time
import requests
from typing import Dict, List, Any, Optional

POLYGON_BASE_URL = "https://api.polygon.io"
API_KEY = os.environ.get("POLYGON_API_KEY")

if not API_KEY:
    raise RuntimeError("POLYGON_API_KEY is not set")

session = requests.Session()

MAX_RETRIES = 5
BACKOFF_FACTOR = 2
BASE_WAIT = 2  # seconds


def polygon_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Call Polygon REST API with retry, backoff, and rate-limit handling
    """
    url = f"{POLYGON_BASE_URL}{path}"
    params = params or {}
    params["apiKey"] = API_KEY

    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, params=params, timeout=30)

            # ---- Rate limit ----
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", BASE_WAIT))
                print(f" Rate limited. Sleeping {wait}s...")
                time.sleep(wait)
                continue

            # ---- Server error ----
            if resp.status_code >= 500:
                wait = BASE_WAIT * (BACKOFF_FACTOR ** attempt)
                print(f" Server error {resp.status_code}. Retry in {wait}s...")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.RequestException as e:
            wait = BASE_WAIT * (BACKOFF_FACTOR ** attempt)
            print(f" Network error: {e}. Retry in {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Polygon API failed after {MAX_RETRIES} retries: {url}")



def build_polygon_path(symbol: str, timeframe: str, start_ts: str, end_ts: str) -> str:
    """
    timeframe:
      - 1d -> daily
      - 1m -> minute
    """
    if timeframe == "1d":
        return f"/v2/aggs/ticker/{symbol}/range/1/day/{start_ts}/{end_ts}"

    if timeframe == "1m":
        return f"/v2/aggs/ticker/{symbol}/range/1/minute/{start_ts}/{end_ts}"

    raise ValueError(f"Unsupported timeframe: {timeframe}")



def fetch_all_pages(path: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetch all paginated results from Polygon API
    """
    all_rows = []
    next_path = path
    first_call = True

    while next_path:
        data = polygon_get(next_path, params if first_call else None)
        first_call = False

        results = data.get("results", [])
        all_rows.extend(results)

        next_url = data.get("next_url")
        if next_url:
            next_path = next_url.replace(POLYGON_BASE_URL, "")
        else:
            next_path = None

    return all_rows


# ============================================================
# Normalization (Spark-safe types)
# ============================================================
def normalize_polygon_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Normalize rows so Spark doesn't explode on type mismatch.
    """
    normalized = []

    for r in rows:
        normalized.append(
            {
                "t": int(r["t"]) if r.get("t") is not None else None,
                "o": float(r["o"]) if r.get("o") is not None else None,
                "h": float(r["h"]) if r.get("h") is not None else None,
                "l": float(r["l"]) if r.get("l") is not None else None,
                "c": float(r["c"]) if r.get("c") is not None else None,
                "v": float(r["v"]) if r.get("v") is not None else None,
                "vw": float(r["vw"]) if r.get("vw") is not None else None,
                "n": float(r["n"]) if r.get("n") is not None else None,
            }
        )

    return normalized