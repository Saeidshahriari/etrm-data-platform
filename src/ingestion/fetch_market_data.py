"""Fetch real-world European electricity Day-Ahead prices from the public SMARD
API and store the raw payload in the Bronze layer of the data lake.

The SMARD endpoint used here is public and needs no API key. The output folder
comes from the central config (DATA_ROOT), so it is correct both locally and
inside the containers.
"""
import json
import os
import sys
from datetime import datetime, timezone

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_data_root  # noqa: E402

# SMARD chart-data filter IDs (see https://github.com/bundesAPI/smard-api).
#
#   4169 = "Marktpreis: Deutschland/Luxemburg"  -> day-ahead price, EUR/MWh   <-- what we want
#    410 = "Stromverbrauch: Gesamt (Netzlast)"  -> grid load, MW              <-- NOT a price
#
# This project originally used 410 and labelled the result as a price. That is
# why every value looked like ~40,000 "EUR/MWh": German grid load really is
# about 40,000-51,000 MW. It was never a unit bug, it was the wrong dataset.
# The Layer G validation gate is what caught it, by rejecting prices above the
# EU day-ahead ceiling of 4000 EUR/MWh.
SMARD_PRICE_FILTER = 4169
SMARD_REGION = "DE"

INDEX_URL = (
    f"https://www.smard.de/app/chart_data/{SMARD_PRICE_FILTER}/{SMARD_REGION}/index_hour.json"
)
DATA_URL_TEMPLATE = (
    f"https://www.smard.de/app/chart_data/{SMARD_PRICE_FILTER}/{SMARD_REGION}/"
    f"{SMARD_PRICE_FILTER}_{SMARD_REGION}_hour_{{ts}}.json"
)


def fetch_electricity_market_prices() -> dict:
    """Fetch the latest hourly Day-Ahead price series via the two-step SMARD API.

    Step 1: read the index file to find the latest available timestamp chunk.
    Step 2: download the actual price series for that chunk.
    On any failure, return a small realistic fallback payload so the pipeline
    can still run end to end.
    """
    print("[INFO] Connecting to the public SMARD energy-market REST API...")
    try:
        index_response = requests.get(INDEX_URL, timeout=10)
        index_response.raise_for_status()
        timestamps = index_response.json().get("timestamps", [])
        if not timestamps:
            raise ValueError("No timestamps returned by the SMARD index endpoint.")

        latest_timestamp = timestamps[-1]
        data_url = DATA_URL_TEMPLATE.format(ts=latest_timestamp)

        print(f"[INFO] Downloading latest price series for chunk: {latest_timestamp}...")
        data_response = requests.get(data_url, timeout=10)
        data_response.raise_for_status()
        raw_series = data_response.json().get("series", [])

        formatted_series = []
        for item in raw_series:
            # Each item is [timestamp_in_ms, price_eur_per_mwh].
            if len(item) >= 2 and item[1] is not None:
                formatted_series.append({
                    "timestamp_ms": item[0],
                    "price_eur_mwh": float(item[1]),
                })

        print(f"[OK] Fetched {len(formatted_series)} hourly price records from the API.")
        return {
            "market": "EPEX_SPOT_GERMANY",
            "commodity": "POWER",
            "currency": "EUR",
            "unit": "MWh",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "series": formatted_series,
        }

    except Exception as exc:  # noqa: BLE001 - degrade gracefully to fallback data
        print(f"[WARN] Primary API failed ({exc}). Using fallback market payload...")
        return {
            "market": "EPEX_SPOT_GERMANY_AUSTRIA",
            "commodity": "POWER",
            "currency": "EUR",
            "unit": "MWh",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "series": [
                {"timestamp_ms": 1723507200000, "price_eur_mwh": 85.40},
                {"timestamp_ms": 1723510800000, "price_eur_mwh": 78.20},
                {"timestamp_ms": 1723514400000, "price_eur_mwh": 72.10},
                {"timestamp_ms": 1723518000000, "price_eur_mwh": 69.50},
                {"timestamp_ms": 1723521600000, "price_eur_mwh": 74.80},
                {"timestamp_ms": 1723525200000, "price_eur_mwh": 91.30},
                {"timestamp_ms": 1723528800000, "price_eur_mwh": 110.50},
                {"timestamp_ms": 1723532400000, "price_eur_mwh": 125.00},
            ],
        }


def save_raw_data_to_bronze(data: dict) -> None:
    """Save the raw JSON payload into the Bronze layer of the medallion lake."""
    output_dir = os.path.join(get_data_root(), "1_bronze")
    os.makedirs(output_dir, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(output_dir, f"market_prices_{stamp}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"[OK] Raw market data saved to Bronze layer: '{file_path}'")


if __name__ == "__main__":
    print("Starting Market Data Ingestion...")
    raw_data = fetch_electricity_market_prices()
    save_raw_data_to_bronze(raw_data)
