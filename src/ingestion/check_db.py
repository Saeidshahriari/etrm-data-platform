"""Quick diagnostic: count and preview the raw_trades table in PostgreSQL.
Credentials come from Vault via the central config module.
"""
import os
import sys

import pandas as pd
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_db_config  # noqa: E402


def check_postgres_data() -> None:
    conn = psycopg2.connect(**get_db_config())

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM raw_trades;")
    total_count = cursor.fetchone()[0]
    print(f"[INFO] Total trades in PostgreSQL: {total_count}")

    query = """
    SELECT trade_id, counterparty, commodity, buy_sell, volume_mw, price_eur_mwh, created_at
    FROM raw_trades
    ORDER BY created_at DESC
    LIMIT 5;
    """
    df = pd.read_sql(query, conn)
    print("\nSample 5 trades:")
    print(df.to_string(index=False))

    cursor.close()
    conn.close()


if __name__ == "__main__":
    check_postgres_data()
