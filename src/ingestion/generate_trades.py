"""Generate synthetic ETRM trades, validate them with Pydantic, and load them
into the PostgreSQL OLTP database. Database credentials come from Vault via the
central config module (no hardcoded credentials).
"""
import os
import random
import sys
from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, Field, PositiveFloat

# Allow "from config import ..." regardless of how the script is launched.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# NOTE ON IMPORTS
# psycopg2 (the database driver) and the Vault-backed config are imported INSIDE
# init_db_and_insert_trades(), not here.
#
# Generating trades is pure computation: it needs no database and no secrets.
# Importing a heavy, optional dependency at module level would force anyone who
# only wants to generate or test trades to install a database driver first, and
# it would make this module fail to import in any environment without one.
#
# This is the same failure that once stopped the Airflow DAG from loading:
# pyspark was imported at module level, so the scheduler could not parse the
# file at all. Keep optional heavy imports next to the code that uses them.


# 1. Pydantic schema for trade validation (data quality gate)
class TradeSchema(BaseModel):
    trade_id: str
    trader_id: str
    counterparty: str
    commodity: Literal["POWER", "NATURAL_GAS"]
    buy_sell: Literal["BUY", "SELL"]
    volume_mw: PositiveFloat
    price_eur_mwh: PositiveFloat
    delivery_start: datetime
    delivery_end: datetime
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# 2. Reference data for the synthetic generator
COMPANIES = ["RWE_Supply", "Uniper_Trading", "Statkraft", "Shell_Energy", "Enel_Global", "Vattenfall"]
COMMODITIES = ["POWER", "NATURAL_GAS"]

# Reference market levels (EUR/MWh) the synthetic book trades around.
MARKET_LEVEL = {"POWER": 85.0, "NATURAL_GAS": 42.5}

# Real traders deal CLOSE to the prevailing market price. A uniform random price
# would mean half the book is off-market, which destroys any surveillance model
# trained on it: the model can never learn what "normal" means. A tight spread
# around the market level is both realistic and what makes anomaly detection work.
PRICE_SPREAD_PCT = 0.06        # ~6% one-sigma spread around the market level
VOLUME_LOG_MEAN = 3.0          # log-normal volumes: many small, few large trades
VOLUME_LOG_SIGMA = 0.7


def generate_synthetic_trades(num_trades: int = 100, seed: int | None = None) -> list[dict]:
    """Create and validate a realistic synthetic trade book.

    Args:
        num_trades: how many trades to produce.
        seed: fix the random seed for reproducible runs (useful in tests and
            when training a model you want to be able to reproduce).
    """
    rng = random.Random(seed)
    trades = []
    base_time = datetime.now(timezone.utc)

    for i in range(1, num_trades + 1):
        commodity = rng.choice(COMMODITIES)
        delivery_start = base_time + timedelta(days=rng.randint(1, 30))
        delivery_end = delivery_start + timedelta(hours=rng.choice([1, 4, 12, 24]))

        # Price: normally distributed around the market level for that commodity.
        level = MARKET_LEVEL[commodity]
        price = rng.gauss(level, level * PRICE_SPREAD_PCT)
        price = max(round(price, 2), 0.01)   # stay strictly positive

        # Volume: log-normal, so most trades are modest and a few are large.
        volume = min(round(rng.lognormvariate(VOLUME_LOG_MEAN, VOLUME_LOG_SIGMA), 2), 500.0)
        volume = max(volume, 0.5)

        trade_raw = {
            "trade_id": f"TRD-{i:05d}",
            "trader_id": f"TRADER-{rng.randint(101, 110)}",
            "counterparty": rng.choice(COMPANIES),
            "commodity": commodity,
            "buy_sell": rng.choice(["BUY", "SELL"]),
            "volume_mw": volume,
            "price_eur_mwh": price,
            "delivery_start": delivery_start,
            "delivery_end": delivery_end,
        }

        # Validate each trade before it is accepted.
        validated_trade = TradeSchema(**trade_raw)
        trades.append(validated_trade.model_dump())

    return trades


# 3. Database ingestion (PostgreSQL)
def init_db_and_insert_trades(trades: list[dict]) -> None:
    """Create the raw_trades table if needed and insert the validated trades.

    The database driver and the Vault-backed config are imported here, so that
    generating trades never requires them.
    """
    import psycopg2

    from config import get_db_config

    conn = psycopg2.connect(**get_db_config())
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_trades (
        trade_id VARCHAR(20) PRIMARY KEY,
        trader_id VARCHAR(20),
        counterparty VARCHAR(50),
        commodity VARCHAR(20),
        buy_sell VARCHAR(10),
        volume_mw NUMERIC(10, 2),
        price_eur_mwh NUMERIC(10, 2),
        delivery_start TIMESTAMP WITH TIME ZONE,
        delivery_end TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE
    );
    """
    cursor.execute(create_table_sql)

    insert_sql = """
    INSERT INTO raw_trades (
        trade_id, trader_id, counterparty, commodity, buy_sell,
        volume_mw, price_eur_mwh, delivery_start, delivery_end, created_at
    ) VALUES (
        %(trade_id)s, %(trader_id)s, %(counterparty)s, %(commodity)s, %(buy_sell)s,
        %(volume_mw)s, %(price_eur_mwh)s, %(delivery_start)s, %(delivery_end)s, %(created_at)s
    )
    ON CONFLICT (trade_id) DO NOTHING;
    """

    cursor.executemany(insert_sql, trades)
    conn.commit()

    print(f"[OK] Successfully ingested {len(trades)} validated trades into PostgreSQL.")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    print("Starting synthetic ETRM trade generation...")
    synthetic_trades = generate_synthetic_trades(num_trades=100)
    init_db_and_insert_trades(synthetic_trades)
