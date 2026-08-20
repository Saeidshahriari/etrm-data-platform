"""Pandera schemas: the contract every dataset must satisfy before it is allowed
into the lakehouse.

This is Security Layer G (data security). A schema is not only a data-quality
tool: it is the gate that stops poisoned or corrupted data from an upstream
source entering the pipeline. A price of 40,000 EUR/MWh is either a unit bug or
an attack; either way it must not silently reach the risk numbers.

Limits below are business rules for European wholesale power/gas markets.
"""
from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column, DataFrameSchema

# --------------------------------------------------------------------------
# Business limits for European wholesale energy markets (EUR/MWh).
# Day-ahead power CAN be negative (too much wind/solar) and CAN spike during a
# crisis. The EU harmonised day-ahead clearing price range is -500 to +4000
# EUR/MWh, so anything outside that is impossible, not merely unusual.
# --------------------------------------------------------------------------
PRICE_MIN_EUR_MWH = -500.0
PRICE_MAX_EUR_MWH = 4000.0

# A "plausible" band used for alerting (not rejection). Values outside this but
# inside the hard limits are suspicious and get flagged for review.
PRICE_PLAUSIBLE_MIN = -50.0
PRICE_PLAUSIBLE_MAX = 1000.0

# Volume limits for a single trade (MW).
VOLUME_MIN_MW = 0.0
VOLUME_MAX_MW = 10_000.0

VALID_COMMODITIES = ["POWER", "NATURAL_GAS"]
VALID_DIRECTIONS = ["BUY", "SELL"]


# --------------------------------------------------------------------------
# Bronze market price payload (raw, straight from the external API).
# --------------------------------------------------------------------------
MARKET_PRICE_SCHEMA = DataFrameSchema(
    {
        "timestamp_ms": Column(
            "int64",
            checks=[
                # Epoch milliseconds between 2000-01-01 and 2100-01-01.
                Check.greater_than(946_684_800_000),
                Check.less_than(4_102_444_800_000),
            ],
            nullable=False,
            description="Epoch milliseconds of the delivery hour",
        ),
        "price_eur_mwh": Column(
            "float64",
            checks=[
                Check.greater_than_or_equal_to(PRICE_MIN_EUR_MWH),
                Check.less_than_or_equal_to(PRICE_MAX_EUR_MWH),
            ],
            nullable=False,
            description="Day-ahead clearing price in EUR per MWh",
        ),
    },
    strict=False,      # allow extra columns from the API
    coerce=True,
    name="bronze_market_prices",
)


# --------------------------------------------------------------------------
# Trades extracted from the OLTP database, before they reach Silver.
# --------------------------------------------------------------------------
TRADE_SCHEMA = DataFrameSchema(
    {
        "trade_id": Column(str, checks=Check.str_matches(r"^TRD-\d{5}$"), nullable=False, unique=True),
        "trader_id": Column(str, nullable=False),
        "counterparty": Column(str, checks=Check.str_length(min_value=2, max_value=50), nullable=False),
        "commodity": Column(str, checks=Check.isin(VALID_COMMODITIES), nullable=False),
        "buy_sell": Column(str, checks=Check.isin(VALID_DIRECTIONS), nullable=False),
        "volume_mw": Column(
            float,
            checks=[Check.greater_than(VOLUME_MIN_MW), Check.less_than_or_equal_to(VOLUME_MAX_MW)],
            nullable=False,
        ),
        "price_eur_mwh": Column(
            float,
            checks=[
                Check.greater_than_or_equal_to(PRICE_MIN_EUR_MWH),
                Check.less_than_or_equal_to(PRICE_MAX_EUR_MWH),
            ],
            nullable=False,
        ),
    },
    strict=False,
    coerce=True,
    name="silver_trades",
)


__all__ = [
    "MARKET_PRICE_SCHEMA",
    "TRADE_SCHEMA",
    "PRICE_MIN_EUR_MWH",
    "PRICE_MAX_EUR_MWH",
    "PRICE_PLAUSIBLE_MIN",
    "PRICE_PLAUSIBLE_MAX",
    "VALID_COMMODITIES",
    "VALID_DIRECTIONS",
]
