"""Feature engineering for trade-anomaly (market-abuse) detection.

The features below are the signals a REMIT market-surveillance analyst actually
looks at when deciding whether a trade is suspicious. Keeping them in one place
guarantees that training and scoring see EXACTLY the same features - a mismatch
between the two ("training/serving skew") is one of the most common and most
expensive bugs in production ML.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# The exact feature list, in a fixed order. Both training and scoring import it.
FEATURE_COLUMNS = [
    "volume_mw",
    "price_eur_mwh",
    "notional_value_eur",
    "price_deviation_pct",
    "abs_price_deviation_pct",
    "delivery_hours",
    "counterparty_trade_share",
    "trader_trade_share",
    "is_off_market",
]


# Fallback market levels (EUR/MWh) used when live market data is unavailable.
DEFAULT_MARKET_LEVEL = {"POWER": 85.0, "NATURAL_GAS": 42.5}


def _reference_price_per_row(
    df: pd.DataFrame,
    price: pd.Series,
    market_price: float | dict | None,
) -> pd.Series:
    """Resolve the reference market price for every row, per commodity.

    Priority for each commodity:
      1. an explicit value passed in via `market_price` (float or dict),
      2. the median traded price of that commodity in this batch,
      3. a hard-coded market level as a last resort.
    """
    # Normalise the caller's argument into a {commodity: price} mapping.
    explicit: dict = {}
    if isinstance(market_price, dict):
        explicit = {k: float(v) for k, v in market_price.items() if v}
    elif market_price is not None and np.isfinite(market_price) and market_price != 0:
        # A single number is interpreted as the POWER reference; gas is scaled
        # from it using the usual market relationship rather than reused blindly.
        explicit = {"POWER": float(market_price)}

    if "commodity" not in df.columns:
        fallback = float(price.median()) if len(price) else DEFAULT_MARKET_LEVEL["POWER"]
        ref = explicit.get("POWER", fallback)
        return pd.Series(ref if ref else 1.0, index=df.index, dtype=float)

    medians = price.groupby(df["commodity"]).median().to_dict()

    def resolve(commodity: str) -> float:
        value = (
            explicit.get(commodity)
            or medians.get(commodity)
            or DEFAULT_MARKET_LEVEL.get(commodity)
            or (float(price.median()) if len(price) else 1.0)
        )
        return float(value) if value else 1.0

    return df["commodity"].map(resolve).astype(float)


def _concentration_ratio(df: pd.DataFrame, column: str) -> pd.Series:
    """How many times its 'fair share' of the book each entity holds.

    fair share = 1 / number_of_distinct_entities
    ratio      = observed_share / fair_share

    A value near 1.0 is normal regardless of how many trades are in the batch,
    which is what makes this feature safe to use in production.
    """
    if column not in df.columns or df.empty:
        return pd.Series(0.0, index=df.index, dtype=float)

    counts = df[column].value_counts()
    n_entities = max(len(counts), 1)
    total = max(len(df), 1)
    observed_share = df[column].map(counts).astype(float) / total
    fair_share = 1.0 / n_entities
    return (observed_share / fair_share).fillna(0.0)


def build_features(df: pd.DataFrame, market_price: float | dict | None = None) -> pd.DataFrame:
    """Turn raw trades into the numeric feature matrix the model expects.

    Args:
        df: trades with at least volume_mw, price_eur_mwh, counterparty, trader_id.
        market_price: reference market price used to measure how far off-market a
            trade is. If None, the median traded price is used as a proxy.

    Returns:
        A DataFrame containing exactly FEATURE_COLUMNS, with no missing values.
    """
    out = pd.DataFrame(index=df.index)

    volume = pd.to_numeric(df.get("volume_mw"), errors="coerce").fillna(0.0)
    price = pd.to_numeric(df.get("price_eur_mwh"), errors="coerce").fillna(0.0)

    out["volume_mw"] = volume
    out["price_eur_mwh"] = price
    out["notional_value_eur"] = volume * price

    # --- The core REMIT signal -------------------------------------------
    # Trading far away from the prevailing market price is the classic
    # fingerprint of market manipulation or of a wash/related-party trade.
    #
    # Power and gas are SEPARATE markets at very different price levels
    # (~85 vs ~42.5 EUR/MWh). Comparing both to one number would make every gas
    # trade look 50% "off-market" and would hide a real gas anomaly. So the
    # reference price is resolved per commodity.
    reference = _reference_price_per_row(df, price, market_price)
    deviation = (price - reference) / reference.abs() * 100.0
    out["price_deviation_pct"] = deviation
    out["abs_price_deviation_pct"] = deviation.abs()
    # A trade more than 30% away from market is "off-market" by convention.
    out["is_off_market"] = (deviation.abs() > 30.0).astype(float)

    # --- Contract shape ---------------------------------------------------
    if "delivery_start" in df.columns and "delivery_end" in df.columns:
        start = pd.to_datetime(df["delivery_start"], errors="coerce", utc=True)
        end = pd.to_datetime(df["delivery_end"], errors="coerce", utc=True)
        hours = (end - start).dt.total_seconds() / 3600.0
        out["delivery_hours"] = hours.fillna(0.0)
    else:
        out["delivery_hours"] = 0.0

    # --- Concentration signals -------------------------------------------
    # A counterparty or trader suddenly responsible for a large share of the
    # book is a concentration risk and a surveillance flag.
    #
    # IMPORTANT: a raw share (count / batch_size) is batch-size dependent, so a
    # model trained on 200 trades misjudges a batch of 60. That is classic
    # training/serving skew. We therefore express concentration RELATIVE TO AN
    # EQUAL SPLIT: 1.0 means "this counterparty has its fair share", 3.0 means
    # "three times its fair share". That ratio is stable across batch sizes.
    out["counterparty_trade_share"] = _concentration_ratio(df, "counterparty")
    out["trader_trade_share"] = _concentration_ratio(df, "trader_id")

    # Guarantee the exact column set and order, and no NaN/inf reaching sklearn.
    out = out.reindex(columns=FEATURE_COLUMNS, fill_value=0.0)
    return out.replace([np.inf, -np.inf], 0.0).fillna(0.0)
