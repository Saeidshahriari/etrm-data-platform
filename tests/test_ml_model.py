"""Tests for the trade-anomaly surveillance model.

An ML model needs behaviour tests, not only accuracy numbers. These tests pin
down the properties that must hold for the model to be trustworthy in a
compliance setting.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion.generate_trades import generate_synthetic_trades  # noqa: E402
from ml.features import FEATURE_COLUMNS, build_features  # noqa: E402


@pytest.fixture
def normal_trades() -> pd.DataFrame:
    return pd.DataFrame(generate_synthetic_trades(200, seed=42))


def test_features_are_stable_and_complete(normal_trades):
    X = build_features(normal_trades)
    assert list(X.columns) == FEATURE_COLUMNS, "feature order must be fixed"
    assert not X.isnull().any().any(), "features must never contain NaN"
    assert len(X) == len(normal_trades)


def test_features_are_batch_size_independent(normal_trades):
    """Regression test for training/serving skew.

    Concentration features once used count/batch_size, so the same trading
    behaviour produced different features in a 200-row batch and a 60-row batch.
    The model then misjudged small batches.
    """
    big = build_features(normal_trades)
    small = build_features(normal_trades.head(60))
    # A "fair share" is 1.0 by construction, whatever the batch size.
    assert abs(big["counterparty_trade_share"].mean() - 1.0) < 0.15
    assert abs(small["counterparty_trade_share"].mean() - 1.0) < 0.15


def test_commodities_are_priced_against_their_own_market(normal_trades):
    """Gas (~42.5) must not be judged against the power price (~85).

    Otherwise every gas trade looks 50% off-market and a real gas anomaly hides.
    """
    X = build_features(normal_trades, market_price={"POWER": 85.0, "NATURAL_GAS": 42.5})
    joined = normal_trades.assign(dev=X["abs_price_deviation_pct"].values)
    for commodity in ("POWER", "NATURAL_GAS"):
        subset = joined[joined["commodity"] == commodity]
        if not subset.empty:
            assert subset["dev"].median() < 20.0, \
                f"{commodity} trades should sit close to their own market price"


def test_off_market_trade_is_detected():
    """The core REMIT signal: a wildly off-market price must stand out."""
    base = datetime.now(timezone.utc)
    trades = pd.DataFrame(generate_synthetic_trades(100, seed=7))
    abusive = pd.DataFrame([{
        "trade_id": "TRD-99001", "trader_id": "TRADER-101", "counterparty": "ShadowCo",
        "commodity": "POWER", "buy_sell": "BUY", "volume_mw": 50.0,
        "price_eur_mwh": 950.0,
        "delivery_start": base, "delivery_end": base + timedelta(hours=4),
        "created_at": base,
    }])
    book = pd.concat([trades, abusive], ignore_index=True)
    X = build_features(book, market_price={"POWER": 85.0, "NATURAL_GAS": 42.5})

    assert X.iloc[-1]["is_off_market"] == 1.0
    assert X.iloc[-1]["abs_price_deviation_pct"] > X.iloc[:-1]["abs_price_deviation_pct"].max()


def test_model_detects_planted_abuse(tmp_path, monkeypatch):
    """End-to-end: train on normal trades, then catch three known abuse patterns."""
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("MODEL_DIR", str(tmp_path / "models"))

    from ml.score_trades import score_trades
    from ml.train_anomaly_model import train

    train(pd.DataFrame(generate_synthetic_trades(400, seed=1)))

    base = datetime.now(timezone.utc)
    abuse = pd.DataFrame([
        # off-market price (manipulation)
        {"trade_id": "TRD-99001", "trader_id": "TRADER-101", "counterparty": "ShadowCo",
         "commodity": "POWER", "buy_sell": "BUY", "volume_mw": 50.0, "price_eur_mwh": 950.0,
         "delivery_start": base, "delivery_end": base + timedelta(hours=4), "created_at": base},
        # enormous volume (cornering)
        {"trade_id": "TRD-99002", "trader_id": "TRADER-102", "counterparty": "RWE_Supply",
         "commodity": "POWER", "buy_sell": "SELL", "volume_mw": 4800.0, "price_eur_mwh": 88.0,
         "delivery_start": base, "delivery_end": base + timedelta(hours=1), "created_at": base},
        # near-zero price (possible wash trade)
        {"trade_id": "TRD-99003", "trader_id": "TRADER-103", "counterparty": "Statkraft",
         "commodity": "NATURAL_GAS", "buy_sell": "BUY", "volume_mw": 30.0, "price_eur_mwh": 0.5,
         "delivery_start": base, "delivery_end": base + timedelta(hours=12), "created_at": base},
    ])
    book = pd.concat([pd.DataFrame(generate_synthetic_trades(60, seed=7)), abuse], ignore_index=True)
    scored = score_trades(book, market_price={"POWER": 85.0, "NATURAL_GAS": 42.5})

    planted = scored[scored["trade_id"].astype(str).str.startswith("TRD-99")]
    assert int(planted["is_anomaly"].sum()) == 3, "all three abuse patterns must be flagged"

    # And they must rank near the top, not merely be flagged somewhere.
    top5 = set(scored.nlargest(5, "anomaly_score")["trade_id"].astype(str))
    assert len(top5 & set(planted["trade_id"].astype(str))) >= 2

    # False positives must stay within a workable range for analysts.
    normal_rows = scored[~scored["trade_id"].astype(str).str.startswith("TRD-99")]
    fp_rate = normal_rows["is_anomaly"].sum() / len(normal_rows)
    assert fp_rate < 0.20, f"false-positive rate too high for compliance use: {fp_rate:.1%}"


def test_every_flagged_trade_has_a_reason(tmp_path, monkeypatch):
    """A compliance alert without an explanation is not actionable."""
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("MODEL_DIR", str(tmp_path / "models"))

    from ml.score_trades import score_trades
    from ml.train_anomaly_model import train

    train(pd.DataFrame(generate_synthetic_trades(300, seed=3)))
    scored = score_trades(pd.DataFrame(generate_synthetic_trades(80, seed=9)))

    flagged = scored[scored["is_anomaly"]]
    assert (flagged["top_reason"].astype(str).str.len() > 0).all()
    assert (flagged["top_reason"] != "n/a").all()
