"""Score trades for suspicious behaviour and write the surveillance Gold table.

This is the inference (serving) half of the model. It runs inside the Airflow
DAG after the Gold layer is built, and produces
`data/3_gold/trade_surveillance`, which the dashboard and the AI agent read.

It also performs a lightweight DRIFT check: if today's trades look statistically
very different from the data the model was trained on, the model's judgement is
no longer trustworthy. In an MLSecOps context, sudden drift is also a possible
sign of an attack on the upstream data source.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_data_root  # noqa: E402
from ml.features import FEATURE_COLUMNS, build_features  # noqa: E402
from ml.train_anomaly_model import get_model_dir  # noqa: E402

# A feature whose mean has moved more than this many training-standard-deviations
# is considered drifted.
DRIFT_SIGMA_THRESHOLD = 2.0

# Minimum usable standard deviation, as a fraction of the feature's mean.
# Without this floor, a feature that is almost constant during training (std
# close to zero) turns any tiny change into a huge sigma value and hijacks both
# the drift alarm and the explanations. Flooring the std keeps near-constant
# features from producing false alarms.
MIN_STD_FRACTION = 0.05
MIN_STD_ABSOLUTE = 1e-6


# Sigma values are capped for reporting. A feature that was constant during
# training divides by an almost-zero std and would otherwise print numbers like
# "1000000 sigma", which is noise rather than information.
MAX_REPORTED_SIGMA = 99.0


def _effective_std(base: dict) -> float:
    """Return a numerically safe standard deviation for a baseline feature."""
    std = float(base.get("std") or 0.0)
    mean = abs(float(base.get("mean") or 0.0))
    return max(std, mean * MIN_STD_FRACTION, MIN_STD_ABSOLUTE)


def _was_constant(base: dict) -> bool:
    """True if the feature never varied during training."""
    return float(base.get("std") or 0.0) <= MIN_STD_ABSOLUTE


def _sigma_shift(value: float, base: dict) -> float:
    """Distance from the training mean, in standard deviations, capped."""
    shift = abs(value - float(base.get("mean") or 0.0)) / _effective_std(base)
    return min(shift, MAX_REPORTED_SIGMA)


def _describe(col: str, value: float, base: dict) -> str:
    """Explain a feature deviation in words a compliance analyst can act on."""
    if _was_constant(base):
        return (f"{col} = {value:g}, a value never seen in normal trading "
                f"(constant {float(base.get('mean') or 0.0):g} during training)")
    return f"{col} is {_sigma_shift(value, base):.1f} sigma from normal"

# Risk banding for the human analyst who reads the alerts.
RISK_BANDS = [(0.75, "CRITICAL"), (0.55, "HIGH"), (0.35, "MEDIUM")]


def load_model_and_metadata():
    model_dir = get_model_dir()
    model_path = os.path.join(model_dir, "trade_anomaly_model.joblib")
    meta_path = os.path.join(model_dir, "model_metadata.json")
    if not os.path.exists(model_path):
        raise FileNotFoundError(
            f"No trained model at '{model_path}'. Run train_anomaly_model.py first."
        )
    model = joblib.load(model_path)
    metadata = {}
    if os.path.exists(meta_path):
        with open(meta_path, encoding="utf-8") as f:
            metadata = json.load(f)
    return model, metadata


def check_drift(X: pd.DataFrame, metadata: dict) -> list[dict]:
    """Compare live feature statistics against the training baseline."""
    baseline = metadata.get("training_baseline", {})
    drifted = []
    for col in FEATURE_COLUMNS:
        base = baseline.get(col)
        if not base:
            continue
        current = float(X[col].mean())
        shift = _sigma_shift(current, base)
        if shift > DRIFT_SIGMA_THRESHOLD:
            drifted.append({
                "feature": col,
                "baseline_mean": round(float(base.get("mean") or 0.0), 4),
                "current_mean": round(current, 4),
                "shift_sigma": round(shift, 2),
                "was_constant_in_training": _was_constant(base),
            })
    return drifted


def _risk_band(score: float) -> str:
    for threshold, label in RISK_BANDS:
        if score >= threshold:
            return label
    return "LOW"


def score_trades(df: pd.DataFrame | None = None, market_price: float | None = None) -> pd.DataFrame:
    """Attach an anomaly score and a risk band to every trade."""
    data_root = get_data_root()

    if df is None:
        gold_pnl = os.path.join(data_root, "3_gold", "trades_pnl")
        silver = os.path.join(data_root, "2_silver", "trades")
        source = gold_pnl if os.path.exists(gold_pnl) else silver
        if not os.path.exists(source):
            raise FileNotFoundError("No trades found to score. Run the pipeline first.")
        df = pd.read_parquet(source)
        # The Gold table already carries the market price used for valuation.
        if market_price is None and "effective_market_price" in df.columns:
            market_price = float(pd.to_numeric(df["effective_market_price"],
                                               errors="coerce").median())

    model, metadata = load_model_and_metadata()
    X = build_features(df, market_price=market_price)

    # score_samples: higher = more normal. Invert and normalise to a 0..1 risk
    # score where 1 = most suspicious, which is far easier for an analyst.
    raw = model.score_samples(X)
    lo, hi = float(raw.min()), float(raw.max())
    risk = (hi - raw) / (hi - lo) if hi > lo else np.zeros_like(raw)

    result = df.copy()
    result["anomaly_score"] = np.round(risk, 4)
    result["is_anomaly"] = (model.predict(X) == -1)
    result["risk_band"] = [_risk_band(s) for s in risk]
    result["scored_at"] = datetime.now(timezone.utc)
    result["model_version"] = metadata.get("trained_at", "unknown")

    # Give the analyst a reason, not just a number. The single most extreme
    # feature is a cheap, honest explanation of why the trade was flagged.
    baseline = metadata.get("training_baseline", {})
    reasons = []
    for i in range(len(X)):
        worst_col, worst_shift, worst_value = "", 0.0, 0.0
        for col in FEATURE_COLUMNS:
            base = baseline.get(col)
            if not base:
                continue
            value = float(X[col].iloc[i])
            shift = _sigma_shift(value, base)
            if shift > worst_shift:
                worst_col, worst_shift, worst_value = col, shift, value
        reasons.append(
            _describe(worst_col, worst_value, baseline[worst_col]) if worst_col else "n/a"
        )
    result["top_reason"] = reasons

    drift = check_drift(X, metadata)
    if drift:
        print(f"[ALERT] Data drift detected on {len(drift)} feature(s):")
        for d in drift:
            print(f"        - {d['feature']}: {d['shift_sigma']} sigma from training baseline")
        print("        Model output may be unreliable; investigate the source and retrain.")
    else:
        print("[OK] No significant drift against the training baseline.")

    # Persist the surveillance table for the dashboard and the agent.
    out_path = os.path.join(data_root, "3_gold", "trade_surveillance")
    os.makedirs(out_path, exist_ok=True)
    result.to_parquet(os.path.join(out_path, "surveillance.parquet"), index=False)

    # Persist a drift report for the audit trail.
    reports_dir = os.path.join(data_root, "reports")
    os.makedirs(reports_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    with open(os.path.join(reports_dir, f"drift_report_{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_scored": len(result),
            "n_flagged": int(result["is_anomaly"].sum()),
            "drifted_features": drift,
        }, f, indent=2)

    flagged = int(result["is_anomaly"].sum())
    print(f"[OK] Scored {len(result)} trades; {flagged} flagged for review "
          f"({flagged / max(len(result), 1):.1%}).")
    return result


if __name__ == "__main__":
    scored = score_trades()
    cols = [c for c in ["trade_id", "counterparty", "commodity", "buy_sell",
                        "volume_mw", "price_eur_mwh", "anomaly_score",
                        "risk_band", "top_reason"] if c in scored.columns]
    print("\nTop 10 most suspicious trades:")
    print(scored.nlargest(10, "anomaly_score")[cols].to_string(index=False))
