"""Train the trade-anomaly detection model (REMIT market-abuse surveillance).

Why Isolation Forest?
  - It is UNSUPERVISED: real market abuse is rare and almost never labelled, so
    a supervised classifier has nothing to learn from. Isolation Forest instead
    learns what "normal" looks like and measures how easily a trade is isolated
    from the rest.
  - It is fast, explainable enough for a compliance conversation, and it does
    not assume the data is normally distributed (energy prices are not).

MLOps practice applied here:
  - Every run is tracked in MLflow (parameters, metrics, the model artefact),
    so a result can always be reproduced and audited.
  - The model and its metadata are versioned on disk as a fallback when MLflow
    is not running, so the pipeline never hard-depends on it.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_data_root  # noqa: E402
from ml.features import FEATURE_COLUMNS, build_features  # noqa: E402

# Expected share of anomalies. 2% is a deliberate compliance choice: high enough
# to surface real abuse, low enough that analysts are not flooded with alerts.
CONTAMINATION = 0.02
N_ESTIMATORS = 200
RANDOM_STATE = 42


def get_model_dir() -> str:
    return os.getenv("MODEL_DIR", os.path.join(get_data_root(), "..", "models"))


def _log_to_mlflow(model, params: dict, metrics: dict) -> str | None:
    """Track the run in MLflow. Returns the run id, or None if unavailable."""
    try:
        import mlflow
        import mlflow.sklearn
    except ImportError:
        print("[WARN] mlflow not installed - skipping experiment tracking.")
        return None

    try:
        tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment("etrm-trade-anomaly")
        with mlflow.start_run() as run:
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.sklearn.log_model(model, name="model")
            mlflow.set_tag("use_case", "REMIT market abuse surveillance")
            print(f"[OK] Logged run {run.info.run_id} to MLflow at {tracking_uri}")
            return run.info.run_id
    except Exception as exc:  # noqa: BLE001 - tracking must never break training
        print(f"[WARN] MLflow tracking unavailable ({exc}). Continuing without it.")
        return None


def load_training_trades() -> pd.DataFrame:
    """Load the Silver trades that the model learns 'normal' from."""
    path = os.path.join(get_data_root(), "2_silver", "trades")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"No Silver trades at '{path}'. Run the bronze_to_silver stage first."
        )
    return pd.read_parquet(path)


def train(df: pd.DataFrame | None = None) -> dict:
    """Fit the anomaly model and persist it with its metadata."""
    if df is None:
        df = load_training_trades()

    if len(df) < 20:
        raise ValueError(f"Only {len(df)} trades available; need at least 20 to train.")

    print(f"[INFO] Training on {len(df)} trades...")
    X = build_features(df)

    model = IsolationForest(
        n_estimators=N_ESTIMATORS,
        contamination=CONTAMINATION,
        random_state=RANDOM_STATE,
        n_jobs=-1,
    )
    model.fit(X)

    # Evaluate on the training set. For an unsupervised detector there is no
    # accuracy to report, so we report the alert rate and the score spread,
    # which is what a compliance team actually monitors.
    raw_scores = model.score_samples(X)
    predictions = model.predict(X)
    n_flagged = int((predictions == -1).sum())

    metrics = {
        "n_training_rows": len(df),
        "n_flagged": n_flagged,
        "alert_rate": round(n_flagged / len(df), 4),
        "score_mean": float(np.mean(raw_scores)),
        "score_std": float(np.std(raw_scores)),
        "score_min": float(np.min(raw_scores)),
    }
    params = {
        "algorithm": "IsolationForest",
        "n_estimators": N_ESTIMATORS,
        "contamination": CONTAMINATION,
        "random_state": RANDOM_STATE,
        "n_features": len(FEATURE_COLUMNS),
    }

    # Persist model + metadata locally (the source of truth for the pipeline).
    model_dir = get_model_dir()
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, "trade_anomaly_model.joblib")
    joblib.dump(model, model_path)

    run_id = _log_to_mlflow(model, params, metrics)

    metadata = {
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "model_path": model_path,
        "features": FEATURE_COLUMNS,
        "params": params,
        "metrics": metrics,
        "mlflow_run_id": run_id,
        # Baseline statistics are stored so drift can be measured later.
        "training_baseline": {
            col: {
                "mean": float(X[col].mean()),
                "std": float(X[col].std()),
                "median": float(X[col].median()),
            }
            for col in FEATURE_COLUMNS
        },
    }
    with open(os.path.join(model_dir, "model_metadata.json"), "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    print(f"[OK] Model saved to {model_path}")
    print(f"[OK] Alert rate {metrics['alert_rate']:.2%} ({n_flagged} of {len(df)} trades flagged)")
    return metadata


if __name__ == "__main__":
    train()
