"""Data-quality and data-poisoning gates (Security Layer G).

Every gate follows the same contract:

    result = validate_market_prices(df)
    result.passed        -> bool
    result.clean         -> DataFrame safe to send downstream
    result.quarantined   -> DataFrame of rejected rows (kept for investigation)
    result.findings      -> list of human-readable problems

Design decision: the pipeline QUARANTINES bad rows instead of crashing. A crash
on one poisoned row would create a denial-of-service: an attacker could stop the
whole platform by injecting a single bad value. Quarantine keeps the platform
alive, keeps the evidence, and raises an alert.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pandera.errors

from .schemas import (
    MARKET_PRICE_SCHEMA,
    PRICE_PLAUSIBLE_MAX,
    PRICE_PLAUSIBLE_MIN,
    TRADE_SCHEMA,
)

# A batch is rejected outright if more than this share of rows fail. A high
# failure rate means the source itself is compromised or the format changed.
MAX_FAILURE_RATIO = 0.20

# Z-score above which a value is a statistical outlier (possible poisoning)
# even though it passes the hard schema limits.
ZSCORE_THRESHOLD = 5.0


@dataclass
class GateResult:
    """Outcome of one validation gate."""

    name: str
    passed: bool
    clean: pd.DataFrame
    quarantined: pd.DataFrame
    findings: list[str] = field(default_factory=list)
    stats: dict = field(default_factory=dict)

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"[{status}] gate={self.name} clean={len(self.clean)} "
            f"quarantined={len(self.quarantined)} findings={len(self.findings)}"
        )


def _failed_indices(exc: pandera.errors.SchemaErrors) -> set:
    """Extract the row indices that failed from a Pandera SchemaErrors object.

    Pandera's failure-case frame has changed shape across versions, so this
    reads defensively and falls back to 'no specific rows' when unsure.
    """
    try:
        cases = exc.failure_cases
        if "index" in cases.columns:
            idx = cases["index"].dropna().unique().tolist()
            return {int(i) for i in idx}
    except Exception:  # noqa: BLE001 - defensive across pandera versions
        pass
    return set()


def _split_on_schema(df: pd.DataFrame, schema, gate_name: str) -> GateResult:
    """Validate df against a Pandera schema, splitting clean vs quarantined rows."""
    findings: list[str] = []
    try:
        clean = schema.validate(df, lazy=True)
        return GateResult(gate_name, True, clean, df.iloc[0:0].copy(), findings)
    except pandera.errors.SchemaErrors as exc:
        bad_idx = _failed_indices(exc)

        # Record each distinct failed check once, with a small sample.
        try:
            for check, group in exc.failure_cases.groupby("check"):
                sample = group["failure_case"].astype(str).head(3).tolist()
                findings.append(
                    f"check '{check}' failed {len(group)}x (examples: {', '.join(sample)})"
                )
        except Exception:  # noqa: BLE001
            findings.append(f"schema validation failed: {exc}")

        if bad_idx:
            mask = df.index.isin(list(bad_idx))
            quarantined = df[mask].copy()
            clean = df[~mask].copy()
        else:
            # Could not localise the rows -> fail the whole batch, safest option.
            quarantined = df.copy()
            clean = df.iloc[0:0].copy()
            findings.append("could not isolate failing rows; whole batch quarantined")

        ratio = len(quarantined) / max(len(df), 1)
        passed = ratio <= MAX_FAILURE_RATIO and len(clean) > 0
        if not passed:
            findings.append(
                f"failure ratio {ratio:.1%} exceeds the {MAX_FAILURE_RATIO:.0%} "
                f"limit -> batch rejected (possible compromised source)"
            )
        return GateResult(
            gate_name, passed, clean, quarantined, findings,
            stats={"failure_ratio": round(ratio, 4)},
        )


def _flag_statistical_outliers(df: pd.DataFrame, column: str, findings: list[str]) -> dict:
    """Robust outlier detection using median and MAD (median absolute deviation).

    Mean and standard deviation are themselves distorted by poisoned values, so
    a robust estimator is used instead. This detects a poisoning attempt that
    stays inside the legal range but is statistically impossible.
    """
    if column not in df.columns or len(df) < 10:
        return {}

    values = pd.to_numeric(df[column], errors="coerce").dropna()
    if values.empty:
        return {}

    median = float(values.median())
    mad = float((values - median).abs().median())
    # 1.4826 scales MAD to be comparable with a standard deviation.
    scale = mad * 1.4826 if mad > 0 else float(values.std() or 0.0)
    if scale <= 0:
        return {"median": median, "outliers": 0}

    zscores = ((values - median).abs() / scale)
    outliers = int((zscores > ZSCORE_THRESHOLD).sum())
    if outliers:
        worst = float(values[zscores.idxmax()]) if not zscores.empty else float("nan")
        findings.append(
            f"{outliers} statistical outlier(s) in '{column}' "
            f"(robust z > {ZSCORE_THRESHOLD}; worst value {worst:,.2f}, median {median:,.2f}) "
            f"- possible data poisoning, review before trusting"
        )
    return {"median": median, "mad_scale": round(scale, 4), "outliers": outliers}


def validate_market_prices(df: pd.DataFrame) -> GateResult:
    """Gate for raw market price data arriving from the external API."""
    result = _split_on_schema(df, MARKET_PRICE_SCHEMA, "market_prices")

    if not result.clean.empty:
        result.stats.update(_flag_statistical_outliers(result.clean, "price_eur_mwh", result.findings))

        # Plausibility band: legal but unusual values are flagged, not dropped.
        prices = pd.to_numeric(result.clean["price_eur_mwh"], errors="coerce")
        implausible = int(((prices < PRICE_PLAUSIBLE_MIN) | (prices > PRICE_PLAUSIBLE_MAX)).sum())
        if implausible:
            result.findings.append(
                f"{implausible} price(s) outside the plausible band "
                f"[{PRICE_PLAUSIBLE_MIN}, {PRICE_PLAUSIBLE_MAX}] EUR/MWh "
                f"- check the source unit (a x1000 unit error looks exactly like this)"
            )
    return result


def validate_trades(df: pd.DataFrame) -> GateResult:
    """Gate for trades extracted from the OLTP database."""
    result = _split_on_schema(df, TRADE_SCHEMA, "trades")

    if not result.clean.empty:
        result.stats.update(_flag_statistical_outliers(result.clean, "volume_mw", result.findings))
        result.stats.update(
            {"price_" + k: v for k, v in
             _flag_statistical_outliers(result.clean, "price_eur_mwh", result.findings).items()}
        )

        # Duplicate trade ids after the schema pass would indicate replay/injection.
        dupes = int(result.clean["trade_id"].duplicated().sum()) if "trade_id" in result.clean else 0
        if dupes:
            result.findings.append(f"{dupes} duplicate trade_id(s) - possible replay injection")
    return result


def write_quality_report(results: list[GateResult], report_dir: str, batch_source: str = "") -> str:
    """Persist a machine-readable quality/security report for audit purposes.

    Regulators (and your future self) need evidence of what was checked, when,
    and what was rejected. This report is that evidence.
    """
    os.makedirs(report_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(report_dir, f"quality_report_{stamp}.json")

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "batch_source": batch_source,
        "overall_passed": all(r.passed for r in results),
        "gates": [
            {
                "name": r.name,
                "passed": r.passed,
                "rows_clean": len(r.clean),
                "rows_quarantined": len(r.quarantined),
                "findings": r.findings,
                "stats": r.stats,
            }
            for r in results
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
    return path


def quarantine(df: pd.DataFrame, quarantine_dir: str, label: str) -> str | None:
    """Save rejected rows so they can be investigated instead of lost."""
    if df.empty:
        return None
    os.makedirs(quarantine_dir, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = os.path.join(quarantine_dir, f"{label}_{stamp}.parquet")
    df.to_parquet(path, index=False)
    return path


def file_checksum(path: str) -> str:
    """SHA-256 of a file: data provenance, so a batch can be proved unaltered."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()
