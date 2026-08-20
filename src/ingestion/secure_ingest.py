"""Secured ingestion: fetch -> scan -> validate -> land in Bronze.

This wraps the plain fetcher with Security Layer G. The order matters and is
deliberate:

    1. FETCH        get the raw payload from the external API
    2. SCAN         inspect the content for injection / malware  (security)
    3. VALIDATE     enforce the schema and business ranges       (quality)
    4. QUARANTINE   set aside anything rejected, keep evidence
    5. LAND         write only verified data to Bronze
    6. REPORT       write an audit record of what was checked

Never validate before scanning: a validator parses attacker-controlled content,
so it should not be the first thing to touch an untrusted payload.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_data_root  # noqa: E402
from ingestion.fetch_market_data import fetch_electricity_market_prices  # noqa: E402
from quality.gates import quarantine, validate_market_prices, write_quality_report  # noqa: E402
from security.scanner import scan_payload  # noqa: E402


class IngestionBlocked(RuntimeError):
    """Raised when a batch is too dangerous or too broken to accept."""


def secure_fetch_and_land() -> dict:
    """Run the full secured ingestion and return a summary of what happened."""
    data_root = get_data_root()
    bronze_dir = os.path.join(data_root, "1_bronze")
    reports_dir = os.path.join(data_root, "reports")
    quarantine_dir = os.path.join(data_root, "quarantine")
    os.makedirs(bronze_dir, exist_ok=True)

    # ---- 1. FETCH -------------------------------------------------------
    print("[1/6] Fetching market data...")
    payload = fetch_electricity_market_prices()

    # ---- 2. SCAN --------------------------------------------------------
    print("[2/6] Scanning payload for injection and malicious content...")
    scan = scan_payload(payload, path="market_payload")
    print(f"      {scan.summary()}")
    if not scan.clean:
        for threat in scan.threats:
            print(f"      THREAT: {threat}")
        # A payload carrying injection is never landed. This is a hard stop:
        # the upstream source must be treated as compromised.
        raise IngestionBlocked(
            f"Payload rejected: {len(scan.threats)} security threat(s) detected. "
            f"The upstream source may be compromised."
        )

    # ---- 3. VALIDATE ----------------------------------------------------
    print("[3/6] Validating schema and business ranges...")
    series = payload.get("series", [])
    if not series:
        raise IngestionBlocked("Payload contained no price series.")

    df = pd.DataFrame(series)
    gate = validate_market_prices(df)
    print(f"      {gate.summary()}")
    for finding in gate.findings:
        print(f"      FINDING: {finding}")

    # ---- 4. QUARANTINE --------------------------------------------------
    q_path = quarantine(gate.quarantined, quarantine_dir, "market_prices_rejected")
    if q_path:
        print(f"[4/6] Quarantined {len(gate.quarantined)} bad row(s) -> {q_path}")
    else:
        print("[4/6] Nothing to quarantine.")

    # ---- 6a. REPORT (written even if we are about to fail) --------------
    report_path = write_quality_report([gate], reports_dir, batch_source=payload.get("market", ""))

    if not gate.passed:
        raise IngestionBlocked(
            f"Data-quality gate failed; batch not landed. See {report_path}. "
            f"Findings: {'; '.join(gate.findings) or 'none recorded'}"
        )

    # ---- 5. LAND --------------------------------------------------------
    verified = dict(payload)
    verified["series"] = gate.clean.to_dict(orient="records")
    verified["security"] = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "injection_scan": "clean",
        "rows_accepted": len(gate.clean),
        "rows_quarantined": len(gate.quarantined),
        "quality_report": os.path.basename(report_path),
    }

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(bronze_dir, f"market_prices_{stamp}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(verified, f, indent=4, ensure_ascii=False)
    print(f"[5/6] Landed {len(gate.clean)} verified record(s) -> {out_path}")
    print(f"[6/6] Audit report -> {report_path}")

    return {
        "bronze_file": out_path,
        "rows_accepted": len(gate.clean),
        "rows_quarantined": len(gate.quarantined),
        "report": report_path,
        "findings": gate.findings,
    }


if __name__ == "__main__":
    secure_fetch_and_land()
