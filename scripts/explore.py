"""Look inside the platform: see what each layer actually contains.

The dashboard shows conclusions. This shows the WORK - the data as it moves
through Bronze -> Silver -> Gold, and what the ML model decided and why.

Usage (run inside the container so paths and libraries are right):

    docker compose exec airflow-worker python /opt/airflow/scripts/explore.py
    docker compose exec airflow-worker python /opt/airflow/scripts/explore.py ml
    docker compose exec airflow-worker python /opt/airflow/scripts/explore.py bronze
"""
from __future__ import annotations

import glob
import json
import os
import sys

import duckdb
import pandas as pd

DATA = os.getenv("DATA_ROOT", "/opt/airflow/data")
W = 78

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 30)


def title(text: str) -> None:
    print()
    print("=" * W)
    print(f" {text}")
    print("=" * W)


def sub(text: str) -> None:
    print(f"\n--- {text}")


def q(sql: str) -> pd.DataFrame:
    """Query the Gold layer with DuckDB, exactly like the AI agent does."""
    con = duckdb.connect(":memory:")
    for t in ("trades_pnl", "counterparty_risk", "portfolio_summary", "trade_surveillance"):
        p = os.path.join(DATA, "3_gold", t)
        if os.path.exists(p):
            g = os.path.join(p, "**", "*.parquet").replace("'", "''")
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('{g}')")
    return con.execute(sql).fetchdf()


def read_layer(layer: str, table: str = "") -> pd.DataFrame:
    path = os.path.join(DATA, layer, table) if table else os.path.join(DATA, layer)
    files = glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True)
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


# ==========================================================================
def show_bronze() -> None:
    title("BRONZE - raw data, exactly as the API returned it")
    files = sorted(glob.glob(os.path.join(DATA, "1_bronze", "market_prices_*.json")))
    if not files:
        print("  (empty - run the pipeline first)")
        return
    print(f"  {len(files)} file(s). Newest: {os.path.basename(files[-1])}")

    with open(files[-1], encoding="utf-8") as f:
        payload = json.load(f)

    sub("payload metadata")
    for k, v in payload.items():
        if k != "series":
            print(f"     {k}: {v}")

    series = payload.get("series", [])
    sub(f"first 5 of {len(series)} raw price records")
    df = pd.DataFrame(series)
    print(df.head().to_string(index=False))

    sub("what the security gate checked before letting this in")
    print(f"     price range in this batch : "
          f"{df['price_eur_mwh'].min():.2f} to {df['price_eur_mwh'].max():.2f} EUR/MWh")
    print(f"     allowed business range    : -500 to 4000 EUR/MWh")
    print(f"     -> accepted, because real German day-ahead prices sit in this band")
    print(f"     (the old code fetched GRID LOAD by mistake: ~40,000 -> rejected)")


def show_silver() -> None:
    title("SILVER - cleaned and standardized by Spark")
    for table in ("market_prices", "trades"):
        df = read_layer("2_silver", table)
        sub(f"2_silver/{table}   ({len(df)} rows)")
        if df.empty:
            print("     (empty)")
            continue
        print("     columns:", ", ".join(f"{c}:{t}" for c, t in df.dtypes.astype(str).items()))
        print()
        print(df.head(5).to_string(index=False, max_colwidth=22))

    sub("what Spark actually did here")
    print("     market_prices : epoch millis -> timestamp, cast price to double,")
    print("                     add commodity label, drop nulls, dedupe by timestamp")
    print("     trades        : cast numerics, parse timestamps, dedupe by trade_id")


def show_gold() -> None:
    title("GOLD - business answers")

    sub("portfolio_summary - the whole book in one table")
    df = read_layer("3_gold", "portfolio_summary")
    print(df.to_string(index=False) if not df.empty else "     (empty)")

    sub("counterparty_risk - who are we most exposed to?")
    df = read_layer("3_gold", "counterparty_risk")
    if not df.empty:
        print(df.head(8).to_string(index=False))

    sub("trades_pnl - how one trade is valued (the actual arithmetic)")
    df = read_layer("3_gold", "trades_pnl")
    if df.empty:
        print("     (empty)")
        return
    r = df.iloc[0]
    print(f"     trade                  : {r.get('trade_id')}  {r.get('buy_sell')}  {r.get('commodity')}")
    print(f"     volume                 : {r.get('volume_mw')} MW")
    print(f"     agreed price           : {r.get('price_eur_mwh')} EUR/MWh")
    print(f"     market price now       : {r.get('effective_market_price')} EUR/MWh")
    print()
    print(f"     notional = volume x agreed price      = {r.get('notional_value_eur')}")
    print(f"     MtM      = volume x market price      = {r.get('mtm_value_eur')}")
    side = str(r.get("buy_sell"))
    formula = ("volume x (market - agreed)" if side == "BUY" else "volume x (agreed - market)")
    print(f"     PnL      = {formula:<24} = {r.get('unrealized_pnl_eur')}")
    print()
    print(f"     A BUY gains when the market rises above the agreed price;")
    print(f"     a SELL gains when it falls below. That is mark-to-market.")


def show_ml() -> None:
    title("ML - market-abuse surveillance, and WHY each trade was flagged")
    df = read_layer("3_gold", "trade_surveillance")
    if df.empty:
        print("  (empty - run the pipeline first)")
        return

    flagged = df[df["is_anomaly"]] if "is_anomaly" in df else df.iloc[0:0]
    print(f"  scored {len(df)} trades, flagged {len(flagged)} "
          f"({len(flagged)/max(len(df),1):.1%})")

    sub("risk bands")
    if "risk_band" in df:
        print(df["risk_band"].value_counts().to_string())

    sub("the 8 most suspicious trades - note the REASON column")
    cols = [c for c in ["trade_id", "counterparty", "commodity", "buy_sell",
                        "volume_mw", "price_eur_mwh", "anomaly_score",
                        "risk_band", "top_reason"] if c in df.columns]
    print(df.nlargest(8, "anomaly_score")[cols].to_string(index=False, max_colwidth=48))

    sub("how the model decides")
    print("     Isolation Forest is UNSUPERVISED - nobody labelled these trades.")
    print("     It learns the shape of normal trading, then measures how easily")
    print("     each trade can be separated from the rest. Easy to isolate =")
    print("     unusual = high score.")
    print()
    print("     Features it looks at (src/ml/features.py):")
    for f in ("price deviation from market   <- the core REMIT signal",
              "off-market flag (>30% away)",
              "volume, notional value",
              "contract length in hours",
              "counterparty / trader concentration"):
        print(f"       - {f}")

    sub("normal vs flagged - the numbers behind the decision")
    if "is_anomaly" in df:
        for col in ("volume_mw", "price_eur_mwh", "anomaly_score"):
            if col in df:
                n = df[~df["is_anomaly"]][col]
                a = df[df["is_anomaly"]][col]
                if len(n) and len(a):
                    print(f"     {col:16s} normal median {n.median():10.2f}   "
                          f"flagged median {a.median():10.2f}")

    meta_path = os.path.join(os.getenv("MODEL_DIR", os.path.join(DATA, "models")),
                             "model_metadata.json")
    if os.path.exists(meta_path):
        with open(meta_path, encoding="utf-8") as f:
            meta = json.load(f)
        sub("the trained model itself")
        print(f"     trained at : {meta.get('trained_at')}")
        print(f"     params     : {meta.get('params')}")
        print(f"     metrics    : {meta.get('metrics')}")


def show_agent() -> None:
    title("AI AGENT - the questions it can answer, and the SQL behind them")
    questions = [
        ("Which counterparty are we most exposed to?",
         "SELECT counterparty, commodity, trade_count, total_notional_eur "
         "FROM counterparty_risk ORDER BY total_notional_eur DESC LIMIT 5"),
        ("Are we making or losing money?",
         "SELECT commodity, total_trades, portfolio_notional_eur, total_portfolio_pnl_eur "
         "FROM portfolio_summary"),
        ("Show me the riskiest trades and why.",
         "SELECT trade_id, counterparty, anomaly_score, risk_band, top_reason "
         "FROM trade_surveillance ORDER BY anomaly_score DESC LIMIT 5"),
    ]
    for question, sql in questions:
        sub(f'"{question}"')
        print(f"     SQL: {sql[:100]}...")
        try:
            print()
            print(q(sql).to_string(index=False, max_colwidth=46))
        except Exception as exc:  # noqa: BLE001
            print(f"     (not available: {exc})")


def show_security() -> None:
    title("SECURITY - the audit trail every run leaves behind")
    reports = sorted(glob.glob(os.path.join(DATA, "reports", "quality_report_*.json")))
    if reports:
        sub(f"newest quality report ({os.path.basename(reports[-1])})")
        with open(reports[-1], encoding="utf-8") as f:
            print(json.dumps(json.load(f), indent=2)[:1200])
    drift = sorted(glob.glob(os.path.join(DATA, "reports", "drift_report_*.json")))
    if drift:
        sub(f"newest drift report ({os.path.basename(drift[-1])})")
        with open(drift[-1], encoding="utf-8") as f:
            print(json.dumps(json.load(f), indent=2)[:900])
    quarantined = glob.glob(os.path.join(DATA, "quarantine", "*.parquet"))
    sub("quarantine (data the gates REFUSED to let in)")
    print(f"     {len(quarantined)} file(s)")
    for p in quarantined[-3:]:
        print(f"       {os.path.basename(p)}")


SECTIONS = {
    "bronze": show_bronze,
    "silver": show_silver,
    "gold": show_gold,
    "ml": show_ml,
    "agent": show_agent,
    "security": show_security,
}


def main() -> None:
    arg = sys.argv[1].lower() if len(sys.argv) > 1 else "all"
    if arg in SECTIONS:
        SECTIONS[arg]()
    elif arg == "all":
        for fn in SECTIONS.values():
            fn()
        print()
        print("=" * W)
        print(" Run one section only:  explore.py [bronze|silver|gold|ml|agent|security]")
        print("=" * W)
    else:
        print(__doc__)
        print("sections:", ", ".join(SECTIONS))


if __name__ == "__main__":
    main()
