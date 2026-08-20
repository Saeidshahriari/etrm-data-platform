"""Live ETRM risk & surveillance dashboard.

Reads the Gold layer directly with DuckDB (no database server needed: DuckDB
queries the Parquet files in place) and refreshes on a timer, giving a
near-real-time view.

Run locally:
    streamlit run dashboard/app.py
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DATA_ROOT = os.getenv("DATA_ROOT", "data")
GOLD = os.path.join(DATA_ROOT, "3_gold")
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "30"))

# Colour system: one meaning per colour, used consistently everywhere.
RISK_COLORS = {
    "CRITICAL": "#e5484d",
    "HIGH": "#f76808",
    "MEDIUM": "#ffb224",
    "LOW": "#30a46c",
}
POSITIVE, NEGATIVE, NEUTRAL = "#30a46c", "#e5484d", "#5b9dff"

st.set_page_config(page_title="ETRM Risk & Surveillance", page_icon="⚡", layout="wide")


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """One DuckDB connection reused across reruns."""
    return duckdb.connect(database=":memory:")


# Only these datasets are ever read, so a table name can never become an
# injection point.
ALLOWED_TABLES = (
    "trades_pnl",
    "counterparty_risk",
    "portfolio_summary",
    "trade_surveillance",
)


def read_gold(table: str) -> pd.DataFrame:
    """Read a Gold dataset. Returns an empty frame when it does not exist yet."""
    if table not in ALLOWED_TABLES:
        st.error(f"Refusing to read unknown dataset: {table}")
        return pd.DataFrame()

    path = os.path.join(GOLD, table)
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        con = get_connection()
        # The path is passed as a bound parameter, never string-formatted in.
        glob = os.path.join(path, "**", "*.parquet")
        return con.execute("SELECT * FROM read_parquet(?)", [glob]).fetchdf()
    except Exception as exc:  # noqa: BLE001 - a missing table must not crash the UI
        st.warning(f"Could not read '{table}': {exc}")
        return pd.DataFrame()


def money(value: float) -> str:
    """Format euros compactly: 1.2M instead of 1234567."""
    if value is None or pd.isna(value):
        return "n/a"
    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(value) >= limit:
            return f"€{value / limit:,.2f}{suffix}"
    return f"€{value:,.0f}"


# --------------------------------------------------------------------------
# Header
# --------------------------------------------------------------------------
left, right = st.columns([3, 1])
with left:
    st.title("⚡ ETRM Risk & Surveillance")
    st.caption("Live view of the Gold layer · portfolio valuation, counterparty risk and REMIT market-abuse alerts")
with right:
    st.metric("Last refreshed", datetime.now(timezone.utc).strftime("%H:%M:%S UTC"))
    if st.button("Refresh now", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

portfolio = read_gold("portfolio_summary")
counterparty = read_gold("counterparty_risk")
trades = read_gold("trades_pnl")
surveillance = read_gold("trade_surveillance")

if portfolio.empty and trades.empty:
    st.info(
        "No Gold data found yet. Run the pipeline first "
        "(`make pipeline`, or trigger the DAG in Airflow), then refresh."
    )
    st.stop()

# --------------------------------------------------------------------------
# KPI row
# --------------------------------------------------------------------------
st.subheader("Portfolio at a glance")
k1, k2, k3, k4 = st.columns(4)

total_trades = int(portfolio["total_trades"].sum()) if "total_trades" in portfolio else len(trades)
notional = float(portfolio["portfolio_notional_eur"].sum()) if "portfolio_notional_eur" in portfolio else 0.0
pnl = float(portfolio["total_portfolio_pnl_eur"].sum()) if "total_portfolio_pnl_eur" in portfolio else 0.0
alerts = int(surveillance["is_anomaly"].sum()) if "is_anomaly" in surveillance else 0

k1.metric("Trades", f"{total_trades:,}")
k2.metric("Notional exposure", money(notional))
k3.metric("Unrealized PnL", money(pnl), delta=f"{'profit' if pnl >= 0 else 'loss'}")
k4.metric("Surveillance alerts", f"{alerts}", delta="review required" if alerts else "clear",
          delta_color="inverse" if alerts else "normal")

st.divider()

# --------------------------------------------------------------------------
# Risk views
# --------------------------------------------------------------------------
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Counterparty exposure")
    if counterparty.empty:
        st.caption("No counterparty data yet.")
    else:
        top = counterparty.nlargest(10, "total_notional_eur")
        fig = px.bar(
            top,
            x="total_notional_eur",
            y="counterparty",
            color="commodity",
            orientation="h",
            labels={"total_notional_eur": "Notional (EUR)", "counterparty": ""},
            color_discrete_sequence=[NEUTRAL, "#a78bfa"],
        )
        fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=0),
                          yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Concentration matters: a large exposure to one counterparty is itself a risk.")

with col_b:
    st.subheader("Unrealized PnL by commodity")
    if portfolio.empty:
        st.caption("No portfolio data yet.")
    else:
        fig = px.bar(
            portfolio,
            x="commodity",
            y="total_portfolio_pnl_eur",
            labels={"total_portfolio_pnl_eur": "Unrealized PnL (EUR)", "commodity": ""},
            color="total_portfolio_pnl_eur",
            color_continuous_scale=[[0, NEGATIVE], [0.5, "#c8ccd4"], [1, POSITIVE]],
        )
        fig.update_layout(height=380, margin=dict(l=0, r=0, t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Mark-to-market against the latest market curve.")

st.divider()

# --------------------------------------------------------------------------
# Surveillance (the ML model output)
# --------------------------------------------------------------------------
st.subheader("🔍 Market-abuse surveillance (REMIT)")

if surveillance.empty:
    st.info("No surveillance data yet. Train and run the model: `make ml`.")
else:
    s1, s2 = st.columns([1, 2])

    with s1:
        if "risk_band" in surveillance:
            counts = (surveillance["risk_band"].value_counts()
                      .reindex(["CRITICAL", "HIGH", "MEDIUM", "LOW"]).fillna(0).reset_index())
            counts.columns = ["risk_band", "count"]
            fig = px.pie(
                counts, names="risk_band", values="count", hole=0.55,
                color="risk_band", color_discrete_map=RISK_COLORS,
            )
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0),
                              showlegend=True, legend=dict(orientation="h", y=-0.1))
            st.plotly_chart(fig, use_container_width=True)

    with s2:
        if {"anomaly_score", "volume_mw", "price_eur_mwh"} <= set(surveillance.columns):
            fig = px.scatter(
                surveillance, x="volume_mw", y="price_eur_mwh",
                color="risk_band", size="anomaly_score",
                color_discrete_map=RISK_COLORS,
                hover_data=[c for c in ["trade_id", "counterparty", "top_reason"]
                            if c in surveillance.columns],
                labels={"volume_mw": "Volume (MW)", "price_eur_mwh": "Price (EUR/MWh)"},
            )
            fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)
            st.caption("Trades far from the main cluster are the ones the model isolates.")

    flagged = surveillance[surveillance.get("is_anomaly", False) == True]  # noqa: E712
    if not flagged.empty:
        st.markdown(f"**{len(flagged)} trade(s) flagged for compliance review**")
        cols = [c for c in ["trade_id", "counterparty", "commodity", "buy_sell", "volume_mw",
                            "price_eur_mwh", "anomaly_score", "risk_band", "top_reason"]
                if c in flagged.columns]
        st.dataframe(
            flagged.nlargest(20, "anomaly_score")[cols],
            use_container_width=True, hide_index=True,
        )
    else:
        st.success("No trades flagged. The book looks normal against the learned baseline.")

# --------------------------------------------------------------------------
# Data quality & security reports
# --------------------------------------------------------------------------
with st.expander("Data quality & security reports"):
    reports_dir = os.path.join(DATA_ROOT, "reports")
    if os.path.exists(reports_dir):
        files = sorted(os.listdir(reports_dir), reverse=True)[:10]
        if files:
            for name in files:
                st.text(name)
        else:
            st.caption("No reports generated yet.")
    else:
        st.caption("No reports directory yet. It is created on the first pipeline run.")

st.caption(f"Auto-refresh every {REFRESH_SECONDS}s · data source: {GOLD}")

# Near-real-time refresh without extra dependencies.
st.markdown(
    f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>",
    unsafe_allow_html=True,
)
