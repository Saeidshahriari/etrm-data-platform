"""ETRM AI agent - an MCP server exposing the Gold layer to an LLM.

MCP (Model Context Protocol) is the open standard that lets an AI assistant call
tools. This server exposes the lakehouse as a small set of safe, well-described
tools, so you can ask questions in plain language:

    "What is my biggest counterparty exposure?"
    "Show me the suspicious trades from today and explain why they were flagged."

SECURITY DESIGN (this is an MLSecOps component, not just a convenience):
  - The database is opened READ-ONLY and only over Parquet files.
  - `query_data` accepts SELECT statements only. Every other statement type is
    rejected, and multiple statements are rejected, so the agent cannot write,
    delete or drop anything even if the LLM is manipulated into trying.
  - Results are row-limited, so one query cannot exhaust memory.
  - The tools return data, never credentials.

Run:
    python agent/etrm_mcp_server.py
"""
from __future__ import annotations

import json
import os
import re
from typing import Any

import duckdb
from mcp.server.fastmcp import FastMCP

DATA_ROOT = os.getenv("DATA_ROOT", "data")
GOLD = os.path.join(DATA_ROOT, "3_gold")
MAX_ROWS = 500

# The complete set of datasets this server will ever expose. Anything not on
# this list cannot be registered or queried, so a table name can never become an
# injection point even if the code around it changes later.
ALLOWED_TABLES = (
    "trades_pnl",
    "counterparty_risk",
    "portfolio_summary",
    "trade_surveillance",
)

_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,63}$")


def _safe_identifier(name: str) -> str:
    """Allow only a known, syntactically safe table identifier."""
    if name not in ALLOWED_TABLES or not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Refusing to use table identifier: {name!r}")
    return name


def _sql_literal(value: str) -> str:
    """Quote a value as a SQL string literal, escaping embedded quotes.

    Needed because DuckDB cannot bind a prepared parameter inside CREATE VIEW
    ("Binder Error: Unexpected prepared parameter"). Doubling single quotes is
    the standard SQL escape and closes the injection path for a string literal.
    """
    return "'" + str(value).replace("'", "''") + "'"

mcp = FastMCP("etrm-lakehouse")

# --------------------------------------------------------------------------
# Query guard
# --------------------------------------------------------------------------
_FORBIDDEN = re.compile(
    r"(?i)\b(insert|update|delete|drop|create|alter|truncate|attach|detach|copy|"
    r"export|install|load|pragma|set|call)\b"
)


def _assert_read_only(sql: str) -> None:
    """Reject anything that is not a single, plain SELECT statement."""
    cleaned = re.sub(r"--[^\n]*", " ", sql)          # strip line comments
    cleaned = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.S)  # strip block comments
    cleaned = cleaned.strip().rstrip(";")

    if ";" in cleaned:
        raise ValueError("Only one statement is allowed per query.")
    if not re.match(r"(?is)^\s*(select|with)\b", cleaned):
        raise ValueError("Only SELECT (or WITH ... SELECT) queries are allowed.")
    match = _FORBIDDEN.search(cleaned)
    if match:
        raise ValueError(f"Statement type '{match.group(0)}' is not permitted.")


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    # Register each Gold dataset as a view over its Parquet files. The table
    # name is validated against the whitelist, and the file path is passed as a
    # BOUND PARAMETER rather than interpolated into the SQL string.
    for table in ALLOWED_TABLES:
        path = os.path.join(GOLD, table)
        if not os.path.exists(path):
            continue
        safe = _safe_identifier(table)
        glob = _sql_literal(os.path.join(path, "**", "*.parquet"))
        con.execute(
            f"CREATE OR REPLACE VIEW {safe} AS SELECT * FROM read_parquet({glob})"  # nosec B608
        )
    return con


def _to_records(df) -> list[dict[str, Any]]:
    return json.loads(df.head(MAX_ROWS).to_json(orient="records", date_format="iso"))


# --------------------------------------------------------------------------
# Tools
# --------------------------------------------------------------------------
@mcp.tool()
def list_tables() -> str:
    """List the available Gold tables and their columns.

    Call this first to discover what data exists before writing a query.
    """
    con = _connect()
    tables = con.execute("SHOW TABLES").fetchdf()
    if tables.empty:
        return "No Gold tables found. Run the pipeline first."

    out = []
    for name in tables["name"]:
        # Every identifier is re-validated against the whitelist before use.
        safe = _safe_identifier(str(name))
        schema = con.execute(f"DESCRIBE {safe}").fetchdf()  # nosec B608
        cols = ", ".join(f"{r.column_name} ({r.column_type})" for r in schema.itertuples())
        rows = con.execute(f"SELECT COUNT(*) AS n FROM {safe}").fetchone()[0]  # nosec B608
        out.append(f"### {safe}  —  {rows} rows\n{cols}")
    return "\n\n".join(out)


@mcp.tool()
def query_data(sql: str) -> str:
    """Run a READ-ONLY SQL query against the Gold lakehouse and return JSON rows.

    Only SELECT / WITH statements are permitted. Available tables:
    trades_pnl, counterparty_risk, portfolio_summary, trade_surveillance.

    Args:
        sql: a single SELECT statement, e.g.
             "SELECT counterparty, SUM(total_notional_eur) AS exposure
              FROM counterparty_risk GROUP BY counterparty ORDER BY exposure DESC"
    """
    try:
        _assert_read_only(sql)
    except ValueError as exc:
        return f"Query rejected for safety: {exc}"

    try:
        con = _connect()
        df = con.execute(sql).fetchdf()
    except Exception as exc:  # noqa: BLE001 - report the error to the model
        return f"Query failed: {exc}"

    if df.empty:
        return "Query returned no rows."
    note = f"\n\n(showing first {MAX_ROWS} of {len(df)} rows)" if len(df) > MAX_ROWS else ""
    return json.dumps(_to_records(df), indent=2) + note


@mcp.tool()
def portfolio_summary() -> str:
    """Get the current portfolio position: trades, notional and unrealized PnL per commodity."""
    con = _connect()
    try:
        df = con.execute("SELECT * FROM portfolio_summary").fetchdf()
    except Exception:  # noqa: BLE001
        return "No portfolio_summary table. Run the pipeline first."
    return json.dumps(_to_records(df), indent=2)


@mcp.tool()
def top_counterparty_exposure(limit: int = 10) -> str:
    """Get the largest counterparty exposures, highest notional first.

    Args:
        limit: how many counterparties to return (default 10).
    """
    con = _connect()
    try:
        df = con.execute(
            "SELECT counterparty, commodity, trade_count, total_notional_eur, "
            "total_mtm_eur, net_unrealized_pnl_eur "
            "FROM counterparty_risk ORDER BY total_notional_eur DESC LIMIT ?",
            [min(limit, 100)],
        ).fetchdf()
    except Exception:  # noqa: BLE001
        return "No counterparty_risk table. Run the pipeline first."
    return json.dumps(_to_records(df), indent=2)


@mcp.tool()
def suspicious_trades(min_score: float = 0.5, limit: int = 20) -> str:
    """Get trades flagged by the market-abuse surveillance model, with the reason.

    Use this to answer questions about compliance risk, unusual trading, or
    possible REMIT market abuse.

    Args:
        min_score: minimum anomaly score from 0 (normal) to 1 (most suspicious).
        limit: maximum number of trades to return.
    """
    con = _connect()
    try:
        df = con.execute(
            "SELECT trade_id, counterparty, commodity, buy_sell, volume_mw, "
            "price_eur_mwh, anomaly_score, risk_band, top_reason "
            "FROM trade_surveillance WHERE anomaly_score >= ? "
            "ORDER BY anomaly_score DESC LIMIT ?",
            [min_score, min(limit, 200)],
        ).fetchdf()
    except Exception:  # noqa: BLE001
        return "No trade_surveillance table. Train and run the model first (`make ml`)."
    if df.empty:
        return f"No trades with an anomaly score at or above {min_score}."
    return json.dumps(_to_records(df), indent=2)


@mcp.tool()
def data_quality_status() -> str:
    """Report the most recent data-quality and drift check results.

    Use this to answer "is the data healthy?" or "were there any security or
    quality problems in the last pipeline run?".
    """
    reports_dir = os.path.join(DATA_ROOT, "reports")
    if not os.path.exists(reports_dir):
        return "No reports directory yet. Run the pipeline first."

    files = sorted(os.listdir(reports_dir), reverse=True)
    latest = {"quality": None, "drift": None}
    for name in files:
        if name.startswith("quality_report") and latest["quality"] is None:
            latest["quality"] = name
        if name.startswith("drift_report") and latest["drift"] is None:
            latest["drift"] = name

    out = []
    for kind, name in latest.items():
        if not name:
            out.append(f"No {kind} report found.")
            continue
        with open(os.path.join(reports_dir, name), encoding="utf-8") as f:
            out.append(f"## Latest {kind} report ({name})\n{f.read()}")
    return "\n\n".join(out)


if __name__ == "__main__":
    mcp.run()
