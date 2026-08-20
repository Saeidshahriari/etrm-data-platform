"""Silver -> Gold PySpark analytics.

Reads Silver trades and market prices, values each trade against the latest
market curve (Mark-to-Market and unrealized PnL), and writes three Gold
datasets: trade-level PnL, counterparty risk, and portfolio summary.

Runs as a spark-submit application; the Spark master is provided by
spark-submit. Data-lake root comes from DATA_ROOT.
"""
import os
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_data_root, get_spark_master  # noqa: E402

# Fallback benchmark prices (EUR/MWh) when a commodity is missing from market data.
GAS_TTF_BENCHMARK = 42.50
POWER_BENCHMARK = 85.00


def get_spark_session(app_name: str = "ETRM-Silver-to-Gold") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        # Local mode runs the driver inside the Airflow worker container, which
        # has a hard memory cap. Keep Spark's appetite well under it.
        .config("spark.driver.memory", "512m")
        .config("spark.driver.maxResultSize", "256m")
        # Spark UI ON. In local mode it serves on http://localhost:4040 while
        # the job runs, showing jobs, stages, the DAG and the SQL plan. This is
        # the main way to SEE what Spark is doing - worth the few MB.
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
    )
    master = get_spark_master()
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def process_silver_to_gold(spark: SparkSession) -> None:
    print("[INFO] Starting Silver-to-Gold processing...")

    data_root = get_data_root()
    trades_path = os.path.join(data_root, "2_silver", "trades")
    market_path = os.path.join(data_root, "2_silver", "market_prices")

    if not os.path.exists(trades_path) or not os.path.exists(market_path):
        print("[WARN] Required Silver data not found. Run bronze_to_silver first.")
        return

    # 1. Load Silver datasets.
    df_trades = spark.read.parquet(trades_path)
    df_market = spark.read.parquet(market_path)

    # 2. Latest market price per commodity.
    window_spec = Window.partitionBy("commodity").orderBy(F.col("timestamp").desc())
    df_latest_market = (
        df_market
        .withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .select(
            F.col("commodity"),
            F.col("price_eur_mwh").alias("latest_market_price"),
            F.col("timestamp").alias("market_price_as_of"),
        )
    )

    # 3. Join trades with the latest market price (fallback for missing commodities).
    df_joined = df_trades.join(df_latest_market, on="commodity", how="left")
    df_enriched = df_joined.withColumn(
        "effective_market_price",
        F.coalesce(
            F.col("latest_market_price"),
            F.when(F.col("commodity") == "NATURAL_GAS", F.lit(GAS_TTF_BENCHMARK))
             .otherwise(F.lit(POWER_BENCHMARK)),
        ),
    )

    # 4. Financial metrics: notional, MtM value, unrealized PnL.
    df_pnl = (
        df_enriched
        .withColumn("notional_value_eur", F.round(F.col("volume_mw") * F.col("price_eur_mwh"), 2))
        .withColumn("mtm_value_eur", F.round(F.col("volume_mw") * F.col("effective_market_price"), 2))
        .withColumn(
            "unrealized_pnl_eur",
            F.round(
                F.when(
                    F.col("buy_sell") == "BUY",
                    F.col("volume_mw") * (F.col("effective_market_price") - F.col("price_eur_mwh")),
                ).when(
                    F.col("buy_sell") == "SELL",
                    F.col("volume_mw") * (F.col("price_eur_mwh") - F.col("effective_market_price")),
                ).otherwise(F.lit(0.0)),
                2,
            ),
        )
        .withColumn("gold_processed_at", F.current_timestamp())
    )

    # Output 1: trade-level PnL.
    gold_trades_path = os.path.join(data_root, "3_gold", "trades_pnl")
    df_pnl.write.mode("overwrite").parquet(gold_trades_path)
    print(f"[OK] Trade-level PnL written to: '{gold_trades_path}'")

    # Output 2: counterparty risk & exposure.
    df_counterparty_risk = (
        df_pnl.groupBy("counterparty", "commodity")
        .agg(
            F.count("trade_id").alias("trade_count"),
            F.round(F.sum("volume_mw"), 2).alias("total_volume_mw"),
            F.round(F.sum("notional_value_eur"), 2).alias("total_notional_eur"),
            F.round(F.sum("mtm_value_eur"), 2).alias("total_mtm_eur"),
            F.round(F.sum("unrealized_pnl_eur"), 2).alias("net_unrealized_pnl_eur"),
        )
        .orderBy(F.col("total_notional_eur").desc())
    )
    gold_cp_path = os.path.join(data_root, "3_gold", "counterparty_risk")
    df_counterparty_risk.write.mode("overwrite").parquet(gold_cp_path)
    print(f"[OK] Counterparty risk written to: '{gold_cp_path}'")

    # Output 3: portfolio summary by commodity.
    df_portfolio = df_pnl.groupBy("commodity").agg(
        F.count("trade_id").alias("total_trades"),
        F.round(F.sum("volume_mw"), 2).alias("net_volume_mw"),
        F.round(F.sum("notional_value_eur"), 2).alias("portfolio_notional_eur"),
        F.round(F.sum("unrealized_pnl_eur"), 2).alias("total_portfolio_pnl_eur"),
    )
    gold_port_path = os.path.join(data_root, "3_gold", "portfolio_summary")
    df_portfolio.write.mode("overwrite").parquet(gold_port_path)
    print(f"[OK] Portfolio summary written to: '{gold_port_path}'")

    # Previews.
    print("\n" + "=" * 60)
    print("GOLD LAYER PREVIEWS")
    print("=" * 60)
    print("\n1. Enriched trades & PnL:")
    df_pnl.select(
        "trade_id", "counterparty", "commodity", "buy_sell",
        "volume_mw", "price_eur_mwh", "effective_market_price", "unrealized_pnl_eur",
    ).show(5, truncate=False)
    print("2. Counterparty exposure & risk:")
    df_counterparty_risk.show(truncate=False)
    print("3. Portfolio performance by commodity:")
    df_portfolio.show(truncate=False)


def main() -> None:
    spark = get_spark_session()
    try:
        process_silver_to_gold(spark)
    finally:
        spark.stop()
    print("[OK] Silver-to-Gold pipeline completed.")


if __name__ == "__main__":
    main()
