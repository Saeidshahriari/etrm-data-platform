"""Bronze -> Silver PySpark transformation.

Runs as a spark-submit application on the Spark standalone cluster. The Spark
master is provided by spark-submit (--master), so this script does NOT hardcode
it. Database credentials come from Vault via the central config module, and the
data-lake root comes from DATA_ROOT.
"""
import glob
import json
import os
import sys

import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Make the 'ingestion' and 'config' modules importable on the driver.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_db_config, get_data_root, get_spark_master  # noqa: E402


def get_spark_session(app_name: str = "ETRM-Bronze-to-Silver") -> SparkSession:
    """Build a SparkSession.

    When launched with spark-submit, the master comes from the --master flag,
    so .master() is intentionally not called. For a direct local run, set the
    SPARK_MASTER_URL environment variable (e.g. local[*]).
    """
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


def process_market_data_to_silver(spark: SparkSession) -> None:
    """Read the latest raw market JSON from Bronze, standardize it, write Silver."""
    bronze_dir = os.path.join(get_data_root(), "1_bronze")
    bronze_files = glob.glob(os.path.join(bronze_dir, "market_prices_*.json"))
    if not bronze_files:
        print("[WARN] No market price files found in the Bronze layer.")
        return

    latest_file = max(bronze_files, key=os.path.getctime)
    print(f"[INFO] Reading raw market data from: {latest_file}")

    with open(latest_file, "r", encoding="utf-8") as f:
        raw_json = json.load(f)

    series_data = raw_json.get("series", [])
    if not series_data:
        print("[WARN] No price series data found in the JSON file.")
        return

    pdf = pd.DataFrame(series_data)
    df_spark = spark.createDataFrame(pdf)

    # Convert millisecond epoch to a proper timestamp and standardize columns.
    df_silver_market = (
        df_spark
        .withColumn("timestamp", (F.col("timestamp_ms") / 1000).cast("timestamp"))
        .withColumn("price_eur_mwh", F.col("price_eur_mwh").cast("double"))
        .withColumn("commodity", F.lit("POWER"))
        .withColumn("processed_at", F.current_timestamp())
        .select("timestamp", "commodity", "price_eur_mwh", "processed_at")
        .filter(F.col("price_eur_mwh").isNotNull())
        .dropDuplicates(["timestamp"])
    )

    output_path = os.path.join(get_data_root(), "2_silver", "market_prices")
    df_silver_market.write.mode("overwrite").parquet(output_path)

    print(f"[OK] Market data written to Silver: '{output_path}'")
    df_silver_market.show(5, truncate=False)


def process_trades_to_silver(spark: SparkSession) -> None:
    """Extract raw trades from PostgreSQL, standardize them, write Silver."""
    print("[INFO] Extracting raw trades from PostgreSQL...")
    try:
        conn = psycopg2.connect(**get_db_config())
        pdf_trades = pd.read_sql("SELECT * FROM raw_trades;", conn)
        conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Database connection failed: {exc}")
        return

    if pdf_trades.empty:
        print("[WARN] No trades found in PostgreSQL. Did the generate_trades task run?")
        return

    df_spark_trades = spark.createDataFrame(pdf_trades)

    df_silver_trades = (
        df_spark_trades
        .withColumn("volume_mw", F.col("volume_mw").cast("double"))
        .withColumn("price_eur_mwh", F.col("price_eur_mwh").cast("double"))
        .withColumn("delivery_start", F.to_timestamp(F.col("delivery_start")))
        .withColumn("delivery_end", F.to_timestamp(F.col("delivery_end")))
        .withColumn("created_at", F.to_timestamp(F.col("created_at")))
        .withColumn("processed_at", F.current_timestamp())
        .dropDuplicates(["trade_id"])
    )

    output_path = os.path.join(get_data_root(), "2_silver", "trades")
    df_silver_trades.write.mode("overwrite").parquet(output_path)

    print(f"[OK] Trades written to Silver: '{output_path}'")
    df_silver_trades.select(
        "trade_id", "counterparty", "commodity", "buy_sell", "volume_mw", "price_eur_mwh"
    ).show(5)


def main() -> None:
    spark = get_spark_session()
    try:
        process_market_data_to_silver(spark)
        process_trades_to_silver(spark)
    finally:
        spark.stop()
    print("[OK] Bronze-to-Silver pipeline completed.")


if __name__ == "__main__":
    main()
