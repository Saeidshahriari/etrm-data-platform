"""Local runner for the ETRM pipeline.

This is a convenience entry point to run the pipeline stages OUTSIDE Airflow,
directly on your machine (useful for quick testing). In production the stages
are orchestrated by the Airflow DAG in dags/etrm_pipeline_dag.py.

Usage:
    python main.py trades      # generate synthetic trades -> Postgres
    python main.py ingest      # fetch market data -> Bronze
    python main.py silver      # Bronze -> Silver (needs SPARK_MASTER_URL, e.g. local[*])
    python main.py gold        # Silver -> Gold
    python main.py all         # run every stage in order

For local runs, set these first (PowerShell example):
    $env:DATA_ROOT="data"; $env:SPARK_MASTER_URL="local[*]"
    $env:VAULT_ADDR="http://localhost:8200"; $env:DB_HOST="localhost"; $env:DB_PORT="15432"
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))


def _trades() -> None:
    from ingestion.generate_trades import generate_synthetic_trades, init_db_and_insert_trades
    init_db_and_insert_trades(generate_synthetic_trades(100))


def _ingest() -> None:
    from ingestion.fetch_market_data import fetch_electricity_market_prices, save_raw_data_to_bronze
    save_raw_data_to_bronze(fetch_electricity_market_prices())


def _silver() -> None:
    from processing.bronze_to_silver import main as silver_main
    silver_main()


def _gold() -> None:
    from processing.silver_to_gold import main as gold_main
    gold_main()


STAGES = {"trades": _trades, "ingest": _ingest, "silver": _silver, "gold": _gold}


def main() -> None:
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    if arg == "all":
        for name in ("trades", "ingest", "silver", "gold"):
            print(f"\n=== Stage: {name} ===")
            STAGES[name]()
    elif arg in STAGES:
        STAGES[arg]()
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
