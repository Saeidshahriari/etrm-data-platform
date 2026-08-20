"""ETRM Medallion pipeline DAG - automated, secured, end to end.

Flow:

    generate_trades ─┐
                     ├─► security_and_quality_gate ─► bronze_to_silver (Spark)
    secure_ingest  ──┘                                        │
                                                              ▼
                                                   silver_to_gold (Spark)
                                                              │
                                                              ▼
                                                  train_or_score_model (ML)
                                                              │
                                                              ▼
                                                     publish_summary

Security checkpoints in this DAG:
  * secure_ingest             - injection scan + schema/poisoning gate on the API payload
  * security_and_quality_gate - scans trades from the database before Spark reads them
  * train_or_score_model      - drift detection; drift can indicate an upstream attack

Design notes:
  - Heavy libraries are imported INSIDE task functions, never at parse time, so
    the scheduler stays light and a missing library cannot break DAG loading.
  - The two Spark stages run PySpark in local mode inside the worker. See
    _run_spark_local() for why cluster mode was abandoned on this machine.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:  # pragma: no cover - older Airflow layout
    from airflow.operators.python import PythonOperator

SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../src"))
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

default_args = {
    "owner": "etrm-data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    # Retries are deliberately low. With 2 retries x 5 minutes, a failing task
    # keeps a DAG run alive for 15+ minutes, and on an hourly schedule the runs
    # pile up faster than they finish. Raise these once the pipeline is stable.
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# --------------------------------------------------------------------------
# Task callables
# --------------------------------------------------------------------------
def run_generate_trades() -> None:
    """Generate synthetic trades and load them into PostgreSQL."""
    from ingestion.generate_trades import generate_synthetic_trades, init_db_and_insert_trades

    trades = generate_synthetic_trades(num_trades=200)
    init_db_and_insert_trades(trades)


def run_secure_ingestion(**context) -> dict:
    """Fetch market data through the security and quality gates."""
    from ingestion.secure_ingest import secure_fetch_and_land

    summary = secure_fetch_and_land()
    # Push findings so downstream tasks and the UI can see them.
    context["ti"].xcom_push(key="ingestion_summary", value=summary)
    return summary


def run_trade_security_gate(**context) -> dict:
    """Scan and validate trades in the database BEFORE Spark consumes them.

    This is the second security checkpoint. Trades may come from an OLTP system
    that other applications write to, so they are untrusted input here.
    """
    import pandas as pd
    import psycopg2

    from config import get_data_root, get_db_config
    from quality.gates import quarantine, validate_trades, write_quality_report
    from security.scanner import scan_dataframe

    conn = psycopg2.connect(**get_db_config())
    df = pd.read_sql("SELECT * FROM raw_trades;", conn)
    conn.close()

    if df.empty:
        raise ValueError("No trades found in PostgreSQL - the generate step must run first.")

    print(f"[INFO] Screening {len(df)} trades...")

    # a) Injection / malicious-content scan on the text columns.
    scan = scan_dataframe(df)
    print(f"      {scan.summary()}")
    if not scan.clean:
        for threat in scan.threats[:20]:
            print(f"      THREAT: {threat}")
        raise ValueError(
            f"Trade data rejected: {len(scan.threats)} injection threat(s) found. "
            f"The OLTP source may be compromised."
        )

    # b) Schema, business-range and poisoning checks.
    gate = validate_trades(df)
    print(f"      {gate.summary()}")
    for finding in gate.findings:
        print(f"      FINDING: {finding}")

    data_root = get_data_root()
    q_path = quarantine(gate.quarantined, os.path.join(data_root, "quarantine"), "trades_rejected")
    if q_path:
        print(f"      Quarantined bad rows -> {q_path}")

    report = write_quality_report([gate], os.path.join(data_root, "reports"), batch_source="raw_trades")
    if not gate.passed:
        raise ValueError(f"Trade quality gate failed; see {report}.")

    summary = {
        "rows_accepted": len(gate.clean),
        "rows_quarantined": len(gate.quarantined),
        "findings": gate.findings,
        "report": report,
    }
    context["ti"].xcom_push(key="trade_gate_summary", value=summary)
    return summary


def _run_spark_local(entrypoint) -> None:
    """Run a PySpark job IN-PROCESS in local mode.

    WHY NOT SparkSubmitOperator + a standalone cluster?

    Spark standalone has three process levels - driver, worker, executor - and
    each resolves its OS user identity separately. On a laptop where the
    container uid is not in /etc/passwd and the data volume is a Windows bind
    mount, that produced five distinct failures in a row: Python version
    mismatch, worker work-dir permissions, executor chmod, worker Hadoop login,
    and finally executor Hadoop login (which survived even
    spark.executorEnv.HADOOP_USER_NAME).

    Local mode collapses all three levels into ONE process with ONE identity,
    which removes that entire class of problem. It is still real PySpark running
    the same transformations and writing the same Parquet - only the scheduling
    backend changes. For production the standalone/YARN/K8s cluster comes back;
    see docker-compose.yml, where spark-master and spark-worker remain defined.
    """
    # local[2] matches the 2 cores we budgeted; leaves room for Airflow itself.
    os.environ.setdefault("SPARK_MASTER_URL", "local[2]")
    entrypoint()


def run_bronze_to_silver() -> None:
    """Bronze -> Silver: clean market prices and trades into Parquet."""
    from processing.bronze_to_silver import main as bronze_main

    _run_spark_local(bronze_main)


def run_silver_to_gold() -> None:
    """Silver -> Gold: MtM valuation, PnL and counterparty risk."""
    from processing.silver_to_gold import main as gold_main

    _run_spark_local(gold_main)


def run_model(**context) -> dict:
    """Train the surveillance model if needed, then score the current book."""
    import os as _os

    from ml.score_trades import score_trades
    from ml.train_anomaly_model import get_model_dir, train

    model_path = _os.path.join(get_model_dir(), "trade_anomaly_model.joblib")
    if not _os.path.exists(model_path):
        print("[INFO] No trained model found - training a new one from Silver trades.")
        train()
    else:
        print("[INFO] Using the existing trained model.")

    scored = score_trades()
    flagged = int(scored["is_anomaly"].sum())
    summary = {
        "scored": int(len(scored)),
        "flagged": flagged,
        "alert_rate": round(flagged / max(len(scored), 1), 4),
    }
    context["ti"].xcom_push(key="surveillance_summary", value=summary)
    return summary


def run_publish_summary(**context) -> None:
    """Print a single consolidated run report into the Airflow log."""
    ti = context["ti"]
    ingestion = ti.xcom_pull(task_ids="secure_ingest_market_data", key="ingestion_summary") or {}
    trades = ti.xcom_pull(task_ids="trade_security_gate", key="trade_gate_summary") or {}
    surveillance = ti.xcom_pull(task_ids="run_surveillance_model", key="surveillance_summary") or {}

    print("=" * 62)
    print(" ETRM PIPELINE RUN SUMMARY")
    print("=" * 62)
    print(f" Market rows accepted    : {ingestion.get('rows_accepted', 'n/a')}")
    print(f" Market rows quarantined : {ingestion.get('rows_quarantined', 'n/a')}")
    print(f" Trades accepted         : {trades.get('rows_accepted', 'n/a')}")
    print(f" Trades quarantined      : {trades.get('rows_quarantined', 'n/a')}")
    print(f" Trades scored           : {surveillance.get('scored', 'n/a')}")
    print(f" Surveillance alerts     : {surveillance.get('flagged', 'n/a')} "
          f"({surveillance.get('alert_rate', 0):.1%})")
    print("=" * 62)

    for label, block in (("INGESTION", ingestion), ("TRADES", trades)):
        for finding in block.get("findings", []):
            print(f" [{label}] {finding}")


# --------------------------------------------------------------------------
# DAG definition
# --------------------------------------------------------------------------
with DAG(
    dag_id="etrm_medallion_pipeline",
    default_args=default_args,
    description="Secured end-to-end ETRM lakehouse: ingest -> gate -> Spark -> ML surveillance",
    # schedule=None -> the DAG runs ONLY when you trigger it, which is what you
    # want while developing: no background runs piling up behind a failure.
    # Switch to "@hourly" (or "@daily") once the pipeline runs green end to end.
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etrm", "pyspark", "medallion", "energy-trading", "mlsecops"],
) as dag:

    generate_trades_task = PythonOperator(
        task_id="generate_trades_postgres",
        python_callable=run_generate_trades,
    )

    secure_ingest_task = PythonOperator(
        task_id="secure_ingest_market_data",
        python_callable=run_secure_ingestion,
    )

    trade_gate_task = PythonOperator(
        task_id="trade_security_gate",
        python_callable=run_trade_security_gate,
    )

    bronze_to_silver_task = PythonOperator(
        task_id="process_bronze_to_silver_spark",
        python_callable=run_bronze_to_silver,
    )

    silver_to_gold_task = PythonOperator(
        task_id="process_silver_to_gold_spark",
        python_callable=run_silver_to_gold,
    )

    surveillance_task = PythonOperator(
        task_id="run_surveillance_model",
        python_callable=run_model,
    )

    publish_task = PythonOperator(
        task_id="publish_run_summary",
        python_callable=run_publish_summary,
        trigger_rule="all_done",   # always report, even if a gate stopped the run
    )

    # Dependency flow
    generate_trades_task >> trade_gate_task
    [secure_ingest_task, trade_gate_task] >> bronze_to_silver_task
    bronze_to_silver_task >> silver_to_gold_task >> surveillance_task >> publish_task
