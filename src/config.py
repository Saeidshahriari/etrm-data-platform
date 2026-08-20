"""Central configuration for the ETRM platform.

Secrets come from HashiCorp Vault as the primary source. If Vault cannot be
reached (for example when running a single script directly on a laptop), the
code falls back to environment variables so development is still possible.

Non-secret settings (data paths, Spark master) come from environment variables
with safe defaults.
"""
import os
from functools import lru_cache

# Make "from ingestion..." work no matter how the script is launched.
import sys
_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.append(_SRC_DIR)

_SECRET_KEYS = ["DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASS", "ENTSOE_API_KEY"]


@lru_cache(maxsize=1)
def _load_secrets() -> dict:
    """Load secrets from Vault, falling back to environment variables.

    The Vault client (and its hvac dependency) is imported HERE, not at module
    level, so that callers who only need a non-secret setting such as
    get_data_root() never have to install a Vault library.
    """
    try:
        from ingestion.vault_client import read_all_secrets

        return read_all_secrets()
    except Exception as exc:  # noqa: BLE001 - missing hvac or Vault down -> env fallback
        print(f"[WARN] Could not read secrets from Vault ({exc}). "
              f"Falling back to environment variables.")
        return {key: os.getenv(key, "") for key in _SECRET_KEYS}


def get_db_config() -> dict:
    """Return psycopg2-compatible connection parameters."""
    secrets = _load_secrets()
    return {
        "host": secrets.get("DB_HOST") or "postgres",
        "port": int(secrets.get("DB_PORT") or 5432),
        "dbname": secrets.get("DB_NAME") or "etrm_db",
        "user": secrets.get("DB_USER") or "etrm_user",
        "password": secrets.get("DB_PASS") or "etrm_password",
    }


def get_entsoe_api_key() -> str:
    """Return the ENTSO-E API key (empty string if not configured)."""
    return _load_secrets().get("ENTSOE_API_KEY", "")


def get_data_root() -> str:
    """Root folder of the medallion data lake.

    Defaults to '/opt/airflow/data' inside the containers, overridable via the
    DATA_ROOT environment variable for local runs (e.g. 'data').
    """
    return os.getenv("DATA_ROOT", "/opt/airflow/data")


def get_spark_master() -> str | None:
    """Optional Spark master URL.

    When jobs are launched with spark-submit (the production path) this stays
    unset so spark-submit's --master flag wins. For a direct local run, set
    SPARK_MASTER_URL=local[*].
    """
    return os.getenv("SPARK_MASTER_URL")
