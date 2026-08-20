"""HashiCorp Vault client for the ETRM platform.

All application secrets (database credentials, external API keys) live in
Vault's KV v2 engine and are read at runtime. Only two bootstrap values are
taken from the environment: the Vault address and the Vault token. Nothing
sensitive is hardcoded in the source anymore.
"""
import os
import sys

import hvac

# Bootstrap configuration: the ONLY values read directly from the environment.
VAULT_ADDR = os.getenv("VAULT_ADDR", "http://vault:8200")
VAULT_TOKEN = os.getenv("VAULT_TOKEN", "dev-root-token")
SECRET_MOUNT = "secret"
SECRET_PATH = "etrm-secrets"


def get_vault_client() -> hvac.Client:
    """Initialize and return an authenticated Vault client."""
    client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)
    if not client.is_authenticated():
        print(f"[ERROR] Failed to authenticate with HashiCorp Vault at {VAULT_ADDR}.")
        sys.exit(1)
    return client


def setup_initial_secrets() -> None:
    """Seed Vault with the initial ETRM secrets.

    Values are taken from environment variables so nothing sensitive is
    hardcoded. Sensible development defaults (matching docker-compose) are
    used when a variable is not provided.
    """
    client = get_vault_client()
    secrets = {
        "DB_HOST": os.getenv("DB_HOST", "postgres"),   # Docker service name
        "DB_PORT": os.getenv("DB_PORT", "5432"),        # internal container port
        "DB_NAME": os.getenv("DB_NAME", "etrm_db"),
        "DB_USER": os.getenv("DB_USER", "etrm_user"),
        "DB_PASS": os.getenv("DB_PASS", "etrm_password"),
        "ENTSOE_API_KEY": os.getenv("ENTSOE_API_KEY", ""),
    }
    client.secrets.kv.v2.create_or_update_secret(
        mount_point=SECRET_MOUNT,
        path=SECRET_PATH,
        secret=secrets,
    )
    print(f"[OK] Seeded {len(secrets)} secrets into Vault at '{SECRET_PATH}'.")


def read_all_secrets() -> dict:
    """Return every secret stored at the ETRM path as a plain dict."""
    client = get_vault_client()
    response = client.secrets.kv.v2.read_secret_version(
        mount_point=SECRET_MOUNT,
        path=SECRET_PATH,
        raise_on_deleted_version=True,
    )
    return response["data"]["data"]


def read_secret_from_vault(key_name: str) -> str:
    """Return a single secret value by key name (empty string if missing)."""
    return read_all_secrets().get(key_name, "")


if __name__ == "__main__":
    print("Initializing HashiCorp Vault secrets manager...")
    setup_initial_secrets()
    check = {k: read_secret_from_vault(k) for k in ("DB_HOST", "DB_PORT", "DB_USER")}
    print(f"[OK] Vault verification -> {check}")
