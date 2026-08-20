# ETRM Data Platform

An automated, secured **Energy Trading & Risk Management** lakehouse. It ingests
real European electricity prices and trades, defends the pipeline at seven
security layers, transforms everything with **Apache Spark**, detects suspicious
trades with a **machine-learning surveillance model**, and serves the result to a
**live dashboard** and an **AI agent** you can ask questions in plain language.

```bash
make up        # start the whole platform
make pipeline  # run it end to end
make security  # run every security scan
```

---

## What this platform does

```
                          ┌──────────────────────────────┐
                          │      Apache Airflow 3         │
                          │  schedules & secures the run  │
                          └───────────────┬──────────────┘
                                          │
      ┌───────────────────────────────────┼───────────────────────────┐
      ▼                                   ▼                           │
 generate_trades                   secure_ingest                       │
 (synthetic book)              SMARD API → scan → validate             │
      │                                   │                           │
      ▼                                   ▼                           │
 PostgreSQL (OLTP)                  Bronze (raw JSON)                  │
      │                                   │                           │
      └──────► trade_security_gate ◄──────┘                           │
                (injection + poisoning checks)                        │
                        │                                             │
                        ▼                                             │
              PySpark 4: Bronze → Silver → Gold                        │
                        │                                             │
                        ▼                                             │
              ML surveillance model (Isolation Forest)                 │
                        │                                             │
          ┌─────────────┴──────────────┐                              │
          ▼                            ▼                              │
   Live dashboard              AI agent (MCP + DuckDB)  ◄──────────────┘
   localhost:8501              ask questions in plain language
```

| Layer | Technology | Role |
|-------|-----------|------|
| Orchestration | Apache Airflow 3 (Celery) | Runs and secures the pipeline |
| Ingestion | Python, Pydantic, Pandera | Fetch, scan, validate |
| OLTP store | PostgreSQL 16 | Raw trades |
| Processing | Apache Spark 4 (local mode) | Bronze → Silver → Gold |
| Data lake | Parquet (medallion) | Bronze / Silver / Gold |
| ML | scikit-learn, MLflow | Market-abuse surveillance |
| Dashboard | Streamlit, DuckDB, Plotly | Near-real-time risk view |
| AI agent | MCP server, DuckDB | Natural-language questions |
| Secrets | HashiCorp Vault (KV v2) | All credentials |

---

## The ML model: REMIT market-abuse surveillance

**What it does.** It learns what normal trading looks like, then flags trades
that do not fit and explains why.

**Why this use case.** Detecting suspicious trading is a legal obligation for
European energy market participants under REMIT, supervised by ACER, so the
demand for it is regulatory rather than optional. It also works on data you
already have, and doubles as a data-poisoning detector.

**Algorithm: Isolation Forest.** Real market abuse is rare and almost never
labelled, so a supervised classifier has nothing to learn from. Isolation Forest
is unsupervised: it learns the shape of normal trading and measures how easily a
trade can be separated from the rest.

**Features** (in `src/ml/features.py`) — the signals a surveillance analyst uses:
price deviation from the market, off-market flag, volume, notional, contract
length, and counterparty/trader concentration.

**Measured behaviour** (see `tests/test_ml_model.py`):

| Test | Result |
|------|--------|
| Planted abuse patterns detected | **3 / 3** |
| False positives on normal trades | **3 / 60 (5%)** |
| Every alert carries a reason | yes |

The three planted patterns are an off-market price (manipulation), an enormous
volume (cornering), and a near-zero price (possible wash trade).

**Example output:**

```
trade_id    volume_mw  price   score  band      reason
TRD-99001      50.00   950.00  1.000  CRITICAL  price_deviation_pct is 99 sigma from normal
TRD-99002    4800.00    88.00  0.844  CRITICAL  volume_mw is 99 sigma from normal
TRD-99003      30.00     0.50  0.779  CRITICAL  is_off_market = 1, never seen in normal trading
```

---

## Security: seven layers

Layers A–F run in CI (`.github/workflows/security.yml`). Layer G runs inside the
pipeline on live data, because only then does real data exist.

| Layer | What it protects | Tool | Where |
|-------|-----------------|------|-------|
| **A** | Leaked secrets in the repo | gitleaks | CI + `make sec-secrets` |
| **B** | Insecure code patterns (SAST) | Bandit, Semgrep | CI + `make sec-code` |
| **C** | Vulnerable dependencies (SCA) | pip-audit | CI + `make sec-deps` |
| **D** | Container & image vulnerabilities | Trivy, Hadolint | CI + `make sec-container` |
| **E** | Unsafe infrastructure config | Checkov | CI |
| **F** | Live attack surface (DAST) | OWASP ZAP | `make sec-dast` |
| **G** | **Malicious or poisoned data** | Pandera + custom scanner | inside Airflow |

### Layer G in detail — the part that matters most for a data platform

If an attacker compromises your upstream source, no amount of code scanning
helps: the code is fine, the *data* is the weapon. Three defences run at
ingestion:

1. **Injection scanning** (`src/security/scanner.py`) — every string in the
   payload is inspected for SQL injection, script injection, path traversal, and
   **prompt injection** (the data is later shown to an AI agent, so an
   instruction hidden in a data field is a real attack vector).

2. **Schema & business-range validation** (`src/quality/schemas.py`) — prices
   must lie within the EU day-ahead clearing range (−500 to +4000 EUR/MWh).
   Negative prices are *allowed*, because they genuinely occur in Europe when
   there is too much wind and solar.

3. **Statistical poisoning detection** (`src/quality/gates.py`) — a robust
   median/MAD test catches a value that is inside the legal range but
   statistically impossible.

**Quarantine, do not crash.** Bad rows are set aside and the run continues.
Crashing on one poisoned row would let an attacker stop the whole platform by
injecting a single value — a denial-of-service. If more than 20% of a batch
fails, the source itself is treated as compromised and the batch is rejected.

This is not theoretical: the gate immediately caught a real bug in this
project's own data, where prices arrived roughly 1000× too high.

### A note on this repository's early history

Commits before the security refactor contain hardcoded **development** values:
a local Postgres password, the Vault dev root token, and a placeholder API key
(`demo-...`). They were only ever valid for a throwaway local stack and none of
them grants access to anything real.

They are left in history deliberately rather than rewritten, because the
before/after is part of the point: the current code holds **no** credentials at
all - everything is read from HashiCorp Vault at runtime, and `.env` is
git-ignored. If this were a production repository the correct response would be
to rotate every value and purge history with `git filter-repo`.

### Agent hardening

The AI agent can only ever run read-only queries. `SELECT`/`WITH` are allowed;
every other statement type is rejected, multiple statements are rejected, and
comment-based bypasses (`SELECT 1 /* */ ; DROP TABLE x`) are rejected too.
Verified by 9 guard tests.

---

## Setup

Prerequisites: Docker and Docker Compose, roughly 8 GB free RAM.

```bash
make init          # create .env from the template
make fernet-key    # generate a key, paste it into .env
# edit .env: set AIRFLOW_FERNET_KEY and AIRFLOW_JWT_SECRET

make build         # build the custom Airflow image (Java 17 + PySpark 4)
make up            # start everything
make health        # check that each service is reachable
```

| Service | URL | Notes |
|---------|-----|-------|
| Airflow | http://localhost:18080 | user `admin`, password from `.env` |
| Dashboard | http://localhost:8501 | live risk & surveillance |
| Spark | http://localhost:8081 | cluster status |
| MLflow | http://localhost:5000 | experiment tracking |
| Vault | http://localhost:8200 | secrets |

The first build downloads Java and PySpark, so it takes several minutes.

## Running

```bash
make pipeline        # trigger the full DAG in Airflow
make pipeline-local  # run the stages locally, without Airflow
make ml              # train the model and score the book
make test            # run the test suite (30 tests)
make security        # run all local security scans
```

## Project layout

```
etrm-data-platform/
├── dags/etrm_pipeline_dag.py      # orchestration + security checkpoints
├── src/
│   ├── config.py                  # Vault-backed configuration
│   ├── ingestion/
│   │   ├── secure_ingest.py       # fetch → scan → validate → land
│   │   ├── fetch_market_data.py   # SMARD API client
│   │   ├── generate_trades.py     # realistic synthetic trade book
│   │   └── vault_client.py        # secret management
│   ├── quality/                   # Layer G: schemas + poisoning gates
│   ├── security/scanner.py        # Layer G: injection & malware scanning
│   ├── processing/                # Spark: Bronze→Silver→Gold
│   └── ml/                        # features, training, scoring, drift
├── dashboard/app.py               # live Streamlit dashboard
├── agent/etrm_mcp_server.py       # AI agent (MCP over DuckDB)
├── tests/                         # 30 security + ML tests
├── security/run_dast.sh           # Layer F: OWASP ZAP
├── .github/workflows/security.yml # Layers A–E in CI
└── Makefile                       # one-command automation
```

## Verified end to end

Every stage below was run on a real machine (Windows 11 + WSL2 + Docker Desktop,
8 GB RAM), not just in tests:

```
secure_ingest_market_data   96 real German day-ahead prices, gate PASSED
trade_security_gate         200 trades, injection scan CLEAN, gate PASSED
bronze_to_silver            Silver market_prices + trades written
silver_to_gold              trades_pnl, counterparty_risk, portfolio_summary
run_surveillance_model      200 trades scored, 17 flagged (8.5%)
```

## A note on Spark mode

The Spark stages run **PySpark in local mode** inside the Airflow worker, not on
the standalone cluster. This was a deliberate decision after the cluster produced
five consecutive environment failures on a Windows-backed Docker volume: Python
version mismatch between driver and executor, worker work-directory permissions,
executor `chmod` on the shared volume, worker Hadoop login, and executor Hadoop
login (which survived even `spark.executorEnv.HADOOP_USER_NAME`).

The root cause is shared: Spark standalone resolves the OS user identity
separately at three process levels, and the container UID is not present in the
image's `/etc/passwd`. Local mode collapses those three levels into one process
with one identity.

It is the same PySpark running the same transformations and writing the same
Parquet - only the scheduling backend differs. `spark-master` and `spark-worker`
remain defined in `docker-compose.yml` for the cluster deployment path; see
`_run_spark_local()` in the DAG for the full reasoning.

## Honest limitations

- **Trades are synthetic.** The generator is realistic (prices cluster around
  the market, log-normal volumes) but it is not a real trade book. Detection
  rates on real data will differ.
- **Near-real-time, not streaming.** The pipeline runs hourly and the dashboard
  refreshes on a timer. True streaming would need Kafka and Spark Structured
  Streaming.
- **Vault runs in dev mode** (in-memory, single root token). Production needs a
  sealed Vault with a real auth method.
- **Automated scanning is not a full penetration test.** Layer F finds known,
  common weaknesses; a real assessment also needs a human tester.
- **The data lake is a shared local mount.** Production would use object storage
  such as S3 or MinIO.
- **Verify the SMARD price unit** before trusting the PnL figures. Sample data
  arrived ~1000× too high, which is exactly what the Layer G gate now blocks.

## References

- [ACER — REMIT market surveillance](https://www.acer.europa.eu/remit/market-surveillance)
- [Isolation Forest](https://en.wikipedia.org/wiki/Isolation_forest)
- [OWASP ZAP](https://www.zaproxy.org/)
- [Pandera](https://pandera.readthedocs.io/) · [MLflow](https://mlflow.org/) · [Model Context Protocol](https://modelcontextprotocol.io/)
