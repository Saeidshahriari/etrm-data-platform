# ==========================================================================
# ETRM Data Platform - one-command automation
#
#   make up        start the whole platform
#   make pipeline  run the full pipeline once, right now
#   make security  run every security scan (layers A-G)
#
# Run `make help` to see everything.
# ==========================================================================
.DEFAULT_GOAL := help
SHELL := /bin/bash

# Local-run environment: host ports and the local data folder.
LOCAL_ENV := DATA_ROOT=data MODEL_DIR=models SPARK_MASTER_URL="local[*]" \
             VAULT_ADDR=http://localhost:8200 DB_HOST=localhost DB_PORT=15432

.PHONY: help
help:  ## Show this help
	@echo ""
	@echo "  ETRM Data Platform"
	@echo "  =================="
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
	@echo ""

# --------------------------------------------------------------------------
# Platform lifecycle
# --------------------------------------------------------------------------
.PHONY: init
init:  ## First-time setup: create .env from the template
	@if [ ! -f .env ]; then \
		cp .env.template .env; \
		echo "Created .env from template. EDIT IT before going further:"; \
		echo "  - set AIRFLOW_FERNET_KEY  (make fernet-key)"; \
		echo "  - set AIRFLOW_JWT_SECRET  (any long random string)"; \
	else \
		echo ".env already exists - leaving it untouched."; \
	fi

.PHONY: fernet-key
fernet-key:  ## Generate a Fernet key for .env
	@python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

.PHONY: build
build:  ## Build the custom Airflow image (Java + PySpark)
	docker compose build

.PHONY: up
up: init  ## Start the entire platform
	docker compose up -d
	@echo ""
	@echo "  Platform starting. Give it 1-2 minutes, then open:"
	@echo "    Airflow    http://localhost:18080"
	@echo "    Spark      http://localhost:8081"
	@echo "    Dashboard  http://localhost:8501"
	@echo "    MLflow     http://localhost:5000"
	@echo "    Vault      http://localhost:8200"
	@echo ""
	@echo "  Watch progress with:  make logs"

.PHONY: down
down:  ## Stop the platform (data is kept)
	docker compose down

.PHONY: clean
clean:  ## Stop and DELETE all data volumes
	docker compose down -v
	@echo "All containers and volumes removed."

.PHONY: ps
ps:  ## Show container status
	docker compose ps

.PHONY: logs
logs:  ## Follow logs from all services
	docker compose logs -f

.PHONY: health
health:  ## Check that every service is reachable
	@echo "Checking services..."
	@for svc in "Airflow:18080" "Spark:8081" "Dashboard:8501" "MLflow:5000" "Vault:8200"; do \
		name=$${svc%%:*}; port=$${svc##*:}; \
		if curl -sf -o /dev/null --max-time 3 "http://localhost:$$port"; then \
			echo "  OK    $$name (port $$port)"; \
		else \
			echo "  DOWN  $$name (port $$port)"; \
		fi; \
	done

# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------
.PHONY: pipeline
pipeline:  ## Trigger the full pipeline in Airflow now
	docker compose exec airflow-scheduler airflow dags trigger etrm_medallion_pipeline
	@echo "Triggered. Follow it at http://localhost:18080"

.PHONY: pipeline-local
pipeline-local:  ## Run the pipeline locally, without Airflow
	$(LOCAL_ENV) python main.py all

.PHONY: ml
ml:  ## Train the surveillance model and score the current book
	$(LOCAL_ENV) python src/ml/train_anomaly_model.py
	$(LOCAL_ENV) python src/ml/score_trades.py

.PHONY: dashboard
dashboard:  ## Run the dashboard locally
	$(LOCAL_ENV) streamlit run dashboard/app.py

.PHONY: agent
agent:  ## Run the AI agent MCP server
	$(LOCAL_ENV) python agent/etrm_mcp_server.py

# --------------------------------------------------------------------------
# Security - layers A to G
# --------------------------------------------------------------------------
.PHONY: security
security: sec-secrets sec-code sec-deps sec-container sec-data  ## Run all local security scans (A-E, G)
	@echo ""
	@echo "  Static scans finished. For the live attack scan (layer F) run:"
	@echo "    make sec-dast     (requires the platform to be running)"

.PHONY: sec-secrets
sec-secrets:  ## Layer A: scan for committed secrets
	@echo "== Layer A: secret scanning =="
	@if command -v gitleaks >/dev/null 2>&1; then \
		gitleaks detect --source . --verbose --no-banner || true; \
	else \
		docker run --rm -v "$$(pwd):/repo" zricethezav/gitleaks:latest \
			detect --source /repo --verbose --no-banner || true; \
	fi

.PHONY: sec-code
sec-code:  ## Layer B: static code security analysis
	@echo "== Layer B: SAST (bandit) =="
	@pip install -q bandit 2>/dev/null || true
	@bandit -r src/ dags/ agent/ dashboard/ --severity-level medium || true

.PHONY: sec-deps
sec-deps:  ## Layer C: vulnerable dependency scan
	@echo "== Layer C: dependency audit (pip-audit) =="
	@pip install -q pip-audit 2>/dev/null || true
	@pip-audit -r requirements.txt --desc || true

.PHONY: sec-container
sec-container:  ## Layer D: container image scan
	@echo "== Layer D: container scan (trivy) =="
	@docker run --rm -v /var/run/docker.sock:/var/run/docker.sock \
		-v "$$(pwd):/src" aquasec/trivy:latest \
		fs /src --severity CRITICAL,HIGH --skip-dirs .venv,data || true

.PHONY: sec-dast
sec-dast:  ## Layer F: live attack scan against the running Airflow UI
	@echo "== Layer F: DAST (OWASP ZAP) =="
	@bash security/run_dast.sh

.PHONY: sec-data
sec-data:  ## Layer G: prove the data-poisoning gates still work
	@echo "== Layer G: data gate tests =="
	@python -m pytest tests/ -q

.PHONY: test
test:  ## Run the full test suite
	python -m pytest tests/ -v

# --------------------------------------------------------------------------
# Convenience
# --------------------------------------------------------------------------
.PHONY: demo
demo: up  ## Start everything and run one full pipeline (the "wow" demo)
	@echo "Waiting 90s for services to become healthy..."
	@sleep 90
	@$(MAKE) health
	@$(MAKE) pipeline
	@echo ""
	@echo "  Now open the dashboard: http://localhost:8501"
