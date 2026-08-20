#!/usr/bin/env bash
# ==========================================================================
# ETRM platform status - one command, the whole truth.
#
#   bash scripts/status.sh          show current state
#   bash scripts/status.sh run      trigger a run, then show state
#
# Always reads the NEWEST DAG run by directory time, so it can never show you
# a stale run (a mistake that cost us several debugging cycles).
# ==========================================================================
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

DAG=etrm_medallion_pipeline
LOGDIR="logs/dag_id=$DAG"
bar() { printf '%s\n' "-------------------------------------------------------------"; }

# --------------------------------------------------------------------------
# Optionally trigger a run first
# --------------------------------------------------------------------------
if [ "${1:-}" = "run" ]; then
  echo "== Pre-flight: is the Spark cluster ready? =="
  ready=$(docker compose exec -T spark-master bash -c 'curl -s http://localhost:8080/json/' 2>/dev/null \
          | python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('aliveworkers') or 0)" 2>/dev/null || echo 0)
  echo "   workers alive: $ready"
  if [ "$ready" -lt 1 ] 2>/dev/null; then
    echo "   >>> No Spark worker. Fix that first:"
    echo "       docker compose up -d --force-recreate spark-master spark-worker"
    exit 1
  fi
  # A PAUSED dag accepts triggers and creates the run, but the scheduler never
  # starts it - the run just sits in "queued" forever. This bit us for 18 runs.
  echo "   unpausing the DAG (a paused DAG queues runs but never starts them)"
  docker compose exec -T airflow-scheduler airflow dags unpause "$DAG" >/dev/null 2>&1
  paused=$(docker compose exec -T postgres-airflow psql -U airflow -d airflow_db -t -A -c \
           "SELECT is_paused FROM dag WHERE dag_id='$DAG';" 2>/dev/null | tr -d '[:space:]')
  echo "   is_paused now: ${paused:-unknown}"
  if [ "$paused" = "t" ]; then
    echo "   >>> STILL PAUSED - unpause it in the UI toggle, then retry"
    exit 1
  fi
  echo "   >>> ready, triggering"
  docker compose exec -T airflow-scheduler airflow dags trigger "$DAG" >/dev/null 2>&1
  echo "   waiting 210s for the pipeline..."
  sleep 210
  echo
fi

# --------------------------------------------------------------------------
# 1. Services
# --------------------------------------------------------------------------
echo "== SERVICES =="
docker compose ps --format '   {{.Name}}  {{.Status}}' 2>/dev/null | sort
bar

# --------------------------------------------------------------------------
# 2. Spark cluster
# --------------------------------------------------------------------------
echo "== SPARK CLUSTER =="
docker compose exec -T spark-master bash -c 'curl -s http://localhost:8080/json/' 2>/dev/null \
  | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(f\"   workers alive : {d.get('aliveworkers')}\")
print(f\"   cores         : {d['cores']-d['coresused']} free / {d['cores']}\")
print(f\"   memory        : {d['memory']-d['memoryused']} free / {d['memory']} MB\")
print(f\"   active apps   : {[(a['name'],a['state']) for a in d.get('activeapps',[])] or 'none'}\")
" 2>/dev/null || echo "   (master unreachable)"
bar

# --------------------------------------------------------------------------
# 3. Newest DAG run - task by task
# --------------------------------------------------------------------------
echo "== NEWEST DAG RUN =="
RUN=$(find "$LOGDIR" -maxdepth 1 -name "run_id=*" -printf '%T@ %f\n' 2>/dev/null \
      | sort -rn | head -1 | cut -d' ' -f2-)
if [ -z "$RUN" ]; then
  echo "   no runs found yet"
else
  echo "   $RUN"
  echo
  for t in generate_trades_postgres secure_ingest_market_data trade_security_gate \
           process_bronze_to_silver_spark process_silver_to_gold_spark \
           run_surveillance_model publish_run_summary; do
    d="$LOGDIR/$RUN/task_id=$t"
    if [ -d "$d" ]; then
      # newest attempt only
      f=$(find "$d" -name "*.log" -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)
      if [ -n "$f" ] && grep -q '"level":"error"' "$f" 2>/dev/null; then
        printf '   FAIL  %s\n' "$t"
      else
        printf '   OK    %s\n' "$t"
      fi
    else
      printf '   ----  %s (not started)\n' "$t"
    fi
  done
fi
bar

# --------------------------------------------------------------------------
# 4. Data actually produced
# --------------------------------------------------------------------------
echo "== DATA LAKE (files from the last 30 minutes) =="
found=$(find data/1_bronze data/2_silver data/3_gold \
        \( -name "*.parquet" -o -name "*.json" \) -newermt "-30 minutes" 2>/dev/null | wc -l)
if [ "$found" -gt 0 ]; then
  find data/1_bronze data/2_silver data/3_gold \
       \( -name "*.parquet" -o -name "*.json" \) -newermt "-30 minutes" \
       -printf '   NEW  %p\n' 2>/dev/null | head -15
else
  echo "   nothing new in the last 30 minutes"
fi
echo
echo "   totals:"
for layer in 1_bronze 2_silver 3_gold; do
  n=$(find "data/$layer" -type f ! -name ".*" 2>/dev/null | wc -l)
  printf '     %-10s %s files\n' "$layer" "$n"
done
bar

# --------------------------------------------------------------------------
# 5. Security + ML results from this run
# --------------------------------------------------------------------------
if [ -n "${RUN:-}" ]; then
  echo "== SECURITY GATES & ML =="
  for t in secure_ingest_market_data trade_security_gate run_surveillance_model publish_run_summary; do
    f=$(find "$LOGDIR/$RUN/task_id=$t" -name "*.log" -printf '%T@ %p\n' 2>/dev/null \
        | sort -rn | head -1 | cut -d' ' -f2-)
    [ -z "$f" ] && continue
    out=$(grep -oE '"event":"[^"]*"' "$f" 2>/dev/null \
          | grep -iE '\[OK\]|\[PASS\]|\[FAIL\]|\[ALERT\]|\[CLEAN\]|FINDING|flagged|scored|rows|alerts' \
          | sed 's/"event":"//; s/"$//' | head -8)
    [ -n "$out" ] && { echo "   --- $t"; echo "$out" | sed 's/^/     /'; }
  done
  bar
fi

echo "UI:  Airflow http://localhost:18080   Spark http://localhost:8081"
echo "     Dashboard http://localhost:8501  MLflow http://localhost:5000"
