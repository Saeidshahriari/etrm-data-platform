#!/usr/bin/env bash
# ==========================================================================
# SECURITY LAYER F - DAST (Dynamic Application Security Testing)
#
# Layers A-E read your code. This layer ATTACKS the running system, which is
# the automated part of penetration testing. It probes the live Airflow UI the
# way an outside attacker would: looking for missing authentication, missing
# security headers, exposed endpoints and injection points.
#
# Honest limitation: an automated scanner finds known, common weaknesses. A
# real penetration test also needs a human who understands your business logic.
# Treat this as continuous baseline testing, not as a substitute for that.
#
# Usage:  ./security/run_dast.sh [target_url]
# ==========================================================================
set -euo pipefail

TARGET="${1:-http://host.docker.internal:8080}"
REPORT_DIR="$(pwd)/security/reports"
mkdir -p "$REPORT_DIR"

echo "=========================================================="
echo " OWASP ZAP baseline scan"
echo " Target : $TARGET"
echo " Reports: $REPORT_DIR"
echo "=========================================================="

# The baseline scan is passive and safe: it spiders the app and reports issues
# without sending destructive payloads. Use zap-full-scan.py for an active
# attack, and ONLY ever against a system you own.
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -v "$REPORT_DIR:/zap/wrk/:rw" \
  ghcr.io/zaproxy/zaproxy:stable \
  zap-baseline.py \
    -t "$TARGET" \
    -r zap_report.html \
    -J zap_report.json \
    -I \
  || SCAN_EXIT=$?

echo ""
echo "Scan finished. Reports written to:"
echo "  $REPORT_DIR/zap_report.html   (open this in a browser)"
echo "  $REPORT_DIR/zap_report.json"
echo ""
echo "Expected findings on a default local Airflow: missing security headers"
echo "(CSP, X-Frame-Options) and cookie flags. Fix those before any exposure"
echo "to a real network."

exit 0
