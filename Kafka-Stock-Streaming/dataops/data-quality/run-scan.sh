#!/bin/bash
# Continuous data quality scanning
# Runs Soda checks every SCAN_INTERVAL seconds

SCAN_INTERVAL=${SCAN_INTERVAL:-300}  # Default: 5 minutes

echo "╔════════════════════════════════════════╗"
echo "║  Soda Core Data Quality Scanner       ║"
echo "║  Interval: ${SCAN_INTERVAL}s           ║"
echo "╚════════════════════════════════════════╝"

# Wait for database to be ready
echo "[$(date)] Waiting for database..."
sleep 15

SCAN_COUNT=0

while true; do
    SCAN_COUNT=$((SCAN_COUNT + 1))
    echo ""
    echo "══════════════════════════════════════════"
    echo "[$(date)] Starting scan #${SCAN_COUNT}"
    echo "══════════════════════════════════════════"

    soda scan -d stockdata \
        -c /app/soda-configuration.yml \
        /app/soda-checks.yml \
        2>&1

    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "[$(date)] Scan #${SCAN_COUNT} PASSED - All checks green"
    elif [ $EXIT_CODE -eq 1 ]; then
        echo "[$(date)] Scan #${SCAN_COUNT} WARNING - Some checks have warnings"
    else
        echo "[$(date)] Scan #${SCAN_COUNT} FAILED - Critical quality issues detected!"
        # In production: trigger PagerDuty/Slack alert here
    fi

    echo "[$(date)] Next scan in ${SCAN_INTERVAL}s..."
    sleep $SCAN_INTERVAL
done
