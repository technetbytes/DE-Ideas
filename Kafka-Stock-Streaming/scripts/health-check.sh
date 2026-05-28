#!/bin/bash
# Health check script for the stock streaming pipeline
# Usage: ./scripts/health-check.sh

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

check_service() {
    local name=$1
    local cmd=$2
    printf "%-20s" "$name"
    if eval "$cmd" > /dev/null 2>&1; then
        echo -e "${GREEN}[OK]${NC}"
        return 0
    else
        echo -e "${RED}[FAIL]${NC}"
        return 1
    fi
}

echo "═══════════════════════════════════════════"
echo "  Stock Streaming Pipeline Health Check"
echo "═══════════════════════════════════════════"
echo ""

FAILURES=0

check_service "Zookeeper" "docker exec zookeeper nc -z localhost 2181" || ((FAILURES++))
check_service "Kafka Broker" "docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092" || ((FAILURES++))
check_service "PostgreSQL" "docker exec postgres pg_isready -U stockuser -d stockdata" || ((FAILURES++))
check_service "Producer" "curl -sf http://localhost:8000/metrics" || ((FAILURES++))
check_service "Consumer" "curl -sf http://localhost:8001/metrics" || ((FAILURES++))
check_service "Prometheus" "curl -sf http://localhost:9090/-/healthy" || ((FAILURES++))
check_service "Grafana" "curl -sf http://localhost:3000/api/health" || ((FAILURES++))
check_service "Kafka UI" "curl -sf http://localhost:8080" || ((FAILURES++))

echo ""
echo "═══════════════════════════════════════════"

# Check data flow
echo ""
echo "── Data Flow ──"
TICK_COUNT=$(docker exec postgres psql -U stockuser -d stockdata -t -c "SELECT COUNT(*) FROM stock_ticks;" 2>/dev/null | tr -d ' ')
if [ -n "$TICK_COUNT" ] && [ "$TICK_COUNT" -gt 0 ]; then
    echo -e "Ticks in DB:        ${GREEN}${TICK_COUNT}${NC}"
else
    echo -e "Ticks in DB:        ${YELLOW}0 (pipeline may still be starting)${NC}"
fi

# Check producer rate
RATE=$(curl -sf http://localhost:8000/metrics 2>/dev/null | grep "stock_producer_messages_per_second" | grep -v "#" | awk '{print $2}')
if [ -n "$RATE" ]; then
    echo -e "Producer rate:      ${GREEN}${RATE} msg/s${NC}"
fi

echo ""
if [ $FAILURES -eq 0 ]; then
    echo -e "${GREEN}All services healthy!${NC}"
    exit 0
else
    echo -e "${RED}${FAILURES} service(s) unhealthy${NC}"
    exit 1
fi
