#!/bin/bash
# ABOUTME: Quick load test runner for LogicMonitor Data Pipeline
# ABOUTME: Runs baseline or custom load test with Locust

set -e

# Configuration
TARGET="${LOAD_TEST_TARGET:-https://ca-cta-lm-ingest.greensea-6af53795.eastus.azurecontainerapps.io}"
RESULTS_DIR="results/load-tests"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
TEST_TYPE="${1:-baseline}"
USERS="${2:-10}"
DURATION="${3:-5m}"

echo -e "${BLUE}🚀 LogicMonitor Data Pipeline - Load Test${NC}"
echo "=============================================="
echo ""
echo "Target: $TARGET"
echo "Test Type: $TEST_TYPE"
echo "Users: $USERS"
echo "Duration: $DURATION"
echo ""

# Check if locust is installed
if ! command -v locust &> /dev/null; then
    echo -e "${YELLOW}⚠️  Locust not found. Installing...${NC}"
    pip install locust websocket-client || uv add --dev locust websocket-client
fi

# Create results directory
mkdir -p "$RESULTS_DIR"

# Determine test parameters
case "$TEST_TYPE" in
    baseline)
        USERS=10
        SPAWN_RATE=1
        DURATION="5m"
        echo "Running baseline test (10 users, 5 minutes)"
        ;;
    load)
        USERS=100
        SPAWN_RATE=10
        DURATION="15m"
        echo "Running load test (100 users, 15 minutes)"
        ;;
    stress)
        USERS=500
        SPAWN_RATE=20
        DURATION="20m"
        echo "Running stress test (500 users, 20 minutes)"
        ;;
    spike)
        USERS=500
        SPAWN_RATE=100
        DURATION="5m"
        echo "Running spike test (500 users, rapid spike)"
        ;;
    custom)
        SPAWN_RATE=$((USERS / 10))
        echo "Running custom test ($USERS users, $DURATION)"
        ;;
    *)
        echo "Unknown test type: $TEST_TYPE"
        echo "Usage: $0 [baseline|load|stress|spike|custom] [users] [duration]"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}Starting load test...${NC}"
echo ""

# Run locust
OUTPUT_PREFIX="$RESULTS_DIR/${TEST_TYPE}_${TIMESTAMP}"

locust -f tests/load/locustfile.py \
    --host "$TARGET" \
    --users "$USERS" \
    --spawn-rate "$SPAWN_RATE" \
    --run-time "$DURATION" \
    --headless \
    --csv "$OUTPUT_PREFIX" \
    --html "${OUTPUT_PREFIX}.html" \
    --loglevel INFO

echo ""
echo "=============================================="
echo -e "${GREEN}✅ Load test complete!${NC}"
echo "=============================================="
echo ""
echo "Results saved to:"
echo "  - ${OUTPUT_PREFIX}_stats.csv"
echo "  - ${OUTPUT_PREFIX}_failures.csv"
echo "  - ${OUTPUT_PREFIX}.html"
echo ""

# Display quick summary
if [ -f "${OUTPUT_PREFIX}_stats.csv" ]; then
    echo "Quick Summary:"
    echo "---"
    head -n 3 "${OUTPUT_PREFIX}_stats.csv" | column -t -s ','
    echo ""
fi

# Check for failures
if [ -f "${OUTPUT_PREFIX}_failures.csv" ]; then
    FAILURE_COUNT=$(wc -l < "${OUTPUT_PREFIX}_failures.csv")
    FAILURE_COUNT=$((FAILURE_COUNT - 1))  # Subtract header

    if [ "$FAILURE_COUNT" -gt 0 ]; then
        echo -e "${YELLOW}⚠️  Found $FAILURE_COUNT failures${NC}"
        echo "Review: ${OUTPUT_PREFIX}_failures.csv"
    else
        echo -e "${GREEN}✅ No failures detected${NC}"
    fi
fi

echo ""
echo "Open HTML report:"
echo "  open ${OUTPUT_PREFIX}.html"
echo ""
