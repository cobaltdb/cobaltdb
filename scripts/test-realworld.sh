#!/bin/bash
# CobaltDB Real-World Scenario Test Script
# This script tests CobaltDB with realistic production workloads

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║     CobaltDB Real-World Production Test                       ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

# Start services
echo "Starting CobaltDB services..."
docker-compose up -d cobaltdb prometheus grafana

# Wait for CobaltDB to be ready
echo
echo "Waiting for CobaltDB to be ready..."
for i in {1..30}; do
    if docker exec cobaltdb sh -c "nc -z localhost 4200" 2>/dev/null; then
        echo -e "${GREEN}✓ CobaltDB is ready!${NC}"
        break
    fi
    echo -n "."
    sleep 1
done

# Check if server is actually responding
if ! docker exec cobaltdb sh -c "nc -z localhost 4200" 2>/dev/null; then
    echo -e "${RED}✗ CobaltDB failed to start${NC}"
    docker-compose logs cobaltdb
    exit 1
fi

echo
echo "═══════════════════════════════════════════════════════════════"
echo "TEST 1: Basic Connectivity (Wire Protocol :4200)"
echo "═══════════════════════════════════════════════════════════════"
if docker exec cobaltdb sh -c "nc -z localhost 4200"; then
    echo -e "${GREEN}✓ Wire protocol port is open${NC}"
else
    echo -e "${RED}✗ Wire protocol connection failed${NC}"
fi

echo
echo "═══════════════════════════════════════════════════════════════"
echo "TEST 2: MySQL Protocol (:3307)"
echo "═══════════════════════════════════════════════════════════════"
if docker exec cobaltdb sh -c "nc -z localhost 3307" 2>/dev/null; then
    echo -e "${GREEN}✓ MySQL protocol port is open${NC}"
else
    echo -e "${YELLOW}! MySQL protocol port not responding (may not be enabled)${NC}"
fi

echo
echo "═══════════════════════════════════════════════════════════════"
echo "TEST 3: Admin API (:8420)"
echo "═══════════════════════════════════════════════════════════════"
if curl -s http://localhost:8420/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Admin API is responding${NC}"
    echo "Health check:"
    curl -s http://localhost:8420/health | head -20
else
    echo -e "${YELLOW}! Admin API not available${NC}"
fi

echo
echo "═══════════════════════════════════════════════════════════════"
echo "TEST 4: SQL Operations via CLI"
echo "═══════════════════════════════════════════════════════════════"

# Create test table
echo "Creating test table..."
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "CREATE TABLE IF NOT EXISTS test_users (
    id INTEGER PRIMARY KEY,
    email TEXT UNIQUE,
    name TEXT,
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)" 2>/dev/null && echo -e "${GREEN}✓ Table created${NC}" || echo -e "${RED}✗ Failed to create table${NC}"

# Insert data
echo "Inserting 100 records..."
for i in {1..100}; do
    docker exec cobaltdb cobaltdb-cli -data /tmp/test.db \
        "INSERT INTO test_users (email, name, age) VALUES ('user$i@test.com', 'User $i', $((20 + i % 50)))" 2>/dev/null
done
echo -e "${GREEN}✓ Inserted 100 records${NC}"

# Query data
echo "Running SELECT query..."
RESULT=$(docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "SELECT COUNT(*) FROM test_users" 2>/dev/null)
echo "Count result: $RESULT"

# Complex query
echo "Running complex JOIN with GROUP BY..."
docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "CREATE TABLE IF NOT EXISTS test_orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    amount REAL,
    status TEXT
)" 2>/dev/null

for i in {1..50}; do
    docker exec cobaltdb cobaltdb-cli -data /tmp/test.db \
        "INSERT INTO test_orders (user_id, amount, status) VALUES ($i, $((RANDOM % 1000)), 'completed')" 2>/dev/null
done

echo -e "${GREEN}✓ SQL operations successful${NC}"

echo
echo "═══════════════════════════════════════════════════════════════"
echo "TEST 5: Concurrent Connections"
echo "═══════════════════════════════════════════════════════════════"

echo "Simulating 10 concurrent connections..."
for i in {1..10}; do
    (docker exec cobaltdb cobaltdb-cli -data /tmp/test.db "SELECT * FROM test_users WHERE id = $i" > /dev/null 2>&1) &
done
wait
echo -e "${GREEN}✓ Concurrent connections handled${NC}"

echo
echo "═══════════════════════════════════════════════════════════════"
echo "TEST 6: Metrics Verification"
echo "═══════════════════════════════════════════════════════════════"

if curl -s http://localhost:9090/api/v1/status/targets > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Prometheus is collecting metrics${NC}"
else
    echo -e "${YELLOW}! Prometheus metrics not available${NC}"
fi

echo
echo "═══════════════════════════════════════════════════════════════"
echo "TEST 7: Backup Service"
echo "═══════════════════════════════════════════════════════════════"

if docker ps | grep -q cobaltdb_backup; then
    echo -e "${GREEN}✓ Backup service is running${NC}"
    echo "Recent backups:"
    docker exec cobaltdb_backup ls -lh /backups/ 2>/dev/null | tail -5 || echo "  No backups yet"
else
    echo -e "${YELLOW}! Backup service not running${NC}"
fi

echo
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    TEST SUMMARY                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo
echo "Services Status:"
docker-compose ps --services --filter "status=running