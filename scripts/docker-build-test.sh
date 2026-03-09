#!/bin/bash
# Docker build test script for CobaltDB
# Usage: ./scripts/docker-build-test.sh

set -e

echo "==================================="
echo "CobaltDB Docker Build Test"
echo "==================================="
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker is not running${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker is running${NC}"

# Test main Dockerfile
echo ""
echo "Testing main Dockerfile..."
if docker build -t cobaltdb:test -f Dockerfile . > /tmp/docker-build-main.log 2>&1; then
    echo -e "${GREEN}✓ Main Dockerfile builds successfully${NC}"
    docker rmi cobaltdb:test > /dev/null 2>&1
else
    echo -e "${RED}✗ Main Dockerfile build failed${NC}"
    echo "Build log:"
    cat /tmp/docker-build-main.log
    exit 1
fi

# Test backup Dockerfile
echo ""
echo "Testing Dockerfile.backup..."
if docker build -t cobaltdb-backup:test -f Dockerfile.backup . > /tmp/docker-build-backup.log 2>&1; then
    echo -e "${GREEN}✓ Dockerfile.backup builds successfully${NC}"
    docker rmi cobaltdb-backup:test > /dev/null 2>&1
else
    echo -e "${RED}✗ Dockerfile.backup build failed${NC}"
    echo "Build log:"
    cat /tmp/docker-build-backup.log
    exit 1
fi

# Test docker-compose config
echo ""
echo "Testing docker-compose configuration..."
if docker-compose config > /tmp/docker-compose-config.log 2>&1; then
    echo -e "${GREEN}✓ docker-compose.yml is valid${NC}"
else
    echo -e "${RED}✗ docker-compose.yml is invalid${NC}"
    cat /tmp/docker-compose-config.log
    exit 1
fi

echo ""
echo "==================================="
echo -e "${GREEN}All Docker tests passed!${NC}"
echo "==================================="
echo ""
echo "To start the services, run:"
echo "  docker-compose up -d"
echo ""
