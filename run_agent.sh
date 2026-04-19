#!/bin/bash

# Script to run a C&C agent
# Usage: ./run_agent.sh [server_address]
# Default server: localhost:54321

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default server address
SERVER_ADDR="${1:-localhost:54321}"

echo -e "${GREEN}Starting C&C agent...${NC}"
echo -e "${YELLOW}Connecting to server: $SERVER_ADDR${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"

# Check if binary exists, build if not
if [ ! -f "bin/agent" ]; then
    echo "Agent binary not found, building..."
    go build -o bin/agent cmd/agent/main.go
fi

./bin/agent -server "$SERVER_ADDR"