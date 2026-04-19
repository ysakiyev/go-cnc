#!/bin/bash

# Script to run the C&C client
# Usage: ./run_client.sh [server_address]
# Default server: localhost:54321

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default server address
SERVER_ADDR="${1:-localhost:54321}"

echo -e "${GREEN}Starting C&C client...${NC}"
echo -e "${YELLOW}Connecting to server: $SERVER_ADDR${NC}"
echo -e "${YELLOW}You will see a list of connected agents to select from${NC}"

# Check if binary exists, build if not
if [ ! -f "bin/client" ]; then
    echo "Client binary not found, building..."
    go build -o bin/client cmd/client/main.go
fi

./bin/client -server "$SERVER_ADDR"