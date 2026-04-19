#!/bin/bash

# Script to run the C&C server
# Usage: ./run_server.sh

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the current directory for config path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="$SCRIPT_DIR"

echo -e "${GREEN}Starting C&C server...${NC}"
echo -e "${YELLOW}Server will listen on localhost:54321${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"

# Check if binary exists, build if not
if [ ! -f "bin/server" ]; then
    echo "Server binary not found, building..."
    go build -o bin/server cmd/server/main.go
fi

./bin/server -cp="$CONFIG_PATH" -cn=config