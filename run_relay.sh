#!/bin/bash

# Script to run the C&C relay plane
# Usage: ./run_relay.sh

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="$SCRIPT_DIR"

echo -e "${GREEN}Starting relay plane...${NC}"
echo -e "${YELLOW}Relay will listen on localhost:54400${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"

if [ ! -f "bin/relay" ]; then
    echo "Relay binary not found, building..."
    go build -o bin/relay cmd/relay/main.go
fi

./bin/relay -cp="$CONFIG_PATH" -cn=config
