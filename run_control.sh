#!/bin/bash

# Script to run the C&C control plane
# Usage: ./run_control.sh

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="$SCRIPT_DIR"

echo -e "${GREEN}Starting control plane...${NC}"
echo -e "${YELLOW}Control will listen on localhost:54321${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop${NC}"

if [ ! -f "bin/control" ]; then
    echo "Control binary not found, building..."
    go build -o bin/control cmd/control/main.go
fi

./bin/control -cp="$CONFIG_PATH" -cn=config
