#!/bin/bash

# Build script for all components
# Usage: ./build.sh

set -e

echo "Building C&C components..."

# Create bin directory if it doesn't exist
mkdir -p bin

echo "Building server..."
go build -o bin/server cmd/server/main.go

echo "Building control..."
go build -o bin/control cmd/control/main.go

echo "Building relay..."
go build -o bin/relay cmd/relay/main.go

echo "Building agent..."
go build -o bin/agent cmd/agent/main.go

echo "Building client..."
go build -o bin/client cmd/client/main.go

echo "All components built successfully in bin/ directory"