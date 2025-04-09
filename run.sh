#!/bin/bash

# Build the server
echo "Building UlinDB SQL Server..."
go build -o ulindb ./cmd/ulindb

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi

# Make sql_tests.sh executable
chmod +x sql_tests.sh

# Run the server with SQL tests
echo "Starting UlinDB SQL Server with tests..."
./ulindb 