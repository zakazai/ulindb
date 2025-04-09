#!/bin/bash

# Build the server
echo "Building UlinDB SQL Server..."
go build -o ulin-db

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi

# Run the server
echo "Starting UlinDB SQL Server..."
./ulin-db 