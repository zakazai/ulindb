#!/bin/bash

# Colors for better output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=======================================================${NC}"
echo -e "${YELLOW}Running UlinDB Integration Tests${NC}"
echo -e "${YELLOW}=======================================================${NC}"

# Clean up any existing data
echo -e "${BLUE}Cleaning up old data...${NC}"
rm -rf data
mkdir -p data
mkdir -p data/parquet

# Build the database
echo -e "${BLUE}Building UlinDB...${NC}"
go build -o ulindb ./cmd/ulindb
if [ $? -ne 0 ]; then
    echo -e "${RED}Build failed${NC}"
    exit 1
fi

# Run the Go integration tests
echo -e "${BLUE}Running integration tests...${NC}"
go test ./internal/integration

# Check exit code
EXIT_CODE=$?

# Print summary
echo -e "\n${YELLOW}=======================================================${NC}"
echo -e "${YELLOW}Integration Test Summary${NC}"
echo -e "${YELLOW}=======================================================${NC}"

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}All tests passed successfully!${NC}"
    echo -e "\n${GREEN}The UlinDB SQL server is ready to use.${NC}"
    echo "Run './ulindb' to start an interactive session"
    
    # Run sample query to demonstrate
    echo -e "\n${BLUE}Sample query:${NC}"
    echo "CREATE TABLE users (id INT, name STRING);" | ./ulindb
    echo "INSERT INTO users VALUES (1, 'Demo User');" | ./ulindb
    echo "SELECT * FROM users;" | ./ulindb
else
    echo -e "${RED}Integration tests failed!${NC}"
fi

# Exit with the same code as the tests
exit $EXIT_CODE