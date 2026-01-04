#!/bin/bash

# Change to script directory
cd "$(dirname "$0")"

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}=======================================================${NC}"
echo -e "${YELLOW}Running UlinDB Integration Tests${NC}"
echo -e "${YELLOW}=======================================================${NC}"

echo -e "${BLUE}Cleaning up old data...${NC}"
rm -rf data
mkdir -p data/parquet

echo -e "${BLUE}Building UlinDB...${NC}"
if go build -o ulindb ./cmd/ulindb; then
    echo -e "${GREEN}Build successful!${NC}"
else
    echo -e "${RED}Build failed${NC}"
    exit 1
fi

echo -e "${BLUE}Running integration tests...${NC}"
go test ./internal/integration
EXIT_CODE=$?

echo -e "\n${YELLOW}=======================================================${NC}"
echo -e "${YELLOW}Integration Test Summary${NC}"
echo -e "${YELLOW}=======================================================${NC}"

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}All tests passed successfully!${NC}"
    echo -e "\n${GREEN}The UlinDB SQL server is ready to use.${NC}"
    echo "Run './ulindb' to start an interactive session"
    
    echo -e "\n${BLUE}Sample queries:${NC}"
    DEMO_SCRIPT=$(mktemp)
    cat > "$DEMO_SCRIPT" << 'EOF'
CREATE TABLE users (id INT, name STRING);
INSERT INTO users VALUES (1, 'Demo User');
SELECT * FROM users;
exit
EOF
    
    echo -e "${YELLOW}Commands:${NC}"
    grep -v "exit" "$DEMO_SCRIPT" | sed 's/^/  /'
    
    echo -e "\n${YELLOW}Output:${NC}"
    if [ -f ./ulindb ]; then
        cat "$DEMO_SCRIPT" | ./ulindb 2>&1 | grep -v "UlinDB SQL Server\|Type 'exit'\|Goodbye" | sed 's/^/  /' || true
    fi
    
    rm -f "$DEMO_SCRIPT"
else
    echo -e "${RED}Integration tests failed!${NC}"
fi

exit $EXIT_CODE
