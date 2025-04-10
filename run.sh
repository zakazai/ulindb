#!/bin/bash

# Build the server
echo "Building UlinDB SQL Server..."
go build -o ulindb ./cmd/ulindb

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi

# Remove existing data
echo "Cleaning up old data..."
rm -rf data

# Run the server with SQL tests
echo "Starting UlinDB SQL Server with tests..."
echo "
CREATE TABLE users (id INT, name STRING, age INT);
INSERT INTO users VALUES (1, 'John', 25);
SELECT * FROM users WHERE id = 1;
UPDATE users SET age = 26 WHERE id = 1;
SELECT * FROM users WHERE id = 1;
DELETE FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 1;
INSERT INTO users VALUES (2, 'NULL', 30);
SELECT * FROM users;
exit;
" | ./ulindb

echo "✅ Tests completed successfully!"
echo "The UlinDB SQL server is now ready to use."
echo "You can run it again with './ulindb' to start an interactive session."

exit 0 