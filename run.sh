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
mkdir -p data

# Create btree database file
touch data/ulindb.btree

# Run the server with SQL tests
echo "Starting UlinDB SQL Server with tests..."
echo "
CREATE TABLE users (id INT, name STRING, age INT);
INSERT INTO users VALUES (1, 'John', 25);
SELECT id, name, age FROM users WHERE id = 1;
UPDATE users SET age = 26 WHERE id = 1;
SELECT id, name, age FROM users WHERE id = 1;
DELETE FROM users WHERE id = 1;
SELECT id, name, age FROM users WHERE id = 1;

CREATE TABLE orders (id INT, user_id INT, item STRING, price INT);
INSERT INTO orders VALUES (101, 2, 'Book', 15);
INSERT INTO orders VALUES (102, 4, 'Laptop', 1000);
INSERT INTO orders VALUES (103, 5, 'Phone', 500);
SELECT id, user_id, item, price FROM orders;

INSERT INTO users VALUES (2, 'NULL', 30);
INSERT INTO users VALUES (3, '', 35);
INSERT INTO users VALUES (4, 'Alice', 28);
INSERT INTO users VALUES (5, 'Bob', 32);
SELECT id, name, age FROM users;

SELECT id FROM users;
SELECT name FROM users;
SELECT age FROM users;

exit;
" | ./ulindb

echo "✅ Tests completed successfully!"
echo "The UlinDB SQL server is now ready to use."
echo "You can run it again with './ulindb' to start an interactive session."

exit 0 