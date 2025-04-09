#!/bin/bash

# Build the server
echo "Building UlinDB SQL Server..."
go build -o ulin-db

# Check if build was successful
if [ $? -ne 0 ]; then
    echo "Build failed"
    exit 1
fi

# Make ulin-db executable
chmod +x ulin-db

# Run the server
echo "Starting UlinDB SQL Server..."
./ulin-db << 'EOF'
CREATE TABLE users (id INT, name STRING, age INT);
INSERT INTO users VALUES (1, 'John', 25);
SELECT * FROM users WHERE id = 1;
UPDATE users SET age = 26 WHERE id = 1;
SELECT * FROM users WHERE id = 1;
DELETE FROM users WHERE id = 1;
SELECT * FROM users WHERE id = 1;
INSERT INTO users VALUES (2, NULL, 30);
SELECT * FROM non_existent;
UPDATE users SET age = 31 WHERE id = 999;
DELETE FROM users WHERE id = 999;
SELECT name, age FROM users;
SELECT * FROM users WHERE age > 20 AND name = 'John';
UPDATE users SET name = 'Jane', age = 27 WHERE id = 1;
INSERT INTO users VALUES (1, 'User1', 21);
INSERT INTO users VALUES (2, 'User2', 22);
INSERT INTO users VALUES (3, 'User3', 23);
INSERT INTO users VALUES (4, 'User4', 24);
INSERT INTO users VALUES (5, 'User5', 25);
UPDATE users SET age = 30 WHERE age < 25;
DELETE FROM users WHERE age = 30;
SELECT * FROM users;
exit;
EOF 