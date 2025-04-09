#!/bin/bash

# Test CREATE TABLE
echo "CREATE TABLE users (id INT, name STRING, age INT);"

# Test INSERT
echo "INSERT INTO users VALUES (1, 'John', 25);"

# Test SELECT
echo "SELECT * FROM users WHERE id = 1;"

# Test UPDATE
echo "UPDATE users SET age = 26 WHERE id = 1;"

# Verify UPDATE
echo "SELECT * FROM users WHERE id = 1;"

# Test DELETE
echo "DELETE FROM users WHERE id = 1;"

# Verify DELETE
echo "SELECT * FROM users WHERE id = 1;"

# Test edge cases
echo "INSERT INTO users VALUES (2, NULL, 30);"  # Test NULL values
echo "SELECT * FROM non_existent;"  # Test non-existent table
echo "UPDATE users SET age = 31 WHERE id = 999;"  # Test non-existent record
echo "DELETE FROM users WHERE id = 999;"  # Test non-existent record

# Test SELECT with specific columns
echo "SELECT name, age FROM users;"

# Test SELECT with complex WHERE condition
echo "SELECT * FROM users WHERE age > 20 AND name = 'John';"

# Test UPDATE multiple columns
echo "UPDATE users SET name = 'Jane', age = 27 WHERE id = 1;"

# Test multiple operations in sequence
for i in {1..5}; do
    echo "INSERT INTO users VALUES ($i, 'User$i', $((20 + i)));"
done

# Update multiple records
echo "UPDATE users SET age = 30 WHERE age < 25;"

# Delete multiple records
echo "DELETE FROM users WHERE age = 30;"

# Verify final state
echo "SELECT * FROM users;"

# Exit
echo "exit;" 