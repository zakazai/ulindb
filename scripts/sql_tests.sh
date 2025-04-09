#!/bin/bash

# Test CREATE TABLE
printf "CREATE TABLE users (id INT, name STRING, age INT);\n"

# Test INSERT
printf "INSERT INTO users VALUES (1, 'John', 25);\n"

# Test SELECT
printf "SELECT * FROM users WHERE id = 1;\n"

# Test UPDATE
printf "UPDATE users SET age = 26 WHERE id = 1;\n"

# Verify UPDATE
printf "SELECT * FROM users WHERE id = 1;\n"

# Test DELETE
printf "DELETE FROM users WHERE id = 1;\n"

# Verify DELETE
printf "SELECT * FROM users WHERE id = 1;\n"

# Test edge cases
printf "INSERT INTO users VALUES (2, NULL, 30);\n"  # Test NULL values
printf "SELECT * FROM non_existent;\n"  # Test non-existent table
printf "UPDATE users SET age = 31 WHERE id = 999;\n"  # Test non-existent record
printf "DELETE FROM users WHERE id = 999;\n"  # Test non-existent record

# Test SELECT with specific columns
printf "SELECT name, age FROM users;\n"

# Test SELECT with complex WHERE condition
printf "SELECT * FROM users WHERE age > 20 AND name = 'John';\n"

# Test UPDATE multiple columns
printf "UPDATE users SET name = 'Jane', age = 27 WHERE id = 1;\n"

# Test multiple operations in sequence
for i in {1..5}; do
    printf "INSERT INTO users VALUES ($i, 'User$i', $((20 + i)));\n"
done

# Update multiple records
printf "UPDATE users SET age = 30 WHERE age < 25;\n"

# Delete multiple records
printf "DELETE FROM users WHERE age = 30;\n"

# Verify final state
printf "SELECT * FROM users;\n"

# Exit
printf "exit;\n" 