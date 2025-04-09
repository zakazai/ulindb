package ulindb

import (
	"testing"
)

func TestSQLIntegration(t *testing.T) {
	// Initialize storage
	storage, err := NewStorage(StorageConfig{
		Type:     InMemoryStorageType,
		FilePath: "",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Test CREATE TABLE
	createTableSQL := "CREATE TABLE users (id INT, name STRING, age INT)"
	stmt := parse(createTableSQL)
	if stmt.err != nil {
		t.Fatalf("Failed to parse CREATE TABLE: %v", stmt.err)
	}

	err = storage.CreateTable(&Table{
		Name:    "users",
		Columns: stmt.createStatement.columns,
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test INSERT
	insertSQL := "INSERT INTO users VALUES (1, 'John', 25)"
	stmt = parse(insertSQL)
	if stmt.err != nil {
		t.Fatalf("Failed to parse INSERT: %v", stmt.err)
	}

	err = storage.Insert("users", map[string]interface{}{
		"id":   1,
		"name": "John",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Test SELECT
	selectSQL := "SELECT * FROM users WHERE id = 1"
	stmt = parse(selectSQL)
	if stmt.err != nil {
		t.Fatalf("Failed to parse SELECT: %v", stmt.err)
	}

	results, err := storage.Select("users", []string{"*"}, "id = 1")
	if err != nil {
		t.Fatalf("Failed to select data: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0]["name"] != "John" || results[0]["age"] != 25 {
		t.Fatalf("Unexpected result: %v", results[0])
	}

	// Test UPDATE
	updateSQL := "UPDATE users SET age = 26 WHERE id = 1"
	stmt = parse(updateSQL)
	if stmt.err != nil {
		t.Fatalf("Failed to parse UPDATE: %v", stmt.err)
	}

	err = storage.Update("users", map[string]interface{}{
		"age": 26,
	}, "id = 1")
	if err != nil {
		t.Fatalf("Failed to update data: %v", err)
	}

	// Verify UPDATE
	results, err = storage.Select("users", []string{"*"}, "id = 1")
	if err != nil {
		t.Fatalf("Failed to select after update: %v", err)
	}
	if results[0]["age"] != 26 {
		t.Fatalf("Update failed, age is still %v", results[0]["age"])
	}

	// Test DELETE
	deleteSQL := "DELETE FROM users WHERE id = 1"
	stmt = parse(deleteSQL)
	if stmt.err != nil {
		t.Fatalf("Failed to parse DELETE: %v", stmt.err)
	}

	err = storage.Delete("users", "id = 1")
	if err != nil {
		t.Fatalf("Failed to delete data: %v", err)
	}

	// Verify DELETE
	results, err = storage.Select("users", []string{"*"}, "id = 1")
	if err != nil {
		t.Fatalf("Failed to select after delete: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("Delete failed, still found %d results", len(results))
	}

	// Test edge cases
	// 1. Insert with missing required field
	err = storage.Insert("users", map[string]interface{}{
		"id": 2,
		// Missing name field
		"age": 30,
	})
	if err == nil {
		t.Error("Expected error for missing required field, got none")
	}

	// 2. Select from non-existent table
	_, err = storage.Select("non_existent", []string{"*"}, "")
	if err == nil {
		t.Error("Expected error for non-existent table, got none")
	}

	// 3. Update non-existent record
	err = storage.Update("users", map[string]interface{}{
		"age": 31,
	}, "id = 999")
	if err != nil {
		t.Errorf("Update on non-existent record should not error: %v", err)
	}

	// 4. Delete non-existent record
	err = storage.Delete("users", "id = 999")
	if err != nil {
		t.Errorf("Delete on non-existent record should not error: %v", err)
	}
}
