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

	// Additional test cases
	// 5. Test SELECT with specific columns
	results, err = storage.Select("users", []string{"name", "age"}, "")
	if err != nil {
		t.Fatalf("Failed to select specific columns: %v", err)
	}

	// 6. Test SELECT with complex WHERE condition
	results, err = storage.Select("users", []string{"*"}, "age > 20 AND name = 'John'")
	if err != nil {
		t.Fatalf("Failed to select with complex condition: %v", err)
	}

	// 7. Test INSERT with NULL values
	err = storage.Insert("users", map[string]interface{}{
		"id":   3,
		"name": nil,
		"age":  35,
	})
	if err != nil {
		t.Fatalf("Failed to insert with NULL value: %v", err)
	}

	// 8. Test UPDATE multiple columns
	err = storage.Update("users", map[string]interface{}{
		"name": "Jane",
		"age":  27,
	}, "id = 1")
	if err != nil {
		t.Fatalf("Failed to update multiple columns: %v", err)
	}

	// 9. Test CREATE TABLE with duplicate column names
	createTableSQL = "CREATE TABLE duplicate (id INT, id INT)"
	stmt = parse(createTableSQL)
	if stmt.err == nil {
		t.Error("Expected error for duplicate column names, got none")
	}

	// 10. Test INSERT with wrong data types
	err = storage.Insert("users", map[string]interface{}{
		"id":   "not_an_int",
		"name": "Test",
		"age":  40,
	})
	if err == nil {
		t.Error("Expected error for wrong data type, got none")
	}

	// 11. Test SELECT with invalid column names
	_, err = storage.Select("users", []string{"invalid_column"}, "")
	if err == nil {
		t.Error("Expected error for invalid column name, got none")
	}

	// 12. Test UPDATE with invalid column names
	err = storage.Update("users", map[string]interface{}{
		"invalid_column": "value",
	}, "id = 1")
	if err == nil {
		t.Error("Expected error for invalid column name in UPDATE, got none")
	}

	// 13. Test DELETE with invalid condition
	err = storage.Delete("users", "invalid_column = 1")
	if err == nil {
		t.Error("Expected error for invalid condition in DELETE, got none")
	}

	// 14. Test multiple operations in sequence
	// Insert multiple records
	for i := 1; i <= 5; i++ {
		err = storage.Insert("users", map[string]interface{}{
			"id":   i,
			"name": "User" + string(rune('0'+i)),
			"age":  20 + i,
		})
		if err != nil {
			t.Fatalf("Failed to insert record %d: %v", i, err)
		}
	}

	// Update multiple records
	err = storage.Update("users", map[string]interface{}{
		"age": 30,
	}, "age < 25")
	if err != nil {
		t.Fatalf("Failed to update multiple records: %v", err)
	}

	// Delete multiple records
	err = storage.Delete("users", "age = 30")
	if err != nil {
		t.Fatalf("Failed to delete multiple records: %v", err)
	}

	// Verify final state
	results, err = storage.Select("users", []string{"*"}, "")
	if err != nil {
		t.Fatalf("Failed to verify final state: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("Expected 0 records after cleanup, got %d", len(results))
	}
}
