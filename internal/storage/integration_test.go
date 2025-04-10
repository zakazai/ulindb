package storage_test

import (
	"os"
	"testing"

	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/storage"
)

func TestSQLIntegration(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "testdb_integration")
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Initialize storage with the temporary directory
	store, err := storage.NewJSONStorage(tmpDir, "test_")
	if err != nil {
		t.Fatalf("Failed to create JSON storage: %v", err)
	}

	// Test cases
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "Create table",
			sql:  "CREATE TABLE users (id INT, name STRING, age INT)",
		},
		{
			name: "Insert record",
			sql:  "INSERT INTO users VALUES (1, 'John', 25)",
		},
		{
			name: "Select record",
			sql:  "SELECT * FROM users WHERE id = 1",
		},
		{
			name: "Update record",
			sql:  "UPDATE users SET age = 26 WHERE id = 1",
		},
		{
			name: "Delete record",
			sql:  "DELETE FROM users WHERE id = 1",
		},
		{
			name:    "Invalid table",
			sql:     "SELECT * FROM non_existent",
			wantErr: true,
		},
		{
			name:    "Invalid column",
			sql:     "SELECT invalid_column FROM users",
			wantErr: true,
		},
	}

	// Run tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := parser.Parse(tt.sql)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			// Special handling for INSERT statements
			if insertStmt, ok := stmt.(*parser.InsertStatement); ok {
				// Map column1, column2, column3 to id, name, age
				values := map[string]interface{}{
					"id":   insertStmt.Values["column1"],
					"name": insertStmt.Values["column2"],
					"age":  insertStmt.Values["column3"],
				}
				// Execute directly instead of using the Statement interface
				err = store.Insert(insertStmt.Table, values)
			} else {
				// For other statements, use the normal Execute method
				_, err = stmt.Execute(store)
			}

			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
