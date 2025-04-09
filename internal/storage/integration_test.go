package storage_test

import (
	"testing"

	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/storage"
)

func TestSQLIntegration(t *testing.T) {
	// Initialize storage
	store := storage.NewJSONStorage("test_data.json")
	if err := store.Init(); err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
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

			_, err = stmt.Execute(store)
			if (err != nil) != tt.wantErr {
				t.Errorf("Execute() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
