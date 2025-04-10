package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/lexer"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *SelectStatement
	}{
		{
			name:  "Select_all_from_table",
			input: "SELECT * FROM users",
			expected: &SelectStatement{
				Columns: []string{"*"},
				Table:   "users",
			},
		},
		{
			name:  "Select_specific_columns",
			input: "SELECT id, name FROM users",
			expected: &SelectStatement{
				Columns: []string{"id", "name"},
				Table:   "users",
			},
		},
		{
			name:  "Select_with_where_clause",
			input: "SELECT * FROM users WHERE id = 1",
			expected: &SelectStatement{
				Columns: []string{"*"},
				Table:   "users",
				Where: map[string]interface{}{
					"id": float64(1),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			p := New(l)
			stmt, err := p.Parse()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, stmt)
		})
	}
}

func TestParseInsert(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *InsertStatement
	}{
		{
			name:  "Insert_into_table",
			input: "INSERT INTO users VALUES (1, 'test')",
			expected: &InsertStatement{
				Table: "users",
				Values: map[string]interface{}{
					"column1": float64(1),
					"column2": "test",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			p := New(l)
			stmt, err := p.Parse()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, stmt)
		})
	}
}

func TestParseUpdate(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *UpdateStatement
	}{
		{
			name:  "Update_table",
			input: "UPDATE users SET name = 'updated' WHERE id = 1",
			expected: &UpdateStatement{
				Table: "users",
				Set: map[string]interface{}{
					"name": "updated",
				},
				Where: map[string]interface{}{
					"id": float64(1),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			p := New(l)
			stmt, err := p.Parse()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, stmt)
		})
	}
}

func TestParseDelete(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *DeleteStatement
	}{
		{
			name:  "Delete_from_table",
			input: "DELETE FROM users WHERE id = 1",
			expected: &DeleteStatement{
				Table: "users",
				Where: map[string]interface{}{
					"id": float64(1),
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			p := New(l)
			stmt, err := p.Parse()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, stmt)
		})
	}
}

func TestParseCreate(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *CreateStatement
	}{
		{
			name:  "Create_table",
			input: "CREATE TABLE users (id INT, name STRING)",
			expected: &CreateStatement{
				Table: "users",
				Columns: []struct {
					Name     string
					Type     string
					Nullable bool
				}{
					{Name: "id", Type: "INT", Nullable: true},
					{Name: "name", Type: "STRING", Nullable: true},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			p := New(l)
			stmt, err := p.Parse()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, stmt)
		})
	}
}
