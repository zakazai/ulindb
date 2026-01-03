package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/lexer"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *Statement
		wantErr bool
	}{
		{
			name:  "Select all from table",
			input: "SELECT * FROM tablex",
			want: &Statement{
				Type: "SELECT",
				SelectStatement: &SelectStatement{
					Table:   "tablex",
					Columns: []string{"*"},
				},
			},
		},
		{
			name:  "Select with where clause",
			input: "SELECT a FROM tablex WHERE a = 1",
			want: &Statement{
				Type: "SELECT",
				SelectStatement: &SelectStatement{
					Table:   "tablex",
					Columns: []string{"a"},
					Where: map[string]interface{}{
						"a": float64(1),
					},
				},
			},
		},
		{
			name:  "Create table",
			input: "CREATE TABLE tablex (id INT, name TEXT)",
			want: &Statement{
				Type: "CREATE",
				CreateStatement: &CreateStatement{
					Table: "tablex",
					Columns: []struct {
						Name     string
						Type     string
						Nullable bool
					}{
						{Name: "id", Type: "INT", Nullable: true},
						{Name: "name", Type: "TEXT", Nullable: true},
					},
				},
			},
		},
		{
			name:    "Invalid SQL",
			input:   "INVALID SQL",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
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
			stmt, err := p.parseInsert()
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
			stmt, err := p.parseUpdate()
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
			stmt, err := p.parseDelete()
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
			stmt, err := p.parseCreate()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, stmt)
		})
	}
}

func TestParserErrors(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedError string
	}{
		{
			name:          "Missing_table_name",
			input:         "CREATE TABLE",
			expectedError: "expected table name",
		},
		{
			name:          "Invalid_column_definition",
			input:         "CREATE TABLE users (id)",
			expectedError: "expected column type",
		},
		{
			name:          "Missing_values",
			input:         "INSERT INTO users",
			expectedError: "expected VALUES",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedError)
		})
	}
}
