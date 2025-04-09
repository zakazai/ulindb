package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSelect(t *testing.T) {
	tests := []struct {
		name   string
		input  []*Token
		output SelectStatement
	}{
		{
			name: "Select all from table",
			input: []*Token{
				{Type: KeywordType, Value: string(SelectKeyword)},
				{Type: SymbolType, Value: "*"},
				{Type: KeywordType, Value: string(FromKeyword)},
				{Type: IdentifierType, Value: "tablex"},
			},
			output: SelectStatement{
				items: []SelectItem{{All: true}},
				from:  FromItem{name: "tablex"},
			},
		},
		{
			name: "Select with where clause",
			input: []*Token{
				{Type: KeywordType, Value: string(SelectKeyword)},
				{Type: IdentifierType, Value: "a"},
				{Type: KeywordType, Value: string(FromKeyword)},
				{Type: IdentifierType, Value: "tablex"},
				{Type: KeywordType, Value: string(WhereKeyword)},
				{Type: IdentifierType, Value: "a = 1"},
			},
			output: SelectStatement{
				items: []SelectItem{{All: false, Column: "a"}},
				from:  FromItem{name: "tablex"},
				where: "a = 1",
			},
		},
		{
			name: "Select multiple columns",
			input: []*Token{
				{Type: KeywordType, Value: string(SelectKeyword)},
				{Type: IdentifierType, Value: "a"},
				{Type: IdentifierType, Value: "b"},
				{Type: KeywordType, Value: string(FromKeyword)},
				{Type: IdentifierType, Value: "tablex"},
			},
			output: SelectStatement{
				items: []SelectItem{{All: false, Column: "a"}, {All: false, Column: "b"}},
				from:  FromItem{name: "tablex"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := parseSelect(test.input)
			assert.Equal(t, test.output, result, "Test case: %s", test.name)
		})
	}
}

func TestParse(t *testing.T) {
	tests := []struct {
		name   string
		query  string
		output *Statement
	}{
		{
			name:  "Select all from table",
			query: "SELECT * FROM tablex",
			output: &Statement{
				err: nil,
				selectStatement: SelectStatement{
					items: []SelectItem{{All: true}},
					from:  FromItem{name: "tablex"},
				},
			},
		},
		{
			name:  "Select with where clause",
			query: "SELECT a FROM tablex WHERE a = 1",
			output: &Statement{
				err: nil,
				selectStatement: SelectStatement{
					items: []SelectItem{{All: false, Column: "a"}},
					from:  FromItem{name: "tablex"},
					where: "a = 1",
				},
			},
		},
		{
			name:  "Insert into table",
			query: "INSERT INTO tablex VALUES (1, 'test')",
			output: &Statement{
				err: nil,
				insertStatement: InsertStatement{
					table: "tablex",
					items: []InsertItem{},
				},
			},
		},
		{
			name:  "Update table",
			query: "UPDATE tablex SET a = 1 WHERE b = 2",
			output: &Statement{
				err: nil,
				updateStatement: UpdateStatement{
					table: "tablex",
					set:   map[string]string{"a": "1"},
					where: "b = 2",
				},
			},
		},
		{
			name:  "Delete from table",
			query: "DELETE FROM tablex WHERE a = 1",
			output: &Statement{
				err: nil,
				deleteStatement: DeleteStatement{
					from:  FromItem{name: "tablex"},
					where: "a = 1",
				},
			},
		},
		{
			name:  "Create table",
			query: "CREATE TABLE tablex (id INT, name TEXT)",
			output: &Statement{
				err: nil,
				createStatement: CreateStatement{
					tableName: "tablex",
					columns: []ColumnDefinition{
						{name: "id", typ: "INT"},
						{name: "name", typ: "TEXT"},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := parse(test.query)
			assert.Equal(t, test.output, result, "Test case: %s", test.name)
		})
	}
}
