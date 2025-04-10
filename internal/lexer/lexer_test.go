package lexer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/lexer"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []lexer.Token
	}{
		{
			name:  "Select_single_column",
			input: "SELECT a",
			expected: []lexer.Token{
				{Type: lexer.KEYWORD, Literal: "SELECT"},
				{Type: lexer.IDENTIFIER, Literal: "a"},
			},
		},
		{
			name:  "Select_number",
			input: "SELECT 1",
			expected: []lexer.Token{
				{Type: lexer.KEYWORD, Literal: "SELECT"},
				{Type: lexer.NUMBER, Literal: "1"},
			},
		},
		{
			name:  "Select_all_from_table",
			input: "SELECT * FROM tablex;",
			expected: []lexer.Token{
				{Type: lexer.KEYWORD, Literal: "SELECT"},
				{Type: lexer.ASTERISK, Literal: "*"},
				{Type: lexer.KEYWORD, Literal: "FROM"},
				{Type: lexer.IDENTIFIER, Literal: "tablex"},
				{Type: lexer.SEMICOLON, Literal: ";"},
			},
		},
		{
			name:  "Create_table",
			input: "CREATE TABLE u (id INT, name TEXT)",
			expected: []lexer.Token{
				{Type: lexer.KEYWORD, Literal: "CREATE"},
				{Type: lexer.KEYWORD, Literal: "TABLE"},
				{Type: lexer.IDENTIFIER, Literal: "u"},
				{Type: lexer.LPAREN, Literal: "("},
				{Type: lexer.IDENTIFIER, Literal: "id"},
				{Type: lexer.KEYWORD, Literal: "INT"},
				{Type: lexer.COMMA, Literal: ","},
				{Type: lexer.IDENTIFIER, Literal: "name"},
				{Type: lexer.KEYWORD, Literal: "TEXT"},
				{Type: lexer.RPAREN, Literal: ")"},
			},
		},
		{
			name:  "Insert_into_table",
			input: "INSERT INTO users VALUES (105, 233)",
			expected: []lexer.Token{
				{Type: lexer.KEYWORD, Literal: "INSERT"},
				{Type: lexer.KEYWORD, Literal: "INTO"},
				{Type: lexer.IDENTIFIER, Literal: "users"},
				{Type: lexer.KEYWORD, Literal: "VALUES"},
				{Type: lexer.LPAREN, Literal: "("},
				{Type: lexer.NUMBER, Literal: "105"},
				{Type: lexer.COMMA, Literal: ","},
				{Type: lexer.NUMBER, Literal: "233"},
				{Type: lexer.RPAREN, Literal: ")"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			tokens := []lexer.Token{}
			for {
				tok := l.NextToken()
				tokens = append(tokens, tok)
				if tok.Type == lexer.EOF {
					break
				}
			}
			assert.Equal(t, tt.expected, tokens[:len(tokens)-1])
		})
	}
}
