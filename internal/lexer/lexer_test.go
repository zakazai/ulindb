package lexer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zakazai/ulin-db/internal/lexer"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		tokens []lexer.Token
		err    error
	}{
		{
			name:  "Select single column",
			input: "select a",
			tokens: []lexer.Token{
				{
					Type:    lexer.KEYWORD,
					Literal: "select",
				},
				{
					Type:    lexer.IDENTIFIER,
					Literal: "a",
				},
			},
			err: nil,
		},
		{
			name:  "Select number",
			input: "select 1",
			tokens: []lexer.Token{
				{
					Type:    lexer.KEYWORD,
					Literal: "select",
				},
				{
					Type:    lexer.NUMBER,
					Literal: "1",
				},
			},
			err: nil,
		},
		{
			name:  "Select all from table",
			input: "select * from tablex;",
			tokens: []lexer.Token{
				{
					Type:    lexer.KEYWORD,
					Literal: "select",
				},
				{
					Type:    lexer.ASTERISK,
					Literal: "*",
				},
				{
					Type:    lexer.KEYWORD,
					Literal: "from",
				},
				{
					Type:    lexer.IDENTIFIER,
					Literal: "tablex",
				},
				{
					Type:    lexer.SEMICOLON,
					Literal: ";",
				},
			},
			err: nil,
		},
		{
			name:  "Create table",
			input: "CREATE TABLE u (id INT, name TEXT)",
			tokens: []lexer.Token{
				{
					Type:    lexer.KEYWORD,
					Literal: "CREATE",
				},
				{
					Type:    lexer.KEYWORD,
					Literal: "TABLE",
				},
				{
					Type:    lexer.IDENTIFIER,
					Literal: "u",
				},
				{
					Type:    lexer.LPAREN,
					Literal: "(",
				},
				{
					Type:    lexer.IDENTIFIER,
					Literal: "id",
				},
				{
					Type:    lexer.KEYWORD,
					Literal: "INT",
				},
				{
					Type:    lexer.COMMA,
					Literal: ",",
				},
				{
					Type:    lexer.IDENTIFIER,
					Literal: "name",
				},
				{
					Type:    lexer.KEYWORD,
					Literal: "TEXT",
				},
				{
					Type:    lexer.RPAREN,
					Literal: ")",
				},
			},
			err: nil,
		},
		{
			name:  "Insert into table",
			input: "insert into users Values (105, 233)",
			tokens: []lexer.Token{
				{
					Type:    lexer.KEYWORD,
					Literal: "insert",
				},
				{
					Type:    lexer.KEYWORD,
					Literal: "into",
				},
				{
					Type:    lexer.IDENTIFIER,
					Literal: "users",
				},
				{
					Type:    lexer.KEYWORD,
					Literal: "Values",
				},
				{
					Type:    lexer.LPAREN,
					Literal: "(",
				},
				{
					Type:    lexer.NUMBER,
					Literal: "105",
				},
				{
					Type:    lexer.COMMA,
					Literal: ",",
				},
				{
					Type:    lexer.NUMBER,
					Literal: "233",
				},
				{
					Type:    lexer.RPAREN,
					Literal: ")",
				},
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lexer.New(tt.input)
			var tokens []lexer.Token
			for {
				tok := l.NextToken()
				if tok.Type == lexer.EOF {
					break
				}
				if tok.Type == lexer.WHITESPACE {
					continue
				}
				tokens = append(tokens, tok)
			}
			assert.Equal(t, tt.tokens, tokens)
		})
	}
}
