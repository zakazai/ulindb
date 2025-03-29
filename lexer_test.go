package ulindb

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		tokens []*Token
		err    error
	}{
		{
			name:  "Select single column",
			input: "select a",
			tokens: []*Token{
				{
					Type:  KeywordType,
					Value: string(SelectKeyword),
					Pos:   Position{Line: 0, Col: 0},
				},
				{
					Type:  IdentifierType,
					Value: "a",
					Pos:   Position{Line: 0, Col: 7},
				},
			},
			err: nil,
		},
		{
			name:  "Select number",
			input: "select 1",
			tokens: []*Token{
				{
					Type:  KeywordType,
					Value: string(SelectKeyword),
					Pos:   Position{Line: 0, Col: 0},
				},
				{
					Type:  NumberType,
					Value: "1",
					Pos:   Position{Line: 0, Col: 7},
				},
			},
			err: nil,
		},
		{
			name:  "Select all from table",
			input: "select * from tablex;",
			tokens: []*Token{
				{
					Type:  KeywordType,
					Value: string(SelectKeyword),
					Pos:   Position{Line: 0, Col: 0},
				},
				{
					Type:  SymbolType,
					Value: "*",
					Pos:   Position{Line: 0, Col: 7},
				},
				{
					Type:  KeywordType,
					Value: string(FromKeyword),
					Pos:   Position{Line: 0, Col: 9},
				},
				{
					Type:  IdentifierType,
					Value: "tablex",
					Pos:   Position{Line: 0, Col: 14},
				},
				{
					Type:  SymbolType,
					Value: ";",
					Pos:   Position{Line: 0, Col: 20},
				},
			},
			err: nil,
		},
		{
			name:  "Select with concatenation",
			input: "select 'foo' || 'bar';",
			tokens: []*Token{
				{
					Pos:   Position{Col: 0, Line: 0},
					Value: string(SelectKeyword),
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 7, Line: 0},
					Value: "foo",
					Type:  StringType,
				},
				{
					Pos:   Position{Col: 13, Line: 0},
					Value: string(ConcatSymbol),
					Type:  SymbolType,
				},
				{
					Pos:   Position{Col: 16, Line: 0},
					Value: "bar",
					Type:  StringType,
				},
				{
					Pos:   Position{Col: 21, Line: 0},
					Value: string(SemicolonSymbol),
					Type:  SymbolType,
				},
			},
			err: nil,
		},
		{
			name:  "Create table",
			input: "CREATE TABLE u (id INT, name TEXT)",
			tokens: []*Token{
				{
					Pos:   Position{Col: 0, Line: 0},
					Value: string(CreateKeyword),
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 7, Line: 0},
					Value: string(TableKeyword),
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 13, Line: 0},
					Value: "u",
					Type:  IdentifierType,
				},
				{
					Pos:   Position{Col: 15, Line: 0},
					Value: "(",
					Type:  SymbolType,
				},
				{
					Pos:   Position{Col: 16, Line: 0},
					Value: "id",
					Type:  IdentifierType,
				},
				{
					Pos:   Position{Col: 19, Line: 0},
					Value: "int",
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 22, Line: 0},
					Value: ",",
					Type:  SymbolType,
				},
				{
					Pos:   Position{Col: 24, Line: 0},
					Value: "name",
					Type:  IdentifierType,
				},
				{
					Pos:   Position{Col: 29, Line: 0},
					Value: "text",
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 33, Line: 0},
					Value: ")",
					Type:  SymbolType,
				},
			},
			err: nil,
		},
		{
			name:  "Insert into table",
			input: "insert into users Values (105, 233)",
			tokens: []*Token{
				{
					Pos:   Position{Col: 0, Line: 0},
					Value: string(InsertKeyword),
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 7, Line: 0},
					Value: string(IntoKeyword),
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 12, Line: 0},
					Value: "users",
					Type:  IdentifierType,
				},
				{
					Pos:   Position{Col: 18, Line: 0},
					Value: string(ValuesKeyword),
					Type:  KeywordType,
				},
				{
					Pos:   Position{Col: 25, Line: 0},
					Value: "(",
					Type:  SymbolType,
				},
				{
					Pos:   Position{Col: 26, Line: 0},
					Value: "105",
					Type:  NumberType,
				},
				{
					Pos:   Position{Col: 29, Line: 0},
					Value: ",",
					Type:  SymbolType,
				},
				{
					Pos:   Position{Col: 31, Line: 0},
					Value: "233",
					Type:  NumberType,
				},
				{
					Pos:   Position{Col: 34, Line: 0},
					Value: ")",
					Type:  SymbolType,
				},
			},
			err: nil,
		},
		{
			name:   "Invalid select statement",
			input:  "selectx * from table",
			tokens: nil,
			err:    fmt.Errorf("expected SELECT, CREATE or INSERT but got selectx"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tokens, err := lex(test.input)
			assert.Equal(t, len(test.tokens), len(tokens), test.input)
			assert.Equal(t, test.err, err)

			if tokens != nil {
				for i, tok := range tokens {
					assert.Equal(t, test.tokens[i], tok, test.input)
				}
			}
		})
	}
}

func TestLexKeyword(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		keyword bool
	}{
		{
			name:    "Valid keyword",
			keyword: true,
			value:   "Select ",
		},
		{
			name:    "Invalid keyword",
			keyword: false,
			value:   "asdasdasdas",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tok, _, ok := lexKeyword(test.value, 0)
			assert.Equal(t, test.keyword, ok, test.value)
			if ok {
				test.value = strings.TrimSpace(test.value)
				assert.Equal(t, strings.ToLower(test.value), tok.Value, test.value)
			}
		})
	}
}

func TestLexSymbol(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		symbol bool
	}{
		{
			name:   "Semicolon symbol",
			symbol: true,
			value:  ";",
		},
		{
			name:   "Asterisk symbol",
			symbol: true,
			value:  "*",
		},
		{
			name:   "Left parenthesis symbol",
			symbol: true,
			value:  "(",
		},
		{
			name:   "Invalid symbol",
			symbol: false,
			value:  "asdas",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tok, _, ok := lexSymbol(test.value, 0)
			assert.Equal(t, test.symbol, ok, test.value)
			if ok {
				test.value = strings.TrimSpace(test.value)
				assert.Equal(t, strings.ToLower(test.value), tok.Value, test.value)
			}
		})
	}
}

func TestLexIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		value string
		ident bool
	}{
		{
			name:  "Valid identifier - Users",
			ident: true,
			value: "Users",
		},
		{
			name:  "Valid identifier - Transaction",
			ident: true,
			value: "Transaction",
		},
		{
			name:  "Invalid identifier - quoted",
			ident: false,
			value: "\"asd\"",
		},
		{
			name:  "Invalid identifier - starts with number",
			ident: false,
			value: "2asdasd",
		},
		{
			name:  "Invalid identifier - number",
			ident: false,
			value: "23",
		},
		{
			name:  "Valid identifier - Tab_Transaction",
			ident: true,
			value: "Tab_Transaction",
		},
		{
			name:  "Invalid identifier - special characters",
			ident: false,
			value: "&*^",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tok, _, ok := lexIdentifier(test.value, 0)
			assert.Equal(t, test.ident, ok, test.value)
			if ok {
				test.value = strings.TrimSpace(test.value)
				assert.Equal(t, test.value, tok.Value, test.value)
			}
		})
	}
}

func TestLexString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		value string
		str   bool
	}{
		{
			name:  "Valid string - Users",
			str:   true,
			input: "'Users'",
			value: "Users",
		},
		{
			name:  "Valid string - Transaction123",
			str:   true,
			input: "'Transaction123'",
			value: "Transaction123",
		},
		{
			name:  "Valid string - 123123asd",
			str:   true,
			input: "'123123asd'",
			value: "123123asd",
		},
		{
			name:  "Invalid string - missing closing quote",
			str:   false,
			input: "'asdasd",
		},
		{
			name:  "Invalid string - missing opening quote",
			str:   false,
			input: "asdasd'",
		},
		{
			name:  "Invalid string - number",
			str:   false,
			input: "23",
		},
		{
			name:  "Invalid string - identifier",
			str:   false,
			input: "Tab_Transaction",
		},
		{
			name:  "Invalid string - special characters",
			str:   false,
			input: "T&*^",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tok, _, ok := lexString(test.input, 0)
			assert.Equal(t, test.str, ok, test.value)
			if ok {
				assert.Equal(t, test.value, tok.Value, test.value)
			}
		})
	}
}

func TestLexNumber(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		number bool
	}{
		{
			name:   "Valid number - integer",
			number: true,
			input:  "12345",
		},
		{
			name:   "Valid number - float",
			number: true,
			input:  "123.45",
		},
		{
			name:   "Invalid number - alphanumeric",
			number: false,
			input:  "12345abcde",
		},
		{
			name:   "Invalid number - starts with letters",
			number: false,
			input:  "abcde1234",
		},
		{
			name:   "Valid number - scientific notation",
			number: true,
			input:  "12345e3",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tok, _, ok := lexNumber(test.input, 0)
			assert.Equal(t, test.number, ok, test.input)
			if ok {
				assert.Equal(t, strings.TrimSpace(test.input), tok.Value, test.input)
			}
		})
	}
}
