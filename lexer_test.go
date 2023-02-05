package ulindb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input  string
		tokens []Token
		err    error
	}{
		{
			input: "select a",
			tokens: []Token{
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
			input: "select 1",
			tokens: []Token{
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
			input: "select * from tablex;",
			tokens: []Token{
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
			input: "select 'foo' || 'bar';",
			tokens: []Token{
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
			input: "CREATE TABLE u (id INT, name TEXT)",
			tokens: []Token{
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
		},
		{
			input: "insert into users Values (105, 233)",
			tokens: []Token{
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
	}

	for _, test := range tests {
		tokens, err := lex(test.input)
		assert.Equal(t, len(test.tokens), len(tokens), test.input)
		assert.Equal(t, test.err, err)

		for i, tok := range tokens {
			assert.Equal(t, &test.tokens[i], tok, test.input)
		}
	}
}

func TestLexKeyword(t *testing.T) {
	tests := []struct {
		value   string
		keyword bool
	}{
		{
			keyword: true,
			value:   "Select ",
		},
		{
			keyword: false,
			value:   "asdasdasdas",
		},
	}

	for _, test := range tests {
		tok, _, ok := lexKeyword(test.value, 0)
		assert.Equal(t, test.keyword, ok, test.value)
		if ok {
			test.value = strings.TrimSpace(test.value)
			assert.Equal(t, strings.ToLower(test.value), tok.Value, test.value)
		}
	}
}

func TestLexSymbol(t *testing.T) {
	tests := []struct {
		value  string
		symbol bool
	}{
		{
			symbol: true,
			value:  ";",
		},
		{
			symbol: true,
			value:  "*",
		},
		{
			symbol: true,
			value:  "(",
		},
		{
			symbol: false,
			value:  "asdas",
		},
	}

	for _, test := range tests {
		tok, _, ok := lexSymbol(test.value, 0)
		assert.Equal(t, test.symbol, ok, test.value)
		if ok {
			test.value = strings.TrimSpace(test.value)
			assert.Equal(t, strings.ToLower(test.value), tok.Value, test.value)
		}
	}
}

func TestLexIdentifier(t *testing.T) {
	tests := []struct {
		value string
		ident bool
	}{
		{
			ident: true,
			value: "Users",
		},
		{
			ident: true,
			value: "Transaction",
		},
		{
			ident: false,
			value: "\"asd\"",
		},
		{
			ident: false,
			value: "2asdasd",
		},
		{
			ident: false,
			value: "23",
		},
		{
			ident: true,
			value: "Tab_Transaction",
		},
		{
			ident: false,
			value: "T&*^",
		},
	}

	for _, test := range tests {
		tok, _, ok := lexIdentifier(test.value, 0)
		assert.Equal(t, test.ident, ok, test.value)
		if ok {
			test.value = strings.TrimSpace(test.value)
			assert.Equal(t, test.value, tok.Value, test.value)
		}
	}
}

func TestLexString(t *testing.T) {
	tests := []struct {
		input string
		value string
		str   bool
	}{
		{
			str:   true,
			input: "'Users'",
			value: "Users",
		},
		{
			str:   true,
			input: "'Transaction123'",
			value: "Transaction123",
		},
		{
			str:   true,
			input: "'123123asd'",
			value: "123123asd",
		},
		{
			str:   false,
			input: "'asdasd",
		},
		{
			str:   false,
			input: "asdasd'",
		},
		{
			str:   false,
			input: "23",
		},
		{
			str:   false,
			input: "Tab_Transaction",
		},
		{
			str:   false,
			input: "T&*^",
		},
	}

	for _, test := range tests {
		tok, _, ok := lexString(test.input, 0)
		assert.Equal(t, test.str, ok, test.value)
		if ok {
			// test.value = strings.TrimSpace(test.value)
			assert.Equal(t, test.value, tok.Value, test.value)
		}
	}
}

func TestLexNumber(t *testing.T) {
	tests := []struct {
		input  string
		number bool
	}{
		{
			number: true,
			input:  "12345",
		},
		{
			number: true,
			input:  "123.45",
		},
		{
			number: false,
			input:  "12345abcde",
		},
		{
			number: false,
			input:  "abcde1234",
		},
		{
			number: true,
			input:  "12345e3",
		},
	}

	for _, test := range tests {
		tok, _, ok := lexNumber(test.input, 0)
		assert.Equal(t, test.number, ok, test.input)
		if ok {
			// test.value = strings.TrimSpace(test.value)
			assert.Equal(t, test.input, tok.Value, test.input)
		}
	}
}
