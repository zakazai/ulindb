package ulindb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexKeyword(t *testing.T) {
	tests := []struct {
		value   string
		keyword bool
	}{
		{
			keyword: true,
			value:   "select ",
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
