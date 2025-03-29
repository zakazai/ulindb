package ulindb

import (
	"fmt"
	"strings"
	"unicode"
)

type Keyword string

const (
	AndKeyword     Keyword = "and"
	BeginKeyword   Keyword = "begin"
	CreateKeyword  Keyword = "create"
	DeleteKeyword  Keyword = "delete"
	FromKeyword    Keyword = "from"
	InsertKeyword  Keyword = "insert"
	IntegerKeyword Keyword = "int"
	IntoKeyword    Keyword = "into"
	SelectKeyword  Keyword = "select"
	SetKeyword     Keyword = "set"
	TableKeyword   Keyword = "table"
	TextKeyword    Keyword = "text"
	UpdateKeyword  Keyword = "update"
	ValuesKeyword  Keyword = "values"
	WhereKeyword   Keyword = "where"
)

type TokenType uint

const (
	KeywordType TokenType = iota
	SymbolType
	IdentifierType
	NumberType
	StringType
)

type Token struct {
	Value string
	Type  TokenType
	Pos   Position
}

type Position struct {
	Line int
	Col  int
}

type Symbol string

const (
	LeftParenSymbol  Symbol = "("
	RightParenSymbol Symbol = ")"
	AsteriskSymbol   Symbol = "*"
	CommaSymbol      Symbol = ","
	ConcatSymbol     Symbol = "||"
	SemicolonSymbol  Symbol = ";"
)

func lexKeyword(source string, pos int) (*Token, int, bool) {
	keywords := []Keyword{
		BeginKeyword, SelectKeyword, AndKeyword, FromKeyword,
		IntoKeyword, IntegerKeyword, TextKeyword, InsertKeyword, TableKeyword,
		CreateKeyword, ValuesKeyword, WhereKeyword, SetKeyword, UpdateKeyword, DeleteKeyword,
	}
	c := pos
	var builder strings.Builder

	for c < len(source) && unicode.IsLetter(rune(source[c])) {
		builder.WriteByte(source[c])
		c++
	}

	value := builder.String()
	for _, k := range keywords {
		if strings.ToLower(value) == string(k) {
			return &Token{
				Type:  KeywordType,
				Value: strings.ToLower(value),
				Pos: Position{
					Line: 0,
					Col:  pos,
				},
			}, c, true
		}
	}

	// If not a keyword, treat it as an identifier
	if len(value) > 0 {
		return &Token{
			Type:  IdentifierType,
			Value: value,
			Pos: Position{
				Line: 0,
				Col:  pos,
			},
		}, c, false
	}

	return nil, pos, false
}

func lexSymbol(source string, pos int) (*Token, int, bool) {
	symbols := []Symbol{LeftParenSymbol, RightParenSymbol, AsteriskSymbol, CommaSymbol, SemicolonSymbol, ConcatSymbol}
	for _, s := range symbols {
		if strings.HasPrefix(source[pos:], string(s)) {
			return &Token{
				Type:  SymbolType,
				Value: string(s),
				Pos: Position{
					Line: 0,
					Col:  pos,
				},
			}, pos + len(s), true
		}
	}
	return nil, pos, false
}

func lexIdentifier(source string, pos int) (*Token, int, bool) {
	c := pos
	var builder strings.Builder

	if unicode.IsLetter(rune(source[c])) || source[c] == '_' {
		for c < len(source) && (unicode.IsLetter(rune(source[c])) || unicode.IsDigit(rune(source[c])) || source[c] == '_') {
			builder.WriteByte(source[c])
			c++
		}
		return &Token{
			Type:  IdentifierType,
			Value: builder.String(),
			Pos: Position{
				Line: 0,
				Col:  pos,
			},
		}, c, true
	}

	return nil, pos, false
}

func lexString(source string, pos int) (*Token, int, bool) {
	c := pos
	var builder strings.Builder

	if source[c] == '\'' {
		c++
		for c < len(source) && source[c] != '\'' {
			builder.WriteByte(source[c])
			c++
		}
		if c < len(source) && source[c] == '\'' {
			return &Token{
				Type:  StringType,
				Value: builder.String(),
				Pos: Position{
					Line: 0,
					Col:  pos,
				},
			}, c + 1, true
		}
	}

	return nil, pos, false
}

func lexNumber(source string, pos int) (*Token, int, bool) {
	c := pos
	var builder strings.Builder

	// Check if the first character is a digit
	if c < len(source) && !unicode.IsDigit(rune(source[c])) {
		return nil, pos, false
	}

	// Parse the integer part
	for c < len(source) && unicode.IsDigit(rune(source[c])) {
		builder.WriteByte(source[c])
		c++
	}

	// Parse the fractional part if present
	if c < len(source) && source[c] == '.' {
		builder.WriteByte(source[c])
		c++
		for c < len(source) && unicode.IsDigit(rune(source[c])) {
			builder.WriteByte(source[c])
			c++
		}
	}

	// Parse the scientific notation if present
	if c < len(source) && (source[c] == 'e' || source[c] == 'E') {
		builder.WriteByte(source[c])
		c++
		if c < len(source) && (source[c] == '+' || source[c] == '-') {
			builder.WriteByte(source[c])
			c++
		}
		if c < len(source) && unicode.IsDigit(rune(source[c])) {
			for c < len(source) && unicode.IsDigit(rune(source[c])) {
				builder.WriteByte(source[c])
				c++
			}
		} else {
			return nil, pos, false // Invalid scientific notation
		}
	}

	// Ensure the next character (if any) is not alphanumeric
	if c < len(source) && (unicode.IsLetter(rune(source[c])) || source[c] == '_') {
		return nil, pos, false
	}

	if builder.Len() > 0 {
		return &Token{
			Type:  NumberType,
			Value: builder.String(),
			Pos: Position{
				Line: 0,
				Col:  pos,
			},
		}, c, true
	}

	return nil, pos, false
}

type Lexer func(string, int) (*Token, int, bool)

func lex(source string) ([]*Token, error) {
	var tokens []*Token
	cursor := 0
	var err error
	var ok bool

	for cursor < len(source) {
		if source[cursor] == ' ' || source[cursor] == '\n' || source[cursor] == '\t' {
			cursor++
			continue
		}

		lexers := []Lexer{
			lexKeyword,
			lexSymbol,
			lexIdentifier,
			lexString,
			lexNumber,
		}

		for _, l := range lexers {
			tok, newpos, valid := l(source, cursor)
			if valid {
				tokens = append(tokens, tok)
				cursor = newpos
				ok = true
				break
			} else {
				ok = false
			}
		}

		if !ok {
			problemPos := cursor
			if len(tokens) > 0 {
				problemPos = tokens[len(tokens)-1].Pos.Col + len(tokens[len(tokens)-1].Value)
			}
			fmt.Println(source)
			fmt.Println(strings.Repeat(" ", problemPos) + "^")
			return nil, fmt.Errorf("failed to parse query after '%v' at pos %d", source[cursor:], cursor)
		}
	}

	if err = isFirstTokenValid(tokens); err != nil {
		return nil, err
	}

	return tokens, nil
}

func isFirstTokenValid(tokens []*Token) error {
	if len(tokens) == 0 {
		return fmt.Errorf("empty query")
	}

	firstTok := tokens[0]
	if firstTok.Type == KeywordType && (firstTok.Value == string(SelectKeyword) || firstTok.Value == string(CreateKeyword) || firstTok.Value == string(InsertKeyword)) {
		return nil
	}

	return fmt.Errorf("expected SELECT, CREATE or INSERT but got %s", firstTok.Value)
}
