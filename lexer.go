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
	EqualSymbol      Symbol = "="
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

	return nil, pos, false
}

func lexSymbol(source string, pos int) (*Token, int, bool) {
	symbols := []Symbol{
		LeftParenSymbol, RightParenSymbol, AsteriskSymbol,
		CommaSymbol, SemicolonSymbol, ConcatSymbol, EqualSymbol,
	}
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
	hasDecimal := false
	hasExponent := false
	isNumber := false

	for c < len(source) {
		if unicode.IsDigit(rune(source[c])) {
			builder.WriteByte(source[c])
			c++
			isNumber = true
		} else if source[c] == '.' && !hasDecimal && !hasExponent {
			builder.WriteByte(source[c])
			hasDecimal = true
			c++
		} else if (source[c] == 'e' || source[c] == 'E') && !hasExponent && isNumber {
			builder.WriteByte(source[c])
			hasExponent = true
			c++
			if c < len(source) && (source[c] == '+' || source[c] == '-') {
				builder.WriteByte(source[c])
				c++
			}
		} else if unicode.IsLetter(rune(source[c])) {
			return nil, pos, false
		} else {
			break
		}
	}

	if builder.Len() > 0 && isNumber {
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

func lexCondition(source string, pos int) (*Token, int, bool) {
	c := pos
	var builder strings.Builder

	// Parse until we encounter a space or a keyword
	for c < len(source) && source[c] != ' ' && source[c] != '\n' && source[c] != '\t' {
		builder.WriteByte(source[c])
		c++
	}

	// Ensure the condition is valid (e.g., contains an operator like '=')
	condition := builder.String()
	if strings.Contains(condition, "=") || strings.Contains(condition, "<") || strings.Contains(condition, ">") {
		return &Token{
			Type:  StringType, // Treat conditions as strings to preserve the exact condition
			Value: condition,
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

	lexers := []Lexer{
		lexKeyword,
		lexSymbol,
		lexCondition, // Add the new condition lexer here
		lexIdentifier,
		lexString,
		lexNumber,
	}

	for cursor < len(source) {
		if source[cursor] == ' ' || source[cursor] == '\n' || source[cursor] == '\t' {
			cursor++
			continue
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
		return fmt.Errorf("no tokens found")
	}

	firstToken := tokens[0]
	if firstToken.Type != KeywordType {
		return fmt.Errorf("expected SELECT, CREATE or INSERT but got %s", firstToken.Value)
	}

	validFirstKeywords := []Keyword{
		SelectKeyword,
		InsertKeyword,
		UpdateKeyword,
		DeleteKeyword,
		CreateKeyword,
	}

	for _, keyword := range validFirstKeywords {
		if firstToken.Value == string(keyword) {
			return nil
		}
	}

	return fmt.Errorf("expected SELECT, CREATE or INSERT but got %s", firstToken.Value)
}
