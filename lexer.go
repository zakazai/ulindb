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
	TableKeyword   Keyword = "table"
	FromKeyword    Keyword = "from"
	SelectKeyword  Keyword = "select"
	InsertKeyword  Keyword = "insert"
	ValuesKeyword  Keyword = "values"
	IntegerKeyword Keyword = "int"
	IntoKeyword    Keyword = "into"
	TextKeyword    Keyword = "text"
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
		CreateKeyword, ValuesKeyword,
	}
	c := pos
	r := 0
	v := ""

	for c < len(source) && unicode.IsLetter(rune(source[c])) {
		v = v + string(source[c])
		c = c + 1
	}

	for _, k := range keywords {
		if strings.ToLower(v) == string(k) {
			return &Token{
				Type:  KeywordType,
				Value: strings.ToLower(v),
				Pos: Position{
					Line: r,
					Col:  pos,
				},
			}, pos + len(string(v)), true
		}
	}

	return nil, pos, false
}

func lexSymbol(source string, pos int) (*Token, int, bool) {
	symbols := []Symbol{LeftParenSymbol, RightParenSymbol, AsteriskSymbol, CommaSymbol, SemicolonSymbol, ConcatSymbol}
	c := pos
	r := 0
	v := ""
	for c < len(source) {
		v = v + string(source[c])
		for _, k := range symbols {
			if strings.ToLower(v) == string(k) || source[c] == '\n' {
				return &Token{
					Type:  SymbolType,
					Value: strings.TrimSuffix(v, "\n"),
					Pos: Position{
						Line: r,
						Col:  pos,
					},
				}, pos + len(string(v)), true
			}
		}
		if string(source[c]) == " " {
			break
		}
		c = c + 1
	}

	return nil, pos, false
}

func lexIdentifier(source string, pos int) (*Token, int, bool) {
	c := pos
	r := 0

	v := ""
	if unicode.IsLetter(rune(source[c])) {
		isValidChar := true
		for c < len(source) {
			if (source[c] == '\n' || source[c] == ' ' || string(source[c]) == string(SemicolonSymbol)) && isValidChar {
				return &Token{
					Type:  IdentifierType,
					Value: strings.ToLower(v),
					Pos: Position{
						Line: r,
						Col:  pos,
					},
				}, pos + len(string(v)), true
			}
			if !(unicode.IsLetter(rune(source[c])) || unicode.IsDigit(rune(source[c])) || source[c] == '_' || source[c] == '$') {
				isValidChar = false
				return nil, pos, false
			}
			if isValidChar {
				v = v + string(source[c])
			}
			c = c + 1
		}
		return &Token{
			Type:  IdentifierType,
			Value: v,
			Pos: Position{
				Line: r,
				Col:  pos,
			},
		}, pos + len(string(v)), true
	}

	return nil, pos, false
}

func lexString(source string, pos int) (*Token, int, bool) {
	c := pos
	r := 0

	v := ""

	if source[c] == '\'' {
		c = c + 1
		for c < len(source) {
			if source[c] == '\'' {
				return &Token{
					Type:  StringType,
					Value: v,
					Pos: Position{
						Line: r,
						Col:  pos,
					},
				}, pos + len(string(v)) + 2, true
			}

			v = v + string(source[c])
			c = c + 1
		}
	}

	return nil, pos, false
}

func lexNumber(source string, pos int) (*Token, int, bool) {
	c := pos
	r := 0
	v := ""

	if unicode.IsDigit(rune(source[c])) {
		for c < len(source) {
			if source[c] == ' ' || source[c] == '\n' || source[c] == ',' || source[c] == ')' {
				return &Token{
					Type:  NumberType,
					Value: v,
					Pos: Position{
						Line: r,
						Col:  pos,
					},
				}, pos + len(string(v)), true
			}

			if unicode.IsDigit(rune(source[c])) || source[c] == '.' || source[c] == 'e' {
				v = v + string(source[c])
			} else {
				return nil, pos, false
			}
			c = c + 1
		}
	}

	if c != pos && unicode.IsDigit(rune(source[c-1])) {
		return &Token{
			Type:  NumberType,
			Value: v,
			Pos: Position{
				Line: r,
				Col:  pos,
			},
		}, pos + len(string(v)), true
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
		if source[cursor] == ' ' {
			cursor = cursor + 1
			continue
		}
		lexers := []Lexer{
			lexKeyword,
			lexIdentifier,
			lexString,
			lexNumber,
			lexSymbol,
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
			fmt.Println(source)
			problem_pos := tokens[len(tokens)-1].Pos.Col + len(tokens[len(tokens)-1].Value)
			fmt.Println(strings.Repeat(" ", problem_pos) + "^")
			return nil, fmt.Errorf("Failed to parse query after '%v' at pos %d\n", tokens[len(tokens)-1].Value, problem_pos)
		}
	}

	return tokens, err
}
