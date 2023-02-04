package ulindb

import (
	"fmt"
	"strings"
	"unicode"
)

type Keyword string

const (
	And    Keyword = "and"
	Begin  Keyword = "begin"
	From   Keyword = "keyword"
	Select Keyword = "select"
)

type Token struct {
	Value string
	Pos   Position
}

type Position struct {
	Line int
	Col  int
}

type Symbol string

const (
	LeftParen  Symbol = "("
	RightParen Symbol = ")"
	Asterisk   Symbol = "*"
	Comma      Symbol = ","
	SemiColon  Symbol = ";"
)

func lexKeyword(source string, pos int) (*Token, int, bool) {
	keywords := []Keyword{Begin, Select}
	c := pos
	r := 0
	v := ""
	for c < len(source) {
		v = v + string(source[c])
		for _, k := range keywords {
			if strings.ToLower(v) == string(k) {
				return &Token{
					Value: v,
					Pos: Position{
						Line: r,
						Col:  c,
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

func lexSymbol(source string, pos int) (*Token, int, bool) {
	symbols := []Symbol{LeftParen, RightParen, Asterisk, Comma, SemiColon}
	c := pos
	r := 0
	v := ""
	for c < len(source) {
		v = v + string(source[c])
		for _, k := range symbols {
			if strings.ToLower(v) == string(k) || source[c] == '\n' {
				return &Token{
					Value: strings.TrimSuffix(v, "\n"),
					Pos: Position{
						Line: r,
						Col:  c,
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
			if (source[c] == '\n' || source[c] == ' ') && isValidChar {
				return &Token{
					Value: v,
					Pos: Position{
						Line: r,
						Col:  c,
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
			Value: v,
			Pos: Position{
				Line: r,
				Col:  c,
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
					Value: v,
					Pos: Position{
						Line: r,
						Col:  c,
					},
				}, pos + len(string(v)), true
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
			if source[c] == ' ' || source[c] == '\n' {
				return &Token{
					Value: v,
					Pos: Position{
						Line: r,
						Col:  c,
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
			Value: v,
			Pos: Position{
				Line: r,
				Col:  c,
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
	for cursor < len(source) {
		lexers := []Lexer{lexKeyword}
		for _, l := range lexers {
			tok, newpos, ok := l(source, cursor)
			if ok {
				tokens = append(tokens, tok)
				cursor = newpos
			} else {
				err = fmt.Errorf("failed to parse query at %v", cursor)
			}
		}
	}

	return tokens, err
}
