package lexer

import (
	"fmt"
	"strings"
)

// Token types
const (
	ILLEGAL    = "ILLEGAL"
	EOF        = "EOF"
	KEYWORD    = "KEYWORD"
	IDENTIFIER = "IDENTIFIER"
	NUMBER     = "NUMBER"
	STRING     = "STRING"
	SYMBOL     = "SYMBOL"

	// Symbols
	ASTERISK  = "ASTERISK"
	COMMA     = "COMMA"
	SEMICOLON = "SEMICOLON"
	LPAREN    = "LPAREN"
	RPAREN    = "RPAREN"
	EQUALS    = "EQUALS"
)

// Keywords
var keywords = map[string]TokenType{
	"select": KEYWORD,
	"from":   KEYWORD,
	"where":  KEYWORD,
	"insert": KEYWORD,
	"into":   KEYWORD,
	"values": KEYWORD,
	"update": KEYWORD,
	"set":    KEYWORD,
	"delete": KEYWORD,
	"create": KEYWORD,
	"table":  KEYWORD,
	"int":    KEYWORD,
	"text":   KEYWORD,
	"string": KEYWORD,
	"show":   KEYWORD,
	"tables": KEYWORD,
}

type TokenType string

type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Col     int
}

// LookupIdent checks if the given identifier is a keyword
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENTIFIER
}

// New creates a new Lexer for the input string
func New(input string) *Lexer {
	l := &Lexer{
		input:    input,
		position: Position{Line: 1, Col: 0},
	}
	l.readChar()
	return l
}

type Position struct {
	Line int
	Col  int
}

type Lexer struct {
	input    string
	position Position
	readPos  int  // current reading position in input (after current char)
	ch       byte // current char under examination
}

func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case '*':
		tok = Token{Type: ASTERISK, Literal: string(l.ch)}
	case ',':
		tok = Token{Type: COMMA, Literal: string(l.ch)}
	case ';':
		tok = Token{Type: SEMICOLON, Literal: string(l.ch)}
	case '(':
		tok = Token{Type: LPAREN, Literal: string(l.ch)}
	case ')':
		tok = Token{Type: RPAREN, Literal: string(l.ch)}
	case '=':
		tok = Token{Type: EQUALS, Literal: string(l.ch)}
	case '\'':
		tok.Type = STRING
		tok.Literal = l.readString()
	case 0:
		tok.Literal = ""
		tok.Type = EOF
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)
			return tok
		} else if isDigit(l.ch) {
			tok.Literal = l.readNumber()
			tok.Type = NUMBER
			return tok
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch)}
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		if l.ch == '\n' {
			l.position.Line++
			l.position.Col = 0
		}
		l.readChar()
	}
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.position.Col++
	l.readPos++
}

func (l *Lexer) readIdentifier() string {
	position := l.readPos - 1
	for isLetter(l.ch) || isDigit(l.ch) {
		l.readChar()
	}
	return l.input[position : l.readPos-1]
}

func (l *Lexer) readNumber() string {
	position := l.readPos - 1
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[position : l.readPos-1]
}

func (l *Lexer) readString() string {
	position := l.readPos
	for {
		l.readChar()
		if l.ch == '\'' || l.ch == 0 {
			break
		}
	}
	return l.input[position : l.readPos-1]
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func isKeyword(word string) bool {
	keywords := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE", "CREATE", "TABLE", "INT",
		"STRING", "SHOW", "TABLES", "NULL", "AND", "OR", "TEXT",
	}
	for _, keyword := range keywords {
		if word == keyword {
			return true
		}
	}
	return false
}

func (t Token) String() string {
	return fmt.Sprintf("Token{Type: %v, Literal: %q}", t.Type, t.Literal)
}
