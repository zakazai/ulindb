package lexer

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents the type of a token
type TokenType int

const (
	// EOF represents the end of file token
	EOF TokenType = iota
	// KEYWORD represents a keyword token
	KEYWORD
	// IDENTIFIER represents an identifier token
	IDENTIFIER
	// NUMBER represents a number token
	NUMBER
	// STRING represents a string token
	STRING
	// SYMBOL represents a symbol token
	SYMBOL
	// LPAREN represents a left parenthesis
	LPAREN
	// RPAREN represents a right parenthesis
	RPAREN
	// COMMA represents a comma
	COMMA
	// SEMICOLON represents a semicolon
	SEMICOLON
	// ASTERISK represents an asterisk
	ASTERISK
	// EQUALS represents an equals sign
	EQUALS
)

// Token represents a lexical token
type Token struct {
	Type    TokenType
	Literal string
}

// Lexer represents a lexical analyzer
type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

// New creates a new lexer with the given input
func New(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case '(':
		tok = Token{Type: LPAREN, Literal: string(l.ch)}
	case ')':
		tok = Token{Type: RPAREN, Literal: string(l.ch)}
	case ',':
		tok = Token{Type: COMMA, Literal: string(l.ch)}
	case ';':
		tok = Token{Type: SEMICOLON, Literal: string(l.ch)}
	case '*':
		tok = Token{Type: ASTERISK, Literal: string(l.ch)}
	case '=':
		tok = Token{Type: EQUALS, Literal: string(l.ch)}
	case 0:
		tok = Token{Type: EOF, Literal: ""}
	case '"', '\'':
		quote := l.ch
		l.readChar()
		literal := l.readString(quote)
		tok = Token{Type: STRING, Literal: fmt.Sprintf("%c%s%c", quote, literal, quote)}
	default:
		if isLetter(l.ch) {
			tok.Literal = l.readIdentifier()
			upperLiteral := strings.ToUpper(tok.Literal)
			if isKeyword(upperLiteral) {
				tok.Type = KEYWORD
				tok.Literal = upperLiteral
			} else {
				tok.Type = IDENTIFIER
			}
			return tok
		} else if isDigit(l.ch) || l.ch == '-' {
			tok.Type = NUMBER
			tok.Literal = l.readNumber()
			return tok
		} else {
			tok = Token{Type: SYMBOL, Literal: string(l.ch)}
		}
	}

	l.readChar()
	return tok
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	if l.ch == '-' {
		l.readChar()
	}
	for isDigit(l.ch) || l.ch == '.' {
		l.readChar()
	}
	return l.input[position:l.position]
}

func (l *Lexer) readString(quote byte) string {
	position := l.position
	for {
		if l.ch == quote || l.ch == 0 {
			break
		}
		if l.ch == '\\' {
			l.readChar()
			if l.ch == quote {
				l.readChar()
			}
		} else {
			l.readChar()
		}
	}
	return l.input[position:l.position]
}

func isLetter(ch byte) bool {
	return unicode.IsLetter(rune(ch))
}

func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
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
