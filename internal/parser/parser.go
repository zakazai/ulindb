package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zakazai/ulin-db/internal/lexer"
	"github.com/zakazai/ulin-db/internal/types"
)

// Token types
const (
	TokenTypeKeyword    = "KEYWORD"
	TokenTypeIdentifier = "IDENTIFIER"
	TokenTypeString     = "STRING"
	TokenTypeNumber     = "NUMBER"
	TokenTypeSymbol     = "SYMBOL"
)

// Token represents a lexical token
type Token struct {
	Type  string
	Value string
}

// Lexer represents a lexical analyzer
type Lexer struct {
	input string
	pos   int
}

// NewLexer creates a new lexer
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
	}
}

// Lex tokenizes the input string
func (l *Lexer) Lex() ([]Token, error) {
	var tokens []Token
	for l.pos < len(l.input) {
		// Skip whitespace
		if l.isWhitespace(l.current()) {
			l.advance()
			continue
		}

		// Handle identifiers and keywords
		if l.isLetter(l.current()) {
			token := l.lexIdentifier()
			tokens = append(tokens, token)
			continue
		}

		// Handle numbers
		if l.isDigit(l.current()) {
			token := l.lexNumber()
			tokens = append(tokens, token)
			continue
		}

		// Handle strings
		if l.current() == '\'' || l.current() == '"' {
			token, err := l.lexString()
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, token)
			continue
		}

		// Handle symbols
		token := l.lexSymbol()
		tokens = append(tokens, token)
	}

	return tokens, nil
}

func (l *Lexer) current() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) advance() {
	l.pos++
}

func (l *Lexer) isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func (l *Lexer) isLetter(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func (l *Lexer) isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func (l *Lexer) lexIdentifier() Token {
	start := l.pos
	for l.pos < len(l.input) && (l.isLetter(l.current()) || l.isDigit(l.current())) {
		l.advance()
	}
	value := l.input[start:l.pos]
	tokenType := TokenTypeIdentifier

	// Check if it's a keyword
	switch strings.ToUpper(value) {
	case "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
		"DELETE", "CREATE", "TABLE", "INT", "STRING", "TEXT", "SHOW", "TABLES":
		tokenType = TokenTypeKeyword
	}

	return Token{Type: tokenType, Value: value}
}

func (l *Lexer) lexNumber() Token {
	start := l.pos
	for l.pos < len(l.input) && (l.isDigit(l.current()) || l.current() == '.') {
		l.advance()
	}
	return Token{Type: TokenTypeNumber, Value: l.input[start:l.pos]}
}

func (l *Lexer) lexString() (Token, error) {
	quote := l.current()
	l.advance() // Skip opening quote
	start := l.pos

	for l.pos < len(l.input) && l.current() != quote {
		l.advance()
	}

	if l.pos >= len(l.input) {
		return Token{}, fmt.Errorf("unterminated string")
	}

	value := l.input[start:l.pos]
	l.advance() // Skip closing quote
	return Token{Type: TokenTypeString, Value: value}, nil
}

func (l *Lexer) lexSymbol() Token {
	ch := l.current()
	l.advance()
	return Token{Type: TokenTypeSymbol, Value: string(ch)}
}

type Statement interface {
	Execute(storage types.Storage) (interface{}, error)
}

type BaseStatement struct {
	Type string
}

// SelectStatement represents a SELECT SQL statement
type SelectStatement struct {
	Table   string
	Columns []string
	Where   map[string]interface{}
}

// InsertStatement represents an INSERT SQL statement
type InsertStatement struct {
	Table  string
	Values map[string]interface{}
}

// UpdateStatement represents an UPDATE SQL statement
type UpdateStatement struct {
	Table string
	Set   map[string]interface{}
	Where map[string]interface{}
}

// DeleteStatement represents a DELETE SQL statement
type DeleteStatement struct {
	Table string
	Where map[string]interface{}
}

// CreateStatement represents a CREATE TABLE SQL statement
type CreateStatement struct {
	Table   string
	Columns []struct {
		Name     string
		Type     string
		Nullable bool
	}
}

type ShowTablesStatement struct {
	BaseStatement
}

func (s *ShowTablesStatement) Execute(storage types.Storage) (interface{}, error) {
	return storage.ShowTables()
}

func (s *SelectStatement) Execute(storage types.Storage) (interface{}, error) {
	return storage.Select(s.Table, s.Columns, s.Where)
}

func (s *InsertStatement) Execute(storage types.Storage) (interface{}, error) {
	return nil, storage.Insert(s.Table, s.Values)
}

func (s *UpdateStatement) Execute(storage types.Storage) (interface{}, error) {
	return nil, storage.Update(s.Table, s.Set, s.Where)
}

func (s *DeleteStatement) Execute(storage types.Storage) (interface{}, error) {
	return nil, storage.Delete(s.Table, s.Where)
}

func (s *CreateStatement) Execute(storage types.Storage) (interface{}, error) {
	// Convert our column type to types.ColumnDefinition
	columns := make([]types.ColumnDefinition, len(s.Columns))
	for i, col := range s.Columns {
		columns[i] = types.ColumnDefinition{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}

	return nil, storage.CreateTable(&types.Table{
		Name:    s.Table,
		Columns: columns,
	})
}

// New creates a new parser with the given lexer
func New(l *lexer.Lexer) *Parser {
	return &Parser{l: l}
}

// Parser represents a SQL parser
type Parser struct {
	l *lexer.Lexer
}

// Parse parses the input SQL statement
func (p *Parser) Parse() (interface{}, error) {
	tok := p.l.NextToken()
	if tok.Type == lexer.EOF {
		return nil, fmt.Errorf("empty statement")
	}

	switch tok.Literal {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "CREATE":
		return p.parseCreate()
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", tok.Literal)
	}
}

func (p *Parser) parseSelect() (*SelectStatement, error) {
	stmt := &SelectStatement{
		Columns: []string{},
	}

	// Parse columns
	for {
		tok := p.l.NextToken()
		if tok.Type == lexer.EOF {
			return nil, fmt.Errorf("unexpected EOF while parsing SELECT")
		}

		if tok.Type == lexer.ASTERISK {
			stmt.Columns = []string{"*"}
		} else if tok.Type == lexer.IDENTIFIER {
			stmt.Columns = append(stmt.Columns, tok.Literal)
		}

		tok = p.l.NextToken()
		if tok.Type == lexer.EOF {
			return nil, fmt.Errorf("unexpected EOF while parsing SELECT")
		}

		if tok.Literal == "FROM" {
			break
		} else if tok.Type != lexer.COMMA {
			return nil, fmt.Errorf("expected comma or FROM, got %s", tok.Literal)
		}
	}

	// Parse table name
	tok := p.l.NextToken()
	if tok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", tok.Literal)
	}
	stmt.Table = tok.Literal

	// Parse WHERE clause if present
	tok = p.l.NextToken()
	if tok.Type == lexer.EOF {
		return stmt, nil
	}

	if tok.Literal == "WHERE" {
		where := make(map[string]interface{})
		for {
			tok = p.l.NextToken()
			if tok.Type == lexer.EOF {
				break
			}

			if tok.Type != lexer.IDENTIFIER {
				return nil, fmt.Errorf("expected column name, got %s", tok.Literal)
			}
			col := tok.Literal

			tok = p.l.NextToken()
			if tok.Type != lexer.EQUALS {
				return nil, fmt.Errorf("expected =, got %s", tok.Literal)
			}

			tok = p.l.NextToken()
			if tok.Type == lexer.NUMBER {
				val, err := strconv.ParseFloat(tok.Literal, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid number: %s", tok.Literal)
				}
				where[col] = val
			} else if tok.Type == lexer.STRING {
				where[col] = strings.Trim(tok.Literal, "'\"")
			} else {
				return nil, fmt.Errorf("expected number or string, got %s", tok.Literal)
			}

			tok = p.l.NextToken()
			if tok.Type == lexer.EOF {
				break
			}
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseInsert() (*InsertStatement, error) {
	stmt := &InsertStatement{
		Values: make(map[string]interface{}),
	}

	// Parse INTO keyword
	tok := p.l.NextToken()
	if tok.Literal != "INTO" {
		return nil, fmt.Errorf("expected INTO, got %s", tok.Literal)
	}

	// Parse table name
	tok = p.l.NextToken()
	if tok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", tok.Literal)
	}
	stmt.Table = tok.Literal

	// Parse VALUES keyword
	tok = p.l.NextToken()
	if tok.Literal != "VALUES" {
		return nil, fmt.Errorf("expected VALUES, got %s", tok.Literal)
	}

	// Parse values
	tok = p.l.NextToken()
	if tok.Type != lexer.LPAREN {
		return nil, fmt.Errorf("expected (, got %s", tok.Literal)
	}

	colIndex := 0
	for {
		tok = p.l.NextToken()
		if tok.Type == lexer.RPAREN {
			break
		}

		if tok.Type == lexer.NUMBER {
			val, err := strconv.ParseFloat(tok.Literal, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid number: %s", tok.Literal)
			}
			stmt.Values[fmt.Sprintf("column%d", colIndex+1)] = val
		} else if tok.Type == lexer.STRING {
			stmt.Values[fmt.Sprintf("column%d", colIndex+1)] = strings.Trim(tok.Literal, "'\"")
		} else {
			return nil, fmt.Errorf("expected number or string, got %s", tok.Literal)
		}

		colIndex++

		tok = p.l.NextToken()
		if tok.Type == lexer.RPAREN {
			break
		}
		if tok.Type != lexer.COMMA {
			return nil, fmt.Errorf("expected comma or ), got %s", tok.Literal)
		}
	}

	return stmt, nil
}

func (p *Parser) parseUpdate() (*UpdateStatement, error) {
	stmt := &UpdateStatement{
		Set: make(map[string]interface{}),
	}

	// Parse table name
	tok := p.l.NextToken()
	if tok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", tok.Literal)
	}
	stmt.Table = tok.Literal

	// Parse SET keyword
	tok = p.l.NextToken()
	if tok.Literal != "SET" {
		return nil, fmt.Errorf("expected SET, got %s", tok.Literal)
	}

	// Parse SET clause
	for {
		tok = p.l.NextToken()
		if tok.Type == lexer.EOF {
			break
		}

		if tok.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected column name, got %s", tok.Literal)
		}
		col := tok.Literal

		tok = p.l.NextToken()
		if tok.Type != lexer.EQUALS {
			return nil, fmt.Errorf("expected =, got %s", tok.Literal)
		}

		tok = p.l.NextToken()
		if tok.Type == lexer.NUMBER {
			val, err := strconv.ParseFloat(tok.Literal, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid number: %s", tok.Literal)
			}
			stmt.Set[col] = val
		} else if tok.Type == lexer.STRING {
			stmt.Set[col] = strings.Trim(tok.Literal, "'\"")
		} else {
			return nil, fmt.Errorf("expected number or string, got %s", tok.Literal)
		}

		tok = p.l.NextToken()
		if tok.Type == lexer.EOF {
			break
		}
		if tok.Literal == "WHERE" {
			break
		}
		if tok.Type != lexer.COMMA {
			return nil, fmt.Errorf("expected comma or WHERE, got %s", tok.Literal)
		}
	}

	// Parse WHERE clause if present
	if tok.Literal == "WHERE" {
		where := make(map[string]interface{})
		for {
			tok = p.l.NextToken()
			if tok.Type == lexer.EOF {
				break
			}

			if tok.Type != lexer.IDENTIFIER {
				return nil, fmt.Errorf("expected column name, got %s", tok.Literal)
			}
			col := tok.Literal

			tok = p.l.NextToken()
			if tok.Type != lexer.EQUALS {
				return nil, fmt.Errorf("expected =, got %s", tok.Literal)
			}

			tok = p.l.NextToken()
			if tok.Type == lexer.NUMBER {
				val, err := strconv.ParseFloat(tok.Literal, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid number: %s", tok.Literal)
				}
				where[col] = val
			} else if tok.Type == lexer.STRING {
				where[col] = strings.Trim(tok.Literal, "'\"")
			} else {
				return nil, fmt.Errorf("expected number or string, got %s", tok.Literal)
			}

			tok = p.l.NextToken()
			if tok.Type == lexer.EOF {
				break
			}
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseDelete() (*DeleteStatement, error) {
	stmt := &DeleteStatement{}

	// Parse FROM keyword
	tok := p.l.NextToken()
	if tok.Literal != "FROM" {
		return nil, fmt.Errorf("expected FROM, got %s", tok.Literal)
	}

	// Parse table name
	tok = p.l.NextToken()
	if tok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", tok.Literal)
	}
	stmt.Table = tok.Literal

	// Parse WHERE clause if present
	tok = p.l.NextToken()
	if tok.Type == lexer.EOF {
		return stmt, nil
	}

	if tok.Literal == "WHERE" {
		where := make(map[string]interface{})
		for {
			tok = p.l.NextToken()
			if tok.Type == lexer.EOF {
				break
			}

			if tok.Type != lexer.IDENTIFIER {
				return nil, fmt.Errorf("expected column name, got %s", tok.Literal)
			}
			col := tok.Literal

			tok = p.l.NextToken()
			if tok.Type != lexer.EQUALS {
				return nil, fmt.Errorf("expected =, got %s", tok.Literal)
			}

			tok = p.l.NextToken()
			if tok.Type == lexer.NUMBER {
				val, err := strconv.ParseFloat(tok.Literal, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid number: %s", tok.Literal)
				}
				where[col] = val
			} else if tok.Type == lexer.STRING {
				where[col] = strings.Trim(tok.Literal, "'\"")
			} else {
				return nil, fmt.Errorf("expected number or string, got %s", tok.Literal)
			}

			tok = p.l.NextToken()
			if tok.Type == lexer.EOF {
				break
			}
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseCreate() (*CreateStatement, error) {
	stmt := &CreateStatement{}

	// Parse TABLE keyword
	tok := p.l.NextToken()
	if tok.Literal != "TABLE" {
		return nil, fmt.Errorf("expected TABLE, got %s", tok.Literal)
	}

	// Parse table name
	tok = p.l.NextToken()
	if tok.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", tok.Literal)
	}
	stmt.Table = tok.Literal

	// Parse column definitions
	tok = p.l.NextToken()
	if tok.Type != lexer.LPAREN {
		return nil, fmt.Errorf("expected (, got %s", tok.Literal)
	}

	for {
		tok = p.l.NextToken()
		if tok.Type == lexer.RPAREN {
			break
		}

		if tok.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected column name, got %s", tok.Literal)
		}
		colName := tok.Literal

		tok = p.l.NextToken()
		// Accept both IDENTIFIER and KEYWORD as column types
		if tok.Type != lexer.IDENTIFIER && tok.Type != lexer.KEYWORD {
			return nil, fmt.Errorf("expected column type, got %s", tok.Literal)
		}
		colType := tok.Literal

		stmt.Columns = append(stmt.Columns, struct {
			Name     string
			Type     string
			Nullable bool
		}{
			Name:     colName,
			Type:     colType,
			Nullable: true,
		})

		tok = p.l.NextToken()
		if tok.Type == lexer.RPAREN {
			break
		}
		if tok.Type != lexer.COMMA {
			return nil, fmt.Errorf("expected comma or ), got %s", tok.Literal)
		}
	}

	return stmt, nil
}

// Parse parses an SQL statement and returns a Statement
func Parse(sql string) (Statement, error) {
	l := lexer.New(sql)
	p := New(l)
	stmt, err := p.Parse()
	if err != nil {
		return nil, err
	}
	return stmt.(Statement), nil
}
