package parser

import (
	"fmt"
	"strconv"
	"strings"

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

type SelectStatement struct {
	BaseStatement
	Table   string
	Columns []string
	Where   string
}

type InsertStatement struct {
	BaseStatement
	Table  string
	Values map[string]interface{}
}

type UpdateStatement struct {
	BaseStatement
	Table string
	Set   map[string]interface{}
	Where string
}

type DeleteStatement struct {
	BaseStatement
	Table string
	Where string
}

type CreateStatement struct {
	BaseStatement
	TableName string
	Columns   []types.ColumnDefinition
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
	return nil, storage.CreateTable(&types.Table{
		Name:    s.TableName,
		Columns: s.Columns,
	})
}

func Parse(input string) (Statement, error) {
	lexer := NewLexer(input)
	tokens, err := lexer.Lex()
	if err != nil {
		return nil, err
	}

	if len(tokens) == 0 {
		return nil, fmt.Errorf("empty statement")
	}

	// Check for SHOW TABLES command
	if len(tokens) >= 2 && tokens[0].Type == TokenTypeKeyword && strings.ToUpper(tokens[0].Value) == "SHOW" &&
		tokens[1].Type == TokenTypeKeyword && strings.ToUpper(tokens[1].Value) == "TABLES" {
		return &ShowTablesStatement{BaseStatement{Type: "SHOW_TABLES"}}, nil
	}

	// Handle other statements
	switch strings.ToUpper(tokens[0].Value) {
	case "SELECT":
		return parseSelect(tokens[1:])
	case "INSERT":
		return parseInsert(tokens[1:])
	case "UPDATE":
		return parseUpdate(tokens[1:])
	case "DELETE":
		return parseDelete(tokens[1:])
	case "CREATE":
		return parseCreate(tokens[1:])
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", tokens[0].Value)
	}
}

func parseSelect(tokens []Token) (Statement, error) {
	selectStmt := &SelectStatement{
		BaseStatement: BaseStatement{Type: "SELECT"},
		Columns:       make([]string, 0),
	}

	var parsingColumns = true
	for i := 0; i < len(tokens); i++ {
		token := tokens[i]

		switch {
		case token.Type == TokenTypeKeyword && strings.ToUpper(token.Value) == "FROM":
			parsingColumns = false
			i++
			if i < len(tokens) {
				selectStmt.Table = tokens[i].Value
			}
		case token.Type == TokenTypeKeyword && strings.ToUpper(token.Value) == "WHERE":
			i++
			var conditionBuilder strings.Builder
			for i < len(tokens) {
				conditionBuilder.WriteString(tokens[i].Value)
				conditionBuilder.WriteString(" ")
				i++
			}
			selectStmt.Where = strings.TrimSpace(conditionBuilder.String())
			i--
		case parsingColumns:
			if token.Value == "*" {
				selectStmt.Columns = []string{"*"}
			} else if token.Value != "," {
				selectStmt.Columns = append(selectStmt.Columns, token.Value)
			}
		}
	}

	return selectStmt, nil
}

func parseInsert(tokens []Token) (Statement, error) {
	insertStmt := &InsertStatement{
		BaseStatement: BaseStatement{Type: "INSERT"},
		Values:        make(map[string]interface{}),
	}

	// Parse table name
	if len(tokens) < 4 || tokens[0].Type != TokenTypeKeyword || tokens[0].Value != "INTO" {
		return nil, fmt.Errorf("invalid INSERT statement")
	}

	insertStmt.Table = tokens[1].Value

	// Check if we have column names
	var hasColumnNames bool
	var valuesStart int
	if tokens[2].Type == TokenTypeSymbol && tokens[2].Value == "(" {
		hasColumnNames = true
		valuesStart = 2
	} else if tokens[2].Type == TokenTypeKeyword && tokens[2].Value == "VALUES" {
		hasColumnNames = false
		valuesStart = 3
	} else {
		return nil, fmt.Errorf("expected VALUES keyword or column list")
	}

	// Parse values
	if hasColumnNames {
		// Parse column names
		var columns []string
		for i := valuesStart + 1; i < len(tokens); i++ {
			if tokens[i].Type == TokenTypeSymbol && tokens[i].Value == ")" {
				valuesStart = i + 1
				break
			}
			if tokens[i].Type == TokenTypeIdentifier {
				columns = append(columns, tokens[i].Value)
			}
		}

		// Find VALUES keyword
		for ; valuesStart < len(tokens); valuesStart++ {
			if tokens[valuesStart].Type == TokenTypeKeyword && tokens[valuesStart].Value == "VALUES" {
				valuesStart++
				break
			}
		}
	}

	// Parse values
	var values []interface{}
	for i := valuesStart; i < len(tokens); i++ {
		if tokens[i].Type == TokenTypeSymbol && tokens[i].Value == "(" {
			continue
		}
		if tokens[i].Type == TokenTypeSymbol && tokens[i].Value == ")" {
			break
		}
		if tokens[i].Type == TokenTypeSymbol && tokens[i].Value == "," {
			continue
		}

		value := tokens[i].Value
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			values = append(values, num)
		} else {
			values = append(values, strings.Trim(value, "'\""))
		}
	}

	// Assign values to columns
	if hasColumnNames {
		// We need to get the column names from the table definition
		// For now, we'll just use the values as is
		for i, value := range values {
			insertStmt.Values[fmt.Sprintf("column%d", i+1)] = value
		}
	} else {
		for i, value := range values {
			insertStmt.Values[fmt.Sprintf("column%d", i+1)] = value
		}
	}

	return insertStmt, nil
}

func parseUpdate(tokens []Token) (Statement, error) {
	updateStmt := &UpdateStatement{
		BaseStatement: BaseStatement{Type: "UPDATE"},
		Set:           make(map[string]interface{}),
	}

	// Parse table name
	if len(tokens) < 1 {
		return nil, fmt.Errorf("invalid UPDATE statement")
	}
	updateStmt.Table = tokens[0].Value

	// Parse SET clause
	var i = 1
	for ; i < len(tokens); i++ {
		if tokens[i].Type == TokenTypeKeyword && strings.ToUpper(tokens[i].Value) == "SET" {
			i++
			break
		}
	}

	// Parse column assignments
	for ; i < len(tokens); i++ {
		if tokens[i].Type == TokenTypeKeyword && strings.ToUpper(tokens[i].Value) == "WHERE" {
			break
		}

		if tokens[i].Type != TokenTypeIdentifier {
			continue
		}

		column := tokens[i].Value
		i += 2 // Skip equals sign
		if i >= len(tokens) {
			break
		}

		value := tokens[i].Value
		if num, err := strconv.ParseFloat(value, 64); err == nil {
			updateStmt.Set[column] = num
		} else {
			updateStmt.Set[column] = strings.Trim(value, "'\"")
		}
	}

	// Parse WHERE clause
	for ; i < len(tokens); i++ {
		if tokens[i].Type == TokenTypeKeyword && strings.ToUpper(tokens[i].Value) == "WHERE" {
			i++
			var whereBuilder strings.Builder
			for ; i < len(tokens); i++ {
				whereBuilder.WriteString(tokens[i].Value)
				whereBuilder.WriteString(" ")
			}
			updateStmt.Where = strings.TrimSpace(whereBuilder.String())
			break
		}
	}

	return updateStmt, nil
}

func parseDelete(tokens []Token) (Statement, error) {
	deleteStmt := &DeleteStatement{
		BaseStatement: BaseStatement{Type: "DELETE"},
	}

	// Parse FROM clause
	var i = 0
	for ; i < len(tokens); i++ {
		if tokens[i].Type == TokenTypeKeyword && strings.ToUpper(tokens[i].Value) == "FROM" {
			i++
			if i < len(tokens) {
				deleteStmt.Table = tokens[i].Value
			}
			break
		}
	}

	// Parse WHERE clause
	for ; i < len(tokens); i++ {
		if tokens[i].Type == TokenTypeKeyword && strings.ToUpper(tokens[i].Value) == "WHERE" {
			i++
			var whereBuilder strings.Builder
			for ; i < len(tokens); i++ {
				whereBuilder.WriteString(tokens[i].Value)
				whereBuilder.WriteString(" ")
			}
			deleteStmt.Where = strings.TrimSpace(whereBuilder.String())
			break
		}
	}

	return deleteStmt, nil
}

func parseCreate(tokens []Token) (Statement, error) {
	createStmt := &CreateStatement{
		BaseStatement: BaseStatement{Type: "CREATE"},
	}

	// Parse TABLE keyword
	if len(tokens) < 2 || tokens[0].Type != TokenTypeKeyword || strings.ToUpper(tokens[0].Value) != "TABLE" {
		return nil, fmt.Errorf("invalid CREATE statement")
	}

	// Parse table name
	createStmt.TableName = tokens[1].Value

	// Parse column definitions
	if len(tokens) < 4 || tokens[2].Value != "(" {
		return nil, fmt.Errorf("expected column definitions")
	}

	columnNames := make(map[string]bool)
	i := 3
	for i < len(tokens) && tokens[i].Value != ")" {
		if tokens[i].Type != TokenTypeIdentifier {
			i++
			continue
		}

		columnName := tokens[i].Value
		if columnNames[columnName] {
			return nil, fmt.Errorf("duplicate column name: %s", columnName)
		}
		columnNames[columnName] = true

		i++
		if i >= len(tokens) {
			return nil, fmt.Errorf("unexpected end of statement")
		}

		columnType := strings.ToUpper(tokens[i].Value)
		createStmt.Columns = append(createStmt.Columns, types.ColumnDefinition{
			Name:     columnName,
			Type:     columnType,
			Nullable: false,
		})

		i++
		if i < len(tokens) && tokens[i].Value == "," {
			i++
		}
	}

	return createStmt, nil
}
