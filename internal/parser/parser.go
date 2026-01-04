package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/zakazai/ulin-db/internal/lexer"
	"github.com/zakazai/ulin-db/internal/types"
)

// Statement types
type Statement struct {
	Type            string
	SelectStatement *SelectStatement
	InsertStatement *InsertStatement
	UpdateStatement *UpdateStatement
	DeleteStatement *DeleteStatement
	CreateStatement *CreateStatement
	Error           error
}

func (stmt *Statement) Execute(s types.Storage) (interface{}, error) {
	switch stmt.Type {
	case "SELECT":
		return stmt.SelectStatement.Execute(s)
	case "INSERT":
		return stmt.InsertStatement.Execute(s)
	case "UPDATE":
		return stmt.UpdateStatement.Execute(s)
	case "DELETE":
		return stmt.DeleteStatement.Execute(s)
	case "CREATE":
		return stmt.CreateStatement.Execute(s)
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", stmt.Type)
	}
}

type SelectStatement struct {
	Table   string
	Columns []string
	Where   map[string]interface{}
}

type InsertStatement struct {
	Table  string
	Values map[string]interface{}
}

type UpdateStatement struct {
	Table string
	Set   map[string]interface{}
	Where map[string]interface{}
}

type DeleteStatement struct {
	Table string
	Where map[string]interface{}
}

type CreateStatement struct {
	Table   string
	Columns []struct {
		Name     string
		Type     string
		Nullable bool
	}
}

type ColumnDefinition struct {
	Name     string
	Type     string
	Nullable bool
}

type BaseStatement struct {
	Type string
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

// Parser represents a SQL parser
type Parser struct {
	l            *lexer.Lexer
	currentToken lexer.Token
	peekToken    lexer.Token
}

// New creates a new parser
func New(l *lexer.Lexer) *Parser {
	p := &Parser{l: l}
	// Read two tokens, so currentToken and peekToken are both set
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// Parse parses a SQL statement and returns a Statement
func Parse(sql string) (*Statement, error) {
	l := lexer.New(sql)
	p := New(l)
	stmt := &Statement{}

	switch p.currentToken.Type {
	case lexer.KEYWORD:
		switch p.currentToken.Literal {
		case "SELECT":
			stmt.Type = "SELECT"
			selectStmt := p.parseSelect()
			stmt.SelectStatement = &selectStmt
		case "INSERT":
			stmt.Type = "INSERT"
			insertStmt, err := p.parseInsert()
			if err != nil {
				return nil, err
			}
			stmt.InsertStatement = insertStmt
		case "UPDATE":
			stmt.Type = "UPDATE"
			updateStmt, err := p.parseUpdate()
			if err != nil {
				return nil, err
			}
			stmt.UpdateStatement = updateStmt
		case "DELETE":
			stmt.Type = "DELETE"
			deleteStmt, err := p.parseDelete()
			if err != nil {
				return nil, err
			}
			stmt.DeleteStatement = deleteStmt
		case "CREATE":
			stmt.Type = "CREATE"
			createStmt, err := p.parseCreate()
			if err != nil {
				return nil, err
			}
			stmt.CreateStatement = createStmt
		default:
			return nil, fmt.Errorf("unexpected keyword: %s", p.currentToken.Literal)
		}
	default:
		return nil, fmt.Errorf("unexpected token type: %s", p.currentToken.Type)
	}

	return stmt, nil
}

func (p *Parser) parseSelect() SelectStatement {
	stmt := SelectStatement{}
	p.nextToken() // move past SELECT

	// Parse columns
	for p.currentToken.Type != lexer.KEYWORD || p.currentToken.Literal != "FROM" {
		if p.currentToken.Type == lexer.ASTERISK {
			stmt.Columns = append(stmt.Columns, "*")
		} else if p.currentToken.Type == lexer.IDENTIFIER {
			stmt.Columns = append(stmt.Columns, p.currentToken.Literal)
		}
		p.nextToken()
		if p.currentToken.Type == lexer.COMMA {
			p.nextToken()
		}
	}

	// Parse FROM clause
	if p.currentToken.Literal == "FROM" {
		p.nextToken()
		if p.currentToken.Type == lexer.IDENTIFIER {
			stmt.Table = p.currentToken.Literal
		}
		p.nextToken()
	}

	// Parse WHERE clause
	if p.currentToken.Type == lexer.KEYWORD && p.currentToken.Literal == "WHERE" {
		p.nextToken()
		where := make(map[string]interface{})
		for p.currentToken.Type != lexer.EOF {
			// Expect column name
			if p.currentToken.Type != lexer.IDENTIFIER {
				break
			}
			col := p.currentToken.Literal
			p.nextToken()
			if p.currentToken.Type != lexer.EQUALS {
				break
			}
			p.nextToken()
			// Parse value according to token type
			if p.currentToken.Type == lexer.NUMBER {
				val, err := strconv.ParseFloat(p.currentToken.Literal, 64)
				if err != nil {
					break
				}
				where[col] = val
			} else if p.currentToken.Type == lexer.STRING {
				where[col] = strings.Trim(p.currentToken.Literal, "'\"")
			} else {
				where[col] = p.currentToken.Literal
			}
			p.nextToken()
			// If next token is AND/OR or SEMICOLON, continue; otherwise loop will handle EOF
		}
		stmt.Where = where
	}

	return stmt
}

func (p *Parser) parseInsert() (*InsertStatement, error) {
	stmt := &InsertStatement{
		Values: make(map[string]interface{}),
	}

	// Parse INTO keyword - use peekToken which was already read by New()
	tok := p.peekToken
	p.nextToken() // Advance to consume the INTO token
	if strings.ToUpper(tok.Literal) != "INTO" {
		return nil, fmt.Errorf("expected INTO, got %s", tok.Literal)
	}

	// Parse table name
	p.nextToken()
	if p.currentToken.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.currentToken.Literal)
	}
	stmt.Table = p.currentToken.Literal

	// Parse VALUES keyword
	p.nextToken()
	if strings.ToUpper(p.currentToken.Literal) != "VALUES" {
		return nil, fmt.Errorf("expected VALUES, got %s", p.currentToken.Literal)
	}

	// Parse values (expect LPAREN)
	p.nextToken()
	if p.currentToken.Type != lexer.LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.currentToken.Literal)
	}

	colIndex := 0
	for {
		p.nextToken()
		if p.currentToken.Type == lexer.RPAREN {
			break
		}

		if p.currentToken.Type == lexer.NUMBER {
			val, err := strconv.ParseFloat(p.currentToken.Literal, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid number: %s", p.currentToken.Literal)
			}
			stmt.Values[fmt.Sprintf("column%d", colIndex+1)] = val
		} else if p.currentToken.Type == lexer.STRING {
			stmt.Values[fmt.Sprintf("column%d", colIndex+1)] = strings.Trim(p.currentToken.Literal, "'\"")
		} else {
			return nil, fmt.Errorf("expected number or string, got %s", p.currentToken.Literal)
		}

		colIndex++

		p.nextToken()
		if p.currentToken.Type == lexer.RPAREN {
			break
		}
		if p.currentToken.Type != lexer.COMMA {
			return nil, fmt.Errorf("expected comma or ), got %s", p.currentToken.Literal)
		}
	}

	return stmt, nil
}

func (p *Parser) parseUpdate() (*UpdateStatement, error) {
	stmt := &UpdateStatement{
		Set: make(map[string]interface{}),
	}

	// Parse table name
	p.nextToken()
	if p.currentToken.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.currentToken.Literal)
	}
	stmt.Table = p.currentToken.Literal

	// Parse SET keyword
	p.nextToken()
	if strings.ToUpper(p.currentToken.Literal) != "SET" {
		return nil, fmt.Errorf("expected SET, got %s", p.currentToken.Literal)
	}

	// Parse SET clause
	for {
		p.nextToken()
		if p.currentToken.Type == lexer.EOF {
			break
		}

		if p.currentToken.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected column name, got %s", p.currentToken.Literal)
		}
		col := p.currentToken.Literal

		p.nextToken()
		if p.currentToken.Type != lexer.EQUALS {
			return nil, fmt.Errorf("expected =, got %s", p.currentToken.Literal)
		}

		p.nextToken()
		if p.currentToken.Type == lexer.NUMBER {
			val, err := strconv.ParseFloat(p.currentToken.Literal, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid number: %s", p.currentToken.Literal)
			}
			stmt.Set[col] = val
		} else if p.currentToken.Type == lexer.STRING {
			stmt.Set[col] = strings.Trim(p.currentToken.Literal, "'\"")
		} else {
			return nil, fmt.Errorf("expected number or string, got %s", p.currentToken.Literal)
		}

		p.nextToken()
		if p.currentToken.Type == lexer.EOF {
			break
		}
		if strings.ToUpper(p.currentToken.Literal) == "WHERE" {
			break
		}
		if p.currentToken.Type != lexer.COMMA {
			return nil, fmt.Errorf("expected comma or WHERE, got %s", p.currentToken.Literal)
		}
	}

	// Parse WHERE clause if present
	if strings.ToUpper(p.currentToken.Literal) == "WHERE" {
		where := make(map[string]interface{})
		for {
			p.nextToken()
			if p.currentToken.Type == lexer.EOF {
				break
			}

			if p.currentToken.Type != lexer.IDENTIFIER {
				return nil, fmt.Errorf("expected column name, got %s", p.currentToken.Literal)
			}
			col := p.currentToken.Literal

			p.nextToken()
			if p.currentToken.Type != lexer.EQUALS {
				return nil, fmt.Errorf("expected =, got %s", p.currentToken.Literal)
			}

			p.nextToken()
			if p.currentToken.Type == lexer.NUMBER {
				val, err := strconv.ParseFloat(p.currentToken.Literal, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid number: %s", p.currentToken.Literal)
				}
				where[col] = val
			} else if p.currentToken.Type == lexer.STRING {
				where[col] = strings.Trim(p.currentToken.Literal, "'\"")
			} else {
				return nil, fmt.Errorf("expected number or string, got %s", p.currentToken.Literal)
			}

			p.nextToken()
			if p.currentToken.Type == lexer.EOF {
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
	p.nextToken()
	if strings.ToUpper(p.currentToken.Literal) != "FROM" {
		return nil, fmt.Errorf("expected FROM, got %s", p.currentToken.Literal)
	}

	// Parse table name
	p.nextToken()
	if p.currentToken.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.currentToken.Literal)
	}
	stmt.Table = p.currentToken.Literal

	// Parse WHERE clause if present
	p.nextToken()
	if p.currentToken.Type == lexer.EOF {
		return stmt, nil
	}

	if strings.ToUpper(p.currentToken.Literal) == "WHERE" {
		where := make(map[string]interface{})
		for {
			p.nextToken()
			if p.currentToken.Type == lexer.EOF {
				break
			}

			if p.currentToken.Type != lexer.IDENTIFIER {
				return nil, fmt.Errorf("expected column name, got %s", p.currentToken.Literal)
			}
			col := p.currentToken.Literal

			p.nextToken()
			if p.currentToken.Type != lexer.EQUALS {
				return nil, fmt.Errorf("expected =, got %s", p.currentToken.Literal)
			}

			p.nextToken()
			if p.currentToken.Type == lexer.NUMBER {
				val, err := strconv.ParseFloat(p.currentToken.Literal, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid number: %s", p.currentToken.Literal)
				}
				where[col] = val
			} else if p.currentToken.Type == lexer.STRING {
				where[col] = strings.Trim(p.currentToken.Literal, "'\"")
			} else {
				return nil, fmt.Errorf("expected number or string, got %s", p.currentToken.Literal)
			}

			p.nextToken()
			if p.currentToken.Type == lexer.EOF {
				break
			}
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseCreate() (*CreateStatement, error) {
	stmt := &CreateStatement{}

	// Parse TABLE keyword - use peekToken which was already read by New()
	tok := p.peekToken
	p.nextToken() // Advance to consume the TABLE token
	if strings.ToUpper(tok.Literal) != "TABLE" {
		return nil, fmt.Errorf("expected TABLE, got %s", tok.Literal)
	}

	// Parse table name
	p.nextToken()
	if p.currentToken.Type != lexer.IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got %s", p.currentToken.Literal)
	}
	stmt.Table = p.currentToken.Literal

	// Parse column definitions
	p.nextToken()
	if p.currentToken.Type != lexer.LPAREN {
		return nil, fmt.Errorf("expected (, got %s", p.currentToken.Literal)
	}

	for {
		p.nextToken()
		if p.currentToken.Type == lexer.RPAREN {
			break
		}

		if p.currentToken.Type != lexer.IDENTIFIER {
			return nil, fmt.Errorf("expected column name, got %s", p.currentToken.Literal)
		}
		colName := p.currentToken.Literal

		p.nextToken()
		// Accept both IDENTIFIER and KEYWORD as column types
		if p.currentToken.Type != lexer.IDENTIFIER && p.currentToken.Type != lexer.KEYWORD {
			return nil, fmt.Errorf("expected column type, got %s", p.currentToken.Literal)
		}
		// Normalize type name to uppercase for consistency
		colType := strings.ToUpper(p.currentToken.Literal)

		stmt.Columns = append(stmt.Columns, struct {
			Name     string
			Type     string
			Nullable bool
		}{
			Name:     colName,
			Type:     colType,
			Nullable: true,
		})

		p.nextToken()
		if p.currentToken.Type == lexer.RPAREN {
			break
		}
		if p.currentToken.Type != lexer.COMMA {
			return nil, fmt.Errorf("expected comma or ), got %s", p.currentToken.Literal)
		}
	}

	return stmt, nil
}
