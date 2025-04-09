package ulindb

import (
	"fmt"
	"strconv"
	"strings"
)

func parseInsert(tokens []*Token) InsertStatement {
	var insertStmt InsertStatement
	insertStmt.Items = []InsertItem{} // Initialize with empty slice
	var currentTokenIndex int

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(IntoKeyword):
				currentTokenIndex++
				insertStmt.Table = tokens[currentTokenIndex].Value
			case string(ValuesKeyword):
				currentTokenIndex++
				if currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type == SymbolType && tokens[currentTokenIndex].Value == "(" {
					currentTokenIndex++
					for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Value != ")" {
						if tokens[currentTokenIndex].Type == SymbolType && tokens[currentTokenIndex].Value == "," {
							currentTokenIndex++
							continue
						}
						value := tokens[currentTokenIndex].Value
						if tokens[currentTokenIndex].Type == StringType {
							// Remove quotes from string values
							value = strings.Trim(value, "'\"")
							insertStmt.Items = append(insertStmt.Items, InsertItem{Value: value})
						} else if tokens[currentTokenIndex].Type == NumberType {
							// Convert numeric values to float64
							if strings.Contains(value, ".") {
								if f, err := strconv.ParseFloat(value, 64); err == nil {
									insertStmt.Items = append(insertStmt.Items, InsertItem{Value: f})
								}
							} else {
								if i, err := strconv.Atoi(value); err == nil {
									insertStmt.Items = append(insertStmt.Items, InsertItem{Value: i})
								}
							}
						} else if tokens[currentTokenIndex].Type == KeywordType && tokens[currentTokenIndex].Value == "NULL" {
							insertStmt.Items = append(insertStmt.Items, InsertItem{Value: nil})
						}
						currentTokenIndex++
					}
				}
			}
		}

		currentTokenIndex++
	}

	return insertStmt
}

type Statement struct {
	Err             error
	SelectStatement SelectStatement
	InsertStatement InsertStatement
	UpdateStatement UpdateStatement
	DeleteStatement DeleteStatement
	CreateStatement CreateStatement
}

type CreateStatement struct {
	TableName string
	Columns   []ColumnDefinition
	Err       error
}

type ColumnDefinition struct {
	Name     string
	Type     string
	Nullable bool
}

type Expression struct {
	Left     string
	Operator string
	Right    string
}

type SelectItem struct {
	All        bool
	Column     string
	Alias      string
	Expression Expression
}

type FromItem struct {
	Name string
}

type SelectStatement struct {
	Items []SelectItem
	From  FromItem
	Where string
}

type InsertItem struct {
	Value interface{}
}

type InsertStatement struct {
	Table string
	Items []InsertItem
}

type UpdateStatement struct {
	Table string
	Set   map[string]interface{}
	Where string
}

type DeleteStatement struct {
	From  FromItem
	Where string
}

func Parse(query string) *Statement {
	// Handle exit and quit commands
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "exit" || query == "quit" {
		return &Statement{}
	}

	tokens, err := lex(query)
	if err != nil {
		return &Statement{Err: err}
	}
	if len(tokens) == 0 {
		return &Statement{Err: fmt.Errorf("empty query")}
	}

	var stmt Statement

	switch tokens[0].Value {
	case string(SelectKeyword):
		stmt.SelectStatement = parseSelect(tokens[1:])
	case string(InsertKeyword):
		stmt.InsertStatement = parseInsert(tokens[1:])
	case string(UpdateKeyword):
		stmt.UpdateStatement = parseUpdate(tokens[1:])
	case string(DeleteKeyword):
		stmt.DeleteStatement = parseDelete(tokens[1:])
	case string(CreateKeyword):
		createStmt := parseCreate(tokens[1:])
		if createStmt.Err != nil {
			stmt.Err = createStmt.Err
		} else {
			stmt.CreateStatement = createStmt
		}
	default:
		stmt.Err = fmt.Errorf("expected SELECT, CREATE, INSERT, UPDATE, or DELETE but got %s", tokens[0].Value)
	}

	return &stmt
}

func parseSelect(tokens []*Token) SelectStatement {
	var selectStmt SelectStatement
	var currentTokenIndex int
	var parsingColumns = true

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(FromKeyword):
				parsingColumns = false
				currentTokenIndex++
				selectStmt.From = FromItem{Name: tokens[currentTokenIndex].Value}
			case string(WhereKeyword):
				currentTokenIndex++
				var conditionBuilder strings.Builder
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					conditionBuilder.WriteString(tokens[currentTokenIndex].Value)
					conditionBuilder.WriteString(" ")
					currentTokenIndex++
				}
				selectStmt.Where = strings.TrimSpace(conditionBuilder.String())
				currentTokenIndex--
			}
		case IdentifierType, SymbolType:
			if parsingColumns {
				if token.Value == "*" {
					selectStmt.Items = append(selectStmt.Items, SelectItem{All: true})
				} else {
					selectStmt.Items = append(selectStmt.Items, SelectItem{All: false, Column: token.Value})
				}
			}
		}

		currentTokenIndex++
	}

	return selectStmt
}

func parseUpdate(tokens []*Token) UpdateStatement {
	var updateStmt UpdateStatement
	var currentTokenIndex int

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(SetKeyword):
				currentTokenIndex++
				updateStmt.Set = make(map[string]interface{})
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					if currentTokenIndex+2 >= len(tokens) {
						break
					}
					column := tokens[currentTokenIndex].Value
					currentTokenIndex += 2 // Skip '='
					value := tokens[currentTokenIndex].Value
					if tokens[currentTokenIndex].Type == StringType {
						// Remove quotes from string values
						value = strings.Trim(value, "'\"")
						updateStmt.Set[column] = value
					} else if tokens[currentTokenIndex].Type == NumberType {
						// Convert numeric values to float64
						if strings.Contains(value, ".") {
							if f, err := strconv.ParseFloat(value, 64); err == nil {
								updateStmt.Set[column] = f
							}
						} else {
							if i, err := strconv.Atoi(value); err == nil {
								updateStmt.Set[column] = i
							}
						}
					} else if tokens[currentTokenIndex].Type == KeywordType && tokens[currentTokenIndex].Value == "NULL" {
						updateStmt.Set[column] = nil
					}
					currentTokenIndex++
					if currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type == SymbolType && tokens[currentTokenIndex].Value == "," {
						currentTokenIndex++
					}
				}
				currentTokenIndex--
			case string(WhereKeyword):
				currentTokenIndex++
				var conditionBuilder strings.Builder
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					conditionBuilder.WriteString(tokens[currentTokenIndex].Value)
					conditionBuilder.WriteString(" ")
					currentTokenIndex++
				}
				updateStmt.Where = strings.TrimSpace(conditionBuilder.String())
				currentTokenIndex--
			}
		case IdentifierType:
			updateStmt.Table = token.Value
		}

		currentTokenIndex++
	}

	return updateStmt
}

func parseDelete(tokens []*Token) DeleteStatement {
	var deleteStmt DeleteStatement
	var currentTokenIndex int

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(FromKeyword):
				currentTokenIndex++
				fromItem := FromItem{
					Name: tokens[currentTokenIndex].Value,
				}
				deleteStmt.From = fromItem
			case string(WhereKeyword):
				currentTokenIndex++
				var conditionBuilder strings.Builder
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					conditionBuilder.WriteString(tokens[currentTokenIndex].Value)
					conditionBuilder.WriteString(" ")
					currentTokenIndex++
				}
				deleteStmt.Where = strings.TrimSpace(conditionBuilder.String())
				currentTokenIndex--
			}
		}

		currentTokenIndex++
	}

	return deleteStmt
}

func parseCreate(tokens []*Token) CreateStatement {
	var createStmt CreateStatement
	var currentTokenIndex int
	columnNames := make(map[string]bool)

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(TableKeyword):
				currentTokenIndex++
				createStmt.TableName = tokens[currentTokenIndex].Value
				currentTokenIndex++
				if currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type == SymbolType && tokens[currentTokenIndex].Value == "(" {
					currentTokenIndex++
					for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != SymbolType {
						columnName := tokens[currentTokenIndex].Value
						if columnNames[columnName] {
							createStmt.Err = fmt.Errorf("duplicate column name: %s", columnName)
							return createStmt
						}
						columnNames[columnName] = true

						column := ColumnDefinition{
							Name:     columnName,
							Nullable: false, // By default, columns are not nullable
						}
						currentTokenIndex++
						column.Type = strings.ToUpper(tokens[currentTokenIndex].Value)
						createStmt.Columns = append(createStmt.Columns, column)
						currentTokenIndex++
						if currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type == SymbolType && tokens[currentTokenIndex].Value == "," {
							currentTokenIndex++
						}
					}
				}
			}
		}

		currentTokenIndex++
	}

	return createStmt
}
