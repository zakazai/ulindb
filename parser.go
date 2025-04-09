package ulindb

import (
	"log"
	"strings"
)

func parseInsert(tokens []*Token) InsertStatement {
	var insertStmt InsertStatement
	insertStmt.items = []InsertItem{} // Initialize with empty slice
	var currentTokenIndex int

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(IntoKeyword):
				currentTokenIndex++
				insertStmt.table = tokens[currentTokenIndex].Value
			case string(ValuesKeyword):
				currentTokenIndex++
				if currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type == SymbolType && tokens[currentTokenIndex].Value == "(" {
					currentTokenIndex++
					for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != SymbolType {
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
	err             error
	selectStatement SelectStatement
	insertStatement InsertStatement
	updateStatement UpdateStatement
	deleteStatement DeleteStatement
	createStatement CreateStatement
}

type CreateStatement struct {
	tableName string
	columns   []ColumnDefinition
}

type ColumnDefinition struct {
	name string
	typ  string
}

type Expression struct {
	left     string
	operator string
	right    string
}

type SelectItem struct {
	All        bool
	Column     string
	Alias      string
	Expression Expression
}

type FromItem struct {
	name string
}

type SelectStatement struct {
	items []SelectItem
	from  FromItem
	where string
}

type InsertItem struct {
}

type InsertStatement struct {
	table string
	items []InsertItem
}

type UpdateStatement struct {
	table string
	set   map[string]string
	where string
}

type DeleteStatement struct {
	from  FromItem
	where string
}

func parse(query string) *Statement {
	tokens, err := lex(query)
	if err != nil {
		log.Fatal("Error found in SQL ", err)
	}

	var queryType = tokens[0]
	switch {
	case queryType.Type == KeywordType && queryType.Value == string(SelectKeyword):
		return &Statement{
			err:             nil,
			selectStatement: parseSelect(tokens),
		}
	case queryType.Type == KeywordType && queryType.Value == string(InsertKeyword):
		return &Statement{
			err:             nil,
			insertStatement: parseInsert(tokens),
		}
	case queryType.Type == KeywordType && queryType.Value == string(UpdateKeyword):
		return &Statement{
			err:             nil,
			updateStatement: parseUpdate(tokens),
		}
	case queryType.Type == KeywordType && queryType.Value == string(DeleteKeyword):
		return &Statement{
			err:             nil,
			deleteStatement: parseDelete(tokens),
		}
	case queryType.Type == KeywordType && queryType.Value == string(CreateKeyword):
		return &Statement{
			err:             nil,
			createStatement: parseCreate(tokens),
		}
	default:
		log.Fatal("Only SELECT, INSERT, UPDATE, DELETE, and CREATE are supported")
	}

	return &Statement{}
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
				selectStmt.from = FromItem{name: tokens[currentTokenIndex].Value}
			case string(WhereKeyword):
				currentTokenIndex++
				var conditionBuilder strings.Builder
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					conditionBuilder.WriteString(tokens[currentTokenIndex].Value)
					conditionBuilder.WriteString(" ")
					currentTokenIndex++
				}
				selectStmt.where = strings.TrimSpace(conditionBuilder.String())
				currentTokenIndex--
			}
		case IdentifierType, SymbolType:
			if parsingColumns {
				if token.Value == "*" {
					selectStmt.items = append(selectStmt.items, SelectItem{All: true})
				} else {
					selectStmt.items = append(selectStmt.items, SelectItem{All: false, Column: token.Value})
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
				updateStmt.set = make(map[string]string)
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					column := tokens[currentTokenIndex].Value
					currentTokenIndex += 2 // Skip '='
					value := tokens[currentTokenIndex].Value
					updateStmt.set[column] = value
					currentTokenIndex++
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
				updateStmt.where = strings.TrimSpace(conditionBuilder.String())
				currentTokenIndex--
			}
		case IdentifierType:
			updateStmt.table = token.Value
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
					name: tokens[currentTokenIndex].Value,
				}
				deleteStmt.from = fromItem
			case string(WhereKeyword):
				currentTokenIndex++
				var conditionBuilder strings.Builder
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					conditionBuilder.WriteString(tokens[currentTokenIndex].Value)
					conditionBuilder.WriteString(" ")
					currentTokenIndex++
				}
				deleteStmt.where = strings.TrimSpace(conditionBuilder.String())
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

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(TableKeyword):
				currentTokenIndex++
				createStmt.tableName = tokens[currentTokenIndex].Value
				currentTokenIndex++
				if currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type == SymbolType && tokens[currentTokenIndex].Value == "(" {
					currentTokenIndex++
					for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != SymbolType {
						column := ColumnDefinition{
							name: tokens[currentTokenIndex].Value,
						}
						currentTokenIndex++
						column.typ = strings.ToUpper(tokens[currentTokenIndex].Value)
						createStmt.columns = append(createStmt.columns, column)
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
