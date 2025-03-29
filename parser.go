package ulindb

import (
	"log"
	"strings"
)

func parseInsert(tokens []*Token) InsertStatement {
	var insertStmt InsertStatement
	var currentTokenIndex int

	for currentTokenIndex < len(tokens) {
		token := tokens[currentTokenIndex]

		switch token.Type {
		case KeywordType:
			switch token.Value {
			case string(ValuesKeyword):
				currentTokenIndex++
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != SymbolType {
					insertItem := InsertItem{}
					insertStmt.items = append(insertStmt.items, insertItem)
					currentTokenIndex++
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
				// Parse the FROM clause
				parsingColumns = false
				currentTokenIndex++
				if currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type == IdentifierType {
					selectStmt.from = FromItem{name: tokens[currentTokenIndex].Value}
				}
			case string(WhereKeyword):
				// Parse the WHERE clause
				currentTokenIndex++
				var whereExpr strings.Builder
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != KeywordType {
					whereExpr.WriteString(tokens[currentTokenIndex].Value)
					if currentTokenIndex+1 < len(tokens) && tokens[currentTokenIndex+1].Type != KeywordType {
						whereExpr.WriteString(" ")
					}
					currentTokenIndex++
				}
				selectStmt.where = whereExpr.String()
				currentTokenIndex-- // Adjust for loop increment
			}
		case IdentifierType, SymbolType:
			if parsingColumns {
				// Parse the SELECT clause
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
				for tokens[currentTokenIndex].Type != KeywordType && tokens[currentTokenIndex].Value != string(WhereKeyword) {
					column := tokens[currentTokenIndex].Value
					currentTokenIndex++ // skip column name
					currentTokenIndex++ // skip '='
					value := tokens[currentTokenIndex].Value
					updateStmt.set[column] = value
					currentTokenIndex++
				}
			case string(WhereKeyword):
				currentTokenIndex++
				updateStmt.where = tokens[currentTokenIndex].Value
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
				deleteStmt.where = tokens[currentTokenIndex].Value
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
				for currentTokenIndex < len(tokens) && tokens[currentTokenIndex].Type != SymbolType {
					column := ColumnDefinition{
						name: tokens[currentTokenIndex].Value,
					}
					currentTokenIndex++
					column.typ = tokens[currentTokenIndex].Value
					createStmt.columns = append(createStmt.columns, column)
					currentTokenIndex++
				}
			}
		}

		currentTokenIndex++
	}

	return createStmt
}
