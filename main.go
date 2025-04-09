package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	ulindb "github.com/zakazai/ulin-db"
)

func main() {
	// Initialize storage
	storage, err := ulindb.NewStorage(ulindb.StorageConfig{
		Type:     ulindb.InMemoryStorageType,
		FilePath: "",
	})
	if err != nil {
		fmt.Printf("Error initializing storage: %v\n", err)
		os.Exit(1)
	}

	// Create REPL
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Welcome to UlinDB SQL Server!")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println("Supported commands: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE")

	for {
		fmt.Print("ulin-db> ")
		query, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Error reading input: %v\n", err)
			continue
		}

		// Trim whitespace and check for exit commands
		query = strings.TrimSpace(query)
		if query == "exit" || query == "quit" {
			break
		}

		// Skip empty queries
		if query == "" {
			continue
		}

		// Parse and execute the query
		stmt := ulindb.Parse(query)
		if stmt.Err != nil {
			fmt.Printf("Error parsing query: %v\n", stmt.Err)
			continue
		}

		// Execute the query based on its type
		switch {
		case stmt.SelectStatement != nil:
			results, err := storage.Select(stmt.SelectStatement.From.Name, stmt.SelectStatement.Columns, stmt.SelectStatement.Where)
			if err != nil {
				fmt.Printf("Error executing SELECT: %v\n", err)
				continue
			}
			// Print results
			for _, row := range results {
				fmt.Println(row)
			}

		case stmt.InsertStatement != nil:
			err := storage.Insert(stmt.InsertStatement.Table, stmt.InsertStatement.Values)
			if err != nil {
				fmt.Printf("Error executing INSERT: %v\n", err)
				continue
			}
			fmt.Println("Insert successful")

		case stmt.UpdateStatement != nil:
			err := storage.Update(stmt.UpdateStatement.Table, stmt.UpdateStatement.Set, stmt.UpdateStatement.Where)
			if err != nil {
				fmt.Printf("Error executing UPDATE: %v\n", err)
				continue
			}
			fmt.Println("Update successful")

		case stmt.DeleteStatement != nil:
			err := storage.Delete(stmt.DeleteStatement.From.Name, stmt.DeleteStatement.Where)
			if err != nil {
				fmt.Printf("Error executing DELETE: %v\n", err)
				continue
			}
			fmt.Println("Delete successful")

		case stmt.CreateStatement != nil:
			table := &ulindb.Table{
				Name:    stmt.CreateStatement.TableName,
				Columns: stmt.CreateStatement.Columns,
			}
			err := storage.CreateTable(table)
			if err != nil {
				fmt.Printf("Error executing CREATE TABLE: %v\n", err)
				continue
			}
			fmt.Println("Table created successfully")

		default:
			fmt.Println("Unsupported query type")
		}
	}

	// Close storage
	if err := storage.Close(); err != nil {
		fmt.Printf("Error closing storage: %v\n", err)
		os.Exit(1)
	}
}
