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
		fmt.Printf("Failed to initialize storage: %v\n", err)
		return
	}
	defer storage.Close()

	// Check if input is being piped
	stat, _ := os.Stdin.Stat()
	isPiped := (stat.Mode() & os.ModeCharDevice) == 0

	// Set up input scanner
	scanner := bufio.NewScanner(os.Stdin)
	if !isPiped {
		fmt.Println("UlinDB SQL Server")
		fmt.Println("Enter SQL commands (or 'exit' to quit):")
	}

	for {
		if !isPiped {
			fmt.Print("> ")
		}
		if !scanner.Scan() {
			break
		}

		// Get input and trim whitespace
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		// Handle exit command
		input = strings.TrimSuffix(input, ";")
		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "quit" {
			break
		}

		// Parse and execute the SQL command
		stmt := ulindb.Parse(input)
		if stmt.Err != nil {
			fmt.Printf("Error parsing SQL: %v\n", stmt.Err)
			continue
		}

		// Execute based on statement type
		var err error
		switch {
		case stmt.CreateStatement.TableName != "":
			err = storage.CreateTable(&ulindb.Table{
				Name:    stmt.CreateStatement.TableName,
				Columns: stmt.CreateStatement.Columns,
			})
		case stmt.InsertStatement.Table != "":
			// Convert InsertStatement to map[string]interface{}
			values := make(map[string]interface{})
			table := storage.GetTable(stmt.InsertStatement.Table)
			if table == nil {
				fmt.Printf("Error: table %s does not exist\n", stmt.InsertStatement.Table)
				continue
			}
			for i, col := range table.Columns {
				if i < len(stmt.InsertStatement.Items) {
					values[col.Name] = stmt.InsertStatement.Items[i].Value
				}
			}
			err = storage.Insert(stmt.InsertStatement.Table, values)
		case stmt.SelectStatement.From.Name != "":
			// Convert SelectItem slice to string slice
			columns := make([]string, len(stmt.SelectStatement.Items))
			for i, item := range stmt.SelectStatement.Items {
				if item.All {
					columns[i] = "*"
				} else {
					columns[i] = item.Column
				}
			}
			results, err := storage.Select(
				stmt.SelectStatement.From.Name,
				columns,
				stmt.SelectStatement.Where,
			)
			if err == nil {
				for _, row := range results {
					fmt.Println(row)
				}
			}
		case stmt.UpdateStatement.Table != "":
			// Convert map[string]string to map[string]interface{}
			values := make(map[string]interface{})
			for k, v := range stmt.UpdateStatement.Set {
				values[k] = v
			}
			err = storage.Update(
				stmt.UpdateStatement.Table,
				values,
				stmt.UpdateStatement.Where,
			)
		case stmt.DeleteStatement.From.Name != "":
			err = storage.Delete(
				stmt.DeleteStatement.From.Name,
				stmt.DeleteStatement.Where,
			)
		}

		if err != nil {
			fmt.Printf("Error executing command: %v\n", err)
		} else {
			fmt.Println("Command executed successfully")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
