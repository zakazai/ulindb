package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/zakazai/ulin-db/internal/parser"
	"github.com/zakazai/ulin-db/internal/storage"
)

func main() {
	db := storage.NewJSONStorage("data.json")
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("UlinDB SQL Server started. Type 'exit' to quit.")

	for scanner.Scan() {
		input := scanner.Text()
		if err := scanner.Err(); err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		input = strings.TrimSpace(input)
		if strings.ToLower(input) == "exit" || strings.ToLower(input) == "exit;" {
			fmt.Println("bye")
			break
		}

		stmt, err := parser.Parse(input)
		if err != nil {
			fmt.Println("Error parsing statement:", err)
			continue
		}

		result, err := stmt.Execute(db)
		if err != nil {
			if strings.HasPrefix(err.Error(), "success:") {
				// This is a success message
				fmt.Println(err.Error())
			} else {
				fmt.Println("Error executing statement:", err)
			}
			continue
		}

		if result != "" {
			fmt.Println(result)
		}
	}
}
