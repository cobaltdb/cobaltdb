// CLI Example - Interactive database tool
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
	fmt.Println("🔷 CobaltDB CLI")
	fmt.Println("===============")

	// Open database
	dbPath := ":memory:"
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}

	fmt.Printf("Opening database: %s\n", dbPath)

	opts := &engine.Options{
		CacheSize:  1024,
		WALEnabled: true,
		InMemory:   dbPath == ":memory:",
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Println("Connected. Type 'help' for commands, 'quit' to exit.")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()

	for {
		fmt.Print("cobaltdb> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "quit", "exit":
			fmt.Println("Goodbye!")
			return
		case "help", "h":
			printHelp()
		case "tables", "t":
			listTables(db)
		case "schema", "s":
			if len(parts) < 2 {
				fmt.Println("Usage: schema <table>")
				continue
			}
			showSchema(db, parts[1])
		case "query":
			query := strings.TrimPrefix(line, parts[0])
			executeQuery(ctx, db, strings.TrimSpace(query))
		case "exec", "e":
			query := strings.TrimPrefix(line, parts[0])
			executeExec(ctx, db, strings.TrimSpace(query))
		default:
			// Assume it's a SQL query
			if strings.HasPrefix(strings.ToUpper(line), "SELECT") {
				executeQuery(ctx, db, line)
			} else {
				executeExec(ctx, db, line)
			}
		}
	}
}

func printHelp() {
	fmt.Println("Commands:")
	fmt.Println("  help, h, ?        - Show this help")
	fmt.Println("  quit, q           - Exit CLI")
	fmt.Println("  tables, t         - List all tables")
	fmt.Println("  schema <table>   - Show table schema")
	fmt.Println("  query <sql>      - Execute SELECT query")
	fmt.Println("  exec <sql>       - Execute INSERT/UPDATE/DELETE")
	fmt.Println()
	fmt.Println("SQL Examples:")
	fmt.Println("  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	fmt.Println("  INSERT INTO users (name) VALUES ('Alice')")
	fmt.Println("  SELECT * FROM users")
	fmt.Println("  UPDATE users SET name = 'Bob' WHERE id = 1")
	fmt.Println("  DELETE FROM users WHERE id = 1")
}

func listTables(db *engine.DB) {
	tables := db.Tables()
	if len(tables) == 0 {
		fmt.Println("No tables found.")
		return
	}

	fmt.Println("Tables:")
	for _, t := range tables {
		fmt.Printf("  - %s\n", t)
	}
}

func showSchema(db *engine.DB, table string) {
	schema, err := db.TableSchema(table)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Schema for '%s':\n%s\n", table, schema)
}

func executeQuery(ctx context.Context, db *engine.DB, query string) {
	if query == "" {
		fmt.Println("No query provided.")
		return
	}

	rows, err := db.Query(ctx, query)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer rows.Close()

	cols := rows.Columns()
	if len(cols) == 0 {
		fmt.Println("No columns returned.")
		return
	}

	// Print header
	fmt.Println()
	for _, col := range cols {
		fmt.Printf("%-15s", col)
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", len(cols)*15))

	// Print rows
	rowCount := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			fmt.Printf("Scan error: %v\n", err)
			continue
		}

		for _, v := range values {
			fmt.Printf("%-15v", v)
		}
		fmt.Println()
		rowCount++
	}

	fmt.Println()
	fmt.Printf("(%d rows)\n", rowCount)
}

func executeExec(ctx context.Context, db *engine.DB, query string) {
	if query == "" {
		fmt.Println("No query provided.")
		return
	}

	result, err := db.Exec(ctx, query)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if result.RowsAffected >= 0 {
		fmt.Printf("OK, %d rows affected\n", result.RowsAffected)
	} else {
		fmt.Println("OK")
	}

	if result.LastInsertID > 0 {
		fmt.Printf("Last insert ID: %d\n", result.LastInsertID)
	}
}
