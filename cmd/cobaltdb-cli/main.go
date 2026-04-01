package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

var (
	flagHelp     bool
	flagInMemory bool
	flagPath     string
	flagServer   bool
	flagPort     int
)

func init() {
	flag.BoolVar(&flagHelp, "help", false, "Show help")
	flag.BoolVar(&flagHelp, "h", false, "Show help (short)")
	flag.BoolVar(&flagInMemory, "memory", false, "Use in-memory database")
	flag.StringVar(&flagPath, "path", ":memory:", "Database path (default: :memory:)")
	flag.BoolVar(&flagServer, "server", false, "Start as server")
	flag.IntVar(&flagPort, "port", 4200, "Server port")
}

func main() {
	flag.Parse()

	if flagHelp || len(os.Args) == 1 {
		printHelp()
		os.Exit(0)
	}

	// Get remaining args as SQL commands
	args := flag.Args()
	if len(args) == 0 {
		// Interactive mode
		runInteractive(flagPath, flagInMemory)
		return
	}

	// Execute single command
	runCommand(strings.Join(args, " "), flagPath, flagInMemory)
}

func printHelp() {
	fmt.Print(`
CobaltDB CLI v2.0

Usage:
  cobaltdb [options] [sql-command...]
  cobaltdb [options]              # Interactive mode

Options:
  -h, -help           Show this help message
  -memory             Use in-memory database (ephemeral)
  -path <path>        Database file path (default: :memory:)
  -server             Start as TCP server
  -port <port>        Server port (default: 4200)

Examples:
  # In-memory database
  cobaltdb -memory "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

  # Disk database
  cobaltdb -path ./mydb.db "SELECT * FROM users"

  # Interactive mode
  cobaltdb -memory

  # Start server
  cobaltdb -server -port 4200

SQL Commands:
  DDL:
    CREATE TABLE <name> (<columns>)
    CREATE INDEX <name> ON <table>(<column>)
    DROP TABLE <name>
    ALTER TABLE <name> ADD COLUMN <col> <type>

  DML:
    INSERT INTO <table> (<cols>) VALUES (<values>)
    SELECT <cols> FROM <table> [WHERE <cond>]
    UPDATE <table> SET <col>=<val> [WHERE <cond>]
    DELETE FROM <table> [WHERE <cond>]

  Transactions:
    BEGIN
    COMMIT
    ROLLBACK

Interactive Commands:
  .tables              List all tables
  .schema <table>      Show table schema
  .quit, .exit         Exit CLI
  .help                Show this help
`)
}

func runCommand(sql, path string, inMemory bool) {
	opts := &engine.Options{
		InMemory: inMemory,
	}
	if !inMemory && path != ":memory:" {
		opts.InMemory = false
	}

	db, err := engine.Open(path, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	executeSQL(db, sql)
}

func executeSQL(db *engine.DB, sql string) {
	ctx := context.Background()
	sql = strings.TrimSpace(sql)

	if sql == "" {
		return
	}

	upperSQL := strings.ToUpper(sql)

	if strings.HasPrefix(upperSQL, "SELECT") || strings.HasPrefix(upperSQL, "WITH") ||
		strings.HasPrefix(upperSQL, "SHOW") || strings.HasPrefix(upperSQL, "DESCRIBE") ||
		strings.HasPrefix(upperSQL, "DESC ") {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		defer rows.Close()

		printRows(rows)
	} else {
		result, err := db.Exec(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		if result.RowsAffected > 0 {
			fmt.Printf("Rows affected: %d\n", result.RowsAffected)
		}
		if result.LastInsertID > 0 {
			fmt.Printf("Last insert ID: %d\n", result.LastInsertID)
		}
		if result.RowsAffected == 0 && result.LastInsertID == 0 {
			fmt.Println("OK")
		}
	}
}

func printRows(rows *engine.Rows) {
	cols := rows.Columns()
	if len(cols) == 0 {
		return
	}

	// Print columns
	for i, col := range cols {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(col)
	}
	fmt.Println()

	// Print separator
	for i, col := range cols {
		if i > 0 {
			fmt.Print("\t")
		}
		fmt.Print(strings.Repeat("-", max(len(col), 4)))
	}
	fmt.Println()

	// Print rows
	count := 0
	for rows.Next() {
		values := make([]interface{}, len(cols))
		rowValues := make([]interface{}, len(cols))
		for i := range values {
			rowValues[i] = &values[i]
		}
		if err := rows.Scan(rowValues...); err != nil {
			fmt.Printf("scan error: %v\n", err)
			continue
		}

		for i, v := range values {
			if i > 0 {
				fmt.Print("\t")
			}
			fmt.Print(formatValue(v))
		}
		fmt.Println()
		count++
	}

	fmt.Printf("(%d rows)\n", count)
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func runInteractive(path string, inMemory bool) {
	opts := &engine.Options{
		InMemory: inMemory,
	}

	db, err := engine.Open(path, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	reader := bufio.NewReader(os.Stdin)

	fmt.Println("CobaltDB Interactive CLI v2.0")
	fmt.Println("Type '.help' for commands, '.quit' to exit")
	fmt.Println("End SQL statements with ';' (multi-line supported)")
	fmt.Println()

	var sqlBuffer strings.Builder
	inMultiLine := false

	for {
		if inMultiLine {
			fmt.Print("      ...> ")
		} else {
			fmt.Print("cobaltdb> ")
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			// Execute any remaining buffer before exit
			if sqlBuffer.Len() > 0 {
				executeSQL(db, sqlBuffer.String())
			}
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Meta commands only when not in multi-line mode
		if !inMultiLine && strings.HasPrefix(line, ".") {
			handleMetaCommand(line, db)
			continue
		}

		// Accumulate SQL
		if sqlBuffer.Len() > 0 {
			sqlBuffer.WriteString(" ")
		}
		sqlBuffer.WriteString(line)

		// Check if statement is complete (ends with semicolon)
		trimmed := strings.TrimSpace(sqlBuffer.String())
		if strings.HasSuffix(trimmed, ";") {
			// Remove trailing semicolon and execute
			sql := strings.TrimSuffix(trimmed, ";")
			executeSQL(db, sql)
			sqlBuffer.Reset()
			inMultiLine = false
		} else {
			// Check for single-line commands that don't need semicolons
			upper := strings.ToUpper(trimmed)
			if strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "COMMIT") ||
				strings.HasPrefix(upper, "ROLLBACK") || strings.HasPrefix(upper, "USE ") {
				executeSQL(db, trimmed)
				sqlBuffer.Reset()
				inMultiLine = false
			} else {
				inMultiLine = true
			}
		}
	}
}

func handleMetaCommand(line string, db *engine.DB) {
	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case ".quit", ".exit":
		fmt.Println("Goodbye!")
		os.Exit(0)

	case ".help":
		printHelp()

	case ".tables":
		tables := db.Tables()
		if len(tables) == 0 {
			fmt.Println("No tables found.")
			return
		}
		sort.Strings(tables)
		for _, t := range tables {
			fmt.Printf("  %s\n", t)
		}

	case ".schema":
		if len(parts) < 2 {
			// Show all schemas
			tables := db.Tables()
			sort.Strings(tables)
			for _, t := range tables {
				schema, err := db.TableSchema(t)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					continue
				}
				fmt.Println(schema)
				fmt.Println()
			}
			return
		}
		schema, err := db.TableSchema(parts[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		fmt.Println(schema)

	default:
		fmt.Printf("Unknown command: %s\nType '.help' for available commands.\n", cmd)
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
