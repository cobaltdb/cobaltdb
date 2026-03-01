package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

var (
	flagHelp    bool
	flagInMemory bool
	flagPath    string
	flagServer  bool
	flagPort    int
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
CobaltDB CLI v1.0

Usage:
  cobaltdb [options] [sql-command...]
  cobaltdb [options] -i          # Interactive mode

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
  cobaltdb -memory -i

  # Start server
  cobaltdb -server -port 4200

SQL Commands:
  DDL:
    CREATE TABLE <name> (<columns>)
    CREATE INDEX <name> ON <table>(<column>)
    DROP TABLE <name>

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

	ctx := context.Background()

	// Check if it's a query or exec
	sql = strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(sql)

	if strings.HasPrefix(upperSQL, "SELECT") {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		defer rows.Close()

		// Print columns
		cols := rows.Columns()
		if len(cols) > 0 {
			for i, col := range cols {
				if i > 0 {
					fmt.Print("\t")
				}
				fmt.Print(col)
			}
			fmt.Println()
		}

		// Print rows
		for rows.Next() {
			values := make([]interface{}, len(cols))
			rowValues := make([]interface{}, len(cols))
			for i := range values {
				rowValues[i] = &values[i]
			}
			rows.Scan(rowValues...)

			for i, v := range values {
				if i > 0 {
					fmt.Print("\t")
				}
				fmt.Print(formatValue(v))
			}
			fmt.Println()
		}
	} else {
		result, err := db.Exec(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
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

	ctx := context.Background()
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("CobaltDB Interactive CLI")
	fmt.Println("Type '.help' for commands, '.quit' to exit")
	fmt.Println()

	for {
		fmt.Print("cobaltdb> ")

		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Interactive commands
		if strings.HasPrefix(line, ".") {
			if handleMetaCommand(line, db, ctx) {
				continue
			}
			continue
		}

		// SQL commands
		runCommand(line, path, inMemory)
	}
}

func handleMetaCommand(line string, db *engine.DB, ctx context.Context) bool {
	switch strings.ToLower(line) {
	case ".quit", ".exit":
		fmt.Println("Goodbye!")
		os.Exit(0)
	case ".help":
		printHelp()
	case ".tables":
		// Use catalog to list tables
		fmt.Println("Tables:")
		// We'll just show a message since there's no system table yet
		fmt.Println("  (use catalog to manage tables)")
	case ".schema":
		fmt.Println("Use: .schema <table-name>")
	default:
		fmt.Printf("Unknown command: %s\n", line)
	}
	return true
}
