package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

var (
	flagHelp     bool
	flagInMemory bool
	flagPath     string
	flagServer   bool
	flagPort     int
)

var version = "dev"

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

	// Get remaining args as SQL commands or subcommands
	args := flag.Args()
	if len(args) == 0 {
		// Interactive mode
		runInteractive(flagPath, flagInMemory)
		return
	}

	// Check for subcommands
	switch args[0] {
	case "backup":
		runBackupCommand(args[1:], flagPath, flagInMemory)
	case "metrics":
		runMetricsCommand(flagPath, flagInMemory)
	case "status":
		runStatusCommand(flagPath, flagInMemory)
	case "vacuum":
		runVacuumCommand(flagPath, flagInMemory)
	case "analyze":
		runAnalyzeCommand(flagPath, flagInMemory)
	case "import":
		runImportCommand(args[1:], flagPath, flagInMemory)
	case "export":
		runExportCommand(args[1:], flagPath, flagInMemory)
	default:
		// Execute single SQL command
		runCommand(strings.Join(args, " "), flagPath, flagInMemory)
	}
}

func printHelp() {
	fmt.Printf(`
CobaltDB CLI %s

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

Subcommands:
  backup create [full|incremental|differential]   Create a database backup
  backup list                                     List all backups
  backup restore <id>                             Restore a backup by ID
  backup delete <id>                             Delete a backup by ID
  metrics                                         Show database metrics
  status                                          Show database status
  vacuum                                          Run VACUUM on the database
  analyze                                         Run ANALYZE on the database
  import <file.csv> <table>                     Import CSV into a table
  export <table> <file.csv> [--format csv|json]  Export table to file

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
  .backup create ...   Create backup
  .backup list         List backups
  .backup restore <id> Restore backup
  .backup delete <id>  Delete backup
  .metrics             Show metrics
  .status              Show status
  .vacuum              Run VACUUM
  .analyze             Run ANALYZE
  .import <csv> <tbl>  Import CSV
  .export <tbl> <csv>  Export table to CSV
`, version)
}

func openDB(path string, inMemory bool) *engine.DB {
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
	return db
}

func runCommand(sql, path string, inMemory bool) {
	db := openDB(path, inMemory)
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
	db := openDB(path, inMemory)
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

	case ".backup":
		if len(parts) < 2 {
			fmt.Println("Usage: .backup create [full|incremental|differential]")
			fmt.Println("       .backup list")
			fmt.Println("       .backup restore <id>")
			fmt.Println("       .backup delete <id>")
			return
		}
		handleBackupCommand(parts[1:], db)

	case ".metrics":
		data, err := db.GetMetrics()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		fmt.Println(string(data))

	case ".status":
		printStatus(db)

	case ".vacuum":
		executeSQL(db, "VACUUM")

	case ".analyze":
		executeSQL(db, "ANALYZE")

	case ".import":
		if len(parts) < 3 {
			fmt.Println("Usage: .import <file.csv> <table>")
			return
		}
		if err := importCSV(db, parts[1], parts[2]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}

	case ".export":
		if len(parts) < 3 {
			fmt.Println("Usage: .export <table> <file.csv> [--format csv|json]")
			return
		}
		format := "csv"
		if len(parts) > 4 && parts[3] == "--format" {
			format = parts[4]
		}
		if err := exportTable(db, parts[1], parts[2], format); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}

	default:
		fmt.Printf("Unknown command: %s\nType '.help' for available commands.\n", cmd)
	}
}

// Subcommand handlers

func runBackupCommand(args []string, path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()

	if len(args) == 0 {
		fmt.Println("Usage: backup create [full|incremental|differential]")
		fmt.Println("       backup list")
		fmt.Println("       backup restore <id>")
		fmt.Println("       backup delete <id>")
		os.Exit(1)
	}
	handleBackupCommand(args, db)
}

func handleBackupCommand(args []string, db *engine.DB) {
	ctx := context.Background()
	sub := strings.ToLower(args[0])

	switch sub {
	case "create":
		backupType := "full"
		if len(args) > 1 {
			backupType = strings.ToLower(args[1])
		}
		b, err := db.CreateBackup(ctx, backupType)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating backup: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Backup created: %s\n", b.ID)
		fmt.Printf("Type: %s, Size: %d bytes\n", backupType, b.Size)

	case "list":
		backups := db.ListBackups()
		if len(backups) == 0 {
			fmt.Println("No backups found.")
			return
		}
		fmt.Printf("%-30s %-12s %-20s %10s\n", "ID", "Type", "Completed", "Size")
		fmt.Println(strings.Repeat("-", 80))
		for _, b := range backups {
			btype := "full"
			if b.Incremental {
				btype = "incremental"
			}
			fmt.Printf("%-30s %-12s %-20s %10d\n", b.ID, btype, b.CompletedAt.Format(time.RFC3339), b.Size)
		}

	case "restore":
		if len(args) < 2 {
			fmt.Println("Usage: backup restore <id>")
			os.Exit(1)
		}
		id := args[1]
		targetPath := db.Path() + ".restored"
		mgr := db.GetBackupManager()
		if err := mgr.Restore(ctx, id, targetPath); err != nil {
			fmt.Fprintf(os.Stderr, "Error restoring backup: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Backup restored to: %s\n", targetPath)

	case "delete":
		if len(args) < 2 {
			fmt.Println("Usage: backup delete <id>")
			os.Exit(1)
		}
		if err := db.DeleteBackup(args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting backup: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Backup deleted.")

	default:
		fmt.Printf("Unknown backup subcommand: %s\n", sub)
		os.Exit(1)
	}
}

func runMetricsCommand(path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()

	data, err := db.GetMetrics()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(string(data))
}

func runStatusCommand(path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()

	printStatus(db)
}

func printStatus(db *engine.DB) {
	fmt.Printf("Database path: %s\n", db.Path())

	tables := db.Tables()
	fmt.Printf("Tables: %d\n", len(tables))

	sched := db.GetScheduler()
	if sched != nil {
		fmt.Printf("Scheduler: running\n")
	} else {
		fmt.Printf("Scheduler: not running\n")
	}
}

func runVacuumCommand(path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()

	executeSQL(db, "VACUUM")
}

func runAnalyzeCommand(path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()

	executeSQL(db, "ANALYZE")
}

func runImportCommand(args []string, path string, inMemory bool) {
	if len(args) < 2 {
		fmt.Println("Usage: import <file.csv> <table>")
		os.Exit(1)
	}
	db := openDB(path, inMemory)
	defer db.Close()

	if err := importCSV(db, args[0], args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func importCSV(db *engine.DB, filePath, table string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("read csv: %w", err)
	}

	if len(records) == 0 {
		return fmt.Errorf("empty csv file")
	}

	ctx := context.Background()
	headers := records[0]
	for i, h := range headers {
		headers[i] = strings.TrimSpace(h)
	}

	imported := 0
	for _, row := range records[1:] {
		if len(row) != len(headers) {
			continue
		}
		placeholders := make([]string, len(headers))
		values := make([]interface{}, len(headers))
		for i, v := range row {
			placeholders[i] = "?"
			values[i] = strings.TrimSpace(v)
		}
		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(headers, ", "), strings.Join(placeholders, ", "))
		_, err := db.Exec(ctx, sql, values...)
		if err != nil {
			return fmt.Errorf("insert row %d: %w", imported+1, err)
		}
		imported++
	}

	fmt.Printf("Imported %d rows into %s\n", imported, table)
	return nil
}

func runExportCommand(args []string, path string, inMemory bool) {
	if len(args) < 2 {
		fmt.Println("Usage: export <table> <file> [--format csv|json]")
		os.Exit(1)
	}
	format := "csv"
	if len(args) > 3 && args[2] == "--format" {
		format = args[3]
	}
	db := openDB(path, inMemory)
	defer db.Close()

	if err := exportTable(db, args[0], args[1], format); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func exportTable(db *engine.DB, table, filePath, format string) error {
	ctx := context.Background()
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return fmt.Errorf("query table: %w", err)
	}
	defer rows.Close()

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	cols := rows.Columns()
	exported := 0

	switch format {
	case "json":
		encoder := json.NewEncoder(file)
		encoder.SetIndent("", "  ")
		for rows.Next() {
			values := make([]interface{}, len(cols))
			rowValues := make([]interface{}, len(cols))
			for i := range values {
				rowValues[i] = &values[i]
			}
			if err := rows.Scan(rowValues...); err != nil {
				continue
			}
			rowMap := make(map[string]interface{})
			for i, c := range cols {
				rowMap[c] = formatValue(values[i])
			}
			if err := encoder.Encode(rowMap); err != nil {
				return fmt.Errorf("encode json: %w", err)
			}
			exported++
		}
	default:
		writer := csv.NewWriter(file)
		if err := writer.Write(cols); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
		for rows.Next() {
			values := make([]interface{}, len(cols))
			rowValues := make([]interface{}, len(cols))
			for i := range values {
				rowValues[i] = &values[i]
			}
			if err := rows.Scan(rowValues...); err != nil {
				continue
			}
			record := make([]string, len(cols))
			for i, v := range values {
				record[i] = formatValue(v)
			}
			if err := writer.Write(record); err != nil {
				return fmt.Errorf("write row: %w", err)
			}
			exported++
		}
		writer.Flush()
	}

	fmt.Printf("Exported %d rows from %s to %s\n", exported, table, filePath)
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
