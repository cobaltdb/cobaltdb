package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/chzyer/readline"
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

type sessionState struct {
	mode    string
	timer   bool
	headers bool
}

func newSessionState() *sessionState {
	return &sessionState{mode: "table", timer: false, headers: true}
}

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
	case "dump":
		runDumpCommand(args[1:], flagPath, flagInMemory)
	case "restore":
		runRestoreCommand(args[1:], flagPath, flagInMemory)
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
  dump [file.sql]                                Export database as SQL dump
  restore <file.sql>                             Restore database from SQL dump

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
  .mode <mode>         Set output mode: table|csv|json|line
  .timer <on|off>      Toggle query execution timer
  .headers <on|off>    Toggle header row for table/csv output
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
  .dump [file.sql]       Export database as SQL
  .restore <file.sql>    Restore database from SQL
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
	executeSQLInteractive(db, sql, newSessionState())
}

func executeSQLInteractive(db *engine.DB, sql string, state *sessionState) {
	ctx := context.Background()
	sql = strings.TrimSpace(sql)

	if sql == "" {
		return
	}

	upperSQL := strings.ToUpper(sql)
	start := time.Now()

	if strings.HasPrefix(upperSQL, "SELECT") || strings.HasPrefix(upperSQL, "WITH") ||
		strings.HasPrefix(upperSQL, "SHOW") || strings.HasPrefix(upperSQL, "DESCRIBE") ||
		strings.HasPrefix(upperSQL, "DESC ") {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		defer rows.Close()

		printRowsWithMode(rows, state)
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

	if state.timer {
		fmt.Printf("Query executed in %s\n", time.Since(start).Round(time.Microsecond))
	}
}

func printRowsWithMode(rows *engine.Rows, state *sessionState) {
	cols := rows.Columns()
	if len(cols) == 0 {
		return
	}

	switch state.mode {
	case "csv":
		printRowsCSV(rows, cols, state.headers)
	case "json":
		printRowsJSON(rows, cols)
	case "line":
		printRowsLine(rows, cols)
	default:
		printRowsTable(rows, cols, state.headers)
	}
}

func printRowsTable(rows *engine.Rows, cols []string, headers bool) {
	colCount := len(cols)
	widths := make([]int, colCount)
	for i, col := range cols {
		widths[i] = len(col)
	}

	var data [][]string
	count := 0
	for rows.Next() {
		values := make([]interface{}, colCount)
		rowValues := make([]interface{}, colCount)
		for i := range values {
			rowValues[i] = &values[i]
		}
		if err := rows.Scan(rowValues...); err != nil {
			fmt.Printf("scan error: %v\n", err)
			continue
		}

		row := make([]string, colCount)
		for i, v := range values {
			s := formatValue(v)
			row[i] = s
			if len(s) > widths[i] {
				widths[i] = len(s)
			}
		}
		data = append(data, row)
		count++
	}

	for i := range widths {
		if widths[i] > 60 {
			widths[i] = 60
		}
	}

	printTableBorder(cols, widths, "top")
	if headers {
		printTableRow(cols, widths)
		printTableBorder(cols, widths, "mid")
	}
	for _, row := range data {
		printTableRow(row, widths)
	}
	printTableBorder(cols, widths, "bottom")
	fmt.Printf("(%d rows)\n", count)
}

func printRowsCSV(rows *engine.Rows, cols []string, headers bool) {
	writer := csv.NewWriter(os.Stdout)
	if headers {
		_ = writer.Write(cols)
	}
	count := 0
	colCount := len(cols)
	for rows.Next() {
		values := make([]interface{}, colCount)
		rowValues := make([]interface{}, colCount)
		for i := range values {
			rowValues[i] = &values[i]
		}
		if err := rows.Scan(rowValues...); err != nil {
			continue
		}
		record := make([]string, colCount)
		for i, v := range values {
			record[i] = formatValue(v)
		}
		_ = writer.Write(record)
		count++
	}
	writer.Flush()
	fmt.Printf("(%d rows)\n", count)
}

func printRowsJSON(rows *engine.Rows, cols []string) {
	count := 0
	colCount := len(cols)
	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, colCount)
		rowValues := make([]interface{}, colCount)
		for i := range values {
			rowValues[i] = &values[i]
		}
		if err := rows.Scan(rowValues...); err != nil {
			continue
		}
		rowMap := make(map[string]interface{})
		for i, c := range cols {
			rowMap[c] = values[i]
		}
		results = append(results, rowMap)
		count++
	}
	data, _ := json.MarshalIndent(results, "", "  ")
	fmt.Println(string(data))
	fmt.Printf("(%d rows)\n", count)
}

func printRowsLine(rows *engine.Rows, cols []string) {
	count := 0
	colCount := len(cols)
	for rows.Next() {
		values := make([]interface{}, colCount)
		rowValues := make([]interface{}, colCount)
		for i := range values {
			rowValues[i] = &values[i]
		}
		if err := rows.Scan(rowValues...); err != nil {
			continue
		}
		for i, c := range cols {
			fmt.Printf("%s = %s\n", c, formatValue(values[i]))
		}
		fmt.Println()
		count++
	}
	fmt.Printf("(%d rows)\n", count)
}

func printTableBorder(cols []string, widths []int, pos string) {
	left, mid, right := "┌", "┬", "┐"
	hline := "─"
	if pos == "mid" {
		left, mid, right = "├", "┼", "┤"
	} else if pos == "bottom" {
		left, mid, right = "└", "┴", "┘"
	}
	fmt.Print(left)
	for i, w := range widths {
		fmt.Print(strings.Repeat(hline, w+2))
		if i < len(widths)-1 {
			fmt.Print(mid)
		}
	}
	fmt.Println(right)
}

func printTableRow(cells []string, widths []int) {
	fmt.Print("│ ")
	for i, cell := range cells {
		if i > 0 {
			fmt.Print(" │ ")
		}
		disp := cell
		if len(disp) > widths[i] {
			disp = disp[:widths[i]-3] + "..."
		}
		fmt.Print(disp)
		fmt.Print(strings.Repeat(" ", widths[i]-len(disp)))
	}
	fmt.Println(" │")
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

type cliCompleter struct {
	db *engine.DB
}

var sqlKeywords = []string{
	"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET",
	"DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "ON", "JOIN",
	"LEFT", "RIGHT", "INNER", "OUTER", "CROSS", "GROUP", "BY", "ORDER",
	"HAVING", "LIMIT", "OFFSET", "DISTINCT", "ALL", "UNION", "INTERSECT",
	"EXCEPT", "ASC", "DESC", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
	"AS", "CASE", "WHEN", "THEN", "ELSE", "END", "EXISTS", "IN", "BETWEEN",
	"LIKE", "IS", "COUNT", "SUM", "AVG", "MAX", "MIN", "BEGIN", "COMMIT",
	"ROLLBACK", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE",
	"NOT NULL", "DEFAULT", "AUTOINCREMENT", "INTEGER", "TEXT", "REAL",
	"BLOB", "BOOLEAN", "DATE", "DATETIME",
}

var metaCommands = []string{
	".tables", ".schema", ".quit", ".exit", ".help",
	".mode", ".timer", ".headers",
	".backup", ".metrics", ".status", ".vacuum", ".analyze",
	".import", ".export", ".dump", ".restore",
}

func (c *cliCompleter) Do(line []rune, pos int) ([][]rune, int) {
	prefix := string(line[:pos])
	words := strings.Fields(prefix)
	if len(words) == 0 {
		return nil, 0
	}
	lastWord := strings.ToUpper(words[len(words)-1])

	var suggestions []string

	// Meta command completion
	if strings.HasPrefix(prefix, ".") {
		for _, cmd := range metaCommands {
			if strings.HasPrefix(cmd, prefix) {
				suggestions = append(suggestions, cmd)
			}
		}
		return strToRunes(suggestions), len([]rune(prefix))
	}

	// Table name completion after FROM, INTO, JOIN, UPDATE, TABLE
	if c.db != nil {
		switch lastWord {
		case "FROM", "INTO", "JOIN", "UPDATE", "TABLE", "DROP", "ALTER":
			suggestions = append(suggestions, c.db.Tables()...)
			}
		}

	// SQL keyword completion
	for _, kw := range sqlKeywords {
		if strings.HasPrefix(kw, lastWord) {
			suggestions = append(suggestions, kw)
		}
	}

	return strToRunes(suggestions), len([]rune(lastWord))
}

func strToRunes(strs []string) [][]rune {
	out := make([][]rune, len(strs))
	for i, s := range strs {
		out[i] = []rune(s)
	}
	return out
}

func runInteractive(path string, inMemory bool) {
	db := openDB(path, inMemory)
	defer db.Close()

	// Setup history directory
	homeDir, _ := os.UserHomeDir()
	historyFile := filepath.Join(homeDir, ".cobaltdb_history")

	completer := &cliCompleter{db: db}
	l, err := readline.NewEx(&readline.Config{
		Prompt:          "cobaltdb> ",
		HistoryFile:     historyFile,
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       ".quit",
		HistoryLimit:    10000,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize readline: %v\n", err)
		return
	}
	defer l.Close()

	fmt.Println("CobaltDB Interactive CLI v2.0")
	fmt.Println("Type '.help' for commands, '.quit' to exit")
	fmt.Println("End SQL statements with ';' (multi-line supported)")
	fmt.Println()

	state := newSessionState()
	var sqlBuffer strings.Builder
	inMultiLine := false

	for {
		if inMultiLine {
			l.SetPrompt("      ...> ")
		} else {
			l.SetPrompt("cobaltdb> ")
		}

		line, err := l.Readline()
		if err != nil {
			// EOF or interrupt
			if sqlBuffer.Len() > 0 {
				executeSQLInteractive(db, sqlBuffer.String(), state)
			}
			fmt.Println("\nGoodbye!")
			break
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Meta commands only when not in multi-line mode
		if !inMultiLine && strings.HasPrefix(line, ".") {
			handleMetaCommand(line, db, state)
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
			executeSQLInteractive(db, sql, state)
			sqlBuffer.Reset()
			inMultiLine = false
		} else {
			// Check for single-line commands that don't need semicolons
			upper := strings.ToUpper(trimmed)
			if strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "COMMIT") ||
				strings.HasPrefix(upper, "ROLLBACK") || strings.HasPrefix(upper, "USE ") {
				executeSQLInteractive(db, trimmed, state)
				sqlBuffer.Reset()
				inMultiLine = false
			} else {
				inMultiLine = true
			}
		}
	}
}

func handleMetaCommand(line string, db *engine.DB, state *sessionState) {
	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case ".quit", ".exit":
		fmt.Println("Goodbye!")
		os.Exit(0)

	case ".help":
		printHelp()

	case ".mode":
		if len(parts) < 2 {
			fmt.Printf("Current mode: %s\n", state.mode)
			fmt.Println("Usage: .mode table|csv|json|line")
			return
		}
		m := strings.ToLower(parts[1])
		switch m {
		case "table", "csv", "json", "line":
			state.mode = m
			fmt.Printf("Output mode set to %s\n", m)
		default:
			fmt.Printf("Unknown mode: %s. Supported: table, csv, json, line\n", m)
		}
		return

	case ".timer":
		if len(parts) < 2 {
			fmt.Printf("Timer: %s\n", map[bool]string{true: "on", false: "off"}[state.timer])
			fmt.Println("Usage: .timer on|off")
			return
		}
		switch strings.ToLower(parts[1]) {
		case "on":
			state.timer = true
			fmt.Println("Timer enabled")
		case "off":
			state.timer = false
			fmt.Println("Timer disabled")
		default:
			fmt.Println("Usage: .timer on|off")
		}
		return

		case ".headers":
			if len(parts) < 2 {
				fmt.Printf("Headers: %s\n", map[bool]string{true: "on", false: "off"}[state.headers])
				fmt.Println("Usage: .headers on|off")
				return
			}
			switch strings.ToLower(parts[1]) {
			case "on":
				state.headers = true
				fmt.Println("Headers enabled")
			case "off":
				state.headers = false
				fmt.Println("Headers disabled")
			default:
				fmt.Println("Usage: .headers on|off")
			}
			return

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

	case ".dump":
		file := ""
		if len(parts) > 1 {
			file = parts[1]
		}
		if err := dumpDatabase(db, file); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		}

	case ".restore":
		if len(parts) < 2 {
			fmt.Println("Usage: .restore <file.sql>")
			return
		}
		if err := restoreDatabase(db, parts[1]); err != nil {
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

func runDumpCommand(args []string, path string, inMemory bool) {
	file := ""
	if len(args) > 0 {
		file = args[0]
	}
	db := openDB(path, inMemory)
	defer db.Close()

	if err := dumpDatabase(db, file); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runRestoreCommand(args []string, path string, inMemory bool) {
	if len(args) < 1 {
		fmt.Println("Usage: restore <file.sql>")
		os.Exit(1)
	}
	db := openDB(path, inMemory)
	defer db.Close()

	if err := restoreDatabase(db, args[0]); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func dumpDatabase(db *engine.DB, filePath string) error {
	ctx := context.Background()
	var out *os.File
	if filePath != "" {
		var err error
		out, err = os.Create(filePath)
		if err != nil {
			return fmt.Errorf("create dump file: %w", err)
		}
		defer out.Close()
	} else {
		out = os.Stdout
	}

	fmt.Fprintln(out, "-- CobaltDB SQL Dump")
	fmt.Fprintln(out, "-- Generated by cobaltdb-cli")
	fmt.Fprintln(out)

	tables := db.Tables()
	sort.Strings(tables)

	for _, table := range tables {
		schema, err := db.TableSchema(table)
		if err != nil {
			return fmt.Errorf("get schema for %s: %w", table, err)
		}
		fmt.Fprintln(out, schema)
		fmt.Fprintln(out)

		rows, err := db.Query(ctx, fmt.Sprintf("SELECT * FROM %s", table))
		if err != nil {
			return fmt.Errorf("query %s: %w", table, err)
		}

		cols := rows.Columns()
		for rows.Next() {
			values := make([]interface{}, len(cols))
			rowValues := make([]interface{}, len(cols))
			for i := range values {
				rowValues[i] = &values[i]
			}
			if err := rows.Scan(rowValues...); err != nil {
				rows.Close()
				return fmt.Errorf("scan %s: %w", table, err)
			}

			valStrs := make([]string, len(cols))
			for i, v := range values {
				valStrs[i] = sqlEscape(v)
			}
			fmt.Fprintf(out, "INSERT INTO %s (%s) VALUES (%s);\n",
				table, strings.Join(cols, ", "), strings.Join(valStrs, ", "))
		}
		rows.Close()
		fmt.Fprintln(out)
	}

	if filePath != "" {
		fmt.Printf("Dumped %d tables to %s\n", len(tables), filePath)
	}
	return nil
}

func sqlEscape(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(val), "'", "''") + "'"
	default:
		return fmt.Sprintf("%v", val)
	}
}

func restoreDatabase(db *engine.DB, filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	ctx := context.Background()
	statements := splitSQLStatements(string(data))
	executed := 0
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmt = stripSQLComments(stmt)
		stmt = strings.TrimSuffix(stmt, ";")
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		_, err := db.Exec(ctx, stmt)
		if err != nil {
			return fmt.Errorf("execute statement: %s: %w", stmt, err)
		}
		executed++
	}

	fmt.Printf("Restored %d statements from %s\n", executed, filePath)
	return nil
}

func stripSQLComments(sql string) string {
	var result strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "--") {
			result.WriteString(line)
			result.WriteByte('\n')
		}
	}
	return result.String()
}

func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := rune(0)

	for _, ch := range sql {
		if inString {
			current.WriteRune(ch)
			if ch == stringChar {
				inString = false
			}
			continue
		}

		if ch == '\'' || ch == '"' {
			inString = true
			stringChar = ch
			current.WriteRune(ch)
			continue
		}

		current.WriteRune(ch)
		if ch == ';' {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	if current.Len() > 0 {
		statements = append(statements, current.String())
	}
	return statements
}

