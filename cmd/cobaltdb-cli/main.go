package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	flagVersion  bool
)

var version = "dev"

const (
	cliOutputFilePerm     = 0600
	maxCLIRestoreFileSize = 256 << 20
	maxCLIImportColumns   = 1024
	maxCLIImportFieldSize = 1 << 20
)

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
	flag.BoolVar(&flagVersion, "version", false, "Print version and exit")
}

func main() {
	flag.Parse()

	if flagVersion {
		fmt.Printf("CobaltDB CLI %s\n", version)
		os.Exit(0)
	}

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

Examples:
  # In-memory database
  cobaltdb -memory "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

  # Disk database
  cobaltdb -path ./mydb.db "SELECT * FROM users"

  # Interactive mode
  cobaltdb -path ./mydb.db

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
		CoreStorage: engine.CoreStorage{
			InMemory:   inMemory,
			WALEnabled: engine.BoolPtr(!inMemory),
		},
	}
	db, err := engine.Open(path, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	return db
}

func closeDBAndExit(db *engine.DB, code int) {
	if db != nil {
		if err := db.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing database: %v\n", err)
		}
	}
	os.Exit(code)
}

func runCommand(sql, path string, inMemory bool) {
	db := openDB(path, inMemory)

	if executeSQL(db, sql) {
		// A SQL error in non-interactive mode is a failure: exit non-zero so
		// scripts and automation can detect it.
		closeDBAndExit(db, 1)
	}
	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error closing database: %v\n", err)
		os.Exit(1)
	}
}

func executeSQL(db *engine.DB, sql string) bool {
	return executeSQLInteractive(db, sql, newSessionState())
}

func isQuery(sql string) bool {
	if len(sql) < 4 {
		return false
	}
	// Allocation-free case-insensitive prefix check
	switch {
	case len(sql) >= 6 && (sql[0] == 'S' || sql[0] == 's') && strings.EqualFold(sql[:6], "SELECT"):
		return true
	case len(sql) >= 4 && (sql[0] == 'W' || sql[0] == 'w') && strings.EqualFold(sql[:4], "WITH"):
		return true
	case len(sql) >= 4 && (sql[0] == 'S' || sql[0] == 's') && strings.EqualFold(sql[:4], "SHOW"):
		return true
	case len(sql) >= 8 && (sql[0] == 'D' || sql[0] == 'd') && strings.EqualFold(sql[:8], "DESCRIBE"):
		return true
	case len(sql) >= 5 && (sql[0] == 'D' || sql[0] == 'd') && strings.EqualFold(sql[:5], "DESC "):
		return true
	case len(sql) >= 7 && (sql[0] == 'E' || sql[0] == 'e') && strings.EqualFold(sql[:7], "EXPLAIN"):
		return true
	}
	return false
}

// executeSQLInteractive runs each ';'-separated statement and reports whether
// any of them failed (used by arg mode to set a non-zero exit code).
func executeSQLInteractive(db *engine.DB, sql string, state *sessionState) bool {
	// A single input line may contain several ';'-separated statements
	// (e.g. "BEGIN; UPDATE ...; COMMIT;"). Execute each in sequence on the
	// same connection so transaction control and multi-statement scripts work
	// instead of silently dropping everything after the first statement.
	errored := false
	for _, stmt := range splitSQLStatements(sql) {
		stmt = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(stmt), ";"))
		if stmt == "" {
			continue
		}
		if executeOneStatement(db, stmt, state) {
			errored = true
		}
	}
	return errored
}

// executeOneStatement runs a single statement and returns true if it errored.
func executeOneStatement(db *engine.DB, sql string, state *sessionState) bool {
	ctx := context.Background()
	sql = strings.TrimSpace(sql)

	if sql == "" {
		return false
	}

	start := time.Now()

	if isQuery(sql) {
		rows, err := db.Query(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return true
		}
		defer func() {
			if err := rows.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "Error closing rows: %v\n", err)
			}
		}()

		if err := printRowsWithMode(rows, state); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return true
		}
	} else {
		result, err := db.Exec(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return true
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
	return false
}

func printRowsWithMode(rows *engine.Rows, state *sessionState) error {
	cols := rows.Columns()
	if len(cols) == 0 {
		return nil
	}

	switch state.mode {
	case "csv":
		return printRowsCSV(rows, cols, state.headers)
	case "json":
		printRowsJSON(rows, cols)
	case "line":
		printRowsLine(rows, cols)
	default:
		printRowsTable(rows, cols, state.headers)
	}
	return nil
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

func printRowsCSV(rows *engine.Rows, cols []string, headers bool) error {
	writer := csv.NewWriter(os.Stdout)
	if headers {
		if err := writer.Write(cols); err != nil {
			return fmt.Errorf("write csv header: %w", err)
		}
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
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("write csv row: %w", err)
		}
		count++
	}
	if err := flushCSVWriter(writer); err != nil {
		return fmt.Errorf("flush csv: %w", err)
	}
	fmt.Printf("(%d rows)\n", count)
	return nil
}

func flushCSVWriter(writer *csv.Writer) error {
	writer.Flush()
	return writer.Error()
}

func writeLine(w io.Writer, args ...interface{}) error {
	_, err := fmt.Fprintln(w, args...)
	return err
}

func writeFormat(w io.Writer, format string, args ...interface{}) error {
	_, err := fmt.Fprintf(w, format, args...)
	return err
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
		words := strings.Fields(prefix)
		if len(words) == 1 {
			for _, cmd := range metaCommands {
				if strings.HasPrefix(cmd, prefix) {
					suggestions = append(suggestions, cmd)
				}
			}
			return strToRunes(suggestions), len([]rune(prefix))
		}
		// Argument completion for meta-commands
		cmd := strings.ToLower(words[0])
		switch cmd {
		case ".mode":
			for _, m := range []string{"table", "csv", "json", "line"} {
				if len(words) == 2 && strings.HasPrefix(m, strings.ToLower(words[1])) || len(words) == 1 {
					suggestions = append(suggestions, m)
				}
			}
		case ".timer", ".headers":
			for _, m := range []string{"on", "off"} {
				if len(words) == 2 && strings.HasPrefix(m, strings.ToLower(words[1])) || len(words) == 1 {
					suggestions = append(suggestions, m)
				}
			}
		}
		return strToRunes(suggestions), len([]rune(words[len(words)-1]))
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
		closeDBAndExit(db, 1)
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
		closeDBAndExit(db, 1)
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
		closeDBAndExit(db, 1)
	}
}

func importCSV(db *engine.DB, filePath, table string) error {
	quotedTable, err := quoteSQLIdentifier(table)
	if err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	filePath, err = cleanCLIFilePath(filePath)
	if err != nil {
		return fmt.Errorf("invalid csv path: %w", err)
	}
	file, err := openCLIImportCSVFile(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("empty csv file")
		}
		return fmt.Errorf("read csv header: %w", err)
	}
	if err := validateCSVRecord(headers); err != nil {
		return fmt.Errorf("invalid csv header: %w", err)
	}

	ctx := context.Background()
	for i, h := range headers {
		headers[i] = strings.TrimSpace(h)
	}
	quotedHeaders := make([]string, len(headers))
	for i, h := range headers {
		quotedHeaders[i], err = quoteSQLIdentifier(h)
		if err != nil {
			return fmt.Errorf("invalid CSV header %q: %w", h, err)
		}
	}

	placeholders := make([]string, len(headers))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quotedTable, strings.Join(quotedHeaders, ", "), strings.Join(placeholders, ", "))

	imported := 0
	rowNumber := 1
	for {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		rowNumber++
		if err != nil {
			return fmt.Errorf("read csv row %d: %w", rowNumber, err)
		}
		if err := validateCSVRecord(row); err != nil {
			return fmt.Errorf("invalid csv row %d: %w", rowNumber, err)
		}
		if len(row) != len(headers) {
			continue
		}
		values := make([]interface{}, len(headers))
		for i, v := range row {
			values[i] = strings.TrimSpace(v)
		}
		_, err = db.Exec(ctx, sql, values...)
		if err != nil {
			return fmt.Errorf("insert row %d: %w", rowNumber, err)
		}
		imported++
	}

	fmt.Printf("Imported %d rows into %s\n", imported, table)
	return nil
}

func openCLIImportCSVFile(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat csv file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("csv file must not be a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("csv file must be a regular file: %s", path)
	}

	file, err := os.Open(path) // #nosec G304 - CLI import path is an explicit user argument and is validated before use.
	if err != nil {
		return nil, err
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, statErr
	}
	if !openedInfo.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("csv file must be a regular file: %s", path)
	}
	if !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("csv file changed while opening: %s", path)
	}
	return file, nil
}

func validateCSVRecord(record []string) error {
	if len(record) > maxCLIImportColumns {
		return fmt.Errorf("too many columns: %d (max %d)", len(record), maxCLIImportColumns)
	}
	for i, field := range record {
		if len(field) > maxCLIImportFieldSize {
			return fmt.Errorf("field %d too large: %d bytes (max %d)", i+1, len(field), maxCLIImportFieldSize)
		}
	}
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
		closeDBAndExit(db, 1)
	}
}

func exportTable(db *engine.DB, table, filePath, format string) (err error) {
	ctx := context.Background()
	quotedTable, err := quoteSQLIdentifier(table)
	if err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}
	rows, err := db.Query(ctx, fmt.Sprintf("SELECT * FROM %s", quotedTable))
	if err != nil {
		return fmt.Errorf("query table: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close export rows: %w", closeErr)
		}
	}()

	file, err := createSecureOutputFile(filePath)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer func() {
		if err == nil {
			if commitErr := file.Commit(); commitErr != nil {
				err = fmt.Errorf("commit export file: %w", commitErr)
			}
			return
		}
		if closeErr := file.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close export file: %w", closeErr))
		}
	}()

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
				return fmt.Errorf("scan row: %w", err)
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
				return fmt.Errorf("scan row: %w", err)
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
		if err := flushCSVWriter(writer); err != nil {
			return fmt.Errorf("flush csv: %w", err)
		}
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
		closeDBAndExit(db, 1)
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
		closeDBAndExit(db, 1)
	}
}

func dumpDatabase(db *engine.DB, filePath string) (err error) {
	ctx := context.Background()
	var out io.Writer
	var outputFile *cliOutputFile
	if filePath != "" {
		var err error
		outputFile, err = createSecureOutputFile(filePath)
		if err != nil {
			return fmt.Errorf("create dump file: %w", err)
		}
		out = outputFile
		defer func() {
			if err == nil {
				if commitErr := outputFile.Commit(); commitErr != nil {
					err = fmt.Errorf("commit dump file: %w", commitErr)
				}
				return
			}
			if closeErr := outputFile.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close dump file: %w", closeErr))
			}
		}()
	} else {
		out = os.Stdout
	}

	if err := writeLine(out, "-- CobaltDB SQL Dump"); err != nil {
		return fmt.Errorf("write dump header: %w", err)
	}
	if err := writeLine(out, "-- Generated by cobaltdb-cli"); err != nil {
		return fmt.Errorf("write dump header: %w", err)
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write dump header: %w", err)
	}

	for _, ddl := range db.ForeignTableDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write foreign table schema: %w", err)
		}
		if err := writeLine(out); err != nil {
			return fmt.Errorf("write foreign table separator: %w", err)
		}
	}

	tables := orderTablesByDependency(db)

	for _, table := range tables {
		quotedTable, err := quoteSQLIdentifier(table)
		if err != nil {
			return fmt.Errorf("invalid table name %q: %w", table, err)
		}

		schema, err := db.TableSchemaWithoutForeignKeys(table)
		if err != nil {
			return fmt.Errorf("get schema for %s: %w", table, err)
		}
		if err := writeLine(out, schema); err != nil {
			return fmt.Errorf("write schema for %s: %w", table, err)
		}
		if err := writeLine(out); err != nil {
			return fmt.Errorf("write schema separator for %s: %w", table, err)
		}

		rows, err := db.Query(ctx, fmt.Sprintf("SELECT * FROM %s", quotedTable))
		if err != nil {
			return fmt.Errorf("query %s: %w", table, err)
		}

		cols := rows.Columns()
		quotedCols := make([]string, len(cols))
		for i, col := range cols {
			quotedCols[i], err = quoteSQLIdentifier(col)
			if err != nil {
				err = errors.Join(err, rows.Close())
				return fmt.Errorf("invalid column name %q in %s: %w", col, table, err)
			}
		}
		bufferRows := len(db.TableSelfForeignKeyRefs(table)) > 0
		var dumpRows []dumpRow
		for rows.Next() {
			values := make([]interface{}, len(cols))
			rowValues := make([]interface{}, len(cols))
			for i := range values {
				rowValues[i] = &values[i]
			}
			if err := rows.Scan(rowValues...); err != nil {
				err = errors.Join(err, rows.Close())
				return fmt.Errorf("scan %s: %w", table, err)
			}
			if bufferRows {
				dumpRows = append(dumpRows, dumpRow{values: values})
				continue
			}
			if err := writeDumpInsert(out, quotedTable, quotedCols, values); err != nil {
				err = errors.Join(err, rows.Close())
				return fmt.Errorf("write row for %s: %w", table, err)
			}
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close rows for %s: %w", table, err)
		}
		if bufferRows {
			dumpRows = orderDumpRowsForSelfReferences(db, table, cols, dumpRows)
			for _, row := range dumpRows {
				if err := writeDumpInsert(out, quotedTable, quotedCols, row.values); err != nil {
					return fmt.Errorf("write row for %s: %w", table, err)
				}
			}
		}
		if err := writeLine(out); err != nil {
			return fmt.Errorf("write table separator for %s: %w", table, err)
		}
	}

	// Emit foreign keys after all table data. This lets restores handle
	// self-references and cross-table FK cycles, then validates the loaded data.
	for _, table := range tables {
		fks := db.TableForeignKeys(table)
		usedNames := foreignKeyConstraintNames(fks)
		for i, fk := range fks {
			ddl, err := foreignKeyAlterDDL(table, i, fk, usedNames)
			if err != nil {
				return fmt.Errorf("build foreign key for %s: %w", table, err)
			}
			if err := writeLine(out, ddl); err != nil {
				return fmt.Errorf("write foreign key for %s: %w", table, err)
			}
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write foreign key separator: %w", err)
	}

	// Emit secondary indexes after all tables and data, so they survive restore.
	for _, table := range tables {
		for _, ddl := range db.TableIndexDDL(table) {
			if err := writeLine(out, ddl); err != nil {
				return fmt.Errorf("write index for %s: %w", table, err)
			}
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write index separator: %w", err)
	}

	for _, ddl := range db.FTSIndexDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write full-text index schema: %w", err)
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write full-text index separator: %w", err)
	}

	for _, ddl := range db.VectorIndexDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write vector index schema: %w", err)
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write vector index separator: %w", err)
	}

	for _, ddl := range db.RLSPolicyDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write RLS policy schema: %w", err)
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write RLS policy separator: %w", err)
	}

	for _, ddl := range db.ViewDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write view schema: %w", err)
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write view separator: %w", err)
	}

	for _, ddl := range db.MaterializedViewDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write materialized view schema: %w", err)
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write materialized view separator: %w", err)
	}

	for _, ddl := range db.TriggerDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write trigger schema: %w", err)
		}
	}
	if err := writeLine(out); err != nil {
		return fmt.Errorf("write trigger separator: %w", err)
	}

	for _, ddl := range db.ProcedureDDL() {
		if err := writeLine(out, ddl); err != nil {
			return fmt.Errorf("write procedure schema: %w", err)
		}
	}

	if outputFile != nil {
		fmt.Printf("Dumped %d tables to %s\n", len(tables), filePath)
	}
	return nil
}

// orderTablesByDependency returns table names ordered so that a table's
// foreign-key-referenced tables appear before it (topological order), so a dump
// restores without "referenced table not found". Falls back to the input order
// for cycles. Ties are broken alphabetically for stable output.
func orderTablesByDependency(db *engine.DB) []string {
	tables := db.Tables()
	sort.Strings(tables)

	known := make(map[string]bool, len(tables))
	for _, t := range tables {
		known[t] = true
	}

	var ordered []string
	visited := make(map[string]int) // 0=unseen, 1=visiting, 2=done
	var visit func(string)
	visit = func(t string) {
		if visited[t] == 2 || visited[t] == 1 {
			return // done, or a cycle — don't recurse further
		}
		visited[t] = 1
		for _, ref := range db.TableForeignKeyRefs(t) {
			if known[ref] {
				visit(ref)
			}
		}
		visited[t] = 2
		ordered = append(ordered, t)
	}
	for _, t := range tables {
		visit(t)
	}
	return ordered
}

func foreignKeyConstraintNames(fks []engine.TableForeignKeyRef) map[string]bool {
	names := make(map[string]bool, len(fks))
	for _, fk := range fks {
		if fk.Name != "" {
			names[strings.ToLower(fk.Name)] = true
		}
	}
	return names
}

func foreignKeyAlterDDL(table string, ordinal int, fk engine.TableForeignKeyRef, usedNames map[string]bool) (string, error) {
	quotedTable, err := quoteSQLIdentifier(table)
	if err != nil {
		return "", err
	}
	constraintName := fk.Name
	if constraintName == "" {
		constraintName = syntheticForeignKeyName(table, ordinal, usedNames)
	}
	usedNames[strings.ToLower(constraintName)] = true
	quotedConstraint, err := quoteSQLIdentifier(constraintName)
	if err != nil {
		return "", err
	}
	quotedColumns, err := quoteSQLIdentifierList(fk.Columns)
	if err != nil {
		return "", err
	}
	quotedRefTable, err := quoteSQLIdentifier(fk.ReferencedTable)
	if err != nil {
		return "", err
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s",
		quotedTable, quotedConstraint, strings.Join(quotedColumns, ", "), quotedRefTable)
	if len(fk.ReferencedColumns) > 0 {
		quotedRefColumns, err := quoteSQLIdentifierList(fk.ReferencedColumns)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&sb, " (%s)", strings.Join(quotedRefColumns, ", "))
	}
	if fk.OnDelete != "" {
		sb.WriteString(" ON DELETE ")
		sb.WriteString(fk.OnDelete)
	}
	if fk.OnUpdate != "" {
		sb.WriteString(" ON UPDATE ")
		sb.WriteString(fk.OnUpdate)
	}
	sb.WriteString(";")
	return sb.String(), nil
}

func syntheticForeignKeyName(table string, ordinal int, usedNames map[string]bool) string {
	for suffix := ordinal + 1; ; suffix++ {
		name := fmt.Sprintf("%s_fk_%d", table, suffix)
		if !usedNames[strings.ToLower(name)] {
			return name
		}
	}
}

func quoteSQLIdentifierList(identifiers []string) ([]string, error) {
	quoted := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		var err error
		quoted[i], err = quoteSQLIdentifier(identifier)
		if err != nil {
			return nil, err
		}
	}
	return quoted, nil
}

type dumpRow struct {
	values []interface{}
}

func writeDumpInsert(out io.Writer, quotedTable string, quotedCols []string, values []interface{}) error {
	valStrs := make([]string, len(values))
	for i, v := range values {
		valStrs[i] = sqlEscape(v)
	}
	return writeFormat(out, "INSERT INTO %s (%s) VALUES (%s);\n",
		quotedTable, strings.Join(quotedCols, ", "), strings.Join(valStrs, ", "))
}

func orderDumpRowsForSelfReferences(db *engine.DB, table string, cols []string, rows []dumpRow) []dumpRow {
	refs := db.TableSelfForeignKeyRefs(table)
	if len(refs) == 0 || len(rows) < 2 {
		return rows
	}
	colIndex := make(map[string]int, len(cols))
	for i, col := range cols {
		colIndex[strings.ToLower(col)] = i
	}

	children := make([][]int, len(rows))
	indegree := make([]int, len(rows))
	seenEdges := make(map[[2]int]struct{})
	for _, ref := range refs {
		localIdx, ok := dumpColumnIndexes(colIndex, ref.Columns)
		if !ok {
			continue
		}
		parentIdx, ok := dumpColumnIndexes(colIndex, ref.ReferencedColumns)
		if !ok {
			continue
		}
		parentByKey := make(map[string]int, len(rows))
		for i, row := range rows {
			key, ok := dumpTupleKey(row.values, parentIdx)
			if ok {
				parentByKey[key] = i
			}
		}
		for child, row := range rows {
			key, ok := dumpTupleKey(row.values, localIdx)
			if !ok {
				continue
			}
			parent, exists := parentByKey[key]
			if !exists || parent == child {
				continue
			}
			edge := [2]int{parent, child}
			if _, duplicate := seenEdges[edge]; duplicate {
				continue
			}
			seenEdges[edge] = struct{}{}
			children[parent] = append(children[parent], child)
			indegree[child]++
		}
	}
	if len(seenEdges) == 0 {
		return rows
	}

	queue := make([]int, 0, len(rows))
	for i := range rows {
		if indegree[i] == 0 {
			queue = append(queue, i)
		}
	}
	var ordered []dumpRow
	emitted := make([]bool, len(rows))
	for len(queue) > 0 {
		idx := queue[0]
		queue = queue[1:]
		if emitted[idx] {
			continue
		}
		emitted[idx] = true
		ordered = append(ordered, rows[idx])
		for _, child := range children[idx] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	for i, row := range rows {
		if !emitted[i] {
			ordered = append(ordered, row)
		}
	}
	return ordered
}

func dumpColumnIndexes(colIndex map[string]int, columns []string) ([]int, bool) {
	indexes := make([]int, len(columns))
	for i, col := range columns {
		idx, ok := colIndex[strings.ToLower(col)]
		if !ok {
			return nil, false
		}
		indexes[i] = idx
	}
	return indexes, true
}

func dumpTupleKey(values []interface{}, indexes []int) (string, bool) {
	parts := make([]string, len(indexes))
	for i, idx := range indexes {
		if idx < 0 || idx >= len(values) || values[idx] == nil {
			return "", false
		}
		parts[i] = fmt.Sprintf("%T:%v", values[idx], values[idx])
	}
	return strings.Join(parts, "\x00"), true
}

// quoteSQLStringLiteral renders s as a single-quoted SQL string literal. It
// escapes backslashes FIRST (the lexer treats `\` as an escape character inside
// quoted strings, MySQL-style) and then doubles single quotes. Without the
// backslash escaping, a value containing or ending in `\` (e.g. a Windows path
// `C:\`) corrupts the dump: on restore the lexer consumes the closing quote as
// an escaped character, producing an unterminated string or shifting parsing
// (a dump/restore-corruption and SQL-injection class bug). This mirrors
// quoteSQLIdentifier, which already escapes `\`.
func quoteSQLStringLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "'", "''")
	return "'" + s + "'"
}

func sqlEscape(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64:
		// Numbers and booleans (true/false) are valid unquoted SQL literals.
		return fmt.Sprintf("%v", val)
	case string:
		return quoteSQLStringLiteral(val)
	case []byte:
		return quoteSQLStringLiteral(string(val))
	case []float64, []float32, []int, []int64, []interface{}:
		data, err := json.Marshal(val)
		if err != nil {
			return quoteSQLStringLiteral(fmt.Sprintf("%v", val))
		}
		return quoteSQLStringLiteral(string(data))
	default:
		// Any other type (e.g. the engine's StringBox string wrapper) is a
		// string value and MUST be quoted, otherwise the dump cannot be
		// restored (an unquoted string parses as a column reference).
		return quoteSQLStringLiteral(fmt.Sprintf("%v", val))
	}
}

func quoteSQLIdentifier(identifier string) (string, error) {
	if identifier == "" || strings.ContainsRune(identifier, 0) {
		return "", fmt.Errorf("identifier must be non-empty and cannot contain NUL")
	}
	escaped := strings.ReplaceAll(identifier, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`, nil
}

type cliOutputFile struct {
	*os.File
	finalPath string
	tmpPath   string
	committed bool
}

func createSecureOutputFile(path string) (*cliOutputFile, error) {
	path, err := cleanCLIFilePath(path)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(path)
	base := filepath.Base(path)
	file, err := os.CreateTemp(dir, "."+base+".tmp-*") // #nosec G304 - CLI output path is an explicit user argument and is cleaned before use.
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(cliOutputFilePerm); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return nil, err
	}
	return &cliOutputFile{File: file, finalPath: path, tmpPath: file.Name()}, nil
}

func (f *cliOutputFile) Commit() error {
	if f == nil || f.committed {
		return nil
	}
	if f.File == nil {
		return fmt.Errorf("output file is closed")
	}
	if err := f.File.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.File.Close(); err != nil {
		f.File = nil
		_ = os.Remove(f.tmpPath)
		return err
	}
	f.File = nil
	if err := os.Rename(f.tmpPath, f.finalPath); err != nil {
		_ = os.Remove(f.tmpPath)
		return err
	}
	f.tmpPath = ""
	f.committed = true
	if err := syncCLIOutputDir(filepath.Dir(f.finalPath)); err != nil {
		return err
	}
	return nil
}

func (f *cliOutputFile) Close() error {
	if f == nil {
		return nil
	}
	var closeErr error
	if f.File != nil {
		closeErr = f.File.Close()
		f.File = nil
	}
	if !f.committed && f.tmpPath != "" {
		_ = os.Remove(f.tmpPath)
		f.tmpPath = ""
	}
	return closeErr
}

func syncCLIOutputDir(dir string) error {
	file, err := os.Open(dir) // #nosec G304 - directory comes from a cleaned output path.
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func restoreDatabase(db *engine.DB, filePath string) error {
	filePath, err := cleanCLIFilePath(filePath)
	if err != nil {
		return fmt.Errorf("invalid restore path: %w", err)
	}
	data, err := readCLIRestoreInputFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	ctx := context.Background()
	statements := splitSQLStatements(string(data))
	executable := make([]string, 0, len(statements))
	hasTxnControl := false
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		stmt = stripSQLComments(stmt)
		stmt = strings.TrimSpace(stmt)
		stmt = strings.TrimSuffix(stmt, ";")
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if isRestoreTransactionControl(stmt) {
			hasTxnControl = true
		}
		executable = append(executable, stmt)
	}

	autoTxn := !hasTxnControl && len(executable) > 0
	if autoTxn {
		if _, err := db.Exec(ctx, "BEGIN"); err != nil {
			return fmt.Errorf("begin restore transaction: %w", err)
		}
	}
	rollback := func() {
		if autoTxn {
			_, _ = db.Exec(ctx, "ROLLBACK")
		}
	}
	executed := 0
	for _, stmt := range executable {
		_, err := db.Exec(ctx, stmt)
		if err != nil {
			rollback()
			return fmt.Errorf("execute statement: %s: %w", stmt, err)
		}
		executed++
	}
	if autoTxn {
		if _, err := db.Exec(ctx, "COMMIT"); err != nil {
			rollback()
			return fmt.Errorf("commit restore transaction: %w", err)
		}
	}

	fmt.Printf("Restored %d statements from %s\n", executed, filePath)
	return nil
}

func readCLIRestoreInputFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat restore file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("restore file must not be a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("restore file must be a regular file: %s", path)
	}
	if info.Size() > maxCLIRestoreFileSize {
		return nil, fmt.Errorf("restore file %s is too large: %d bytes (max %d)", path, info.Size(), maxCLIRestoreFileSize)
	}

	file, err := os.Open(path) // #nosec G304 - CLI restore path is an explicit user argument and is validated before use.
	if err != nil {
		return nil, err
	}
	defer file.Close()

	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("restore file must be a regular file: %s", path)
	}
	if openedInfo.Size() > maxCLIRestoreFileSize {
		return nil, fmt.Errorf("restore file %s is too large: %d bytes (max %d)", path, openedInfo.Size(), maxCLIRestoreFileSize)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("restore file changed while opening: %s", path)
	}

	data, err := io.ReadAll(io.LimitReader(file, maxCLIRestoreFileSize+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxCLIRestoreFileSize {
		return nil, fmt.Errorf("restore file %s is too large: %d bytes (max %d)", path, len(data), maxCLIRestoreFileSize)
	}
	return data, nil
}

func isRestoreTransactionControl(stmt string) bool {
	fields := strings.Fields(strings.ToUpper(strings.TrimSpace(stmt)))
	if len(fields) == 0 {
		return false
	}
	switch fields[0] {
	case "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return true
	case "END":
		return true
	}
	return false
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

// splitSQLStatements splits a script into ';'-terminated statements while
// respecting string literals and compound-statement bodies. A ';' inside a
// trigger/procedure/function BEGIN...END block (including nested BEGIN and
// CASE...END) is part of the body, not a statement terminator. A standalone
// transaction "BEGIN;" still splits normally because it is not a compound
// CREATE statement.
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	var stringChar byte
	inCompound := false // inside a CREATE TRIGGER/PROCEDURE/FUNCTION body
	blockDepth := 0     // BEGIN/CASE ... END nesting within the body

	isWordByte := func(b byte) bool {
		return b == '_' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
	}
	isLetter := func(b byte) bool {
		return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
	}

	n := len(sql)
	for i := 0; i < n; i++ {
		ch := sql[i]
		if inString {
			current.WriteByte(ch)
			if ch == stringChar {
				inString = false
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			inString = true
			stringChar = ch
			current.WriteByte(ch)
			continue
		}

		// Keyword detection on word boundaries.
		if isLetter(ch) && (i == 0 || !isWordByte(sql[i-1])) {
			j := i
			for j < n && isWordByte(sql[j]) {
				j++
			}
			word := sql[i:j]
			current.WriteString(word)
			switch strings.ToUpper(word) {
			case "BEGIN":
				if inCompound {
					blockDepth++
				} else {
					up := strings.ToUpper(strings.TrimSpace(current.String()))
					if strings.HasPrefix(up, "CREATE") &&
						(strings.Contains(up, "TRIGGER") || strings.Contains(up, "PROCEDURE") || strings.Contains(up, "FUNCTION")) {
						inCompound = true
						blockDepth = 1
					}
				}
			case "CASE":
				if inCompound {
					blockDepth++
				}
			case "END":
				if inCompound {
					blockDepth--
					if blockDepth <= 0 {
						inCompound = false
						blockDepth = 0
					}
				}
			}
			i = j - 1
			continue
		}

		current.WriteByte(ch)
		if ch == ';' && !inCompound {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	if current.Len() > 0 {
		statements = append(statements, current.String())
	}
	return statements
}

func cleanCLIFilePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("path cannot be empty")
	}
	return filepath.Clean(path), nil
}
