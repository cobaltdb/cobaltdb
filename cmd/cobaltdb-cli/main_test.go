package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}

func TestPrintHelp(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	printHelp()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	outputStr := buf.String()
	if !strings.Contains(outputStr, "Usage:") {
		t.Error("Expected help to contain 'Usage:'")
	}

	if !strings.Contains(outputStr, "CobaltDB") {
		t.Error("Expected help to contain 'CobaltDB'")
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{"hello", "hello"},
		{123, "123"},
		{int64(456), "456"},
		{3.14, "3.14"},
		{true, "true"},
		{false, "false"},
		{nil, "NULL"},
		{[]byte("test"), "test"},
	}

	for _, test := range tests {
		result := formatValue(test.input)
		if result != test.expected {
			t.Errorf("formatValue(%v) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestRunCommand(t *testing.T) {
	t.Run("HelpCommand", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Help command panicked: %v", r)
			}
		}()

		// Would need proper setup to test fully
		// runCommand("help", nil)
	})

	t.Run("EmptyCommand", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Empty command panicked: %v", r)
			}
		}()

		// Would need proper setup to test fully
		// runCommand("", nil)
	})
}

func TestHandleMetaCommand(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected bool
	}{
		{"quit", ".quit", true},
		{"exit", ".exit", true},
		{"tables", ".tables", false},
		{"schema", ".schema", false},
		{"help", ".help", false},
		{"not meta", "SELECT 1", false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Meta command panicked: %v", r)
				}
			}()

			// Would need proper setup to test fully
			// result := handleMetaCommand(test.command, nil)
			_ = test.expected
		})
	}
}

func TestRunInteractive(t *testing.T) {
	t.Run("InteractiveMode", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Interactive mode panicked: %v", r)
			}
		}()

		// Would need proper setup to test fully
		// runInteractive(nil)
	})
}

func TestInit(t *testing.T) {
	// Test that init() doesn't panic
}

func TestSqlEscape(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{[]byte("test"), "'test'"},
		{123, "123"},
		{int64(9007199254740993), "9007199254740993"},
		{3.14, "3.14"},
		{true, "true"},
		{false, "false"},
		// A non-standard string type (like the engine's StringBox) must be
		// quoted, otherwise the dump cannot be restored.
		{stringerVal("alice"), "'alice'"},
		{stringerVal("it's"), "'it''s'"},
	}
	for _, test := range tests {
		result := sqlEscape(test.input)
		if result != test.expected {
			t.Errorf("sqlEscape(%v) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

// stringerVal mimics the engine's StringBox: a non-string type whose %v form is
// the underlying text, used to verify sqlEscape quotes it for dump/restore.
type stringerVal string

func (s stringerVal) String() string { return string(s) }

func TestQuoteSQLIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"users", `"users"`},
		{"rank", `"rank"`},
		{`users"; DROP TABLE users;--`, `"users\"; DROP TABLE users;--"`},
		{`path\name`, `"path\\name"`},
	}
	for _, test := range tests {
		result, err := quoteSQLIdentifier(test.input)
		if err != nil {
			t.Fatalf("quoteSQLIdentifier(%q) returned error: %v", test.input, err)
		}
		if result != test.expected {
			t.Errorf("quoteSQLIdentifier(%q) = %q, expected %q", test.input, result, test.expected)
		}
	}

	if _, err := quoteSQLIdentifier(""); err == nil {
		t.Error("Expected empty identifier to be rejected")
	}
	if _, err := quoteSQLIdentifier("bad\x00name"); err == nil {
		t.Error("Expected NUL-containing identifier to be rejected")
	}
}

func TestImportCSVQuotesReservedColumnNames(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE reserved_import ("rank" TEXT, "key" TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	csvPath := filepath.Join(t.TempDir(), "reserved.csv")
	if err := os.WriteFile(csvPath, []byte("rank,key\nfirst,alpha\n"), 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	if err := importCSV(db, csvPath, "reserved_import"); err != nil {
		t.Fatalf("importCSV failed: %v", err)
	}

	rows, err := db.Query(ctx, `SELECT "rank", "key" FROM reserved_import`)
	if err != nil {
		t.Fatalf("query imported rows: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected imported row")
	}
	var rank, key interface{}
	if err := rows.Scan(&rank, &key); err != nil {
		t.Fatalf("scan row: %v", err)
	}
	if formatValue(rank) != "first" || formatValue(key) != "alpha" {
		t.Fatalf("unexpected imported row: rank=%v key=%v", rank, key)
	}
}

func TestImportCSVRejectsEmptyFile(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	csvPath := filepath.Join(t.TempDir(), "empty.csv")
	if err := os.WriteFile(csvPath, nil, 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	if err := importCSV(db, csvPath, "missing_table"); err == nil || !strings.Contains(err.Error(), "empty csv file") {
		t.Fatalf("expected empty csv error, got %v", err)
	}
}

func TestImportCSVRejectsSymlink(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	target := filepath.Join(dir, "target.csv")
	if err := os.WriteFile(target, []byte("id\n1\n"), 0600); err != nil {
		t.Fatalf("write target csv: %v", err)
	}
	link := filepath.Join(dir, "link.csv")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err = importCSV(db, link, "symlink_import")
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected csv symlink error, got %v", err)
	}
}

func TestImportCSVRejectsDirectory(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	err = importCSV(db, t.TempDir(), "directory_import")
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected csv regular file error, got %v", err)
	}
}

func TestImportCSVRejectsTooManyColumns(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	header := make([]string, maxCLIImportColumns+1)
	for i := range header {
		header[i] = fmt.Sprintf("c%d", i)
	}
	csvPath := filepath.Join(t.TempDir(), "wide.csv")
	if err := os.WriteFile(csvPath, []byte(strings.Join(header, ",")+"\n"), 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	err = importCSV(db, csvPath, "wide_import")
	if err == nil || !strings.Contains(err.Error(), "too many columns") {
		t.Fatalf("expected too many columns error, got %v", err)
	}
}

func TestImportCSVRejectsOversizedField(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE field_limit_import (payload TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	csvPath := filepath.Join(t.TempDir(), "large-field.csv")
	data := "payload\n" + strings.Repeat("x", maxCLIImportFieldSize+1) + "\n"
	if err := os.WriteFile(csvPath, []byte(data), 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	err = importCSV(db, csvPath, "field_limit_import")
	if err == nil || !strings.Contains(err.Error(), "field 1 too large") {
		t.Fatalf("expected oversized field error, got %v", err)
	}
}

func TestImportCSVStreamsRows(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.Exec(ctx, `CREATE TABLE stream_import (id TEXT, name TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	var b strings.Builder
	b.WriteString("id,name\n")
	for i := 0; i < 128; i++ {
		fmt.Fprintf(&b, "%d,name_%d\n", i, i)
	}
	csvPath := filepath.Join(t.TempDir(), "stream.csv")
	if err := os.WriteFile(csvPath, []byte(b.String()), 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	if err := importCSV(db, csvPath, "stream_import"); err != nil {
		t.Fatalf("importCSV failed: %v", err)
	}

	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM stream_import`)
	if err != nil {
		t.Fatalf("query count: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected count row")
	}
	var count interface{}
	if err := rows.Scan(&count); err != nil {
		t.Fatalf("scan count: %v", err)
	}
	if formatValue(count) != "128" {
		t.Fatalf("imported row count = %v, want 128", count)
	}
}

func TestDumpRestoreSelfReferentialForeignKeyOrdersParentsFirst(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE employees (id INTEGER PRIMARY KEY, manager_id INTEGER, FOREIGN KEY (manager_id) REFERENCES employees(id))`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO employees VALUES (2, NULL)`); err != nil {
		t.Fatalf("insert manager: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO employees VALUES (1, 2)`); err != nil {
		t.Fatalf("insert report: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "self-fk.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}

	rows, err := dst.Query(ctx, `SELECT id, manager_id FROM employees ORDER BY id`)
	if err != nil {
		t.Fatalf("query restored employees: %v", err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var id, manager interface{}
		if err := rows.Scan(&id, &manager); err != nil {
			t.Fatalf("scan restored employees: %v", err)
		}
		got = append(got, formatValue(id)+":"+formatValue(manager))
	}
	if strings.Join(got, ",") != "1:2,2:NULL" {
		t.Fatalf("restored employees = %v, want 1:2,2:NULL", got)
	}
}

func TestDumpRestoreQuotesSchemaIdentifiers(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE quoted_schema ("full name" INTEGER PRIMARY KEY, "key" TEXT NOT NULL)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO quoted_schema ("full name", "key") VALUES (1, 'value')`); err != nil {
		t.Fatalf("insert source row: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE INDEX quoted_schema_full_name_idx ON quoted_schema ("full name")`); err != nil {
		t.Fatalf("create source index: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "quoted-schema.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT "key" FROM quoted_schema WHERE "full name" = 1`); got != "value" {
		t.Fatalf("restored reserved identifier row = %s, want value", got)
	}
	if got := dst.TableIndexDDL("quoted_schema"); len(got) != 1 {
		t.Fatalf("restored index DDL count = %d, want 1: %v", len(got), got)
	}
}

func TestDumpRestoreForeignTableDefinition(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "people.csv")
	if err := os.WriteFile(csvPath, []byte("id,name\n1,alice\n2,bob\n"), 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, fmt.Sprintf(`CREATE FOREIGN TABLE ext_people (id INTEGER, name TEXT) WRAPPER 'csv' OPTIONS (file '%s')`, strings.ReplaceAll(csvPath, "'", "''"))); err != nil {
		t.Fatalf("create foreign table: %v", err)
	}
	if got := scalarCLI(t, src, `SELECT name FROM ext_people WHERE id = 2`); got != "bob" {
		t.Fatalf("source foreign table row = %s, want bob", got)
	}

	dumpPath := filepath.Join(dir, "foreign.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT name FROM ext_people WHERE id = 2`); got != "bob" {
		t.Fatalf("restored foreign table row = %s, want bob", got)
	}
}

func TestDumpRestoreViewDefinition(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE view_base (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO view_base VALUES (1, 'alice')`); err != nil {
		t.Fatalf("insert source row: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE VIEW view_names AS SELECT name FROM view_base WHERE id = 1`); err != nil {
		t.Fatalf("create source view: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "view.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT name FROM view_names`); got != "alice" {
		t.Fatalf("restored view row = %s, want alice", got)
	}
}

func TestDumpRestoreTriggerDefinition(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE trigger_base (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create source base table: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE TABLE trigger_audit (event TEXT)`); err != nil {
		t.Fatalf("create source audit table: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE TRIGGER trigger_base_ai AFTER INSERT ON trigger_base BEGIN INSERT INTO trigger_audit VALUES ('inserted'); END`); err != nil {
		t.Fatalf("create source trigger: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "trigger.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if _, err := dst.Exec(ctx, `INSERT INTO trigger_base VALUES (1)`); err != nil {
		t.Fatalf("insert restored base row: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT COUNT(*) FROM trigger_audit`); got != "1" {
		t.Fatalf("restored trigger audit count = %s, want 1", got)
	}
}

func TestDumpRestoreProcedureDefinition(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE proc_log (id INTEGER PRIMARY KEY, label TEXT)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE PROCEDURE insert_proc_log(p_id INTEGER, p_label TEXT) BEGIN INSERT INTO proc_log VALUES (p_id, p_label); END`); err != nil {
		t.Fatalf("create source procedure: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "procedure.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if _, err := dst.Exec(ctx, `CALL insert_proc_log(1, 'restored')`); err != nil {
		t.Fatalf("call restored procedure: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT label FROM proc_log WHERE id = 1`); got != "restored" {
		t.Fatalf("restored procedure row = %s, want restored", got)
	}
}

func TestDumpRestoreMaterializedViewDefinition(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE mv_base (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO mv_base VALUES (1, 'north', 10)`); err != nil {
		t.Fatalf("insert source row: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE VIEW mv_source AS SELECT region, amount FROM mv_base`); err != nil {
		t.Fatalf("create source view: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE MATERIALIZED VIEW mv_totals AS SELECT region, SUM(amount) AS total FROM mv_source GROUP BY region`); err != nil {
		t.Fatalf("create source materialized view: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO mv_base VALUES (2, 'north', 90)`); err != nil {
		t.Fatalf("insert post-snapshot source row: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "materialized-view.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT total FROM mv_totals WHERE region = 'north'`); got != "100" {
		t.Fatalf("restored materialized view total = %s, want 100", got)
	}
	if _, err := dst.Exec(ctx, `INSERT INTO mv_base VALUES (3, 'north', 5)`); err != nil {
		t.Fatalf("insert restored source row: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT total FROM mv_totals WHERE region = 'north'`); got != "100" {
		t.Fatalf("materialized view changed without refresh = %s, want 100", got)
	}
	if _, err := dst.Exec(ctx, `REFRESH MATERIALIZED VIEW mv_totals`); err != nil {
		t.Fatalf("refresh restored materialized view: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT total FROM mv_totals WHERE region = 'north'`); got != "105" {
		t.Fatalf("refreshed materialized view total = %s, want 105", got)
	}
}

func TestDumpRestoreFullTextIndexDefinition(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE fts_docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO fts_docs VALUES (1, 'Go Guide', 'Go database internals')`); err != nil {
		t.Fatalf("insert source row: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE FULLTEXT INDEX fts_docs_idx ON fts_docs(title, body)`); err != nil {
		t.Fatalf("create source full-text index: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "fulltext-index.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT COUNT(*) FROM fts_docs WHERE MATCH(title, body) AGAINST('database')`); got != "1" {
		t.Fatalf("restored full-text search count = %s, want 1", got)
	}
	if _, err := dst.Exec(ctx, `DROP INDEX fts_docs_idx`); err != nil {
		t.Fatalf("drop restored full-text index: %v", err)
	}
}

func TestDumpRestoreVectorIndexDefinition(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE vector_docs (id INTEGER PRIMARY KEY, name TEXT, embedding VECTOR(3))`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO vector_docs VALUES (1, 'x-axis', [1.0, 0.0, 0.0])`); err != nil {
		t.Fatalf("insert source row: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE VECTOR INDEX vector_docs_idx ON vector_docs(embedding)`); err != nil {
		t.Fatalf("create source vector index: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "vector-index.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	keys, _, err := dst.SearchVectorKNN("vector_docs_idx", []float64{1.0, 0.0, 0.0}, 1)
	if err != nil {
		t.Fatalf("search restored vector index: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("restored vector search keys = %v, want one key", keys)
	}
	if _, err := dst.Exec(ctx, `DROP INDEX vector_docs_idx`); err != nil {
		t.Fatalf("drop restored vector index: %v", err)
	}
}

func TestDumpRestoreRLSPolicyDefinition(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE rls_docs (id INTEGER PRIMARY KEY, owner TEXT)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO rls_docs VALUES (1, 'alice'), (2, 'bob')`); err != nil {
		t.Fatalf("insert source rows: %v", err)
	}
	if _, err := src.Exec(ctx, `ALTER TABLE rls_docs ENABLE ROW LEVEL SECURITY`); err != nil {
		t.Fatalf("enable source RLS: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE POLICY rls_docs_visible ON rls_docs FOR SELECT USING (true)`); err != nil {
		t.Fatalf("create source RLS policy: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE POLICY rls_docs_owner ON rls_docs AS RESTRICTIVE FOR SELECT USING (owner = current_user())`); err != nil {
		t.Fatalf("create source restrictive RLS policy: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE POLICY rls_docs_insert_owner ON rls_docs FOR INSERT WITH CHECK (owner = current_user())`); err != nil {
		t.Fatalf("create source RLS insert policy: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "rls-policy.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}
	dumpSQL, err := os.ReadFile(dumpPath)
	if err != nil {
		t.Fatalf("read dump: %v", err)
	}
	dumpText := string(dumpSQL)
	if !strings.Contains(dumpText, `rls_docs_owner`) || !strings.Contains(dumpText, `AS RESTRICTIVE FOR SELECT`) {
		t.Fatalf("dump omitted AS RESTRICTIVE policy: %s", dumpSQL)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	aliceCtx := context.WithValue(ctx, security.RLSUserKey, "alice")
	if got := rlsVisibleIDs(t, dst, aliceCtx); got != "1" {
		t.Fatalf("alice restored RLS ids = %s, want 1", got)
	}
	bobCtx := context.WithValue(ctx, security.RLSUserKey, "bob")
	if got := rlsVisibleIDs(t, dst, bobCtx); got != "2" {
		t.Fatalf("bob restored RLS ids = %s, want 2", got)
	}
	if _, err := dst.Exec(bobCtx, `INSERT INTO rls_docs VALUES (3, 'alice')`); err == nil {
		t.Fatal("restored WITH CHECK policy allowed bob to insert an alice-owned row")
	}
	if _, err := dst.Exec(bobCtx, `INSERT INTO rls_docs VALUES (4, 'bob')`); err != nil {
		t.Fatalf("restored WITH CHECK policy rejected bob's own row: %v", err)
	}
	if _, err := dst.Exec(ctx, `DROP POLICY rls_docs_owner ON rls_docs`); err != nil {
		t.Fatalf("drop restored RLS policy: %v", err)
	}
}

func TestDumpRestoreRLSEnabledTableWithoutPolicies(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE rls_locked_docs (id INTEGER PRIMARY KEY, owner TEXT)`); err != nil {
		t.Fatalf("create source table: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO rls_locked_docs VALUES (1, 'alice')`); err != nil {
		t.Fatalf("insert source row: %v", err)
	}
	if _, err := src.Exec(ctx, `ALTER TABLE rls_locked_docs ENABLE ROW LEVEL SECURITY`); err != nil {
		t.Fatalf("enable source RLS: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "rls-enabled-no-policies.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	bobCtx := context.WithValue(ctx, security.RLSUserKey, "bob")
	if got := rlsLockedVisibleIDs(t, dst, bobCtx); got != "" {
		t.Fatalf("restored policy-less RLS ids = %s, want none", got)
	}
	if ddl := dst.RLSPolicyDDL(); len(ddl) != 1 {
		t.Fatalf("restored policy-less RLS DDL count = %d, want 1: %v", len(ddl), ddl)
	}
}

func rlsVisibleIDs(t *testing.T, db *engine.DB, ctx context.Context) string {
	t.Helper()
	rows, err := db.Query(ctx, `SELECT id, owner FROM rls_docs ORDER BY id`)
	if err != nil {
		t.Fatalf("query RLS-visible rows: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id, owner interface{}
		if err := rows.Scan(&id, &owner); err != nil {
			t.Fatalf("scan RLS-visible rows: %v", err)
		}
		ids = append(ids, formatValue(id))
	}
	return strings.Join(ids, ",")
}

func rlsLockedVisibleIDs(t *testing.T, db *engine.DB, ctx context.Context) string {
	t.Helper()
	rows, err := db.Query(ctx, `SELECT id, owner FROM rls_locked_docs ORDER BY id`)
	if err != nil {
		t.Fatalf("query policy-less RLS-visible rows: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id, owner interface{}
		if err := rows.Scan(&id, &owner); err != nil {
			t.Fatalf("scan policy-less RLS-visible rows: %v", err)
		}
		ids = append(ids, formatValue(id))
	}
	return strings.Join(ids, ",")
}

func TestDumpRestoreCyclicForeignKeys(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE cycle_a (id INTEGER PRIMARY KEY, b_id INTEGER)`); err != nil {
		t.Fatalf("create cycle_a: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE TABLE cycle_b (id INTEGER PRIMARY KEY, a_id INTEGER)`); err != nil {
		t.Fatalf("create cycle_b: %v", err)
	}
	if _, err := src.Exec(ctx, `ALTER TABLE cycle_a ADD CONSTRAINT cycle_a_b_fk FOREIGN KEY (b_id) REFERENCES cycle_b(id)`); err != nil {
		t.Fatalf("add cycle_a FK: %v", err)
	}
	if _, err := src.Exec(ctx, `ALTER TABLE cycle_b ADD CONSTRAINT cycle_b_a_fk FOREIGN KEY (a_id) REFERENCES cycle_a(id)`); err != nil {
		t.Fatalf("add cycle_b FK: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO cycle_a VALUES (1, NULL)`); err != nil {
		t.Fatalf("insert cycle_a: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO cycle_b VALUES (1, 1)`); err != nil {
		t.Fatalf("insert cycle_b: %v", err)
	}
	if _, err := src.Exec(ctx, `UPDATE cycle_a SET b_id = 1 WHERE id = 1`); err != nil {
		t.Fatalf("update cycle_a: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "cyclic-fk.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT b_id FROM cycle_a WHERE id = 1`); got != "1" {
		t.Fatalf("restored cycle_a.b_id = %s, want 1", got)
	}
	if got := scalarCLI(t, dst, `SELECT a_id FROM cycle_b WHERE id = 1`); got != "1" {
		t.Fatalf("restored cycle_b.a_id = %s, want 1", got)
	}
}

func TestDumpRestoreUnnamedForeignKeySyntheticNameAvoidsCollision(t *testing.T) {
	ctx := context.Background()
	src, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open source db: %v", err)
	}
	defer src.Close()

	if _, err := src.Exec(ctx, `CREATE TABLE fk_name_parent (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if _, err := src.Exec(ctx, `CREATE TABLE fk_name_child (id INTEGER PRIMARY KEY, parent_id INTEGER, parent2_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_name_parent(id))`); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if _, err := src.Exec(ctx, `ALTER TABLE fk_name_child ADD CONSTRAINT fk_name_child_fk_1 FOREIGN KEY (parent2_id) REFERENCES fk_name_parent(id)`); err != nil {
		t.Fatalf("add named FK: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO fk_name_parent VALUES (1)`); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	if _, err := src.Exec(ctx, `INSERT INTO fk_name_child VALUES (1, 1, 1)`); err != nil {
		t.Fatalf("insert child: %v", err)
	}

	dumpPath := filepath.Join(t.TempDir(), "unnamed-fk-name.sql")
	if err := dumpDatabase(src, dumpPath); err != nil {
		t.Fatalf("dump database: %v", err)
	}

	dst, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open restore db: %v", err)
	}
	defer dst.Close()
	if err := restoreDatabase(dst, dumpPath); err != nil {
		t.Fatalf("restore database: %v", err)
	}
	if got := scalarCLI(t, dst, `SELECT parent_id FROM fk_name_child WHERE id = 1`); got != "1" {
		t.Fatalf("restored parent_id = %s, want 1", got)
	}
	if got := scalarCLI(t, dst, `SELECT parent2_id FROM fk_name_child WHERE id = 1`); got != "1" {
		t.Fatalf("restored parent2_id = %s, want 1", got)
	}
}

func scalarCLI(t *testing.T, db *engine.DB, sql string) string {
	t.Helper()
	return scalarCLIContext(t, db, context.Background(), sql)
}

func scalarCLIContext(t *testing.T, db *engine.DB, ctx context.Context, sql string) string {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query scalar: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected scalar row")
	}
	var v interface{}
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan scalar: %v", err)
	}
	return formatValue(v)
}

func TestRestoreDatabaseRollsBackOnFailure(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	restorePath := filepath.Join(t.TempDir(), "bad-restore.sql")
	sql := `
CREATE TABLE restore_atomic (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES missing_parent(id));
INSERT INTO restore_atomic VALUES (1, 99);
`
	if err := os.WriteFile(restorePath, []byte(sql), 0600); err != nil {
		t.Fatalf("write restore file: %v", err)
	}

	if err := restoreDatabase(db, restorePath); err == nil {
		t.Fatal("restore should fail on invalid foreign key reference")
	}
	if _, err := db.TableSchema("restore_atomic"); err == nil {
		t.Fatal("failed restore left partial table behind")
	}
}

func TestRestoreDatabaseRollsBackFullTextIndexOnFailure(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	restorePath := filepath.Join(t.TempDir(), "bad-fts-restore.sql")
	sql := `
CREATE TABLE restore_fts (id INTEGER PRIMARY KEY, body TEXT);
INSERT INTO restore_fts VALUES (1, 'rollback searchable text');
CREATE FULLTEXT INDEX restore_fts_idx ON restore_fts(body);
INSERT INTO missing_restore_fts VALUES (1);
`
	if err := os.WriteFile(restorePath, []byte(sql), 0600); err != nil {
		t.Fatalf("write restore file: %v", err)
	}

	if err := restoreDatabase(db, restorePath); err == nil {
		t.Fatal("restore should fail on missing table")
	}
	if _, err := db.TableSchema("restore_fts"); err == nil {
		t.Fatal("failed restore left partial table behind")
	}
	if _, err := db.Exec(t.Context(), `DROP INDEX restore_fts_idx`); err == nil {
		t.Fatal("failed restore left full-text index metadata behind")
	}
}

func TestRestoreDatabaseRollsBackVectorIndexOnFailure(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	restorePath := filepath.Join(t.TempDir(), "bad-vector-restore.sql")
	sql := `
CREATE TABLE restore_vector (id INTEGER PRIMARY KEY, embedding VECTOR(3));
INSERT INTO restore_vector VALUES (1, [1.0, 0.0, 0.0]);
CREATE VECTOR INDEX restore_vector_idx ON restore_vector(embedding);
INSERT INTO missing_restore_vector VALUES (1);
`
	if err := os.WriteFile(restorePath, []byte(sql), 0600); err != nil {
		t.Fatalf("write restore file: %v", err)
	}

	if err := restoreDatabase(db, restorePath); err == nil {
		t.Fatal("restore should fail on missing table")
	}
	if _, err := db.TableSchema("restore_vector"); err == nil {
		t.Fatal("failed restore left partial table behind")
	}
	if _, err := db.Exec(t.Context(), `DROP INDEX restore_vector_idx`); err == nil {
		t.Fatal("failed restore left vector index metadata behind")
	}
}

func TestRestoreDatabaseRollsBackRLSPolicyOnFailure(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	restorePath := filepath.Join(t.TempDir(), "bad-rls-restore.sql")
	sql := `
CREATE TABLE restore_rls (id INTEGER PRIMARY KEY, owner TEXT);
ALTER TABLE restore_rls ENABLE ROW LEVEL SECURITY;
CREATE POLICY restore_rls_owner ON restore_rls FOR SELECT USING (owner = current_user());
INSERT INTO missing_restore_rls VALUES (1);
`
	if err := os.WriteFile(restorePath, []byte(sql), 0600); err != nil {
		t.Fatalf("write restore file: %v", err)
	}

	if err := restoreDatabase(db, restorePath); err == nil {
		t.Fatal("restore should fail on missing table")
	}
	if _, err := db.TableSchema("restore_rls"); err == nil {
		t.Fatal("failed restore left partial table behind")
	}
	if _, err := db.Exec(t.Context(), `DROP POLICY restore_rls_owner ON restore_rls`); err == nil {
		t.Fatal("failed restore left RLS policy metadata behind")
	}
}

func TestRestoreDatabaseHonorsExplicitTransactionControl(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	restorePath := filepath.Join(t.TempDir(), "explicit-txn.sql")
	sql := `
BEGIN;
CREATE TABLE explicit_restore (id INTEGER PRIMARY KEY);
INSERT INTO explicit_restore VALUES (1);
COMMIT;
`
	if err := os.WriteFile(restorePath, []byte(sql), 0600); err != nil {
		t.Fatalf("write restore file: %v", err)
	}
	if err := restoreDatabase(db, restorePath); err != nil {
		t.Fatalf("restore with explicit transaction failed: %v", err)
	}
	rows, err := db.Query(context.Background(), "SELECT id FROM explicit_restore")
	if err != nil {
		t.Fatalf("query explicit restore: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected restored row")
	}
	var id interface{}
	if err := rows.Scan(&id); err != nil {
		t.Fatalf("scan restored row: %v", err)
	}
	if formatValue(id) != "1" {
		t.Fatalf("restored id = %v, want 1", id)
	}
}

func TestRestoreDatabaseRejectsOversizedFile(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	restorePath := filepath.Join(t.TempDir(), "oversized.sql")
	file, err := os.OpenFile(restorePath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatalf("create restore file: %v", err)
	}
	if err := file.Truncate(maxCLIRestoreFileSize + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate restore file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close restore file: %v", err)
	}

	err = restoreDatabase(db, restorePath)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("expected oversized restore file error, got %v", err)
	}
}

func TestRestoreDatabaseRejectsSymlink(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()
	target := filepath.Join(dir, "target.sql")
	if err := os.WriteFile(target, []byte("CREATE TABLE restore_symlink (id INTEGER);"), 0600); err != nil {
		t.Fatalf("write restore target: %v", err)
	}
	link := filepath.Join(dir, "link.sql")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err = restoreDatabase(db, link)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected restore symlink error, got %v", err)
	}
}

func TestReadCLIRestoreInputFileValidatesOpenedFile(t *testing.T) {
	dir := t.TempDir()
	restorePath := filepath.Join(dir, "restore.sql")
	want := "CREATE TABLE restore_validated (id INTEGER);"
	if err := os.WriteFile(restorePath, []byte(want), 0600); err != nil {
		t.Fatalf("write restore file: %v", err)
	}

	got, err := readCLIRestoreInputFile(restorePath)
	if err != nil {
		t.Fatalf("readCLIRestoreInputFile failed: %v", err)
	}
	if string(got) != want {
		t.Fatalf("restore file content = %q, want %q", got, want)
	}

	_, err = readCLIRestoreInputFile(dir)
	if err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected directory restore path rejection, got %v", err)
	}
}

func TestCreateSecureOutputFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export.csv")
	file, err := createSecureOutputFile(path)
	if err != nil {
		t.Fatalf("createSecureOutputFile failed: %v", err)
	}
	if _, err := file.WriteString("id,name\n1,Alice\n"); err != nil {
		t.Fatalf("write output file: %v", err)
	}
	if err := file.Commit(); err != nil {
		t.Fatalf("commit output file: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat output file: %v", err)
	}
	if info.Mode().Perm() != cliOutputFilePerm {
		t.Fatalf("Expected output file permissions %o, got %o", cliOutputFilePerm, info.Mode().Perm())
	}
}

func TestCreateSecureOutputFileCloseWithoutCommitPreservesExistingTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "export.csv")
	original := []byte("existing output\n")
	if err := os.WriteFile(path, original, cliOutputFilePerm); err != nil {
		t.Fatalf("seed output file: %v", err)
	}

	file, err := createSecureOutputFile(path)
	if err != nil {
		t.Fatalf("createSecureOutputFile failed: %v", err)
	}
	if _, err := file.WriteString("partial replacement\n"); err != nil {
		t.Fatalf("write output file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close output file: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if string(got) != string(original) {
		t.Fatalf("uncommitted output changed target: got %q, want %q", got, original)
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".export.csv.tmp-*"))
	if err != nil {
		t.Fatalf("glob temp files: %v", err)
	}
	if len(matches) != 0 {
		t.Fatalf("uncommitted output left temp files: %v", matches)
	}
}

func TestFlushCSVWriterReturnsBufferedWriteError(t *testing.T) {
	writeErr := errors.New("write failed")
	writer := csv.NewWriter(failingWriter{err: writeErr})

	if err := writer.Write([]string{"id", "name"}); err != nil {
		t.Fatalf("unexpected buffered write error: %v", err)
	}
	err := flushCSVWriter(writer)
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected buffered write error, got %v", err)
	}
}

func TestWriteHelpersReturnWriteErrors(t *testing.T) {
	writeErr := errors.New("write failed")

	if err := writeLine(failingWriter{err: writeErr}, "header"); !errors.Is(err, writeErr) {
		t.Fatalf("writeLine should return write error, got %v", err)
	}
	if err := writeFormat(failingWriter{err: writeErr}, "value=%d", 1); !errors.Is(err, writeErr) {
		t.Fatalf("writeFormat should return write error, got %v", err)
	}
	if err := writeDumpInsert(failingWriter{err: writeErr}, `"users"`, []string{`"id"`}, []interface{}{int64(1)}); !errors.Is(err, writeErr) {
		t.Fatalf("writeDumpInsert should return write error, got %v", err)
	}
}

func TestStripSQLComments(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"SELECT 1", "SELECT 1\n"},
		{"-- comment\nSELECT 1", "SELECT 1\n"},
		{"SELECT 1\n-- comment\nSELECT 2", "SELECT 1\nSELECT 2\n"},
		{"SELECT 1 -- inline", "SELECT 1 -- inline\n"},
	}
	for _, test := range tests {
		result := stripSQLComments(test.input)
		if result != test.expected {
			t.Errorf("stripSQLComments(%q) = %q, expected %q", test.input, result, test.expected)
		}
	}
}

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{"SELECT 1; SELECT 2;", []string{"SELECT 1;", " SELECT 2;"}},
		{"SELECT 'a;b';", []string{"SELECT 'a;b';"}},
		{"INSERT INTO t VALUES ('a', 'b');", []string{"INSERT INTO t VALUES ('a', 'b');"}},
		{"SELECT 1", []string{"SELECT 1"}},
		// A transaction BEGIN is not a compound body: still splits on ';'.
		{"BEGIN; UPDATE t SET v=1; COMMIT;", []string{"BEGIN;", " UPDATE t SET v=1;", " COMMIT;"}},
		// A trigger BEGIN...END body keeps its internal ';' and splits the
		// trailing statement separately.
		{
			"CREATE TRIGGER tg AFTER INSERT ON t BEGIN INSERT INTO l VALUES (1); END; SELECT 1;",
			[]string{"CREATE TRIGGER tg AFTER INSERT ON t BEGIN INSERT INTO l VALUES (1); END;", " SELECT 1;"},
		},
		// CASE...END nested inside a trigger body must not close the block early.
		{
			"CREATE TRIGGER tg AFTER INSERT ON t BEGIN INSERT INTO l VALUES (CASE WHEN 1>0 THEN 'a' ELSE 'b' END); END;",
			[]string{"CREATE TRIGGER tg AFTER INSERT ON t BEGIN INSERT INTO l VALUES (CASE WHEN 1>0 THEN 'a' ELSE 'b' END); END;"},
		},
	}
	for _, test := range tests {
		result := splitSQLStatements(test.input)
		if len(result) != len(test.expected) {
			t.Errorf("splitSQLStatements(%q) len = %d, expected %d", test.input, len(result), len(test.expected))
			continue
		}
		for i := range result {
			if result[i] != test.expected[i] {
				t.Errorf("splitSQLStatements(%q)[%d] = %q, expected %q", test.input, i, result[i], test.expected[i])
			}
		}
	}
}

func TestStrToRunes(t *testing.T) {
	input := []string{"SELECT", "FROM"}
	result := strToRunes(input)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if string(result[0]) != "SELECT" {
		t.Errorf("expected SELECT, got %s", string(result[0]))
	}
}

// TestSqlEscapeBackslashRoundTrip verifies that values containing backslashes
// are escaped such that a dumped INSERT round-trips through the engine's
// lexer/parser intact. Before the fix, sqlEscape left backslashes unescaped and
// the lexer (which treats `\` as an escape char) corrupted/failed the restore.
func TestSqlEscapeBackslashRoundTrip(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 256},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, `CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}

	values := []string{`C:\`, `a\`, `foo\bar`, `back\slash\end\`, `quote'and\back`}
	for i, v := range values {
		insert := fmt.Sprintf("INSERT INTO t (id, v) VALUES (%d, %s)", i, sqlEscape(v))
		if _, err := db.Exec(ctx, insert); err != nil {
			t.Fatalf("restore of escaped value %q failed: %v (stmt=%s)", v, err, insert)
		}
	}

	for i, want := range values {
		row := db.QueryRow(ctx, fmt.Sprintf("SELECT v FROM t WHERE id = %d", i))
		var got string
		if err := row.Scan(&got); err != nil {
			t.Fatalf("scan id=%d: %v", i, err)
		}
		if got != want {
			t.Errorf("round-trip mismatch for id=%d: got %q, want %q", i, got, want)
		}
	}
}
