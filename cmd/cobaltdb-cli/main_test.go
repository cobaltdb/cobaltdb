package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
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

func TestCreateSecureOutputFilePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "export.csv")
	file, err := createSecureOutputFile(path)
	if err != nil {
		t.Fatalf("createSecureOutputFile failed: %v", err)
	}
	if _, err := file.WriteString("id,name\n1,Alice\n"); err != nil {
		t.Fatalf("write output file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close output file: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat output file: %v", err)
	}
	if info.Mode().Perm() != cliOutputFilePerm {
		t.Fatalf("Expected output file permissions %o, got %o", cliOutputFilePerm, info.Mode().Perm())
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
