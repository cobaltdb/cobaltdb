package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// captureStream runs fn with os.Stdout/os.Stderr redirected and returns both.
func captureStream(t *testing.T, fn func()) (stdout, stderr string) {
	t.Helper()
	origOut, origErr := os.Stdout, os.Stderr
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stdout: %v", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe stderr: %v", err)
	}
	os.Stdout, os.Stderr = outW, errW

	outCh := make(chan string, 1)
	errCh := make(chan string, 1)
	go func() { b, _ := io.ReadAll(outR); outCh <- string(b) }()
	go func() { b, _ := io.ReadAll(errR); errCh <- string(b) }()

	fn()

	outW.Close()
	errW.Close()
	os.Stdout, os.Stderr = origOut, origErr
	return <-outCh, <-errCh
}

// TestJSONModeEmitsParseableJSON pins that `.mode json` writes stdout that
// encoding/json can parse.
//
// Contract basis: the mode exists to give machine-readable output, and the
// codebase's own `.export --format json` path writes a bare JSON document with
// no trailing decoration. printRowsJSON used to append a "(N rows)" line to
// STDOUT after the JSON document, which makes stdout invalid JSON
// ("invalid character '(' after top-level value") and breaks every consumer
// that pipes the CLI into jq/python. The row-count trailer now goes to stderr,
// so humans still see it while stdout stays machine-parseable.
//
// Table/csv/line modes are intentionally human-formatted and keep their
// trailer on stdout; only the json mode is machine-read.
func TestJSONModeEmitsParseableJSON(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE TABLE t (id INTEGER PRIMARY KEY, s TEXT, n INTEGER)",
		"INSERT INTO t VALUES (1,'a',10),(2,'b',20)",
	} {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}

	rows, err := db.Query(ctx, "SELECT id, s, n FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	cols := rows.Columns()

	stdout, stderr := captureStream(t, func() { printRowsJSON(rows, cols) })
	rows.Close()

	t.Logf("stdout:\n%s", stdout)
	t.Logf("stderr: %q", stderr)

	// THE CONTRACT: stdout must be a bare, parseable JSON document.
	var back []map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &back); err != nil {
		t.Fatalf("stdout is not valid JSON: %v\nstdout was:\n%s", err, stdout)
	}
	if len(back) != 2 {
		t.Fatalf("parsed %d rows, want 2", len(back))
	}
	// Secondary check: JSON type fidelity survives the round trip.
	if id, ok := back[0]["id"].(float64); !ok || id != 1 {
		t.Errorf("id = %#v, want numeric 1 (JSON type fidelity lost)", back[0]["id"])
	}
	// The row count is still reported, just not on stdout.
	if !strings.Contains(stderr, "(2 rows)") {
		t.Errorf("stderr = %q, want it to contain the row-count trailer \"(2 rows)\"", stderr)
	}
	if strings.Contains(stdout, "rows)") {
		t.Errorf("stdout must not carry the row-count trailer: %q", stdout)
	}
}
