package main

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func captureStdoutCSV(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(b)
}

func TestCSVModeEmitsParseableCSV(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id INTEGER, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1,'a'),(2,'b')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	rows, err := db.Query(ctx, "SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	cols := []string{"id", "name"}

	out := captureStdoutCSV(t, func() {
		if err := printRowsCSV(rows, cols, true); err != nil {
			t.Errorf("printRowsCSV: %v", err)
		}
	})

	// CONTROL: exactly 2 data rows + header are expected.
	recs, perr := csv.NewReader(strings.NewReader(out)).ReadAll()
	if perr == nil && len(recs) == 3 && recs[0][0] == "id" && recs[1][0] == "1" && recs[2][0] == "2" {
		t.Logf("PASS: csv parses cleanly")
		return
	}
	t.Fatalf("FAIL: `.mode csv` output is not machine-parseable CSV (err=%v, %d records).\nstdout was:\n%s",
		perr, len(recs), out)
}
