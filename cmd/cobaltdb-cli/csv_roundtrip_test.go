package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestCSVExportImportRoundTrip drives the REAL exportTable and
// importCSV: export a table to CSV, re-import it into a second table, and
// require every TEXT value to come back byte-for-byte identical.
func TestCSVExportImportRoundTrip(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE rt (id INTEGER PRIMARY KEY, txt TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	// cases: in-scope shapes named by the round, plus whitespace padding.
	cases := []struct {
		id  int
		txt string
	}{
		{1, "plain"},
		{2, "has,comma"},
		{3, `has"quote`},
		{4, "line1\nline2"},
		{5, "  padded both sides  "},
		{6, "trailing space "},
		{7, ""},
	}
	for _, c := range cases {
		if _, err := db.Exec(ctx, "INSERT INTO rt (id, txt) VALUES (?, ?)", c.id, c.txt); err != nil {
			t.Fatalf("insert %d: %v", c.id, err)
		}
	}

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "rt.csv")
	if err := exportTable(db, "rt", csvPath, "csv"); err != nil {
		t.Fatalf("exportTable: %v", err)
	}
	raw, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	t.Logf("exported CSV:\n%s", string(raw))

	if _, err := db.Exec(ctx, "CREATE TABLE rt2 (id INTEGER PRIMARY KEY, txt TEXT)"); err != nil {
		t.Fatalf("create rt2: %v", err)
	}
	if err := importCSV(db, csvPath, "rt2"); err != nil {
		t.Fatalf("importCSV: %v", err)
	}

	rows, err := db.Query(ctx, "SELECT id, txt FROM rt2 ORDER BY id")
	if err != nil {
		t.Fatalf("query rt2: %v", err)
	}
	defer rows.Close()

	got := map[int]string{}
	n := 0
	for rows.Next() {
		var id int
		var txt string
		if err := rows.Scan(&id, &txt); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[id] = txt
		n++
	}
	if n != len(cases) {
		t.Errorf("imported %d rows, want %d (rows may have been silently skipped)", n, len(cases))
	}

	var fails []string
	for _, c := range cases {
		want := c.txt
		have, ok := got[c.id]
		if !ok {
			fails = append(fails, fmt.Sprintf("id=%d MISSING from re-import", c.id))
			continue
		}
		if have != want {
			fails = append(fails, fmt.Sprintf("id=%d round-trip mismatch: want %q (%d bytes) got %q (%d bytes)", c.id, want, len(want), have, len(have)))
		}
	}
	if len(fails) > 0 {
		t.Fatalf("FAIL: %d/%d rows did not round-trip byte-for-byte:\n  %s",
			len(fails), len(cases), strings.Join(fails, "\n  "))
	}
	t.Log("PASS: every row round-tripped byte-for-byte")
}
