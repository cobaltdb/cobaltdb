package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestExportJSONPreservesValueTypes pins that `export <table> <file> --format
// json` emits typed JSON values.
//
// Contract basis: the interactive `.mode json` emitter printRowsJSON assigns
// the scanned value directly (rowMap[c] = values[i]) and therefore keeps JSON
// types. exportTable instead assigned formatValue(values[i]), which
// fmt.Sprintf's everything into a string — so numbers and booleans were
// silently stringified in the exported file and every consumer had to re-parse
// each field to recover its type. The two JSON emitters disagreed on the same
// data.
//
// The CSV branch deliberately still uses formatValue, because encoding/csv
// requires []string; that is the control.
func TestExportJSONPreservesValueTypes(t *testing.T) {
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE TABLE ex (id INTEGER PRIMARY KEY, n INTEGER, f REAL, s TEXT)",
		"INSERT INTO ex VALUES (1, 42, 2.5, 'text'), (2, -7, 0.25, '')",
	} {
		if _, err := db.Exec(ctx, s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "out.json")

	// THE REAL PRODUCTION FUNCTION.
	if err := exportTable(db, "ex", out, "json"); err != nil {
		t.Fatalf("exportTable(json): %v", err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read export: %v", err)
	}
	t.Logf("exported JSON:\n%s", string(raw))

	// exportTable emits a STREAM of indented JSON values (one
	// json.Encoder.Encode per row, with SetIndent), so decode successive
	// values with json.Decoder rather than unmarshalling the file as one
	// document or splitting on newlines.
	dec := json.NewDecoder(bytes.NewReader(raw))
	var recs []map[string]interface{}
	for {
		var rec map[string]interface{}
		if err := dec.Decode(&rec); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("exported file is not a valid JSON stream: %v\n%s", err, string(raw))
		}
		recs = append(recs, rec)
	}
	if len(recs) != 2 {
		t.Fatalf("parsed %d records, want 2", len(recs))
	}
	for _, k := range []string{"id", "n", "f"} {
		if _, ok := recs[0][k].(float64); !ok {
			t.Errorf("FAIL: %s = %#v (%T), want a JSON number — export stringified it via formatValue",
				k, recs[0][k], recs[0][k])
		}
	}
	if _, ok := recs[0]["s"].(string); !ok {
		t.Errorf("FAIL: s = %#v (%T), want a JSON string", recs[0]["s"], recs[0]["s"])
	}
	// NULL must stay null, not become the literal string "NULL".
	if v, ok := recs[0]["s"]; ok && v == "NULL" {
		t.Error("FAIL: s was exported as the string \"NULL\"")
	}

	// CONTROL: the CSV branch still emits []string (encoding/csv requires it).
	csvOut := filepath.Join(dir, "out.csv")
	if err := exportTable(db, "ex", csvOut, "csv"); err != nil {
		t.Fatalf("exportTable(csv): %v", err)
	}
	f, err := os.Open(csvOut)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("csv parse: %v", err)
	}
	if len(records) != 3 { // header + 2 rows
		t.Fatalf("csv records = %d, want 3 (header + 2 rows)", len(records))
	}
	if records[1][1] != "42" {
		t.Errorf("control: csv n = %q, want %q (CSV must stay stringified)", records[1][1], "42")
	}
}
