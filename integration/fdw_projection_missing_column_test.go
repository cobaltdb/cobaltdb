package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestFDWCSVProjectedColumnAbsentFromHeader pins the wrapper/catalog
// projection contract.
//
// The catalog re-expands every streamed FDW row positionally against
// ScanOptions.Columns (catalog.expandFDWProjectedRow), which is only sound if
// the wrapper honours that column list and returns rows in the same order.
// The CSV wrapper used to abandon the projection (return nil = "no
// projection") as soon as ONE projected column was missing from the header,
// so the cursor returned the full header-ordered row and the catalog mapped
// the surviving columns by the wrong offsets — silently reporting one
// column's value under another column's name.
//
// A projected column that is absent from the CSV header must read as NULL.
func TestFDWCSVProjectedColumnAbsentFromHeader(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	dir := t.TempDir()

	// Header declares id,name but NOT score.
	missingPath := filepath.Join(dir, "missing.csv")
	if err := os.WriteFile(missingPath, []byte("id,name\n1,alice\n2,bob\n"), 0o644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}
	// Control: header declares every column of the foreign table.
	fullPath := filepath.Join(dir, "full.csv")
	if err := os.WriteFile(fullPath, []byte("id,name,score\n1,alice,95\n"), 0o644); err != nil {
		t.Fatalf("Failed to write CSV: %v", err)
	}

	for _, tc := range []struct{ table, path string }{
		{"ext_missing", missingPath},
		{"ext_full", fullPath},
	} {
		if _, err := db.Exec(ctx, fmt.Sprintf(
			`CREATE FOREIGN TABLE %s (id TEXT, name TEXT, score TEXT) WRAPPER 'csv' OPTIONS (file '%s')`,
			tc.table, tc.path)); err != nil {
			t.Fatalf("Failed to create foreign table %s: %v", tc.table, err)
		}
	}

	tests := []struct {
		name      string
		query     string
		wantID    string
		wantScore interface{}
	}{
		{
			name:      "absent column reads as NULL",
			query:     `SELECT id, score FROM ext_missing`,
			wantID:    "1",
			wantScore: nil,
		},
		{
			// Control: neighbouring valid input on the same code path.
			name:      "present columns keep their own values",
			query:     `SELECT id, score FROM ext_full`,
			wantID:    "1",
			wantScore: "95",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(ctx, tt.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			defer rows.Close()

			if !rows.Next() {
				t.Fatal("expected at least one row")
			}
			var id, score interface{}
			if err := rows.Scan(&id, &score); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}

			if fmt.Sprintf("%v", id) != tt.wantID {
				t.Errorf("id = %v, want %v", id, tt.wantID)
			}
			if fmt.Sprintf("%v", score) != fmt.Sprintf("%v", tt.wantScore) {
				t.Errorf("score = %v (%T), want %v (%T)", score, score, tt.wantScore, tt.wantScore)
			}
		})
	}
}
