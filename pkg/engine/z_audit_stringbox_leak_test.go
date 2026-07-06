package engine

import (
	"context"
	"testing"
)

// TestScanTextColumnsReturnGoString verifies that reading multiple TEXT columns
// (which triggers the zero-allocation fast decoder that wraps values in an
// internal catalog.StringBox) yields plain Go strings through the public Scan
// API rather than leaking the internal catalog.StringBox type.
//
// Regression: cloneScannedValue did not unwrap catalog.StringBox, so
// Scan(&interface{}) returned catalog.StringBox for ordinary TEXT columns.
func TestScanTextColumnsReturnGoString(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 256}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec(ctx, "INSERT INTO t VALUES (?, ?, ?, ?)", i, "alpha", "beta", "gamma"); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	rows, err := db.Query(ctx, "SELECT id, a, b, c FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()

	want := []string{"alpha", "beta", "gamma"}
	got := 0
	for rows.Next() {
		vals := make([]interface{}, 4)
		ptrs := make([]interface{}, 4)
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if _, ok := vals[0].(int64); !ok {
			t.Errorf("id column: got %T, want int64", vals[0])
		}
		for i, w := range want {
			s, ok := vals[i+1].(string)
			if !ok {
				t.Errorf("column %d: got %T (%v), want Go string", i+1, vals[i+1], vals[i+1])
				continue
			}
			if s != w {
				t.Errorf("column %d: got %q, want %q", i+1, s, w)
			}
		}
		got++
	}
	if got != 5 {
		t.Fatalf("row count: got %d, want 5", got)
	}
}
