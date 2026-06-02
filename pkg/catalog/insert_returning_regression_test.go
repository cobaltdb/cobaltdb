package catalog

import (
	"fmt"
	"testing"
)

// ExecuteQuery must surface RETURNING rows for INSERT, consistently with UPDATE
// and DELETE (the INSERT case previously discarded them and returned empty).
func TestExecuteQueryInsertReturning(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	if _, err := c.ExecuteQuery("CREATE TABLE ir (id INTEGER PRIMARY KEY, v TEXT, n INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	r, err := c.ExecuteQuery("INSERT INTO ir VALUES (1, 'a', 5) RETURNING id, n")
	if err != nil {
		t.Fatalf("insert returning: %v", err)
	}
	if len(r.Rows) != 1 || fmt.Sprintf("%v", r.Rows[0][0]) != "1" || fmt.Sprintf("%v", r.Rows[0][1]) != "5" {
		t.Fatalf("INSERT RETURNING rows = %v, want [[1 5]]", r.Rows)
	}

	// RETURNING with an expression.
	r2, err := c.ExecuteQuery("INSERT INTO ir VALUES (2, 'b', 7) RETURNING n + 1")
	if err != nil {
		t.Fatalf("insert returning expr: %v", err)
	}
	if len(r2.Rows) != 1 || fmt.Sprintf("%v", r2.Rows[0][0]) != "8" {
		t.Fatalf("INSERT RETURNING expr rows = %v, want [[8]]", r2.Rows)
	}

	// A plain INSERT (no RETURNING) must not surface stale RETURNING rows.
	r3, err := c.ExecuteQuery("INSERT INTO ir VALUES (3, 'c', 9)")
	if err != nil {
		t.Fatalf("plain insert: %v", err)
	}
	if len(r3.Rows) != 0 {
		t.Fatalf("plain INSERT must return no rows, got %v", r3.Rows)
	}
}
