package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestFinalizeInsertSetsLastReturning verifies that finalizeInsert stores
// the projected RETURNING rows via setLastReturning so the next call to
// GetLastReturningRows / GetLastReturningColumns returns them.
func TestFinalizeInsertSetsLastReturning(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE ret1 (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO ret1 (id, name) VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO ret1 (id, name) VALUES (3, 'carol') RETURNING id, name"); err != nil {
		t.Fatalf("returning insert: %v", err)
	}
	rows := c.GetLastReturningRows()
	cols := c.GetLastReturningColumns()
	if len(rows) != 1 {
		t.Fatalf("GetLastReturningRows: got %d rows, want 1", len(rows))
	}
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Fatalf("GetLastReturningColumns: got %v, want [id name]", cols)
	}
	if len(rows[0]) != 2 {
		t.Fatalf("row len: got %d, want 2", len(rows[0]))
	}
	if got, ok := rows[0][0].(int64); !ok || got != 3 {
		t.Fatalf("row[0]: got %v, want int64(3)", rows[0][0])
	}
	if got, ok := rows[0][1].(string); !ok || got != "carol" {
		t.Fatalf("row[1]: got %v, want 'carol'", rows[0][1])
	}
}

// TestFinalizeInsertNoReturningClearsStale verifies that a follow-up INSERT
// without RETURNING clears the previously-stored RETURNING rows.
func TestFinalizeInsertNoReturningClearsStale(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE ret2 (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO ret2 VALUES (1, 'x') RETURNING id"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if rows := c.GetLastReturningRows(); len(rows) != 1 {
		t.Fatalf("expected 1 row from first insert, got %d", len(rows))
	}

	// A plain INSERT (no RETURNING) should clear the stored rows.
	if _, err := c.ExecuteQuery("INSERT INTO ret2 VALUES (2, 'y')"); err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if rows := c.GetLastReturningRows(); len(rows) != 0 {
		t.Fatalf("LastReturning should be cleared by non-RETURNING insert, got %d rows", len(rows))
	}
}

// TestFinalizeInsertErrorPropagates verifies that a malformed RETURNING
// expression causes finalizeInsert to return a wrapped error.
func TestFinalizeInsertErrorPropagates(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE ret3 (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Build an InsertStmt that references a non-existent column in
	// RETURNING — evaluateReturning should fail, finalizeInsert should
	// return an error.
	stmt := &query.InsertStmt{
		Table:   "ret3",
		Columns: []string{"id"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}},
		},
		Returning: []query.Expression{
			&query.Identifier{Name: "no_such_col"},
		},
	}
	_, _, err := c.Insert(context.Background(), stmt, nil)
	if err == nil {
		t.Fatal("expected error from RETURNING on missing column, got nil")
	}
	if !strings.Contains(err.Error(), "RETURNING") {
		t.Fatalf("expected 'RETURNING' in error, got: %v", err)
	}
}
