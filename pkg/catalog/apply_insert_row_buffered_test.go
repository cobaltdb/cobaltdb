package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestApplyInsertRowBufferedBasic verifies that a row in a fresh table
// (no PK conflict) is buffered and the insertedRow copy is returned when
// needsInsertedRows=true.
func TestApplyInsertRowBufferedBasic(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE buf1 (id INTEGER PRIMARY KEY, v TEXT)")

	c.BeginTransaction(1)
	defer func() { _ = c.RollbackTransaction() }()

	table, err := c.getTableLocked("buf1")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "buf1"}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}
	ts := c.getCurrentTxn()

	rowValues := []interface{}{int64(1), "alice"}
	// formatKey(1) returns the 20-digit zero-padded decimal "00000000000000000001".
	insertedRow, skipRow, err := c.applyInsertRowBuffered(
		stmt, table, tree, ts, rowValues,
		"00000000000000000001",
		[]byte("v"), true,
	)
	if err != nil {
		t.Fatalf("applyInsertRowBuffered: %v", err)
	}
	if skipRow {
		t.Fatal("applyInsertRowBuffered: skipRow=true on a fresh row")
	}
	if insertedRow == nil {
		t.Fatal("applyInsertRowBuffered: insertedRow=nil with needsInsertedRows=true")
	}
	if insertedRow[0] != int64(1) || insertedRow[1] != "alice" {
		t.Fatalf("applyInsertRowBuffered: insertedRow=%v, want [1 alice]", insertedRow)
	}
}

// TestApplyInsertRowBufferedNoNeedsInsertedRows verifies that with
// needsInsertedRows=false the returned insertedRow is nil.
func TestApplyInsertRowBufferedNoNeedsInsertedRows(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE buf_noins (id INTEGER PRIMARY KEY, v TEXT)")

	c.BeginTransaction(1)
	defer func() { _ = c.RollbackTransaction() }()

	table, err := c.getTableLocked("buf_noins")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "buf_noins"}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}
	ts := c.getCurrentTxn()

	rowValues := []interface{}{int64(1), "alice"}
	insertedRow, skipRow, err := c.applyInsertRowBuffered(
		stmt, table, tree, ts, rowValues,
		"00000000000000000001",
		[]byte("v"), false,
	)
	if err != nil {
		t.Fatalf("applyInsertRowBuffered: %v", err)
	}
	if skipRow {
		t.Fatal("skipRow=true on a fresh row")
	}
	if insertedRow != nil {
		t.Fatalf("insertedRow=%v with needsInsertedRows=false, want nil", insertedRow)
	}
}

// TestApplyInsertRowBufferedPKConflictIgnore verifies that IGNORE on
// a duplicate in pending writes skips the row.
func TestApplyInsertRowBufferedPKConflictIgnore(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE buf_ign (id INTEGER PRIMARY KEY, v TEXT)")

	c.BeginTransaction(1)
	defer func() { _ = c.RollbackTransaction() }()

	// First insert: id=1
	bcExec(t, c, "INSERT INTO buf_ign VALUES (1, 'first')")

	table, err := c.getTableLocked("buf_ign")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "buf_ign", ConflictAction: query.ConflictIgnore}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}
	ts := c.getCurrentTxn()

	// Second insert: id=1 again with IGNORE → should skip
	rowValues := []interface{}{int64(1), "second"}
	insertedRow, skipRow, err := c.applyInsertRowBuffered(
		stmt, table, tree, ts, rowValues,
		"00000000000000000001",
		[]byte("v"), true,
	)
	if err != nil {
		t.Fatalf("applyInsertRowBuffered: %v", err)
	}
	if !skipRow {
		t.Fatal("skipRow=false on duplicate PK with IGNORE")
	}
	if insertedRow != nil {
		t.Fatalf("insertedRow=%v on skip path, want nil", insertedRow)
	}
}

// TestApplyInsertRowBufferedNeedsInsertedRowsCopy verifies that the
// returned insertedRow is a defensive copy (mutating it after the call
// doesn't affect the input).
func TestApplyInsertRowBufferedNeedsInsertedRowsCopy(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	bcExec(t, c, "CREATE TABLE buf_copy (id INTEGER PRIMARY KEY, v TEXT)")

	c.BeginTransaction(1)
	defer func() { _ = c.RollbackTransaction() }()

	table, err := c.getTableLocked("buf_copy")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "buf_copy"}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}
	ts := c.getCurrentTxn()

	rowValues := []interface{}{int64(1), "alice"}
	insertedRow, _, err := c.applyInsertRowBuffered(
		stmt, table, tree, ts, rowValues,
		"00000000000000000001",
		[]byte("v"), true,
	)
	if err != nil {
		t.Fatalf("applyInsertRowBuffered: %v", err)
	}
	if insertedRow == nil {
		t.Fatal("insertedRow=nil")
	}
	// Mutate the returned copy; the original rowValues should be unchanged.
	insertedRow[1] = "MUTATED"
	if rowValues[1] != "alice" {
		t.Fatalf("applyInsertRowBuffered: returned copy shares memory with input; got %v", rowValues)
	}
}
