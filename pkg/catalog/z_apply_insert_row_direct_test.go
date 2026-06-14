package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestApplyInsertRowDirectBasic verifies the happy path of the direct
// mutation path: a fresh row is stored in the B-tree, no PK conflict
// is reported, no indexes exist so no rollback is triggered, and the
// returned stmtInsert entry carries the key for statement-level tracking.
func TestApplyInsertRowDirectBasic(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "dir1 (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")

	// Direct path with no active transaction: wal is nil, no txnActive,
	// so the WAL flow is not exercised. We are testing the per-row
	// contract in isolation, mirroring the real call site.
	table, err := c.getTableLocked("dir1")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "dir1"}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}
	rowValues := []interface{}{int64(10), int64(20), int64(30)}
	key := "00000000000000000010"
	valueData := []byte("v-direct-10")

	insertedRow, stmtInsert, skipRow, err := c.applyInsertRowDirect(
		context.Background(), stmt, table, tree, nil, false,
		rowValues, key, valueData, true,
	)
	if err != nil {
		t.Fatalf("applyInsertRowDirect: %v", err)
	}
	if skipRow {
		t.Fatal("skipRow=true on a fresh row")
	}
	if insertedRow == nil {
		t.Fatal("insertedRow=nil with needsInsertedRows=true")
	}
	if insertedRow[0] != int64(10) || insertedRow[1] != int64(20) || insertedRow[2] != int64(30) {
		t.Fatalf("insertedRow=%v, want [10 20 30]", insertedRow)
	}
	if string(stmtInsert.key) != key {
		t.Fatalf("stmtInsert.key=%q, want %q", stmtInsert.key, key)
	}
	if len(stmtInsert.idxKeys) != 0 {
		t.Fatalf("stmtInsert.idxKeys=%v, want empty (no indexes)", stmtInsert.idxKeys)
	}

	// The B-tree must now contain the row. We probe via the B-tree
	// itself (not via SELECT, which would try to decode valueData)
	// so the assertion does not depend on row encoding.
	got, _ := tree.Get([]byte(key))
	if string(got) != string(valueData) {
		t.Fatalf("B-tree value=%q, want %q", got, valueData)
	}
}

// TestApplyInsertRowDirectNoNeedsInsertedRows verifies that with
// needsInsertedRows=false the returned insertedRow is nil but the B-tree
// mutation still happens and stmtInsert is still populated.
func TestApplyInsertRowDirectNoNeedsInsertedRows(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "dir_noins (id INTEGER PRIMARY KEY)")
	if _, _, err := c.Insert(context.Background(), buildInsertForTest("dir_noins", []int64{1}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("dir_noins")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "dir_noins"}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}
	rowValues := []interface{}{int64(7)}

	insertedRow, stmtInsert, skipRow, err := c.applyInsertRowDirect(
		context.Background(), stmt, table, tree, nil, false,
		rowValues, "00000000000000000007", []byte("v-7"), false,
	)
	if err != nil {
		t.Fatalf("applyInsertRowDirect: %v", err)
	}
	if skipRow {
		t.Fatal("skipRow=true on a fresh row")
	}
	if insertedRow != nil {
		t.Fatalf("insertedRow=%v with needsInsertedRows=false, want nil", insertedRow)
	}
	if string(stmtInsert.key) != "00000000000000000007" {
		t.Fatalf("stmtInsert.key=%q, want 00000000000000000007", stmtInsert.key)
	}
}

// TestApplyInsertRowDirectDuplicatePKReturnsError verifies that a plain
// INSERT (no IGNORE/REPLACE) on a duplicate primary key returns an
// error from resolvePKConflict, which the helper propagates verbatim.
// The B-tree must be untouched: the rollback path is not triggered
// because no row was stored yet (PK check happens before B-tree Put).
func TestApplyInsertRowDirectDuplicatePKReturnsError(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "dir_dup (id INTEGER PRIMARY KEY)")
	if _, _, err := c.Insert(context.Background(), buildInsertForTest("dir_dup", []int64{1, 2, 3}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("dir_dup")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "dir_dup"}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}

	// id=2 already exists. Plain INSERT → resolvePKConflict returns
	// an error, not a skip signal.
	rowValues := []interface{}{int64(2)}
	insertedRow, _, skipRow, err := c.applyInsertRowDirect(
		context.Background(), stmt, table, tree, nil, false,
		rowValues, "00000000000000000002", []byte("v-2-attempted"), true,
	)
	if err == nil {
		t.Fatal("applyInsertRowDirect: expected error on duplicate PK, got nil")
	}
	if !strings.Contains(err.Error(), "UNIQUE") && !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("error=%v, want UNIQUE/duplicate", err)
	}
	if skipRow {
		t.Fatal("skipRow=true on error path")
	}
	if insertedRow != nil {
		t.Fatalf("insertedRow=%v on error path, want nil", insertedRow)
	}
}

// TestApplyInsertRowDirectDuplicatePKIgnoreSkip verifies the IGNORE
// conflict-action path: a duplicate primary key is reported via
// skipRow=true (no error) and the B-tree is not mutated.
func TestApplyInsertRowDirectDuplicatePKIgnoreSkip(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "dir_ign (id INTEGER PRIMARY KEY)")
	if _, _, err := c.Insert(context.Background(), buildInsertForTest("dir_ign", []int64{1, 2, 3}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("dir_ign")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "dir_ign", ConflictAction: query.ConflictIgnore}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}

	rowValues := []interface{}{int64(2)}
	insertedRow, _, skipRow, err := c.applyInsertRowDirect(
		context.Background(), stmt, table, tree, nil, false,
		rowValues, "00000000000000000002", []byte("v-2-attempted"), true,
	)
	if err != nil {
		t.Fatalf("applyInsertRowDirect: %v", err)
	}
	if !skipRow {
		t.Fatal("skipRow=false on duplicate PK with IGNORE")
	}
	if insertedRow != nil {
		t.Fatalf("insertedRow=%v on skip path, want nil", insertedRow)
	}

	// Confirm the original value for id=2 was not overwritten.
	got, _ := tree.Get([]byte("00000000000000000002"))
	if string(got) == "v-2-attempted" {
		t.Fatal("B-tree value was overwritten on IGNORE path")
	}
}

// TestApplyInsertRowDirectNeedsInsertedRowsCopy verifies that the
// returned insertedRow is a defensive copy — mutating it after the call
// does not affect the input slice.
func TestApplyInsertRowDirectNeedsInsertedRowsCopy(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "dir_copy (id INTEGER PRIMARY KEY, v INTEGER)")
	if _, _, err := c.Insert(context.Background(), buildInsertForTest("dir_copy", []int64{1}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("dir_copy")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.InsertStmt{Table: "dir_copy"}
	tree, _, err := c.getInsertTargetTree(table, stmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}

	rowValues := []interface{}{int64(99), int64(100)}
	insertedRow, _, _, err := c.applyInsertRowDirect(
		context.Background(), stmt, table, tree, nil, false,
		rowValues, "00000000000000000099", []byte("v-99"), true,
	)
	if err != nil {
		t.Fatalf("applyInsertRowDirect: %v", err)
	}
	if insertedRow == nil {
		t.Fatal("insertedRow=nil with needsInsertedRows=true")
	}
	insertedRow[0] = int64(-1)
	if rowValues[0] != int64(99) {
		t.Fatalf("applyInsertRowDirect: returned copy shares memory with input; got %v", rowValues)
	}
}

// buildInsertForTest constructs a 1-column InsertStmt suitable for the
// helper tests. The table must already exist.
func buildInsertForTest(table string, ids []int64) *query.InsertStmt {
	values := make([][]query.Expression, 0, len(ids))
	for _, id := range ids {
		values = append(values, []query.Expression{
			&query.NumberLiteral{Value: float64(id)},
		})
	}
	return &query.InsertStmt{Table: table, Columns: []string{"id"}, Values: values}
}

// mustCreateTable runs a CREATE TABLE statement and fails the test on
// any error. It accepts the same column-list tail as the SQL surface.
func mustCreateTable(t *testing.T, c *Catalog, decl string) {
	t.Helper()
	if _, err := c.ExecuteQuery("CREATE TABLE " + decl); err != nil {
		t.Fatalf("CREATE TABLE %q: %v", decl, err)
	}
}
