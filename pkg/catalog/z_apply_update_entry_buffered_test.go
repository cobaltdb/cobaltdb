package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestBufferUpdateEntryBasic verifies the happy path of the buffered update
// path: no indexes exist, the helper encodes the new row, appends a single
// pending write to ts.pendingWrites, and returns an empty idxUpdates slice.
func TestBufferUpdateEntryBasic(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "buf_basic (id INTEGER PRIMARY KEY, v INTEGER)")

	// Seed a row.
	if _, _, err := c.Insert(context.Background(), buildInsertForTest("buf_basic", []int64{42}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("buf_basic")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.UpdateStmt{Table: "buf_basic"}
	oldKey := []byte("00000000000000000042")
	oldRow := []interface{}{int64(42), int64(100)}
	newRow := []interface{}{int64(42), int64(200)}
	entry := &updateEntry{key: oldKey, oldRow: oldRow, newRow: newRow, treeName: "buf_basic"}

	ts := &catalogTxnState{}
	c.registerGoroutineTxn(ts)
	defer c.unregisterGoroutineTxn()

	newValueData, idxUpdates, err := c.bufferUpdateEntry(table, stmt, entry, ts)
	if err != nil {
		t.Fatalf("bufferUpdateEntry: %v", err)
	}
	if len(newValueData) == 0 {
		t.Fatal("newValueData is empty")
	}
	if len(idxUpdates) != 0 {
		t.Fatalf("idxUpdates=%v, want empty (no indexes)", idxUpdates)
	}

	// ts.pendingWrites must have exactly one entry with the correct fields.
	if len(ts.pendingWrites) != 1 {
		t.Fatalf("pendingWrites len=%d, want 1", len(ts.pendingWrites))
	}
	pw := ts.pendingWrites[0]
	if pw.TreeName != "buf_basic" {
		t.Errorf("pw.TreeName=%q, want %q", pw.TreeName, "buf_basic")
	}
	if string(pw.Key) != string(oldKey) {
		t.Errorf("pw.Key=%q, want %q", pw.Key, oldKey)
	}
	if len(pw.Value) == 0 {
		t.Error("pw.Value is empty")
	}
}

// TestBufferUpdateEntryIdxUpdates verifies that for a UNIQUE index, the helper
// builds two idxUpdates: a delete for the old key and an insert for the new key.
func TestBufferUpdateEntryIdxUpdates(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "buf_idx (id INTEGER PRIMARY KEY, code TEXT)")

	// Seed one row so the index has something to read.
	if _, _, err := c.Insert(context.Background(),
		buildInsertForTestWithText("buf_idx", []int64{1}, []string{"alpha"}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	// CREATE UNIQUE INDEX so idxDef.Unique=true in bufferUpdateEntry.
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX buf_idx_code ON buf_idx (code)"); err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	table, err := c.getTableLocked("buf_idx")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	stmt := &query.UpdateStmt{Table: "buf_idx"}
	oldKey := []byte("00000000000000000001")
	oldRow := []interface{}{int64(1), "alpha"}
	newRow := []interface{}{int64(1), "beta"}
	entry := &updateEntry{key: oldKey, oldRow: oldRow, newRow: newRow, treeName: "buf_idx"}

	ts := &catalogTxnState{}
	c.registerGoroutineTxn(ts)
	defer c.unregisterGoroutineTxn()

	_, idxUpdates, err := c.bufferUpdateEntry(table, stmt, entry, ts)
	if err != nil {
		t.Fatalf("bufferUpdateEntry: %v", err)
	}
	// One delete (old key) + one insert (new key) = 2.
	if len(idxUpdates) != 2 {
		t.Fatalf("idxUpdates len=%d, want 2 (delete+insert)", len(idxUpdates))
	}

	deleteCount := 0
	insertCount := 0
	for _, u := range idxUpdates {
		if u.IsDelete {
			deleteCount++
			if !strings.HasSuffix(u.Key, "alpha") {
				t.Errorf("delete key=%q, want suffix 'alpha'", u.Key)
			}
		} else {
			insertCount++
			if !strings.HasSuffix(u.Key, "beta") {
				t.Errorf("insert key=%q, want suffix 'beta'", u.Key)
			}
		}
	}
	if deleteCount != 1 {
		t.Errorf("deleteCount=%d, want 1", deleteCount)
	}
	if insertCount != 1 {
		t.Errorf("insertCount=%d, want 1", insertCount)
	}
}

// TestBufferUpdateEntryUniqueConstraintViolation verifies that when the live
// index tree already contains the new key, bufferUpdateEntry returns a
// UNIQUE constraint error.
func TestBufferUpdateEntryUniqueConstraintViolation(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "buf_uniq (id INTEGER PRIMARY KEY, code TEXT)")

	// Insert two rows with distinct code values.
	stmt := buildInsertForTestWithText("buf_uniq", []int64{1, 2}, []string{"alpha", "beta"})
	if _, _, err := c.Insert(context.Background(), stmt, nil); err != nil {
		t.Fatalf("insert rows: %v", err)
	}

	// CREATE UNIQUE INDEX so idxDef.Unique=true in bufferUpdateEntry.
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX buf_uniq_code ON buf_uniq (code)"); err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	table, err := c.getTableLocked("buf_uniq")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	// Attempt to update row1's code to "beta" — row2 already has "beta" in the index.
	updateStmt := &query.UpdateStmt{Table: "buf_uniq"}
	entry := &updateEntry{
		key:      []byte("00000000000000000001"),
		oldRow:   []interface{}{int64(1), "alpha"},
		newRow:   []interface{}{int64(1), "beta"},
		treeName: "buf_uniq",
	}

	ts := &catalogTxnState{}
	c.registerGoroutineTxn(ts)
	defer c.unregisterGoroutineTxn()

	_, _, err = c.bufferUpdateEntry(table, updateStmt, entry, ts)
	if err == nil {
		t.Fatal("expected UNIQUE constraint error, got nil")
	}
	if !strings.Contains(err.Error(), "UNIQUE constraint failed") {
		t.Errorf("error=%q, want contains 'UNIQUE constraint failed'", err.Error())
	}
}

// TestBufferUpdateEntryUniqueConstraintInPendingWrites verifies that when the
// new key is already in the pending writes (from a prior entry in the same
// batch), bufferUpdateEntry returns a UNIQUE constraint error even though
// the live index tree does not yet contain the key.
func TestBufferUpdateEntryUniqueConstraintInPendingWrites(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "buf_pen (id INTEGER PRIMARY KEY, code TEXT)")

	// Insert two rows with distinct code values.
	stmt := buildInsertForTestWithText("buf_pen", []int64{1, 2}, []string{"alpha", "gamma"})
	if _, _, err := c.Insert(context.Background(), stmt, nil); err != nil {
		t.Fatalf("insert rows: %v", err)
	}

	// CREATE UNIQUE INDEX — the live index tree will have "alpha" and "gamma"
	// but NOT "beta" (which is only in pending writes from entry1).
	if _, err := c.ExecuteQuery("CREATE UNIQUE INDEX buf_pen_code ON buf_pen (code)"); err != nil {
		t.Fatalf("CREATE UNIQUE INDEX: %v", err)
	}

	table, err := c.getTableLocked("buf_pen")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	ts := &catalogTxnState{}
	c.registerGoroutineTxn(ts)
	defer c.unregisterGoroutineTxn()

	updateStmt := &query.UpdateStmt{Table: "buf_pen"}

	// entry1: change row1's code from "alpha" → "beta". This succeeds and
	// records "beta" in pending writes. The live index tree still has "alpha"
	// and "gamma" (no conflict at this point).
	entry1 := &updateEntry{
		key:      []byte("00000000000000000001"),
		oldRow:   []interface{}{int64(1), "alpha"},
		newRow:   []interface{}{int64(1), "beta"},
		treeName: "buf_pen",
	}
	_, _, err = c.bufferUpdateEntry(table, updateStmt, entry1, ts)
	if err != nil {
		t.Fatalf("entry1 bufferUpdateEntry: %v", err)
	}

	// entry2: change row2's code from "gamma" → "beta". The live index tree
	// does not contain "beta" (only "alpha" and "gamma"), but indexKeyInPendingWrites
	// finds "beta" in entry1's pending write, so the UNIQUE check fires.
	entry2 := &updateEntry{
		key:      []byte("00000000000000000002"),
		oldRow:   []interface{}{int64(2), "gamma"},
		newRow:   []interface{}{int64(2), "beta"},
		treeName: "buf_pen",
	}
	_, _, err = c.bufferUpdateEntry(table, updateStmt, entry2, ts)
	if err == nil {
		t.Fatal("expected UNIQUE constraint error from pending writes, got nil")
	}
	if !strings.Contains(err.Error(), "UNIQUE constraint failed") {
		t.Errorf("error=%q, want contains 'UNIQUE constraint failed'", err.Error())
	}
}

// TestBufferUpdateEntryTsNil verifies that passing ts=nil is safe and does
// not panic — appendPendingWriteTs guards against nil ts.
func TestBufferUpdateEntryTsNil(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "buf_nil (id INTEGER PRIMARY KEY, v INTEGER)")

	if _, _, err := c.Insert(context.Background(), buildInsertForTest("buf_nil", []int64{1}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("buf_nil")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	entry := &updateEntry{
		key:      []byte("00000000000000000001"),
		oldRow:   []interface{}{int64(1), int64(10)},
		newRow:   []interface{}{int64(1), int64(20)},
		treeName: "buf_nil",
	}
	stmt := &query.UpdateStmt{Table: "buf_nil"}

	// ts=nil must not panic.
	newValueData, idxUpdates, err := c.bufferUpdateEntry(table, stmt, entry, nil)
	if err != nil {
		t.Fatalf("bufferUpdateEntry(ts=nil): %v", err)
	}
	if len(newValueData) == 0 {
		t.Fatal("newValueData is empty")
	}
	if len(idxUpdates) != 0 {
		t.Fatalf("idxUpdates=%v, want empty", idxUpdates)
	}
}
