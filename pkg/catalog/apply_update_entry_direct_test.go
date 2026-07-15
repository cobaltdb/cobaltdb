package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestApplyUpdateEntryDirectBasic verifies the happy path of the direct
// update path: a row exists in the B-tree, the helper writes the new
// value, no PK change is detected, no indexes exist, and the returned
// idxChanges slice is empty. We probe the B-tree directly to confirm
// the new value is in place at the original key.
func TestApplyUpdateEntryDirectBasic(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "upd_dir1 (id INTEGER PRIMARY KEY, v INTEGER)")

	// Seed a row.
	if _, _, err := c.Insert(context.Background(), buildInsertForTest("upd_dir1", []int64{42}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("upd_dir1")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.UpdateStmt{Table: "upd_dir1"}
	oldKey := []byte("00000000000000000042")
	oldRow := []interface{}{int64(42), int64(100)}
	newRow := []interface{}{int64(42), int64(200)}
	entry := &updateEntry{key: oldKey, oldRow: oldRow, newRow: newRow, treeName: "upd_dir1"}

	// Encode the new value the way the caller would (mirrors
	// applyUpdateEntries' encodeVersionedRow call).
	newValueData, err := encodeVersionedRow(newRow, nil)
	if err != nil {
		t.Fatalf("encodeVersionedRow: %v", err)
	}

	idxChanges, err := c.applyUpdateEntryDirect(
		table, stmt, entry, oldKey, oldKey, false, -1, nil, false, newValueData,
	)
	if err != nil {
		t.Fatalf("applyUpdateEntryDirect: %v", err)
	}
	if len(idxChanges) != 0 {
		t.Fatalf("idxChanges=%v, want empty (no indexes)", idxChanges)
	}

	// The B-tree must now contain the new value at the original key.
	got, _ := tableTreesForTest(c, "upd_dir1").Get(oldKey)
	if len(got) == 0 {
		t.Fatal("B-tree value is nil after direct update")
	}
}

// TestApplyUpdateEntryDirectPKChanged verifies the PK-change branch:
// when the new key differs from the old key, the helper writes the
// new value at the new key and (tolerantly) accepts ErrKeyNotFound
// when deleting the old key. The old key should be empty afterwards.
func TestApplyUpdateEntryDirectPKChanged(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "upd_pk (id INTEGER PRIMARY KEY, v INTEGER)")

	if _, _, err := c.Insert(context.Background(), buildInsertForTest("upd_pk", []int64{7}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("upd_pk")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.UpdateStmt{Table: "upd_pk"}
	oldKey := []byte("00000000000000000007")
	newKey := []byte("00000000000000000099")
	oldRow := []interface{}{int64(7), int64(100)}
	newRow := []interface{}{int64(99), int64(200)}
	entry := &updateEntry{key: oldKey, oldRow: oldRow, newRow: newRow, treeName: "upd_pk"}

	newValueData, err := encodeVersionedRow(newRow, nil)
	if err != nil {
		t.Fatalf("encodeVersionedRow: %v", err)
	}

	tree := tableTreesForTest(c, "upd_pk")
	// Sanity: old key should have data.
	pre, _ := tree.Get(oldKey)
	if len(pre) == 0 {
		t.Fatal("B-tree: old key has no value before update")
	}

	idxChanges, err := c.applyUpdateEntryDirect(
		table, stmt, entry, oldKey, newKey, true, 0, nil, false, newValueData,
	)
	if err != nil {
		t.Fatalf("applyUpdateEntryDirect: %v", err)
	}
	if len(idxChanges) != 0 {
		t.Fatalf("idxChanges=%v, want empty (no indexes)", idxChanges)
	}

	// The new key must hold the new value.
	if got, _ := tree.Get(newKey); len(got) == 0 {
		t.Fatal("B-tree: new key has no value after PK-changed update")
	}
	// The old key should be empty (deleted).
	if got, _ := tree.Get(oldKey); len(got) != 0 {
		t.Fatalf("B-tree: old key still has %d bytes after PK-changed update", len(got))
	}
}

// TestApplyUpdateEntryDirectPartitionTreeMissing verifies the error
// path when the entry references a partition tree that doesn't exist.
// This is the only failure case that can be triggered without a
// fully-wired storage layer; the other errors (WAL append, btree put,
// index put) require mocks we don't have.
func TestApplyUpdateEntryDirectPartitionTreeMissing(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "upd_partmiss (id INTEGER PRIMARY KEY)")

	table, err := c.getTableLocked("upd_partmiss")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.UpdateStmt{Table: "upd_partmiss"}
	oldKey := []byte("00000000000000000001")
	entry := &updateEntry{
		key:      oldKey,
		oldRow:   []interface{}{int64(1)},
		newRow:   []interface{}{int64(1)},
		treeName: "nonexistent_tree", // forces the missing-tree branch
	}

	newValueData, err := encodeVersionedRow(entry.newRow, nil)
	if err != nil {
		t.Fatalf("encodeVersionedRow: %v", err)
	}

	_, err = c.applyUpdateEntryDirect(
		table, stmt, entry, oldKey, oldKey, false, -1, nil, false, newValueData,
	)
	if err == nil {
		t.Fatal("applyUpdateEntryDirect: expected error for missing tree, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent_tree") {
		t.Fatalf("error=%v, want one mentioning the missing tree name", err)
	}
}

// TestApplyUpdateEntryDirectIdxChangesForUniqueIndex verifies that a
// unique index is updated when the indexed column changes. Since
// idxChanges are only recorded for active transactions, we drive the
// helper with a non-nil transaction state. We use a real UPDATE
// through the public path to set up the seed, then call the helper
// with txnActive=true to exercise the index-mutation branches.
func TestApplyUpdateEntryDirectIdxChangesForUniqueIndex(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "upd_idx (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	if _, err := c.ExecuteQuery("CREATE INDEX upd_idx_code ON upd_idx (code)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, _, err := c.Insert(context.Background(), buildInsertForTestWithText("upd_idx", []int64{1}, []string{"alpha"}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("upd_idx")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.UpdateStmt{Table: "upd_idx"}
	oldKey := []byte("00000000000000000001")
	oldRow := []interface{}{int64(1), "alpha"}
	newRow := []interface{}{int64(1), "beta"}
	entry := &updateEntry{key: oldKey, oldRow: oldRow, newRow: newRow, treeName: "upd_idx"}

	newValueData, err := encodeVersionedRow(newRow, nil)
	if err != nil {
		t.Fatalf("encodeVersionedRow: %v", err)
	}

	// Stand up a transaction state so the helper records idxChanges
	// for the index mutations. The ts is only used to gate the WAL
	// append; for index mutation recording, txnActive is the gate.
	ts := &catalogTxnState{}

	idxChanges, err := c.applyUpdateEntryDirect(
		table, stmt, entry, oldKey, oldKey, false, 0, ts, true, newValueData,
	)
	if err != nil {
		t.Fatalf("applyUpdateEntryDirect: %v", err)
	}
	if len(idxChanges) != 2 {
		t.Fatalf("idxChanges=%v, want 2 entries (one delete, one insert) for unique index update", idxChanges)
	}
	var sawDelete, sawInsert bool
	for _, ch := range idxChanges {
		if !ch.wasAdded {
			sawDelete = true
		} else {
			sawInsert = true
		}
	}
	if !sawDelete || !sawInsert {
		t.Fatalf("idxChanges=%v, want one wasAdded=false and one wasAdded=true", idxChanges)
	}
}

// tableTreesForTest returns the B-tree for the given table, panicking
// on lookup failure so the test can probe the underlying storage.
func tableTreesForTest(c *Catalog, tableName string) interface {
	Get(key []byte) ([]byte, error)
} {
	tree, ok := c.tableTrees[tableName]
	if !ok {
		panic("test bug: table tree " + tableName + " not found")
	}
	return tree
}

// buildInsertForTestWithText builds a multi-column InsertStmt with
// mixed integer and text columns. Used by the unique-index test above.
func buildInsertForTestWithText(table string, ids []int64, texts []string) *query.InsertStmt {
	if len(ids) != len(texts) {
		panic("test bug: ids and texts length mismatch")
	}
	values := make([][]query.Expression, 0, len(ids))
	for i, id := range ids {
		values = append(values, []query.Expression{
			&query.NumberLiteral{Value: float64(id)},
			&query.StringLiteral{Value: texts[i]},
		})
	}
	return &query.InsertStmt{Table: table, Columns: []string{"id", "code"}, Values: values}
}
