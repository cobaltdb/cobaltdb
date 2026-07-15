package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestApplyDeleteEntryDirectBasic verifies the happy path of the
// direct delete path: a row is seeded, the helper soft-deletes it,
// the B-tree still holds the key (with a soft-delete marker), and
// the dead-tuple counter for the table is incremented.
func TestApplyDeleteEntryDirectBasic(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "del_dir1 (id INTEGER PRIMARY KEY, v INTEGER)")

	if _, _, err := c.Insert(context.Background(), buildInsertForTest("del_dir1", []int64{1}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("del_dir1")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.DeleteStmt{Table: "del_dir1"}
	key := []byte("00000000000000000001")
	tree := c.tableTrees["del_dir1"]
	preValue, _ := tree.Get(key)
	if len(preValue) == 0 {
		t.Fatal("B-tree: seed row missing before delete")
	}

	entry := &deleteEntry{
		key:      key,
		value:    preValue,
		row:      []interface{}{int64(1), int64(0)},
		treeName: "del_dir1",
	}

	deadBefore := c.deadTuples["del_dir1"]

	if err := c.applyDeleteEntryDirect(context.Background(), table, stmt, entry, nil, false); err != nil {
		t.Fatalf("applyDeleteEntryDirect: %v", err)
	}

	// The B-tree must still have the key (soft delete: the row is
	// marked deleted but not physically removed).
	postValue, _ := tree.Get(key)
	if len(postValue) == 0 {
		t.Fatal("B-tree: row missing after soft delete (expected soft-delete marker)")
	}
	// The pre- and post-delete values must differ (the soft-delete
	// marker is a timestamp appended to the versioned row).
	if string(preValue) == string(postValue) {
		t.Fatal("B-tree: soft-delete did not change the value (expected a new soft-delete marker)")
	}

	// Dead-tuple counter must have been incremented.
	if c.deadTuples["del_dir1"] != deadBefore+1 {
		t.Fatalf("deadTuples=%d, want %d", c.deadTuples["del_dir1"], deadBefore+1)
	}
}

// TestApplyDeleteEntryDirectPartitionTreeMissing verifies the error
// path when the entry references a partition tree that doesn't exist.
// This is the only failure case that can be triggered without a
// fully-wired storage layer; the other errors (WAL append, btree put,
// index put, trigger failure) require mocks we don't have.
func TestApplyDeleteEntryDirectPartitionTreeMissing(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "del_partmiss (id INTEGER PRIMARY KEY)")

	if _, _, err := c.Insert(context.Background(), buildInsertForTest("del_partmiss", []int64{1}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("del_partmiss")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.DeleteStmt{Table: "del_partmiss"}
	key := []byte("00000000000000000001")
	tree := c.tableTrees["del_partmiss"]
	preValue, _ := tree.Get(key)

	entry := &deleteEntry{
		key:      key,
		value:    preValue,
		row:      []interface{}{int64(1)},
		treeName: "nonexistent_tree", // forces the missing-tree branch
	}

	err = c.applyDeleteEntryDirect(context.Background(), table, stmt, entry, nil, false)
	if err == nil {
		t.Fatal("applyDeleteEntryDirect: expected error for missing tree, got nil")
	}
	if !strings.Contains(err.Error(), "nonexistent_tree") {
		t.Fatalf("error=%v, want one mentioning the missing tree name", err)
	}
}

// TestApplyDeleteEntryDirectIdxChangesForUniqueIndex verifies that
// a unique index entry is removed when the row is deleted. Since
// idxChanges are only recorded for active transactions, we drive
// the helper with a non-nil transaction state. We use a real
// INSERT through the public path to set up the seed, then call
// the helper with txnActive=true to exercise the index-mutation
// branches.
func TestApplyDeleteEntryDirectIdxChangesForUniqueIndex(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "del_idx (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	if _, err := c.ExecuteQuery("CREATE INDEX del_idx_code ON del_idx (code)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, _, err := c.Insert(context.Background(), buildInsertForTestWithText("del_idx", []int64{1}, []string{"alpha"}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("del_idx")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.DeleteStmt{Table: "del_idx"}
	key := []byte("00000000000000000001")
	tree := c.tableTrees["del_idx"]
	preValue, _ := tree.Get(key)
	entry := &deleteEntry{
		key:      key,
		value:    preValue,
		row:      []interface{}{int64(1), "alpha"},
		treeName: "del_idx",
	}

	// Sanity: index entry should be present before delete. The index
	// stores a non-unique compound key "S:alpha\x00<pk>".
	idxTree, ok := c.indexTrees["del_idx_code"]
	if !ok {
		t.Fatal("sanity: del_idx_code index not found")
	}
	idxKey := []byte("S:alpha\x00" + string(key))
	preIdxVal, _ := idxTree.Get(idxKey)
	if len(preIdxVal) == 0 {
		t.Fatal("sanity: index entry for alpha missing before delete")
	}

	// Stand up a transaction state so the helper records idxChanges
	// for the index mutation.
	ts := &catalogTxnState{}

	if err := c.applyDeleteEntryDirect(context.Background(), table, stmt, entry, ts, true); err != nil {
		t.Fatalf("applyDeleteEntryDirect: %v", err)
	}

	// The index entry for "alpha" must be gone after delete.
	postIdxVal, _ := idxTree.Get(idxKey)
	if len(postIdxVal) != 0 {
		t.Fatal("B-tree: index entry not deleted")
	}
}
