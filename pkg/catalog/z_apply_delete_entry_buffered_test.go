package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestApplyDeleteEntryBufferedBasic verifies the happy path of the
// buffered delete path: a row is seeded, the helper buffers the
// soft-deleted entry into ts.pendingWrites, and the B-tree is
// untouched (since the buffered write is deferred to commit time).
func TestApplyDeleteEntryBufferedBasic(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "delbuf_dir1 (id INTEGER PRIMARY KEY, v INTEGER)")

	if _, _, err := c.Insert(context.Background(), buildInsertForTest("delbuf_dir1", []int64{1}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("delbuf_dir1")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.DeleteStmt{Table: "delbuf_dir1"}
	key := []byte("00000000000000000001")
	tree := c.tableTrees["delbuf_dir1"]
	preValue, _ := tree.Get(key)
	if len(preValue) == 0 {
		t.Fatal("B-tree: seed row missing before delete")
	}

	entry := &deleteEntry{
		key:      key,
		value:    preValue,
		row:      []interface{}{int64(1), int64(0)},
		treeName: "delbuf_dir1",
	}

	ts := &catalogTxnState{txnActive: true}

	if err := c.applyDeleteEntryBuffered(context.Background(), table, stmt, entry, ts); err != nil {
		t.Fatalf("applyDeleteEntryBuffered: %v", err)
	}

	// B-tree must be UNCHANGED — the buffered write is deferred to
	// commit time, so the row's value at the original key must
	// still be the pre-delete value.
	postValue, _ := tree.Get(key)
	if string(postValue) != string(preValue) {
		t.Fatal("B-tree: buffered delete must not mutate the live tree")
	}

	// The pending-writes list must contain exactly one entry.
	if len(ts.pendingWrites) != 1 {
		t.Fatalf("ts.pendingWrites len=%d, want 1", len(ts.pendingWrites))
	}
	pw := ts.pendingWrites[0]
	if pw.TreeName != "delbuf_dir1" {
		t.Fatalf("pw.TreeName=%q, want delbuf_dir1", pw.TreeName)
	}
	if pw.Key != string(key) {
		t.Fatalf("pw.Key=%q, want %q", pw.Key, key)
	}
	if len(pw.Value) == 0 {
		t.Fatal("pw.Value is empty (helper did not encode soft-deleted row)")
	}
	// The soft-deleted encoded value must differ from the original.
	if string(pw.Value) == string(preValue) {
		t.Fatal("pw.Value equals pre-delete value (helper did not mark deleted)")
	}
	// No index updates expected (no indexes defined for this table).
	if len(pw.IndexUpdates) != 0 {
		t.Fatalf("pw.IndexUpdates=%v, want empty (no indexes)", pw.IndexUpdates)
	}
}

// TestApplyDeleteEntryBufferedIdxUpdatesForUniqueIndex verifies that
// the helper builds a PendingIndexUpdate for a unique index, marked
// as IsDelete. The compound non-unique form is also tested. We
// assert on the helper's output shape rather than commit-time
// application, since the test doesn't drive the commit step.
func TestApplyDeleteEntryBufferedIdxUpdatesForUniqueIndex(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "delbuf_idx (id INTEGER PRIMARY KEY, code TEXT UNIQUE)")
	if _, err := c.ExecuteQuery("CREATE INDEX delbuf_idx_code ON delbuf_idx (code)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, _, err := c.Insert(context.Background(), buildInsertForTestWithText("delbuf_idx", []int64{1}, []string{"alpha"}), nil); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	table, err := c.getTableLocked("delbuf_idx")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}
	stmt := &query.DeleteStmt{Table: "delbuf_idx"}
	key := []byte("00000000000000000001")
	tree := c.tableTrees["delbuf_idx"]
	preValue, _ := tree.Get(key)

	entry := &deleteEntry{
		key:      key,
		value:    preValue,
		row:      []interface{}{int64(1), "alpha"},
		treeName: "delbuf_idx",
	}

	ts := &catalogTxnState{txnActive: true}

	if err := c.applyDeleteEntryBuffered(context.Background(), table, stmt, entry, ts); err != nil {
		t.Fatalf("applyDeleteEntryBuffered: %v", err)
	}

	if len(ts.pendingWrites) != 1 {
		t.Fatalf("ts.pendingWrites len=%d, want 1", len(ts.pendingWrites))
	}
	pw := ts.pendingWrites[0]
	if len(pw.IndexUpdates) != 1 {
		t.Fatalf("pw.IndexUpdates=%v, want 1 entry (one non-unique index)", pw.IndexUpdates)
	}
	upd := pw.IndexUpdates[0]
	if !upd.IsDelete {
		t.Fatal("IndexUpdates[0].IsDelete=false, want true")
	}
	if upd.IndexName != "delbuf_idx_code" {
		t.Fatalf("IndexUpdates[0].IndexName=%q, want delbuf_idx_code", upd.IndexName)
	}
	// Non-unique compound key: "S:alpha\x00<pk>"
	wantKey := "S:alpha\x00" + string(key)
	if upd.Key != wantKey {
		t.Fatalf("IndexUpdates[0].Key=%q, want %q", upd.Key, wantKey)
	}
}
