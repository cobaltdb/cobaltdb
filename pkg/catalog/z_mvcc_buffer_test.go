package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"github.com/cobaltdb/cobaltdb/pkg/txn"
)

// createCatalogWithTxnManager creates a Catalog wired to a txn.Manager for
// testing the MVCC buffered-write path.
func createCatalogWithTxnManager(t *testing.T) (*Catalog, *txn.Manager) {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	catalogTree, _ := btree.NewBTree(pool)
	c := &Catalog{
		tables:            make(map[string]*TableDef),
		tableTrees:        make(map[string]*btree.BTree),
		indexes:           make(map[string]*IndexDef),
		indexTrees:        make(map[string]*btree.BTree),
		tree:              catalogTree,
		pool:              pool,
		views:             make(map[string]*query.SelectStmt),
		triggers:          make(map[string]*query.CreateTriggerStmt),
		procedures:        make(map[string]*query.CreateProcedureStmt),
		materializedViews: make(map[string]*MaterializedViewDef),
		ftsIndexes:        make(map[string]*FTSIndexDef),
		jsonIndexes:       make(map[string]*JSONIndexDef),
		vectorIndexes:     make(map[string]*VectorIndexDef),
		stats:             make(map[string]*StatsTableStats),
		deadTuples:        make(map[string]int64),
		liveTuples:        make(map[string]int64),
		activeTxns:        make(map[uint64]*catalogTxnState),
	}
	mgr := txn.NewManager(pool, nil)
	c.SetTxnManager(mgr)
	c.EnableBufferedWrites()
	return c, mgr
}

// TestBufferedInsert_Basic verifies that INSERT in buffered mode defers B-tree
// mutation until commit time.
func TestBufferedInsert_Basic(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE buf_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("INSERT INTO buf_t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Before commit, the row SHOULD be visible to the writing transaction
	// (read-your-writes).
	r, err := c.ExecuteQuery("SELECT * FROM buf_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row before commit (read-your-writes), got %d", len(r.Rows))
	}

	// Commit should apply buffered writes.
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// After commit, the row should still be visible.
	r, err = c.ExecuteQuery("SELECT * FROM buf_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after commit, got %d", len(r.Rows))
	}
}

// TestBufferedInsert_Rollback verifies that ROLLBACK discards buffered writes.
func TestBufferedInsert_Rollback(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE rb_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("INSERT INTO rb_t VALUES (1, 'before')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	r, err := c.ExecuteQuery("SELECT * FROM rb_t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows after rollback, got %d", len(r.Rows))
	}
}

// TestBufferedInsert_Savepoint verifies that ROLLBACK TO SAVEPOINT truncates
// buffered writes correctly.
func TestBufferedInsert_Savepoint(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE sp_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("INSERT INTO sp_t VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("INSERT 1 failed: %v", err)
	}
	if err := c.Savepoint("s1"); err != nil {
		t.Fatalf("SAVEPOINT failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO sp_t VALUES (2, 'b')")
	if err != nil {
		t.Fatalf("INSERT 2 failed: %v", err)
	}
	if err := c.RollbackToSavepoint("s1"); err != nil {
		t.Fatalf("ROLLBACK TO SAVEPOINT failed: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	r, err := c.ExecuteQuery("SELECT COUNT(*) FROM sp_t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 result row, got %d", len(r.Rows))
	}
	cnt, ok := toInt64(r.Rows[0][0])
	if !ok || cnt != 1 {
		t.Fatalf("expected count=1, got %v", r.Rows[0][0])
	}
}

// TestBufferedInsert_WithIndex verifies that buffered INSERT works correctly
// when the table has secondary indexes (index mutations are also deferred).
func TestBufferedInsert_WithIndex(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE idx_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE UNIQUE INDEX idx_t_val ON idx_t(val)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("INSERT INTO idx_t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	// Row should be visible via read-your-writes (full scan, since index is stale).
	r, err := c.ExecuteQuery("SELECT * FROM idx_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(r.Rows))
	}

	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// After commit, index scan should work.
	r, err = c.ExecuteQuery("SELECT * FROM idx_t WHERE val = 'hello'")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after commit, got %d", len(r.Rows))
	}
}

// TestBufferedInsert_UniqueIndexConflict verifies that UNIQUE constraint
// violations are caught in buffered mode before commit.
func TestBufferedInsert_UniqueIndexConflict(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE uniq_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE UNIQUE INDEX uniq_t_val ON uniq_t(val)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO uniq_t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("INSERT INTO uniq_t VALUES (2, 'hello')")
	if err == nil {
		t.Fatalf("expected UNIQUE constraint error, got nil")
	}
	c.RollbackTransaction()
}

func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

// TestBufferedUpdate_Basic verifies that UPDATE in buffered mode defers B-tree
// mutation until commit time.
func TestBufferedUpdate_Basic(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE upd_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO upd_t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("UPDATE upd_t SET val = 'world' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	// Before commit, the updated row should be visible (read-your-writes).
	r, err := c.ExecuteQuery("SELECT val FROM upd_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row before commit, got %d", len(r.Rows))
	}
	if r.Rows[0][0] != "world" {
		t.Fatalf("expected val='world' before commit, got %v", r.Rows[0][0])
	}

	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// After commit, the update should persist.
	r, err = c.ExecuteQuery("SELECT val FROM upd_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after commit, got %d", len(r.Rows))
	}
	if r.Rows[0][0] != "world" {
		t.Fatalf("expected val='world' after commit, got %v", r.Rows[0][0])
	}
}

// TestBufferedUpdate_Rollback verifies that ROLLBACK discards buffered updates.
func TestBufferedUpdate_Rollback(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE upd_rb (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO upd_rb VALUES (1, 'before')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("UPDATE upd_rb SET val = 'after' WHERE id = 1")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	r, err := c.ExecuteQuery("SELECT val FROM upd_rb WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", len(r.Rows))
	}
	if r.Rows[0][0] != "before" {
		t.Fatalf("expected val='before' after rollback, got %v", r.Rows[0][0])
	}
}

// TestBufferedDelete_Basic verifies that DELETE in buffered mode defers B-tree
// mutation until commit time.
func TestBufferedDelete_Basic(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE del_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO del_t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("DELETE FROM del_t WHERE id = 1")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Before commit, the row should NOT be visible (read-your-writes sees delete).
	r, err := c.ExecuteQuery("SELECT * FROM del_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows before commit (buffered delete), got %d", len(r.Rows))
	}

	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// After commit, the row should still be gone.
	r, err = c.ExecuteQuery("SELECT * FROM del_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows after commit, got %d", len(r.Rows))
	}
}

// TestBufferedDelete_Rollback verifies that ROLLBACK discards buffered deletes.
func TestBufferedDelete_Rollback(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE del_rb (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO del_rb VALUES (1, 'before')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("DELETE FROM del_rb WHERE id = 1")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}

	r, err := c.ExecuteQuery("SELECT * FROM del_rb WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row after rollback, got %d", len(r.Rows))
	}
}

// TestBufferedDelete_WithIndex verifies that buffered DELETE works correctly
// when the table has secondary indexes (index deletions are also deferred).
func TestBufferedDelete_WithIndex(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE del_idx_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("CREATE UNIQUE INDEX del_idx_t_val ON del_idx_t(val)")
	if err != nil {
		t.Fatalf("CREATE INDEX failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO del_idx_t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("DELETE FROM del_idx_t WHERE id = 1")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Delete is buffered; row should not be visible via read-your-writes.
	r, err := c.ExecuteQuery("SELECT * FROM del_idx_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(r.Rows))
	}

	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// After commit, index scan should also return nothing.
	r, err = c.ExecuteQuery("SELECT * FROM del_idx_t WHERE val = 'hello'")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows after commit, got %d", len(r.Rows))
	}
}

// TestBufferedMixed_DML verifies INSERT, UPDATE, and DELETE in the same
// buffered transaction with read-your-writes visibility.
func TestBufferedMixed_DML(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	_, err := c.ExecuteQuery("CREATE TABLE mix_t (id INTEGER PRIMARY KEY, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO mix_t VALUES (1, 'a')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	_, err = c.ExecuteQuery("INSERT INTO mix_t VALUES (2, 'b')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	c.BeginTransaction(1)
	_, err = c.ExecuteQuery("INSERT INTO mix_t VALUES (3, 'c')")
	if err != nil {
		t.Fatalf("INSERT 3 failed: %v", err)
	}
	_, err = c.ExecuteQuery("UPDATE mix_t SET val = 'bb' WHERE id = 2")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	_, err = c.ExecuteQuery("DELETE FROM mix_t WHERE id = 1")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	// Full scan should see: id=2 val='bb', id=3 val='c' (id=1 deleted).
	r, err := c.ExecuteQuery("SELECT id, val FROM mix_t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(r.Rows))
	}
	id0, _ := toInt64(r.Rows[0][0])
	id1, _ := toInt64(r.Rows[1][0])
	if id0 != 2 || r.Rows[0][1] != "bb" {
		t.Fatalf("expected row 2='bb', got %v %v", r.Rows[0][0], r.Rows[0][1])
	}
	if id1 != 3 || r.Rows[1][1] != "c" {
		t.Fatalf("expected row 3='c', got %v %v", r.Rows[1][0], r.Rows[1][1])
	}

	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// After commit, state should be the same.
	r, err = c.ExecuteQuery("SELECT id, val FROM mix_t ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT after commit failed: %v", err)
	}
	if len(r.Rows) != 2 {
		t.Fatalf("expected 2 rows after commit, got %d", len(r.Rows))
	}
}
