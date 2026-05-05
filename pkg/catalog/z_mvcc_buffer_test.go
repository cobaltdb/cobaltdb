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

	// Before commit, the row should NOT be visible in the B-tree.
	r, err := c.ExecuteQuery("SELECT * FROM buf_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 0 {
		t.Fatalf("expected 0 rows before commit, got %d", len(r.Rows))
	}

	// Commit should apply buffered writes.
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}

	// After commit, the row should be visible.
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

// TestBufferedInsert_FallsBackWithIndex verifies that tables with secondary
// indexes still use the direct mutation path (buffered mode disabled).
func TestBufferedInsert_FallsBackWithIndex(t *testing.T) {
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

	// With secondary indexes, buffered mode is disabled; row is visible immediately.
	r, err := c.ExecuteQuery("SELECT * FROM idx_t WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(r.Rows) != 1 {
		t.Fatalf("expected 1 row (direct path), got %d", len(r.Rows))
	}

	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}
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
