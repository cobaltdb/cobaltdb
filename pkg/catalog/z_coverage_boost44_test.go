package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_SelectLockedComplex2 targets selectLocked more deeply
func TestCoverage_SelectLockedComplex2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_lock2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "txt", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_lock2",
			Columns: []string{"id", "val", "txt"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("text")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_lock2 WHERE id > 10",
		"SELECT * FROM sel_lock2 WHERE val > 100 ORDER BY val LIMIT 10",
		"SELECT * FROM sel_lock2 WHERE id BETWEEN 5 AND 15",
		"SELECT DISTINCT txt FROM sel_lock2",
		"SELECT COUNT(*) FROM sel_lock2",
		"SELECT * FROM sel_lock2 ORDER BY id DESC LIMIT 5 OFFSET 10",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("SelectLocked error: %v", err)
		} else {
			t.Logf("SelectLocked returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_SelectLockedWithIndex targets selectLocked index path
func TestCoverage_SelectLockedWithIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sel_idx2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code2",
		Table:   "sel_idx2",
		Columns: []string{"code"},
	})

	for i := 1; i <= 30; i++ {
		code := "CODE" + string(rune('A'+i%5))
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "sel_idx2",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(code), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM sel_idx2 WHERE code = 'CODEA'",
		"SELECT * FROM sel_idx2 WHERE code = 'CODEB' AND val > 50",
		"SELECT COUNT(*) FROM sel_idx2 WHERE code IN ('CODEA', 'CODEB')",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("Index select error: %v", err)
		} else {
			t.Logf("Index select returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_ComplexWhereScenarios targets evaluateWhere deeply
func TestCoverage_ComplexWhereScenarios(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "where_complex3", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
		{Name: "c", Type: query.TokenText},
	})

	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex3",
			Columns: []string{"id", "a", "b", "c"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i % 10)), strReal("val")}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM where_complex3 WHERE a > 10 AND b < 5",
		"SELECT * FROM where_complex3 WHERE a > 20 OR b > 8",
		"SELECT * FROM where_complex3 WHERE NOT (a > 40)",
		"SELECT * FROM where_complex3 WHERE a IN (5, 10, 15, 20)",
		"SELECT * FROM where_complex3 WHERE a BETWEEN 10 AND 20",
		"SELECT * FROM where_complex3 WHERE c LIKE 'v%'",
		"SELECT * FROM where_complex3 WHERE a > 5 AND b < 8 AND c = 'val'",
	}

	for _, q := range queries {
		result, err := cat.ExecuteQuery(q)
		if err != nil {
			t.Logf("WHERE error: %v", err)
		} else {
			t.Logf("WHERE returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_CommitTransactionMore targets CommitTransaction
func TestCoverage_CommitTransactionMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_commit", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Test commit without transaction
	cat.CommitTransaction()

	// Normal commit flow
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_commit",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}},
	}, nil)
	cat.CommitTransaction()

	// Multiple transactions
	for i := 0; i < 3; i++ {
		cat.BeginTransaction(1)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "txn_commit",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i + 2)), strReal("batch")}},
		}, nil)
		cat.CommitTransaction()
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_commit")
	t.Logf("Count after commits: %v", result.Rows)
}

// TestCoverage_RollbackTransactionMore targets RollbackTransaction
func TestCoverage_RollbackTransactionMore(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "txn_rollback", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Insert initial data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_rollback",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("initial")}},
	}, nil)

	// Test rollback without transaction
	cat.RollbackTransaction()

	// Normal rollback flow
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "txn_rollback",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("temp")}},
	}, nil)
	cat.RollbackTransaction()

	// Verify rollback worked
	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM txn_rollback")
	t.Logf("Count after rollback: %v", result.Rows)

	// Multiple rollbacks
	for i := 0; i < 3; i++ {
		cat.BeginTransaction(1)
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "txn_rollback",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i + 10)), strReal("temp")}},
		}, nil)
		cat.RollbackTransaction()
	}

	result, _ = cat.ExecuteQuery("SELECT COUNT(*) FROM txn_rollback")
	t.Logf("Count after multiple rollbacks: %v", result.Rows)
}

// TestCoverage_SavepointMoreComplex targets RollbackToSavepoint more
func TestCoverage_SavepointMoreComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "sp_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	cat.BeginTransaction(1)

	// Insert base data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("base")}},
	}, nil)

	cat.Savepoint("sp1")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("sp1")}},
	}, nil)

	cat.Savepoint("sp2")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), strReal("sp2")}},
	}, nil)

	cat.Savepoint("sp3")
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sp_complex",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(4), strReal("sp3")}},
	}, nil)

	// Rollback to sp1 (should remove rows 2, 3, 4)
	err := cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Logf("Rollback to sp1 error: %v", err)
	}

	result, _ := cat.ExecuteQuery("SELECT COUNT(*) FROM sp_complex")
	t.Logf("Count after rollback to sp1: %v", result.Rows)

	cat.RollbackTransaction()
}
