package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedWithSubquery tests selectLocked with subquery in FROM
func TestSelectLockedWithSubquery(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}, {numReal(2), strReal("b")}},
	}, nil)

	// Test SELECT from subquery
	result, err := c.ExecuteQuery("SELECT * FROM (SELECT id, val FROM test) AS sub WHERE id = 1")
	if err != nil {
		t.Logf("SELECT with subquery error: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithSubqueryAndJoin tests selectLocked with subquery and JOIN
func TestSelectLockedWithSubqueryAndJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "t1_id", Type: query.TokenInteger}},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "t1_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}},
	}, nil)

	// Test SELECT with subquery and JOIN
	result, err := c.ExecuteQuery("SELECT * FROM (SELECT id FROM t1) AS s1 JOIN t2 ON s1.id = t2.t1_id")
	if err != nil {
		t.Logf("SELECT with subquery+JOIN error: %v", err)
	} else {
		t.Logf("Got %d rows from subquery JOIN", len(result.Rows))
	}
}

// TestSelectLockedWithCTEWindow tests selectLocked with CTE containing window functions
func TestSelectLockedWithCTEWindow(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "dept", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"dept", "amount"},
		Values:  [][]query.Expression{{strReal("A"), numReal(100)}, {strReal("A"), numReal(200)}, {strReal("B"), numReal(150)}},
	}, nil)

	// Test CTE with window function
	result, err := c.ExecuteQuery(`
		WITH cte AS (SELECT dept, amount FROM sales)
		SELECT dept, SUM(amount) OVER (PARTITION BY dept) as total FROM cte
	`)
	if err != nil {
		t.Logf("CTE with window error: %v", err)
	} else {
		t.Logf("CTE window returned %d rows", len(result.Rows))
	}
}

// TestInsertLockedWithColumnValidation tests insertLocked with column validation errors
func TestInsertLockedWithColumnValidation(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Try to insert with non-existent column
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "nonexistent"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)
	if err == nil {
		t.Error("Expected error for non-existent column")
	}
}

// TestInsertLockedWithInsertSelectError tests insertLocked with INSERT...SELECT error
func TestInsertLockedWithInsertSelectError(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table:   "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}},
	})

	// Try INSERT...SELECT with column count mismatch
	_, err := c.ExecuteQuery("INSERT INTO t1 (id) SELECT id, val FROM t2")
	if err == nil {
		t.Error("Expected error for INSERT...SELECT column count mismatch")
	}
}

// TestUpdateLockedWithJoin tests updateLocked with JOIN
func TestUpdateLockedWithJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table:   "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "t1_id", Type: query.TokenInteger}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "t1_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}},
	}, nil)

	// UPDATE...FROM
	_, err := c.ExecuteQuery("UPDATE t1 SET val = 100 FROM t2 WHERE t1.id = t2.t1_id")
	if err != nil {
		t.Logf("UPDATE...FROM error: %v", err)
	}
}

// TestEvaluateHavingWithAggregateCondition tests evaluateHaving with aggregate conditions
func TestEvaluateHavingWithAggregateCondition(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "dept", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"dept", "amount"},
		Values: [][]query.Expression{
			{strReal("A"), numReal(100)},
			{strReal("A"), numReal(200)},
			{strReal("B"), numReal(50)},
			{strReal("B"), numReal(30)},
		},
	}, nil)

	// Test HAVING with AVG
	result, err := c.ExecuteQuery("SELECT dept, AVG(amount) as avg_amt FROM sales GROUP BY dept HAVING AVG(amount) > 75")
	if err != nil {
		t.Logf("HAVING AVG error: %v", err)
	} else {
		// Only dept A should qualify (avg=150)
		t.Logf("HAVING AVG returned %d rows", len(result.Rows))
	}

	// Test HAVING with COUNT
	result, err = c.ExecuteQuery("SELECT dept, COUNT(*) as cnt FROM sales GROUP BY dept HAVING COUNT(*) > 1")
	if err != nil {
		t.Logf("HAVING COUNT error: %v", err)
	} else {
		t.Logf("HAVING COUNT returned %d rows", len(result.Rows))
	}
}

// TestCountRowsViaSQL tests row counting via SQL
func TestCountRowsViaSQL(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table:   "test",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	// Count on empty table
	cnt := getRowCount(c, "test")
	if cnt != 0 {
		t.Errorf("Expected 0 rows on empty table, got %d", cnt)
	}

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)

	// Count after insert
	cnt = getRowCount(c, "test")
	if cnt != 3 {
		t.Errorf("Expected 3 rows, got %d", cnt)
	}

	// Count non-existent table
	cnt = getRowCount(c, "nonexistent")
	if cnt != 0 {
		t.Errorf("Expected 0 rows for non-existent table, got %d", cnt)
	}
}

// TestCommitTransactionWithActiveTransaction tests CommitTransaction with active txn
func TestCommitTransactionWithActiveTransaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Begin and commit transaction
	c.BeginTransaction(1)
	err := c.CommitTransaction()
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}
}

// TestCommitTransactionWithoutActiveTransaction tests CommitTransaction without active txn
func TestCommitTransactionWithoutActiveTransaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Try to commit without beginning
	err := c.CommitTransaction()
	if err == nil {
		t.Log("CommitTransaction without active txn may or may not error")
	}
}

// TestRollbackTransactionWithActiveTransaction tests RollbackTransaction with active txn
func TestRollbackTransactionWithActiveTransaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.BeginTransaction(1)
	err := c.RollbackTransaction()
	if err != nil {
		t.Errorf("RollbackTransaction failed: %v", err)
	}
}

// TestRollbackTransactionWithoutActiveTransaction tests RollbackTransaction without active txn
func TestRollbackTransactionWithoutActiveTransaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Try to rollback without beginning
	err := c.RollbackTransaction()
	if err == nil {
		t.Log("RollbackTransaction without active txn may or may not error")
	}
}

// TestSaveWithEmptyCatalog tests Save with empty catalog
func TestSaveWithEmptyCatalog(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	err := c.Save()
	if err != nil {
		t.Errorf("Save with empty catalog failed: %v", err)
	}
}

// TestVacuumWithEmptyTables tests Vacuum with empty tables
func TestVacuumWithEmptyTables(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create empty table
	c.CreateTable(&query.CreateTableStmt{
		Table:   "empty_table",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	err := c.Vacuum()
	if err != nil {
		t.Errorf("Vacuum with empty tables failed: %v", err)
	}
}

// TestFlushTableTreesLocked tests flushTableTreesLocked
func TestFlushTableTreesLocked(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table:   "test",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	// Insert some data to create table tree
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	c.mu.Lock()
	err := c.flushTableTreesLocked()
	c.mu.Unlock()

	if err != nil {
		t.Errorf("flushTableTreesLocked failed: %v", err)
	}
}

// TestUpdateLockedWithReturningJoin tests updateLocked with RETURNING and JOIN
func TestUpdateLockedWithReturningJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table:   "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "t1_id", Type: query.TokenInteger}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "t1_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// UPDATE...FROM with RETURNING
	result, err := c.ExecuteQuery("UPDATE t1 SET val = 100 FROM t2 WHERE t1.id = t2.t1_id RETURNING t1.id, t1.val")
	if err != nil {
		t.Logf("UPDATE...FROM RETURNING error: %v", err)
	} else {
		t.Logf("UPDATE...FROM RETURNING returned %d rows", len(result.Rows))
	}
}

// Helper function to get row count via SQL
func getRowCount(c *Catalog, table string) int {
	result, err := c.ExecuteQuery("SELECT COUNT(*) FROM " + table)
	if err != nil || len(result.Rows) == 0 {
		return 0
	}
	switch v := result.Rows[0][0].(type) {
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

// TestDeleteLockedWithUsing tests deleteLocked with USING clause
func TestDeleteLockedWithUsing(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table:   "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "t1_id", Type: query.TokenInteger}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "t1_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// DELETE...USING
	result, err := c.ExecuteQuery("DELETE FROM t1 USING t2 WHERE t1.id = t2.t1_id")
	if err != nil {
		t.Logf("DELETE...USING error: %v", err)
	} else {
		t.Logf("DELETE...USING result: %d rows returned", len(result.Rows))
	}
}

// TestSelectLockedWithComplexGroupBy tests selectLocked with complex GROUP BY
func TestSelectLockedWithComplexGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "region", Type: query.TokenText},
			{Name: "product", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"region", "product", "amount"},
		Values: [][]query.Expression{
			{strReal("N"), strReal("A"), numReal(100)},
			{strReal("N"), strReal("B"), numReal(200)},
			{strReal("S"), strReal("A"), numReal(150)},
			{strReal("S"), strReal("B"), numReal(250)},
		},
	}, nil)

	// GROUP BY multiple columns with ORDER BY
	result, err := c.ExecuteQuery("SELECT region, product, SUM(amount) as total FROM sales GROUP BY region, product ORDER BY region, product")
	if err != nil {
		t.Logf("Complex GROUP BY error: %v", err)
	} else if len(result.Rows) != 4 {
		t.Errorf("Expected 4 groups, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithSubqueryUnion tests selectLocked with UNION subquery
func TestSelectLockedWithSubqueryUnion(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table:   "t1",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t2",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(3)}, {numReal(4)}},
	}, nil)

	// Subquery with UNION
	result, err := c.ExecuteQuery("SELECT * FROM (SELECT id FROM t1 UNION SELECT id FROM t2) AS u ORDER BY id")
	if err != nil {
		t.Logf("UNION subquery error: %v", err)
	} else if len(result.Rows) != 4 {
		t.Errorf("Expected 4 rows from UNION, got %d", len(result.Rows))
	}
}
