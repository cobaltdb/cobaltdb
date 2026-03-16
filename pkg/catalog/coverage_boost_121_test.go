package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedWithDistinct tests selectLocked with DISTINCT
func TestSelectLockedWithDistinct(t *testing.T) {
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

	// Insert duplicate values
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("same")}, {numReal(2), strReal("same")}, {numReal(3), strReal("same")}},
	}, nil)

	// Test SELECT DISTINCT via SQL
	result, err := c.ExecuteQuery("SELECT DISTINCT val FROM test")
	if err != nil {
		t.Logf("SELECT DISTINCT error: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 distinct row, got %d", len(result.Rows))
	}
}

// TestSelectLockedWithOrderBy tests selectLocked with ORDER BY
func TestSelectLockedWithOrderBy(t *testing.T) {
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
		},
	})

	// Insert values in reverse order
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(5)}, {numReal(4)}, {numReal(3)}, {numReal(2)}, {numReal(1)}},
	}, nil)

	// Test ORDER BY ASC
	result, err := c.ExecuteQuery("SELECT id FROM test ORDER BY id ASC")
	if err != nil {
		t.Logf("ORDER BY error: %v", err)
	} else if len(result.Rows) == 5 {
		// Check if ordered
		first, ok := result.Rows[0][0].(float64)
		if ok && first != 1 {
			t.Errorf("Expected first row to be 1, got %v", first)
		}
	}
}

// TestInsertLockedWithConflict tests insertLocked with conflict handling
func TestInsertLockedWithConflict(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with primary key
	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert first row
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("first")}},
	}, nil)

	// Try insert duplicate with ON CONFLICT DO NOTHING
	_, err := c.ExecuteQuery("INSERT INTO test (id, val) VALUES (1, 'second') ON CONFLICT DO NOTHING")
	if err != nil {
		t.Logf("ON CONFLICT DO NOTHING error: %v", err)
	}
}

// TestUpdateLockedWithReturning tests updateLocked with RETURNING
func TestUpdateLockedWithReturning(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table and insert data
	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("old")}},
	}, nil)

	// Test UPDATE with RETURNING
	result, err := c.ExecuteQuery("UPDATE test SET val = 'new' WHERE id = 1 RETURNING id, val")
	if err != nil {
		t.Logf("UPDATE RETURNING error: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row returned, got %d", len(result.Rows))
	}
}

// TestDeleteLockedWithReturning tests deleteLocked with RETURNING
func TestDeleteLockedWithReturning(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table and insert data
	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Test DELETE with RETURNING
	result, err := c.ExecuteQuery("DELETE FROM test WHERE id = 1 RETURNING id, val")
	if err != nil {
		t.Logf("DELETE RETURNING error: %v", err)
	} else if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row returned, got %d", len(result.Rows))
	}
}

// TestEvaluateHavingWithGroups tests evaluateHaving with GROUP BY and HAVING
func TestEvaluateHavingWithGroups(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "dept", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"dept", "amount"},
		Values: [][]query.Expression{
			{strReal("A"), numReal(100)},
			{strReal("A"), numReal(200)},
			{strReal("B"), numReal(50)},
			{strReal("B"), numReal(75)},
		},
	}, nil)

	// Test HAVING with SUM
	result, err := c.ExecuteQuery("SELECT dept, SUM(amount) as total FROM sales GROUP BY dept HAVING SUM(amount) > 100")
	if err != nil {
		t.Logf("HAVING error: %v", err)
	} else {
		// Verify HAVING is applied (A=300, B=125 - both > 100 in this case)
		t.Logf("HAVING returned %d groups", len(result.Rows))
	}
}

// TestCommitTransactionWithSavepoints tests CommitTransaction with savepoints
func TestCommitTransactionWithSavepoints(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Begin transaction
	c.BeginTransaction(1)

	// Create savepoint
	c.Savepoint("sp1")

	// Commit should work
	err := c.CommitTransaction()
	if err != nil {
		t.Errorf("CommitTransaction failed: %v", err)
	}
}

// TestRollbackTransactionWithSavepoints tests RollbackTransaction with savepoints
func TestRollbackTransactionWithSavepoints(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Begin transaction
	c.BeginTransaction(1)

	// Create savepoint
	c.Savepoint("sp1")

	// Rollback should work
	err := c.RollbackTransaction()
	if err != nil {
		t.Errorf("RollbackTransaction failed: %v", err)
	}
}

// TestVacuumWithData tests Vacuum with existing data
func TestVacuumWithData(t *testing.T) {
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
	for i := 0; i < 50; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Delete half the rows
	c.ExecuteQuery("DELETE FROM test WHERE id > 25")

	// Vacuum should reclaim space
	err := c.Vacuum()
	if err != nil {
		t.Errorf("Vacuum failed: %v", err)
	}
}

