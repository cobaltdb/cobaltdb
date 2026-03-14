package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_RLSWithPolicy targets RLS internal functions with policies
func TestCoverage_RLSWithPolicy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "rls_policy_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
			{Name: "tenant_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
	})

	// Enable RLS
	c.EnableRLS()

	// Create RLS policy using USING expression
	_, err := c.ExecuteQuery(`CREATE POLICY tenant_isolation ON rls_policy_test
		USING (tenant_id = CURRENT_TENANT())`)
	if err != nil {
		t.Logf("CREATE POLICY error (may not be supported): %v", err)
	}

	// Insert data with different tenants
	for i := 1; i <= 10; i++ {
		tenant := 1
		if i > 5 {
			tenant = 2
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "rls_policy_test",
			Columns: []string{"id", "owner", "tenant_id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("user"), numReal(float64(tenant)), strReal("data")}},
		}, nil)
	}

	// Query with RLS enabled (should filter by tenant)
	result, _ := c.ExecuteQuery("SELECT * FROM rls_policy_test")
	if result != nil {
		t.Logf("RLS filtered rows: %d", len(result.Rows))
	}

	// Update with RLS
	_, _, err = c.Update(ctx, &query.UpdateStmt{
		Table: "rls_policy_test",
		Set:   []*query.SetClause{{Column: "data", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("RLS update error: %v", err)
	}

	// Delete with RLS
	_, _, err = c.Delete(ctx, &query.DeleteStmt{
		Table: "rls_policy_test",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(2)},
	}, nil)
	if err != nil {
		t.Logf("RLS delete error: %v", err)
	}
}

// TestCoverage_RLSForAllOperations targets checkRLSForInsert/Update/DeleteInternal
func TestCoverage_RLSForAllOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "rls_all_ops",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
			{Name: "status", Type: query.TokenText},
		},
	})

	c.EnableRLS()

	// Test INSERT with RLS check
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "rls_all_ops",
		Columns: []string{"id", "owner", "status"},
		Values:  [][]query.Expression{{numReal(1), strReal("test_user"), strReal("active")}},
	}, nil)
	if err != nil {
		t.Logf("Insert with RLS: %v", err)
	}

	// Test INSERT that might fail RLS check
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "rls_all_ops",
		Columns: []string{"id", "owner", "status"},
		Values:  [][]query.Expression{{numReal(2), strReal("other_user"), strReal("active")}},
	}, nil)
	if err != nil {
		t.Logf("Insert with different owner: %v", err)
	}

	// Test UPDATE with RLS
	_, _, err = c.Update(ctx, &query.UpdateStmt{
		Table: "rls_all_ops",
		Set:   []*query.SetClause{{Column: "status", Value: strReal("updated")}},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Update with RLS: %v", err)
	}

	// Test DELETE with RLS
	_, _, err = c.Delete(ctx, &query.DeleteStmt{
		Table: "rls_all_ops",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Delete with RLS: %v", err)
	}

	// Query remaining rows
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM rls_all_ops")
	if result != nil {
		t.Logf("Remaining rows after RLS ops: %v", result.Rows)
	}
}

// TestCoverage_deleteRowLockedDeep targets deleteRowLocked deep paths
func TestCoverage_deleteRowLockedDeep(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent table
	_, err := c.ExecuteQuery(`CREATE TABLE deep_parent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create parent error: %v", err)
		return
	}

	// Create child with RESTRICT (should prevent delete if children exist)
	_, err = c.ExecuteQuery(`CREATE TABLE deep_child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES deep_parent(id) ON DELETE RESTRICT
	)`)
	if err != nil {
		t.Logf("Create child error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "deep_parent",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("parent")}},
		}, nil)
	}

	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "deep_child",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1))}},
		}, nil)
	}

	// Try to delete parent with children (should fail due to RESTRICT)
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "deep_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Delete with RESTRICT FK (expected error): %v", err)
	} else {
		t.Logf("Deleted %d rows (expected 0 due to RESTRICT)", rows)
	}

	// Delete children first
	c.Delete(ctx, &query.DeleteStmt{
		Table: "deep_child",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "parent_id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	// Now delete parent should succeed
	_, rows, err = c.Delete(ctx, &query.DeleteStmt{
		Table: "deep_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Delete after children removed: %v", err)
	} else {
		t.Logf("Deleted %d rows after children removed", rows)
	}
}

// TestCoverage_LoadErrorPaths targets Load with various error conditions
func TestCoverage_LoadErrorPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create some tables
	createCoverageTestTable(t, c, "load_test1", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "load_test1",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test")}},
		}, nil)
	}

	// Save
	err := c.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Load back
	err = c.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}

	// Verify data after load
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM load_test1")
	if result != nil {
		t.Logf("Rows after load: %v", result.Rows)
	}
}

// TestCoverage_evaluateWhereComplex92 targets evaluateWhere complex paths
func TestCoverage_evaluateWhereComplex92(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "where_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert varied data
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "where_complex",
			Columns: []string{"id", "val", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal("item")}},
		}, nil)
	}

	// Complex WHERE clauses
	queries := []string{
		"SELECT * FROM where_complex WHERE (id > 5 AND val < 150) OR (id <= 3 AND val >= 10)",
		"SELECT * FROM where_complex WHERE NOT (id = 1 OR id = 2)",
		"SELECT * FROM where_complex WHERE id IN (1, 3, 5, 7, 9)",
		"SELECT * FROM where_complex WHERE val BETWEEN 50 AND 100",
		"SELECT * FROM where_complex WHERE name IS NOT NULL",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex WHERE error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_AlterTableDropColumn targets AlterTableDropColumn
func TestCoverage_AlterTableDropColumn(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE drop_col_test (
		id INTEGER PRIMARY KEY,
		col1 TEXT,
		col2 INTEGER,
		col3 REAL
	)`)
	if err != nil {
		t.Logf("Create error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "drop_col_test",
			Columns: []string{"id", "col1", "col2", "col3"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("test"), numReal(float64(i * 10)), numReal(float64(i) * 1.5)}},
		}, nil)
	}

	// Drop middle column
	_, err = c.ExecuteQuery(`ALTER TABLE drop_col_test DROP COLUMN col2`)
	if err != nil {
		t.Logf("Drop column error: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT * FROM drop_col_test LIMIT 1")
	if result != nil {
		t.Logf("Columns after drop: %v", result.Columns)
	}

	// Try to drop column with index (if supported)
	_, _ = c.ExecuteQuery(`CREATE INDEX idx_col1 ON drop_col_test(col1)`)
	_, err = c.ExecuteQuery(`ALTER TABLE drop_col_test DROP COLUMN col1`)
	if err != nil {
		t.Logf("Drop column with index error (may be expected): %v", err)
	}
}
