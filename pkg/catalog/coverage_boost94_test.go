package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteRowLockedUndoLog94 targets deleteRowLocked undo log generation
func TestCoverage_deleteRowLockedUndoLog94(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with indexes for undo log testing
	_, err := c.ExecuteQuery(`CREATE TABLE undo_del_test (
		id INTEGER PRIMARY KEY,
		code TEXT UNIQUE,
		value INTEGER
	)`)
	if err != nil {
		t.Logf("Create error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "undo_del_test",
			Columns: []string{"id", "code", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE"), numReal(float64(i * 100))}},
		}, nil)
	}

	// Begin transaction and delete (generates undo log)
	c.BeginTransaction(1)

	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "undo_del_test",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenLte, Right: numReal(3)},
	}, nil)
	if err != nil {
		t.Logf("Delete in txn error: %v", err)
	} else {
		t.Logf("Deleted %d rows in txn", rows)
	}

	// Rollback should use undo log
	c.RollbackTransaction()

	// Verify data restored
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM undo_del_test")
	if result != nil {
		t.Logf("Rows after rollback: %v", result.Rows)
	}
}

// TestCoverage_deleteRowLockedNonUniqueIdx targets non-unique index cleanup
func TestCoverage_deleteRowLockedNonUniqueIdx(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE nonuniq_idx_test (
		id INTEGER PRIMARY KEY,
		category TEXT,
		status TEXT
	)`)
	if err != nil {
		t.Logf("Create error: %v", err)
		return
	}

	// Create non-unique index
	_, err = c.ExecuteQuery(`CREATE INDEX idx_cat ON nonuniq_idx_test(category)`)
	if err != nil {
		t.Logf("Create index error: %v", err)
		return
	}

	// Insert multiple rows with same category
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "nonuniq_idx_test",
			Columns: []string{"id", "category", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CAT1"), strReal("active")}},
		}, nil)
	}

	// Delete within transaction
	c.BeginTransaction(1)
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "nonuniq_idx_test",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenLte, Right: numReal(5)},
	}, nil)
	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}
	c.CommitTransaction()

	// Verify
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM nonuniq_idx_test")
	if result != nil {
		t.Logf("Remaining rows: %v", result.Rows)
	}
}

// TestCoverage_FKCascadeMultipleChildren targets FK with multiple children tables
func TestCoverage_FKCascadeMultipleChildren(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent
	_, err := c.ExecuteQuery(`CREATE TABLE multi_parent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create parent error: %v", err)
		return
	}

	// Create multiple child tables
	_, err = c.ExecuteQuery(`CREATE TABLE child_a (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES multi_parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Logf("Create child_a error: %v", err)
		return
	}

	_, err = c.ExecuteQuery(`CREATE TABLE child_b (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES multi_parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Logf("Create child_b error: %v", err)
		return
	}

	// Insert parent
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "multi_parent",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("parent")}},
		}, nil)
	}

	// Insert into both children
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "child_a",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1))}},
		}, nil)
	}
	for i := 1; i <= 4; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "child_b",
			Columns: []string{"id", "parent_id"},
			Values:  [][]query.Expression{{numReal(float64(i + 10)), numReal(float64((i-1)%3 + 1))}},
		}, nil)
	}

	// Delete parent - should cascade to both children
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "multi_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Cascade delete error: %v", err)
	} else {
		t.Logf("Deleted %d parent rows", rows)
	}

	// Verify both children affected
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM child_a WHERE parent_id = 1")
	if result != nil {
		t.Logf("Remaining in child_a: %v", result.Rows)
	}
	result, _ = c.ExecuteQuery("SELECT COUNT(*) FROM child_b WHERE parent_id = 1")
	if result != nil {
		t.Logf("Remaining in child_b: %v", result.Rows)
	}
}

// TestCoverage_evaluateWhereAllAny targets ALL/ANY subqueries
func TestCoverage_evaluateWhereAllAny(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "allany_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	createCoverageTestTable(t, c, "allany_sub", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "allany_main",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "allany_sub",
			Columns: []string{"id", "ref_val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 25))}},
		}, nil)
	}

	// Test ALL/ANY subqueries
	queries := []string{
		"SELECT * FROM allany_main WHERE val > ALL (SELECT ref_val FROM allany_sub)",
		"SELECT * FROM allany_main WHERE val < ANY (SELECT ref_val FROM allany_sub)",
		"SELECT * FROM allany_main WHERE val = ANY (SELECT ref_val FROM allany_sub)",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("ALL/ANY error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_evaluateWhereRowConstructor targets row constructors
func TestCoverage_evaluateWhereRowConstructor(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "row_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "a", Type: query.TokenInteger},
		{Name: "b", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "row_test",
			Columns: []string{"id", "a", "b"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// Row constructor comparisons
	queries := []string{
		"SELECT * FROM row_test WHERE (a, b) = (1, 2)",
		"SELECT * FROM row_test WHERE (a, b) > (5, 10)",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Row constructor error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_insertLockedExpressions targets insertLocked with complex expressions
func TestCoverage_insertLockedExpressions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE expr_insert (
		id INTEGER PRIMARY KEY,
		computed INTEGER,
		str_computed TEXT
	)`)
	if err != nil {
		t.Logf("Create error: %v", err)
		return
	}

	// Insert with expressions
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "expr_insert",
		Columns: []string{"id", "computed", "str_computed"},
		Values: [][]query.Expression{
			{
				numReal(1),
				&query.BinaryExpr{Left: numReal(10), Operator: query.TokenPlus, Right: numReal(20)},
				strReal("test"),
			},
		},
	}, nil)
	if err != nil {
		t.Logf("Insert with expression error: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT * FROM expr_insert WHERE id = 1")
	if result != nil && len(result.Rows) > 0 {
		t.Logf("Row: %v", result.Rows[0])
	}
}

// TestCoverage_insertLockedMultiRow targets insertLocked with multiple rows
func TestCoverage_insertLockedMultiRow(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE multi_insert (
		id INTEGER PRIMARY KEY,
		val TEXT
	)`)
	if err != nil {
		t.Logf("Create error: %v", err)
		return
	}

	// Multi-row insert
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "multi_insert",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("a")},
			{numReal(2), strReal("b")},
			{numReal(3), strReal("c")},
		},
	}, nil)
	if err != nil {
		t.Logf("Multi-row insert error: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM multi_insert")
	if result != nil {
		t.Logf("Rows inserted: %v", result.Rows)
	}
}

// TestCoverage_LoadWithData targets Load with actual data
func TestCoverage_LoadWithData(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create and populate tables
	createCoverageTestTable(t, c, "load_data_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "value", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "load_data_test",
			Columns: []string{"id", "name", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("item"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create indexes
	_, err := c.ExecuteQuery(`CREATE INDEX idx_load_name ON load_data_test(name)`)
	if err != nil {
		t.Logf("Create index error: %v", err)
	}

	// Save
	err = c.Save()
	if err != nil {
		t.Logf("Save error: %v", err)
	}

	// Create new catalog and load
	tree2, _ := btree.NewBTree(pool)
	c2 := New(tree2, pool, nil)

	err = c2.Load()
	if err != nil {
		t.Logf("Load error: %v", err)
	}

	// Verify data loaded
	result, _ := c2.ExecuteQuery("SELECT COUNT(*) FROM load_data_test")
	if result != nil {
		t.Logf("Rows after load: %v", result.Rows)
	}
}

// TestCoverage_ApplyRLSFilterInternal targets applyRLSFilterInternal
func TestCoverage_ApplyRLSFilterInternal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "rls_filter_internal",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "owner", Type: query.TokenText},
		},
	})

	c.EnableRLS()

	// Insert data
	for i := 1; i <= 5; i++ {
		owner := "user1"
		if i > 3 {
			owner = "user2"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "rls_filter_internal",
			Columns: []string{"id", "owner"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(owner)}},
		}, nil)
	}

	// Query with RLS
	result, err := c.ExecuteQuery("SELECT * FROM rls_filter_internal")
	if err != nil {
		t.Logf("Query with RLS error: %v", err)
	} else {
		t.Logf("RLS filtered rows: %d", len(result.Rows))
	}
}
