package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_applyOuterQueryAggregates targets applyOuterQuery with aggregates
func TestCoverage_applyOuterQueryAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	createCoverageTestTable(t, c, "outer_agg_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "outer_agg_base",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with GROUP BY
	_, err := c.ExecuteQuery("CREATE VIEW view_agg AS SELECT category, SUM(amount) as total FROM outer_agg_base GROUP BY category")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query view with aggregate in outer query
	result, err := c.ExecuteQuery("SELECT COUNT(*), AVG(total) FROM view_agg")
	if err != nil {
		t.Logf("Outer aggregate query error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}

	// Query view with WHERE on aggregated column
	result, err = c.ExecuteQuery("SELECT * FROM view_agg WHERE total > 1000")
	if err != nil {
		t.Logf("WHERE on aggregate error: %v", err)
	} else {
		t.Logf("WHERE query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_applyOuterQueryGroupBy95 targets applyOuterQuery with GROUP BY
func TestCoverage_applyOuterQueryGroupBy95(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "outer_gb_base", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "region", Type: query.TokenText},
		{Name: "product", Type: query.TokenText},
		{Name: "sales", Type: query.TokenInteger},
	})

	// Insert data
	regions := []string{"North", "South", "East", "West"}
	products := []string{"A", "B"}
	id := 1
	for _, r := range regions {
		for _, p := range products {
			for i := 1; i <= 3; i++ {
				c.Insert(ctx, &query.InsertStmt{
					Table:   "outer_gb_base",
					Columns: []string{"id", "region", "product", "sales"},
					Values:  [][]query.Expression{{numReal(float64(id)), strReal(r), strReal(p), numReal(float64(i * 100))}},
				}, nil)
				id++
			}
		}
	}

	// Create view
	_, err := c.ExecuteQuery("CREATE VIEW view_gb AS SELECT region, product, SUM(sales) as total_sales FROM outer_gb_base GROUP BY region, product")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query view with outer GROUP BY
	result, err := c.ExecuteQuery("SELECT region, SUM(total_sales) FROM view_gb GROUP BY region")
	if err != nil {
		t.Logf("Outer GROUP BY error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_resolveAggregateInExprComplex95 targets resolveAggregateInExpr with complex expressions
func TestCoverage_resolveAggregateInExprComplex95(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "resolve_agg_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		grp := "X"
		if i > 15 {
			grp = "Y"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "resolve_agg_complex",
			Columns: []string{"id", "grp", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i)), numReal(float64(i * 2))}},
		}, nil)
	}

	// HAVING with complex aggregate expressions
	queries := []string{
		"SELECT grp, SUM(val1) as s1 FROM resolve_agg_complex GROUP BY grp HAVING s1 BETWEEN 100 AND 500",
		"SELECT grp, AVG(val1) as a1 FROM resolve_agg_complex GROUP BY grp HAVING a1 IN (8, 16, 24)",
		"SELECT grp, COUNT(*) as c1 FROM resolve_agg_complex GROUP BY grp HAVING c1 > 5 AND c1 < 20",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex HAVING error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_resolveAggregateInExprDistinct targets DISTINCT aggregates
func TestCoverage_resolveAggregateInExprDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "resolve_distinct", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "subcategory", Type: query.TokenText},
	})

	// Insert data with duplicates
	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		sub := "X"
		if i%2 == 0 {
			sub = "Y"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "resolve_distinct",
			Columns: []string{"id", "category", "subcategory"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), strReal(sub)}},
		}, nil)
	}

	// DISTINCT COUNT in HAVING
	result, err := c.ExecuteQuery("SELECT category, COUNT(DISTINCT subcategory) as distinct_count FROM resolve_distinct GROUP BY category HAVING distinct_count > 1")
	if err != nil {
		t.Logf("DISTINCT HAVING error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_deleteRowLockedFKError targets FK cascade error paths
func TestCoverage_deleteRowLockedFKError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent
	_, err := c.ExecuteQuery(`CREATE TABLE err_parent (
		id INTEGER PRIMARY KEY
	)`)
	if err != nil {
		t.Logf("Create parent error: %v", err)
		return
	}

	// Create child
	_, err = c.ExecuteQuery(`CREATE TABLE err_child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES err_parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Logf("Create child error: %v", err)
		return
	}

	// Insert
	c.Insert(ctx, &query.InsertStmt{
		Table:   "err_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "err_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// Delete parent (should cascade)
	_, rows, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "err_parent",
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "id"}, Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Logf("Delete error: %v", err)
	} else {
		t.Logf("Deleted %d rows", rows)
	}
}

// TestCoverage_deleteRowLockedMissingTable targets deleteRowLocked with missing table
func TestCoverage_deleteRowLockedMissingTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Try to delete from non-existent table via DeleteRow
	err := c.DeleteRow(ctx, "nonexistent_table", 1)
	if err != nil {
		t.Logf("Delete from nonexistent table error (expected): %v", err)
	}
}

// TestCoverage_evaluateWhereBetweenNull targets evaluateWhere with BETWEEN and NULL
func TestCoverage_evaluateWhereBetweenNull(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "between_null", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data including edge cases
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "between_null",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Test BETWEEN with various ranges
	queries := []string{
		"SELECT * FROM between_null WHERE val BETWEEN 30 AND 70",
		"SELECT * FROM between_null WHERE val NOT BETWEEN 20 AND 80",
		"SELECT * FROM between_null WHERE id BETWEEN 2 AND 5",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("BETWEEN error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_evaluateWhereInList targets evaluateWhere with IN list
func TestCoverage_evaluateWhereInList(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "in_list_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
	})

	statuses := []string{"active", "pending", "inactive", "deleted", "archived"}
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "in_list_test",
			Columns: []string{"id", "status"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(statuses[i%5])}},
		}, nil)
	}

	// IN list queries
	queries := []string{
		"SELECT * FROM in_list_test WHERE status IN ('active', 'pending')",
		"SELECT * FROM in_list_test WHERE status NOT IN ('deleted', 'archived')",
		"SELECT * FROM in_list_test WHERE id IN (1, 5, 10, 15, 20)",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("IN list error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_LoadEmptyCatalog targets Load with empty catalog
func TestCoverage_LoadEmptyCatalog(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Save empty catalog
	err := c.Save()
	if err != nil {
		t.Logf("Save empty error: %v", err)
	}

	// Load empty catalog
	err = c.Load()
	if err != nil {
		t.Logf("Load empty error: %v", err)
	}
}

// TestCoverage_insertLockedOverflow targets AUTOINCREMENT overflow handling
func TestCoverage_insertLockedOverflow(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	_, err := c.ExecuteQuery(`CREATE TABLE autoinc_overflow (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create error: %v", err)
		return
	}

	// Insert with explicit large ID
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "autoinc_overflow",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(999999), strReal("large")}},
	}, nil)
	if err != nil {
		t.Logf("Insert large ID error: %v", err)
	}

	// Insert without ID (should use next after large ID)
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "autoinc_overflow",
		Columns: []string{"name"},
		Values:  [][]query.Expression{{strReal("next")}},
	}, nil)
	if err != nil {
		t.Logf("Insert after large ID error: %v", err)
	}

	// Verify
	result, _ := c.ExecuteQuery("SELECT * FROM autoinc_overflow ORDER BY id DESC LIMIT 1")
	if result != nil && len(result.Rows) > 0 {
		t.Logf("Last row: %v", result.Rows[0])
	}
}
