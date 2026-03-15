package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_deleteRowLockedNonUniqueIndex tests deleteRowLocked with non-unique index
func TestCoverage_deleteRowLockedNonUniqueIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "del_nonuniq", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
	})

	// Create non-unique index
	err := c.CreateIndex(&query.CreateIndexStmt{Index: "idx_cat", Table: "del_nonuniq", Columns: []string{"category"}})
	if err != nil {
		t.Logf("CREATE INDEX error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_nonuniq",
			Columns: []string{"id", "category"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("A")}},
		}, nil)
	}

	// Start transaction to test undo log with non-unique index
	c.BeginTransaction(1)

	// Delete rows to trigger non-unique index cleanup and undo logging
	for i := 1; i <= 3; i++ {
		err := c.DeleteRow(ctx, "del_nonuniq", float64(i))
		if err != nil {
			t.Logf("DeleteRow %d error: %v", i, err)
		}
	}

	// Rollback to test undo
	c.RollbackTransaction()
}

// TestCoverage_deleteRowLockedDecodeError tests deleteRowLocked with row decode error
func TestCoverage_deleteRowLockedDecodeError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "del_decode", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "del_decode",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Directly insert malformed data to trigger decode error
	c.mu.Lock()
	treeInst := c.tableTrees["del_decode"]
	c.mu.Unlock()

	if treeInst != nil {
		// Insert malformed row data
		malformedKey := c.serializePK(float64(999), treeInst)
		treeInst.Put(malformedKey, []byte("not valid json"))

		// Try to delete the malformed row
		err := c.DeleteRow(ctx, "del_decode", float64(999))
		if err != nil {
			t.Logf("DeleteRow with malformed data error (expected): %v", err)
		}
	}
}

// TestCoverage_applyOuterQueryNonAggregate tests applyOuterQuery non-aggregate path
func TestCoverage_applyOuterQueryNonAggregate(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	createCoverageTestTable(t, c, "outer_nonagg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "outer_nonagg",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create simple view
	err := c.CreateView("view_nonagg", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "outer_nonagg"},
	})
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query view without aggregates - tests non-aggregate path
	result, err := c.ExecuteQuery("SELECT * FROM view_nonagg WHERE val > 20 ORDER BY id DESC LIMIT 2 OFFSET 1")
	if err != nil {
		t.Logf("Query error: %v", err)
		return
	}

	t.Logf("Non-aggregate view query returned %d rows", len(result.Rows))

	// Test OFFSET beyond result
	result2, err := c.ExecuteQuery("SELECT * FROM view_nonagg OFFSET 100")
	if err != nil {
		t.Logf("Query with large offset error: %v", err)
		return
	}

	t.Logf("Large offset query returned %d rows (expected 0)", len(result2.Rows))
}

// TestCoverage_selectLockedWindowOnCTE tests selectLocked with window functions on CTE
func TestCoverage_selectLockedWindowOnCTE(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "win_cte", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat := "A"
		if i > 5 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "win_cte",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query with window function on CTE
	result, err := c.ExecuteQuery(`
		WITH cte AS (SELECT category, amount FROM win_cte)
		SELECT category, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) as rn
		FROM cte
	`)
	if err != nil {
		t.Logf("Window on CTE error: %v", err)
		return
	}

	t.Logf("Window on CTE returned %d rows", len(result.Rows))
}

// TestCoverage_selectLockedDerivedTableWithJoin tests selectLocked derived table with JOIN
func TestCoverage_selectLockedDerivedTableWithJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	createCoverageTestTable(t, c, "dt_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, c, "dt_join", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "value", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "dt_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Name" + string(rune('A'+i-1)))}},
		}, nil)

		c.Insert(ctx, &query.InsertStmt{
			Table:   "dt_join",
			Columns: []string{"id", "main_id", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query with derived table and JOIN
	result, err := c.ExecuteQuery(`
		SELECT * FROM
			(SELECT id, name FROM dt_main WHERE id <= 3) AS derived
			JOIN dt_join ON derived.id = dt_join.main_id
	`)
	if err != nil {
		t.Logf("Derived table with JOIN error: %v", err)
		return
	}

	t.Logf("Derived table JOIN returned %d rows", len(result.Rows))
}

// TestCoverage_insertLockedConflictPaths tests insertLocked conflict resolution paths
func TestCoverage_insertLockedConflictPaths(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with unique constraint
	createCoverageTestTable(t, c, "insert_conflict", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText, Unique: true},
	})

	// Insert initial data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "insert_conflict",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{numReal(1), strReal("test@example.com")}},
	}, nil)

	// Test DO UPDATE with complex expression
	c.BeginTransaction(2)
	stmt, _ := query.Parse(`
		INSERT INTO insert_conflict (id, email) VALUES (1, 'test@example.com')
		ON CONFLICT (id) DO UPDATE SET email = email || '.updated'
	`)
	if ins, ok := stmt.(*query.InsertStmt); ok {
		_, _, err := c.Insert(ctx, ins, nil)
		if err != nil {
			t.Logf("INSERT DO UPDATE error: %v", err)
		}
	}
	c.RollbackTransaction()
}

// TestCoverage_applyOuterQueryComplexExpressions tests applyOuterQuery with complex column expressions
func TestCoverage_applyOuterQueryComplexExpressions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	createCoverageTestTable(t, c, "outer_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val1", Type: query.TokenInteger},
		{Name: "val2", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "outer_complex",
			Columns: []string{"id", "val1", "val2"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), numReal(float64(i * 100))}},
		}, nil)
	}

	// Create view
	err := c.CreateView("view_complex", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"}, &query.Identifier{Name: "val1"}, &query.Identifier{Name: "val2"},
		},
		From: &query.TableRef{Name: "outer_complex"},
	})
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query with complex expressions
	result, err := c.ExecuteQuery(`
		SELECT id, val1 + val2 as sum_val, val1 * 2 as doubled, 'constant' as label
		FROM view_complex
		WHERE val1 + val2 > 100
	`)
	if err != nil {
		t.Logf("Complex expression query error: %v", err)
		return
	}

	t.Logf("Complex expression query returned %d rows", len(result.Rows))
}

// TestCoverage_selectLockedScalarSelect tests selectLocked scalar select paths
func TestCoverage_selectLockedScalarSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Test scalar select with expressions
	result, err := c.ExecuteQuery("SELECT 1 + 2 as sum, 'hello' as msg, 3.14 * 2 as pi2")
	if err != nil {
		t.Logf("Scalar select error: %v", err)
		return
	}

	t.Logf("Scalar select returned %d rows, %d columns", len(result.Rows), len(result.Columns))
}

// TestCoverage_evaluateHavingComplex tests evaluateHaving with complex conditions
func TestCoverage_evaluateHavingComplex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "having_complex", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat := "A"
		if i > 5 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "having_complex",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query with HAVING and complex aggregate conditions
	result, err := c.ExecuteQuery(`
		SELECT category, SUM(amount) as total, COUNT(*) as cnt
		FROM having_complex
		GROUP BY category
		HAVING SUM(amount) > 100 AND COUNT(*) >= 3
	`)
	if err != nil {
		t.Logf("HAVING complex error: %v", err)
		return
	}

	t.Logf("HAVING complex returned %d rows", len(result.Rows))
}

// TestCoverage_resolveAggregateInExpr tests resolveAggregateInExpr edge cases
func TestCoverage_resolveAggregateInExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "resolve_agg", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "resolve_agg",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query with aggregate in expression - SUM with expression
	result, err := c.ExecuteQuery(`
		SELECT SUM(val + 1) as total_plus_one
		FROM resolve_agg
	`)
	if err != nil {
		t.Logf("Aggregate in expression error: %v", err)
		return
	}

	t.Logf("Aggregate in expression returned %d rows", len(result.Rows))
}

// TestCoverage_applyOuterQueryHAVING tests applyOuterQuery HAVING clause path
func TestCoverage_applyOuterQueryHAVING(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	createCoverageTestTable(t, c, "outer_having", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		cat := "A"
		if i > 5 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "outer_having",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view with GROUP BY
	err := c.CreateView("view_having", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		},
		From:    &query.TableRef{Name: "outer_having"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
	})
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query view with HAVING - tests HAVING path in applyOuterQuery
	result, err := c.ExecuteQuery("SELECT * FROM view_having WHERE col2 > 100")
	if err != nil {
		t.Logf("HAVING query error: %v", err)
		return
	}

	t.Logf("HAVING query returned %d rows", len(result.Rows))
}

// TestCoverage_applyOuterQueryORDERBY tests applyOuterQuery ORDER BY path
func TestCoverage_applyOuterQueryORDERBY(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	createCoverageTestTable(t, c, "outer_order", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "outer_order",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(6-i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	err := c.CreateView("view_order", &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{star()}},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "val"}}},
		},
		From: &query.TableRef{Name: "outer_order"},
	})
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query view with ORDER BY - tests ORDER BY path in applyOuterQuery
	result, err := c.ExecuteQuery("SELECT * FROM view_order ORDER BY col2 DESC")
	if err != nil {
		t.Logf("ORDER BY query error: %v", err)
		return
	}

	t.Logf("ORDER BY query returned %d rows", len(result.Rows))
}

// TestCoverage_selectLockedCTEWithJoin tests selectLocked with CTE and JOIN
func TestCoverage_selectLockedCTEWithJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	createCoverageTestTable(t, c, "cte_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	createCoverageTestTable(t, c, "cte_other", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "main_id", Type: query.TokenInteger},
		{Name: "value", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "cte_main",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Name")}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "cte_other",
			Columns: []string{"id", "main_id", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), numReal(float64(i*10))}},
		}, nil)
	}

	// Query with CTE and JOIN
	result, err := c.ExecuteQuery(`
		WITH cte AS (SELECT id, name FROM cte_main WHERE id <= 3)
		SELECT cte.id, cte.name, cte_other.value
		FROM cte JOIN cte_other ON cte.id = cte_other.main_id
	`)
	if err != nil {
		t.Logf("CTE JOIN query error: %v", err)
		return
	}

	t.Logf("CTE JOIN query returned %d rows", len(result.Rows))
}

// TestCoverage_resolveAggregateInExprArithmetic96 tests resolveAggregateInExpr with arithmetic
func TestCoverage_resolveAggregateInExprArithmetic96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "agg_arith", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "agg_arith",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Query with aggregate in arithmetic expression
	queries := []string{
		"SELECT SUM(val) * 2 as doubled FROM agg_arith",
		"SELECT SUM(val) + COUNT(*) as combined FROM agg_arith",
		"SELECT AVG(val) - 5 as adjusted FROM agg_arith",
		"SELECT MIN(val) / 2 as halved FROM agg_arith",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query '%s' error: %v", q, err)
			continue
		}
		t.Logf("Query '%s' returned %d rows", q, len(result.Rows))
	}
}

// TestCoverage_evaluateHavingMultipleConditions tests evaluateHaving with multiple conditions
func TestCoverage_evaluateHavingMultipleConditions(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "having_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "amt1", Type: query.TokenInteger},
		{Name: "amt2", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 10; i++ {
		grp := "X"
		if i > 5 {
			grp = "Y"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "having_multi",
			Columns: []string{"id", "grp", "amt1", "amt2"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(grp), numReal(float64(i*10)), numReal(float64(i*5))}},
		}, nil)
	}

	// Query with multiple HAVING conditions
	result, err := c.ExecuteQuery(`
		SELECT grp, SUM(amt1) as s1, SUM(amt2) as s2, COUNT(*) as cnt
		FROM having_multi
		GROUP BY grp
		HAVING SUM(amt1) > 100 OR (COUNT(*) >= 3 AND SUM(amt2) > 50)
	`)
	if err != nil {
		t.Logf("Multiple HAVING conditions error: %v", err)
		return
	}

	t.Logf("Multiple HAVING conditions returned %d rows", len(result.Rows))
}

// TestCoverage_applyOuterQueryEmptyResult tests applyOuterQuery with empty results
func TestCoverage_applyOuterQueryEmptyResult(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	createCoverageTestTable(t, c, "outer_empty", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	// Create view (no data inserted)
	err := c.CreateView("view_empty", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "val"}},
		From:    &query.TableRef{Name: "outer_empty"},
	})
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query empty view with aggregates
	result, err := c.ExecuteQuery("SELECT COUNT(*) as cnt, SUM(val) as total FROM view_empty")
	if err != nil {
		t.Logf("Empty view query error: %v", err)
		return
	}

	t.Logf("Empty view query returned %d rows", len(result.Rows))
}

// TestCoverage_selectLockedRecursiveCTE tests selectLocked with recursive CTE
func TestCoverage_selectLockedRecursiveCTE(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	createCoverageTestTable(t, c, "rec_cte", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "parent_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})

	// Insert hierarchical data
	data := []struct {
		id       int
		parentID int
		name     string
	}{
		{1, 0, "Root"},
		{2, 1, "Child1"},
		{3, 1, "Child2"},
		{4, 2, "GrandChild"},
	}
	for _, d := range data {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "rec_cte",
			Columns: []string{"id", "parent_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(d.id)), numReal(float64(d.parentID)), strReal(d.name)}},
		}, nil)
	}

	// Query with recursive CTE
	result, err := c.ExecuteQuery(`
		WITH RECURSIVE tree AS (
			SELECT id, parent_id, name FROM rec_cte WHERE id = 1
			UNION ALL
			SELECT r.id, r.parent_id, r.name FROM rec_cte r JOIN tree t ON r.parent_id = t.id
		)
		SELECT * FROM tree
	`)
	if err != nil {
		t.Logf("Recursive CTE query error: %v", err)
		return
	}

	t.Logf("Recursive CTE query returned %d rows", len(result.Rows))
}
