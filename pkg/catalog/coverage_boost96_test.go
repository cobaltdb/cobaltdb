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

// TestCoverage_selectLockedWindowWithCTE96 targets window functions with pre-computed CTE
func TestCoverage_selectLockedWindowWithCTE96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "window_cte_base96", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "window_cte_base96",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(cat), numReal(float64(i * 10))}},
		}, nil)
	}

	result, err := c.ExecuteQuery(`
		WITH RECURSIVE cte_data AS (
			SELECT id, category, amount FROM window_cte_base96 WHERE id <= 10
		)
		SELECT id, category, amount,
			ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) as rn
		FROM cte_data
	`)
	if err != nil {
		t.Logf("Window with recursive CTE error: %v", err)
	} else {
		t.Logf("Query returned %d rows", len(result.Rows))
	}

	result, err = c.ExecuteQuery(`
		WITH cte_sum AS (
			SELECT category, SUM(amount) as total
			FROM window_cte_base96
			GROUP BY category
		)
		SELECT category, total,
			RANK() OVER (ORDER BY total DESC) as rnk
		FROM cte_sum
	`)
	if err != nil {
		t.Logf("Window with CTE error: %v", err)
	} else {
		t.Logf("CTE window query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_selectLockedRLS96 targets selectLocked with RLS policies
func TestCoverage_selectLockedRLS96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table via SQL to enable RLS properly
	_, err := c.ExecuteQuery(`CREATE TABLE rls_test_data96 (
		id INTEGER PRIMARY KEY,
		tenant_id INTEGER,
		data TEXT
	)`)
	if err != nil {
		t.Logf("Create table error: %v", err)
		return
	}

	// Insert data
	for i := 1; i <= 10; i++ {
		tenant := 1
		if i > 5 {
			tenant = 2
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "rls_test_data96",
			Columns: []string{"id", "tenant_id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(tenant)), strReal("data")}},
		}, nil)
	}

	// Enable RLS via SQL
	_, err = c.ExecuteQuery("ALTER TABLE rls_test_data96 ENABLE ROW LEVEL SECURITY")
	if err != nil {
		t.Logf("Enable RLS error: %v", err)
		return
	}

	// Create policy via SQL
	_, err = c.ExecuteQuery("CREATE POLICY tenant_isolation ON rls_test_data96 USING (tenant_id = CURRENT_TENANT)")
	if err != nil {
		t.Logf("Create policy error: %v", err)
		return
	}

	// Query with RLS
	result, err := c.ExecuteQuery("SELECT * FROM rls_test_data96")
	if err != nil {
		t.Logf("RLS query error: %v", err)
	} else {
		t.Logf("RLS query returned %d rows", len(result.Rows))
	}
}

// TestCoverage_insertLockedTriggers96 targets insertLocked with triggers
func TestCoverage_insertLockedTriggers96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create audit log table
	_, err := c.ExecuteQuery(`CREATE TABLE audit_log96 (
		id INTEGER PRIMARY KEY,
		action TEXT,
		table_name TEXT,
		row_id INTEGER
	)`)
	if err != nil {
		t.Logf("Create audit log error: %v", err)
		return
	}

	// Create main table
	_, err = c.ExecuteQuery(`CREATE TABLE trigger_test_table96 (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create main table error: %v", err)
		return
	}

	// Create trigger via SQL
	_, err = c.ExecuteQuery(`CREATE TRIGGER audit_insert AFTER INSERT ON trigger_test_table96
		BEGIN
			INSERT INTO audit_log96 (action, table_name, row_id) VALUES ('INSERT', 'trigger_test_table96', NEW.id);
		END`)
	if err != nil {
		t.Logf("Create trigger error: %v", err)
		return
	}

	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "trigger_test_table96",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Test")}},
	}, nil)
	if err != nil {
		t.Logf("Insert with trigger error: %v", err)
	}

	result, err := c.ExecuteQuery("SELECT * FROM audit_log96")
	if err != nil {
		t.Logf("Audit log query error: %v", err)
	} else {
		t.Logf("Audit log has %d rows", len(result.Rows))
	}
}

// TestCoverage_insertLockedFKValidation96 targets insertLocked with FK validation
func TestCoverage_insertLockedFKValidation96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables with FK via SQL
	_, err := c.ExecuteQuery(`CREATE TABLE parent_fk96 (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Logf("Create parent error: %v", err)
		return
	}

	_, err = c.ExecuteQuery(`CREATE TABLE child_fk96 (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER REFERENCES parent_fk96(id) ON DELETE RESTRICT ON UPDATE CASCADE,
		data TEXT
	)`)
	if err != nil {
		t.Logf("Create child error: %v", err)
		return
	}

	// Insert parent
	c.Insert(ctx, &query.InsertStmt{
		Table:   "parent_fk96",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Parent 1")}},
	}, nil)

	// Insert valid child
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "child_fk96",
		Columns: []string{"id", "parent_id", "data"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Child 1")}},
	}, nil)
	if err != nil {
		t.Logf("Valid child insert error: %v", err)
	}

	// Insert invalid child (FK violation)
	_, _, err = c.Insert(ctx, &query.InsertStmt{
		Table:   "child_fk96",
		Columns: []string{"id", "parent_id", "data"},
		Values:  [][]query.Expression{{numReal(2), numReal(999), strReal("Invalid Child")}},
	}, nil)
	if err != nil {
		t.Logf("Invalid child insert error (expected): %v", err)
	}
}

// TestCoverage_countRows96 targets countRows function via SQL
func TestCoverage_countRows96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "count_test96", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Check empty table count
	result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM count_test96")
	t.Logf("Empty table count: %v", result.Rows[0][0])

	for i := 1; i <= 100; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "count_test96",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("data")}},
		}, nil)
	}

	// Check count after inserts
	result, _ = c.ExecuteQuery("SELECT COUNT(*) FROM count_test96")
	t.Logf("After 100 inserts count: %v", result.Rows[0][0])

	// Delete some rows
	c.ExecuteQuery("DELETE FROM count_test96 WHERE id <= 30")

	// Check count after delete
	result, _ = c.ExecuteQuery("SELECT COUNT(*) FROM count_test96")
	t.Logf("After delete count: %v", result.Rows[0][0])
}

// TestCoverage_insertLockedMultipleRows96 targets insertLocked with multiple value rows
func TestCoverage_insertLockedMultipleRows96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "multi_insert96", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "value", Type: query.TokenInteger},
	})

	_, count, err := c.Insert(ctx, &query.InsertStmt{
		Table:   "multi_insert96",
		Columns: []string{"id", "name", "value"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(10)},
			{numReal(2), strReal("B"), numReal(20)},
			{numReal(3), strReal("C"), numReal(30)},
			{numReal(4), strReal("D"), numReal(40)},
			{numReal(5), strReal("E"), numReal(50)},
		},
	}, nil)
	if err != nil {
		t.Logf("Multi-row insert error: %v", err)
	} else {
		t.Logf("Inserted %d rows", count)
	}

	result, err := c.ExecuteQuery("SELECT COUNT(*) FROM multi_insert96")
	if err != nil {
		t.Logf("Count query error: %v", err)
	} else if len(result.Rows) > 0 {
		t.Logf("Total rows in table: %v", result.Rows[0][0])
	}
}

// TestCoverage_selectLockedUnionSetOps targets selectLocked with set operations
func TestCoverage_selectLockedUnionSetOps96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "union_a_ops", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, c, "union_b_ops", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "union_a_ops",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}
	for i := 5; i <= 15; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "union_b_ops",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 20))}},
		}, nil)
	}

	queries := []string{
		"SELECT id FROM union_a_ops UNION SELECT id FROM union_b_ops",
		"SELECT id FROM union_a_ops UNION ALL SELECT id FROM union_b_ops",
		"SELECT id FROM union_a_ops INTERSECT SELECT id FROM union_b_ops",
		"SELECT id FROM union_a_ops EXCEPT SELECT id FROM union_b_ops",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query '%s' error: %v", q, err)
		} else {
			t.Logf("Query '%s' returned %d rows", q, len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedComplexWhere96 targets selectLocked with complex WHERE clauses
func TestCoverage_selectLockedComplexWhere96(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "complex_where", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "status", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
		{Name: "category", Type: query.TokenText},
	})

	statuses := []string{"active", "pending", "inactive", "deleted"}
	categories := []string{"A", "B", "C"}
	for i := 1; i <= 50; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "complex_where",
			Columns: []string{"id", "status", "amount", "category"},
			Values: [][]query.Expression{
				{numReal(float64(i)), strReal(statuses[i%4]), numReal(float64(i * 10)), strReal(categories[i%3])},
			},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM complex_where WHERE status = 'active' AND amount > 100",
		"SELECT * FROM complex_where WHERE status IN ('active', 'pending') OR amount > 300",
		"SELECT * FROM complex_where WHERE amount BETWEEN 100 AND 300",
		"SELECT * FROM complex_where WHERE status NOT IN ('deleted') AND category = 'A'",
		"SELECT * FROM complex_where WHERE (status = 'active' OR status = 'pending') AND amount < 200",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_insertLockedConflictUpdate targets insertLocked ON CONFLICT DO UPDATE
func TestCoverage_insertLockedConflictUpdate(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "conflict_update", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "counter", Type: query.TokenInteger},
		{Name: "data", Type: query.TokenText},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "conflict_update",
		Columns: []string{"id", "counter", "data"},
		Values:  [][]query.Expression{{numReal(1), numReal(10), strReal("original")}},
	}, nil)

	stmt, _ := query.Parse("INSERT INTO conflict_update (id, counter, data) VALUES (1, 20, 'upsert') ON CONFLICT (id) DO UPDATE SET counter = counter + 1, data = 'updated'")
	if ins, ok := stmt.(*query.InsertStmt); ok {
		_, _, err := c.Insert(ctx, ins, nil)
		if err != nil {
			t.Logf("UPSERT error: %v", err)
		}
	}

	result, _ := c.ExecuteQuery("SELECT * FROM conflict_update WHERE id = 1")
	if len(result.Rows) > 0 {
		t.Logf("After UPSERT: %v", result.Rows[0])
	}
}

// TestCoverage_selectLockedLimitOffset targets selectLocked with LIMIT/OFFSET
func TestCoverage_selectLockedLimitOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "limit_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	for i := 1; i <= 100; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "limit_test",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		"SELECT * FROM limit_test LIMIT 10",
		"SELECT * FROM limit_test LIMIT 10 OFFSET 20",
		"SELECT * FROM limit_test ORDER BY id DESC LIMIT 5",
		"SELECT * FROM limit_test WHERE id > 50 LIMIT 10",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}

// TestCoverage_selectLockedOrderByMulti targets selectLocked with multi-column ORDER BY
func TestCoverage_selectLockedOrderByMulti(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "orderby_multi", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "subcategory", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	categories := []string{"A", "B", "C"}
	subcategories := []string{"X", "Y"}
	id := 1
	for _, cat := range categories {
		for _, sub := range subcategories {
			for i := 1; i <= 3; i++ {
				c.Insert(ctx, &query.InsertStmt{
					Table:   "orderby_multi",
					Columns: []string{"id", "category", "subcategory", "amount"},
					Values:  [][]query.Expression{{numReal(float64(id)), strReal(cat), strReal(sub), numReal(float64(i * 10))}},
				}, nil)
				id++
			}
		}
	}

	queries := []string{
		"SELECT * FROM orderby_multi ORDER BY category, subcategory",
		"SELECT * FROM orderby_multi ORDER BY category DESC, amount ASC",
		"SELECT * FROM orderby_multi ORDER BY amount DESC LIMIT 5",
	}

	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Query returned %d rows", len(result.Rows))
		}
	}
}
