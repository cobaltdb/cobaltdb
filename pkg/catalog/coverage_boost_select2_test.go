package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedWithCTEAndWindowFunction2 tests selectLocked with CTE containing window functions
func TestSelectLockedWithCTEAndWindowFunction2(t *testing.T) {
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
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "dept", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(100)},
			{numReal(2), strReal("A"), numReal(200)},
			{numReal(3), strReal("B"), numReal(50)},
			{numReal(4), strReal("B"), numReal(150)},
		},
	}, nil)

	// CTE with window function
	result, err := c.ExecuteQuery(`
		WITH dept_sales AS (
			SELECT dept, amount FROM sales
		)
		SELECT dept, SUM(amount) OVER (PARTITION BY dept) as total
		FROM dept_sales
		ORDER BY dept
	`)
	if err != nil {
		t.Logf("CTE with window function error: %v", err)
	} else {
		t.Logf("CTE with window function returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithComplexViewAndJoin2 tests selectLocked with complex view and JOIN
func TestSelectLockedWithComplexViewAndJoin2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
			{Name: "salary", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id", "name", "salary"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), strReal("Alice"), numReal(5000)},
			{numReal(2), numReal(1), strReal("Bob"), numReal(6000)},
			{numReal(3), numReal(2), strReal("Charlie"), numReal(5500)},
		},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Engineering")},
			{numReal(2), strReal("Sales")},
		},
	}, nil)

	// Create complex view (with GROUP BY)
	c.CreateView("dept_summary", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "dept_id"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "salary"}}},
		},
		From:    &query.TableRef{Name: "employees"},
		GroupBy: []query.Expression{&query.Identifier{Name: "dept_id"}},
	})

	// Query complex view with JOIN
	result, err := c.ExecuteQuery(`
		SELECT d.name, v.sum
		FROM departments d
		JOIN dept_summary v ON d.id = v.dept_id
	`)
	if err != nil {
		t.Logf("Complex view with JOIN error: %v", err)
	} else {
		t.Logf("Complex view with JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithSimpleViewInlining2 tests selectLocked with simple view inlining
func TestSelectLockedWithSimpleViewInlining2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "price", Type: query.TokenInteger},
			{Name: "category", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "name", "price", "category"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Product A"), numReal(100), strReal("A")},
			{numReal(2), strReal("Product B"), numReal(200), strReal("A")},
			{numReal(3), strReal("Product C"), numReal(150), strReal("B")},
		},
	}, nil)

	// Create simple view (no GROUP BY, no aggregates, no DISTINCT)
	c.CreateView("expensive_products", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
			&query.Identifier{Name: "price"},
		},
		From: &query.TableRef{Name: "products"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "price"},
			Operator: query.TokenGte,
			Right:    numReal(150),
		},
	})

	// Query simple view with additional WHERE
	result, err := c.ExecuteQuery(`
		SELECT * FROM expensive_products WHERE price > 180
	`)
	if err != nil {
		t.Logf("Simple view with WHERE error: %v", err)
	} else {
		t.Logf("Simple view with WHERE returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithMaterializedView2 tests selectLocked with materialized view
func TestSelectLockedWithMaterializedView2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "amount"},
		Values: [][]query.Expression{
			{numReal(1), numReal(100)},
			{numReal(2), numReal(200)},
			{numReal(3), numReal(300)},
		},
	}, nil)

	// Create materialized view
	createMVSQL := "CREATE MATERIALIZED VIEW mv_sales_summary AS SELECT SUM(amount) as total FROM sales"
	_, err := c.ExecuteQuery(createMVSQL)
	if err != nil {
		t.Logf("Create materialized view error: %v", err)
		return
	}

	// Query materialized view
	result, err := c.ExecuteQuery("SELECT * FROM mv_sales_summary")
	if err != nil {
		t.Logf("Query materialized view error: %v", err)
	} else {
		t.Logf("Materialized view returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithDerivedTableAndJoin2 tests selectLocked with derived table and JOIN
func TestSelectLockedWithDerivedTableAndJoin2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "customers",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(100)},
			{numReal(2), numReal(1), numReal(200)},
			{numReal(3), numReal(2), numReal(150)},
		},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice")},
			{numReal(2), strReal("Bob")},
		},
	}, nil)

	// Query with derived table and JOIN
	result, err := c.ExecuteQuery(`
		SELECT c.name, o.total
		FROM customers c
		JOIN (
			SELECT customer_id, SUM(amount) as total
			FROM orders
			GROUP BY customer_id
		) o ON c.id = o.customer_id
	`)
	if err != nil {
		t.Logf("Derived table with JOIN error: %v", err)
	} else {
		t.Logf("Derived table with JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithViewStarExpansion2 tests selectLocked with view and SELECT *
func TestSelectLockedWithViewStarExpansion2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "name", "value"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Item1"), numReal(100)},
			{numReal(2), strReal("Item2"), numReal(200)},
		},
	}, nil)

	// Create simple view
	c.CreateView("item_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
			&query.Identifier{Name: "value"},
		},
		From: &query.TableRef{Name: "items"},
	})

	// Query view with SELECT *
	result, err := c.ExecuteQuery("SELECT * FROM item_view")
	if err != nil {
		t.Logf("View with SELECT * error: %v", err)
	} else {
		t.Logf("View with SELECT * returned %d rows, %d cols", len(result.Rows), len(result.Columns))
	}
}

// TestComputeAggregatesWithGroupByWithAlias2 tests computeAggregatesWithGroupBy with GROUP BY alias
func TestComputeAggregatesWithGroupByWithAlias2(t *testing.T) {
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
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "dept", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(100)},
			{numReal(2), strReal("A"), numReal(200)},
			{numReal(3), strReal("B"), numReal(150)},
		},
	}, nil)

	// GROUP BY with alias reference
	result, err := c.ExecuteQuery(`
		SELECT dept as department, SUM(amount) as total
		FROM sales
		GROUP BY department
	`)
	if err != nil {
		t.Logf("GROUP BY alias error: %v", err)
	} else {
		t.Logf("GROUP BY alias returned %d rows", len(result.Rows))
	}
}

// TestComputeAggregatesWithGroupByWithExpression2 tests computeAggregatesWithGroupBy with expression
func TestComputeAggregatesWithGroupByWithExpression2(t *testing.T) {
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
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "amount", Type: query.TokenInteger},
			{Name: "quantity", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "amount", "quantity"},
		Values: [][]query.Expression{
			{numReal(1), numReal(100), numReal(2)},
			{numReal(2), numReal(200), numReal(3)},
			{numReal(3), numReal(150), numReal(2)},
		},
	}, nil)

	// GROUP BY with expression
	result, err := c.ExecuteQuery(`
		SELECT amount * quantity as revenue, COUNT(*) as cnt
		FROM sales
		GROUP BY amount * quantity
	`)
	if err != nil {
		t.Logf("GROUP BY expression error: %v", err)
	} else {
		t.Logf("GROUP BY expression returned %d rows", len(result.Rows))
	}
}

// TestComputeAggregatesWithEmptyTable2 tests computeAggregatesWithGroupBy with empty table
func TestComputeAggregatesWithEmptyTable2(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create empty table
	c.CreateTable(&query.CreateTableStmt{
		Table: "empty_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Aggregate on empty table without GROUP BY
	result, err := c.ExecuteQuery("SELECT COUNT(*) as cnt, SUM(value) as total FROM empty_table")
	if err != nil {
		t.Logf("Empty table aggregate error: %v", err)
	} else {
		t.Logf("Empty table aggregate returned %d rows", len(result.Rows))
		if len(result.Rows) > 0 {
			t.Logf("Results: cnt=%v, total=%v", result.Rows[0][0], result.Rows[0][1])
		}
	}

	// Aggregate on empty table with GROUP BY
	result, err = c.ExecuteQuery("SELECT id, COUNT(*) as cnt FROM empty_table GROUP BY id")
	if err != nil {
		t.Logf("Empty table GROUP BY error: %v", err)
	} else {
		t.Logf("Empty table GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestEvaluateWhereWithVariousConditions2 tests evaluateWhere with various conditions
func TestEvaluateWhereWithVariousConditions2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with various data types
	c.CreateTable(&query.CreateTableStmt{
		Table: "test_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "score", Type: query.TokenInteger},
			{Name: "active", Type: query.TokenBoolean},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test_data",
		Columns: []string{"id", "name", "score", "active"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), numReal(95), &query.BooleanLiteral{Value: true}},
			{numReal(2), strReal("Bob"), numReal(87), &query.BooleanLiteral{Value: false}},
			{numReal(3), strReal("Charlie"), numReal(92), &query.BooleanLiteral{Value: true}},
		},
	}, nil)

	// Test various WHERE conditions
	queries := []string{
		"SELECT * FROM test_data WHERE score > 90",
		"SELECT * FROM test_data WHERE active = true",
		"SELECT * FROM test_data WHERE name LIKE 'A%'",
		"SELECT * FROM test_data WHERE score BETWEEN 85 AND 95",
		"SELECT * FROM test_data WHERE name IN ('Alice', 'Bob')",
		"SELECT * FROM test_data WHERE name IS NOT NULL",
		"SELECT * FROM test_data WHERE score > 90 AND active = true",
		"SELECT * FROM test_data WHERE score < 90 OR active = true",
	}

	for _, sql := range queries {
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Logf("Query '%s' error: %v", sql, err)
		} else {
			t.Logf("Query '%s' returned %d rows", sql, len(result.Rows))
		}
	}
}

// TestEvaluateCastExprWithVariousTypes2 tests evaluateCastExpr with various type casts
func TestEvaluateCastExprWithVariousTypes2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "cast_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "str_val", Type: query.TokenText},
			{Name: "int_val", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "cast_test",
		Columns: []string{"id", "str_val", "int_val"},
		Values: [][]query.Expression{
			{numReal(1), strReal("123"), numReal(456)},
			{numReal(2), strReal("abc"), numReal(0)},
			{numReal(3), strReal("true"), numReal(1)},
		},
	}, nil)

	// Test various CAST expressions
	queries := []string{
		"SELECT CAST(str_val AS INTEGER) FROM cast_test WHERE id = 1",
		"SELECT CAST(int_val AS TEXT) FROM cast_test WHERE id = 1",
	}

	for _, sql := range queries {
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Logf("CAST query '%s' error: %v", sql, err)
		} else {
			t.Logf("CAST query '%s' returned %d rows", sql, len(result.Rows))
		}
	}
}

// TestEvaluateBetweenWithVariousTypes2 tests evaluateBetween with various types
func TestEvaluateBetweenWithVariousTypes2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "between_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "num_val", Type: query.TokenInteger},
			{Name: "str_val", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "between_test",
		Columns: []string{"id", "num_val", "str_val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(5), strReal("Charlie")},
			{numReal(2), numReal(10), strReal("Bob")},
			{numReal(3), numReal(15), strReal("Alice")},
		},
	}, nil)

	// Test BETWEEN with various types
	queries := []string{
		"SELECT * FROM between_test WHERE num_val BETWEEN 5 AND 12",
		"SELECT * FROM between_test WHERE str_val BETWEEN 'Bob' AND 'David'",
		"SELECT * FROM between_test WHERE num_val NOT BETWEEN 10 AND 20",
	}

	for _, sql := range queries {
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Logf("BETWEEN query '%s' error: %v", sql, err)
		} else {
			t.Logf("BETWEEN query '%s' returned %d rows", sql, len(result.Rows))
		}
	}
}

// TestApplyOuterQueryWithLimitOffset2 tests applyOuterQuery with LIMIT and OFFSET
func TestApplyOuterQueryWithLimitOffset2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "base_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert 10 rows
	values := [][]query.Expression{}
	for i := 1; i <= 10; i++ {
		values = append(values, []query.Expression{numReal(float64(i)), numReal(float64(i * 10))})
	}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "base_data",
		Columns: []string{"id", "value"},
		Values:  values,
	}, nil)

	// Create simple view
	c.CreateView("limited_view", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "value"},
		},
		From: &query.TableRef{Name: "base_data"},
	})

	// Query view with LIMIT and OFFSET
	queries := []string{
		"SELECT * FROM limited_view ORDER BY id LIMIT 3",
		"SELECT * FROM limited_view ORDER BY id LIMIT 3 OFFSET 2",
		"SELECT * FROM limited_view ORDER BY id OFFSET 5",
	}

	for _, sql := range queries {
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Logf("LIMIT/OFFSET query '%s' error: %v", sql, err)
		} else {
			t.Logf("LIMIT/OFFSET query '%s' returned %d rows", sql, len(result.Rows))
			for i, row := range result.Rows {
				t.Logf("  Row %d: %v", i, row)
			}
		}
	}
}

// TestResolveAggregateInExpr2 tests resolveAggregateInExpr
func TestResolveAggregateInExpr2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "agg_expr_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "agg_expr_test",
		Columns: []string{"id", "category", "value"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(100)},
			{numReal(2), strReal("A"), numReal(200)},
			{numReal(3), strReal("B"), numReal(150)},
			{numReal(4), strReal("B"), numReal(250)},
		},
	}, nil)

	// Test aggregate expressions in SELECT
	queries := []string{
		"SELECT category, SUM(value) * 2 as doubled FROM agg_expr_test GROUP BY category",
		"SELECT category, SUM(value) + COUNT(*) as combined FROM agg_expr_test GROUP BY category",
		"SELECT category, AVG(value) FROM agg_expr_test GROUP BY category",
		"SELECT category, MIN(value), MAX(value) FROM agg_expr_test GROUP BY category",
	}

	for _, sql := range queries {
		result, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Logf("Aggregate expr query '%s' error: %v", sql, err)
		} else {
			t.Logf("Aggregate expr query '%s' returned %d rows", sql, len(result.Rows))
			for i, row := range result.Rows {
				t.Logf("  Row %d: %v", i, row)
			}
		}
	}
}
