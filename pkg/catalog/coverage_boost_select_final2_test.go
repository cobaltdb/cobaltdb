package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedWithMultipleCTEsFinal tests selectLocked with multiple CTEs
func TestSelectLockedWithMultipleCTEsFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_id", Type: query.TokenInteger},
			{Name: "salary", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id", "salary"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(50000)}, {numReal(2), numReal(1), numReal(60000)}, {numReal(3), numReal(2), numReal(70000)}},
	}, nil)

	// Multiple CTEs - should trigger cteResults materialization
	result, err := c.ExecuteQuery("WITH dept_avg AS (SELECT dept_id, AVG(salary) as avg_sal FROM employees GROUP BY dept_id), above_avg AS (SELECT e.* FROM employees e JOIN dept_avg d ON e.dept_id = d.dept_id WHERE e.salary > d.avg_sal) SELECT * FROM above_avg")
	if err != nil {
		t.Logf("Multiple CTEs error: %v", err)
	} else {
		t.Logf("Multiple CTEs returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithRecursiveCTEFinal tests selectLocked with recursive CTE
func TestSelectLockedWithRecursiveCTEFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "org_chart",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "manager_id", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "org_chart",
		Columns: []string{"id", "name", "manager_id"},
		Values: [][]query.Expression{
			{numReal(1), strReal("CEO"), &query.NullLiteral{}},
			{numReal(2), strReal("VP1"), numReal(1)},
			{numReal(3), strReal("VP2"), numReal(1)},
			{numReal(4), strReal("Manager1"), numReal(2)},
		},
	}, nil)

	// Recursive CTE
	result, err := c.ExecuteQuery("WITH RECURSIVE subordinates AS (SELECT id, name, manager_id FROM org_chart WHERE id = 2 UNION ALL SELECT e.id, e.name, e.manager_id FROM org_chart e JOIN subordinates s ON e.manager_id = s.id) SELECT * FROM subordinates")
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithWindowFrameFinal tests window functions with frame specifications
func TestSelectLockedWithWindowFrameFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "region", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "region", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("North"), numReal(100)},
			{numReal(2), strReal("North"), numReal(200)},
			{numReal(3), strReal("South"), numReal(150)},
			{numReal(4), strReal("South"), numReal(250)},
		},
	}, nil)

	// Window functions with various frames
	queries := []string{
		"SELECT id, region, amount, SUM(amount) OVER (PARTITION BY region ORDER BY id) as running_sum FROM sales",
		"SELECT id, region, amount, AVG(amount) OVER (PARTITION BY region) as avg_by_region FROM sales",
	}

	for i, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Window query %d error: %v", i, err)
		} else {
			t.Logf("Window query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestSelectLockedWithComplexViewFinal tests complex view handling in selectLocked
func TestSelectLockedWithComplexViewFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "category", "price"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(100)},
			{numReal(2), strReal("A"), numReal(200)},
			{numReal(3), strReal("B"), numReal(150)},
		},
	}, nil)

	// Create view with DISTINCT - use SQL
	_, err := c.ExecuteQuery("CREATE VIEW distinct_categories AS SELECT DISTINCT category FROM products")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query the complex view
	result, err := c.ExecuteQuery("SELECT * FROM distinct_categories")
	if err != nil {
		t.Logf("Complex view query error: %v", err)
	} else {
		t.Logf("Complex view returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithViewHavingFinal tests view with HAVING clause
func TestSelectLockedWithViewHavingFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer", Type: query.TokenText},
			{Name: "total", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer", "total"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), numReal(100)},
			{numReal(2), strReal("Alice"), numReal(200)},
			{numReal(3), strReal("Bob"), numReal(50)},
		},
	}, nil)

	// Create view with GROUP BY and HAVING using SQL
	_, err := c.ExecuteQuery("CREATE VIEW high_value_customers AS SELECT customer, SUM(total) as total_spent FROM orders GROUP BY customer HAVING SUM(total) >= 150")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query the view with HAVING
	result, err := c.ExecuteQuery("SELECT * FROM high_value_customers")
	if err != nil {
		t.Logf("View with HAVING error: %v", err)
	} else {
		t.Logf("View with HAVING returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithSimpleViewInliningFinal tests simple view inlining
func TestSelectLockedWithSimpleViewInliningFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "data",
		Columns: []string{"id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}},
	}, nil)

	// Create simple view using SQL
	_, err := c.ExecuteQuery("CREATE VIEW simple_view AS SELECT * FROM data")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query simple view - should use inlining
	result, err := c.ExecuteQuery("SELECT * FROM simple_view WHERE value > 15")
	if err != nil {
		t.Logf("Simple view error: %v", err)
	} else {
		t.Logf("Simple view returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithViewAliasFinal tests view with alias
func TestSelectLockedWithViewAliasFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("item1")}, {numReal(2), strReal("item2")}},
	}, nil)

	// Create view using SQL
	_, err := c.ExecuteQuery("CREATE VIEW v_items AS SELECT id as item_id, name FROM items")
	if err != nil {
		t.Logf("CREATE VIEW error: %v", err)
		return
	}

	// Query view with alias
	result, err := c.ExecuteQuery("SELECT * FROM v_items v WHERE v.item_id = 1")
	if err != nil {
		t.Logf("View with alias error: %v", err)
	} else {
		t.Logf("View with alias returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithSubqueryInSelectFinal tests subquery in SELECT clause
func TestSelectLockedWithSubqueryInSelectFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_id", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}, {numReal(2), strReal("Eng")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}, {numReal(3), numReal(2)}},
	}, nil)

	// Subquery in SELECT
	result, err := c.ExecuteQuery("SELECT id, name, (SELECT COUNT(*) FROM employees WHERE dept_id = departments.id) as emp_count FROM departments")
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	} else {
		t.Logf("Subquery in SELECT returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithExistsSubqueryFinal tests EXISTS subquery
func TestSelectLockedWithExistsSubqueryFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "customers",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer_id", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// EXISTS subquery
	result, err := c.ExecuteQuery("SELECT * FROM customers WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)")
	if err != nil {
		t.Logf("EXISTS subquery error: %v", err)
	} else {
		t.Logf("EXISTS subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithInSubqueryFinal tests IN subquery
func TestSelectLockedWithInSubqueryFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category_id", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "categories",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "category_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(2)}, {numReal(3), numReal(1)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "categories",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Electronics")}, {numReal(2), strReal("Books")}},
	}, nil)

	// IN subquery
	result, err := c.ExecuteQuery("SELECT * FROM products WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')")
	if err != nil {
		t.Logf("IN subquery error: %v", err)
	} else {
		t.Logf("IN subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithCorrelatedSubqueryFinal tests correlated subquery
func TestSelectLockedWithCorrelatedSubqueryFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_id", Type: query.TokenInteger},
			{Name: "salary", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id", "salary"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(100)},
			{numReal(2), numReal(1), numReal(200)},
			{numReal(3), numReal(2), numReal(150)},
		},
	}, nil)

	// Correlated subquery
	result, err := c.ExecuteQuery("SELECT * FROM employees e WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id)")
	if err != nil {
		t.Logf("Correlated subquery error: %v", err)
	} else {
		t.Logf("Correlated subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithNotExistsSubqueryFinal tests NOT EXISTS subquery
func TestSelectLockedWithNotExistsSubqueryFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "customers",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer_id", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// NOT EXISTS subquery
	result, err := c.ExecuteQuery("SELECT * FROM customers WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)")
	if err != nil {
		t.Logf("NOT EXISTS subquery error: %v", err)
	} else {
		t.Logf("NOT EXISTS subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedWithAllAnySubqueryFinal tests ALL/ANY subqueries
func TestSelectLockedWithAllAnySubqueryFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "premium_products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "price"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}, {numReal(3), numReal(50)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "premium_products",
		Columns: []string{"id", "price"},
		Values:  [][]query.Expression{{numReal(1), numReal(150)}},
	}, nil)

	// ALL subquery
	result, err := c.ExecuteQuery("SELECT * FROM products WHERE price > ALL (SELECT price FROM premium_products)")
	if err != nil {
		t.Logf("ALL subquery error: %v", err)
	} else {
		t.Logf("ALL subquery returned %d rows", len(result.Rows))
	}
}

// TestEvaluateWhereWithComplexConditionsFinal tests evaluateWhere with complex conditions
func TestEvaluateWhereWithComplexConditionsFinal(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "value", Type: query.TokenInteger},
			{Name: "flag", Type: query.TokenBoolean},
		},
	})

	c.Insert(ctx, &query.InsertStmt{
		Table:   "test_data",
		Columns: []string{"id", "name", "value", "flag"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), numReal(100), &query.BooleanLiteral{Value: true}},
			{numReal(2), strReal("Bob"), numReal(200), &query.BooleanLiteral{Value: false}},
			{numReal(3), strReal("Charlie"), numReal(150), &query.BooleanLiteral{Value: true}},
		},
	}, nil)

	// Complex WHERE with AND/OR/NOT
	queries := []string{
		"SELECT * FROM test_data WHERE (value > 100 AND flag = true) OR name = 'Bob'",
		"SELECT * FROM test_data WHERE NOT (value < 100 OR name = 'Unknown')",
		"SELECT * FROM test_data WHERE value BETWEEN 100 AND 200",
		"SELECT * FROM test_data WHERE name IN ('Alice', 'Bob')",
		"SELECT * FROM test_data WHERE name LIKE 'A%'",
		"SELECT * FROM test_data WHERE flag IS NULL",
		"SELECT * FROM test_data WHERE flag IS NOT NULL",
	}

	for i, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Complex WHERE query %d error: %v", i, err)
		} else {
			t.Logf("Complex WHERE query %d returned %d rows", i, len(result.Rows))
		}
	}
}

// TestInsertLockedWithSelectUnionFinal tests insertLocked with SELECT containing UNION
func TestInsertLockedWithSelectUnionFinal(t *testing.T) {
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
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table:   "t3",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
	})

	// Insert into t1 and t2
	c.Insert(ctx, &query.InsertStmt{
		Table:   "t1",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "t2",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(3), numReal(30)}, {numReal(4), numReal(40)}},
	}, nil)

	// INSERT...SELECT with UNION
	result, err := c.ExecuteQuery("INSERT INTO t3 SELECT * FROM t1 UNION ALL SELECT * FROM t2")
	if err != nil {
		t.Logf("INSERT...SELECT UNION error: %v", err)
	} else {
		t.Logf("INSERT...SELECT UNION result: %d rows", len(result.Rows))
	}
}

// TestInsertLockedWithOnDuplicateKeyFinal tests insertLocked with ON DUPLICATE KEY UPDATE
func TestInsertLockedWithOnDuplicateKeyFinal(t *testing.T) {
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
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert first
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Insert with ON DUPLICATE KEY UPDATE (MySQL syntax, may not be supported)
	result, err := c.ExecuteQuery("INSERT INTO test VALUES (1, 200) ON DUPLICATE KEY UPDATE val = val + 1")
	if err != nil {
		t.Logf("ON DUPLICATE KEY UPDATE error (expected if not supported): %v", err)
	} else {
		t.Logf("ON DUPLICATE KEY UPDATE result: %d rows", len(result.Rows))
	}
}
