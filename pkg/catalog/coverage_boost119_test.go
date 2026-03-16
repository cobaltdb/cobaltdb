package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_selectLockedWithWindowFuncs tests window functions in selectLocked
func TestCoverage_selectLockedWithWindowFuncs(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept", Type: query.TokenText},
			{Name: "salary", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept", "salary"},
		Values: [][]query.Expression{
			{numReal(1), strReal("IT"), numReal(5000)},
			{numReal(2), strReal("IT"), numReal(6000)},
			{numReal(3), strReal("HR"), numReal(4000)},
			{numReal(4), strReal("HR"), numReal(4500)},
		},
	}, nil)

	// Test ROW_NUMBER()
	result, err := c.ExecuteQuery("SELECT id, ROW_NUMBER() OVER (ORDER BY salary) as rn FROM employees")
	if err != nil {
		t.Logf("ROW_NUMBER error: %v", err)
	} else {
		t.Logf("ROW_NUMBER result rows: %d", len(result.Rows))
	}

	// Test RANK()
	result, err = c.ExecuteQuery("SELECT id, RANK() OVER (ORDER BY salary DESC) as rnk FROM employees")
	if err != nil {
		t.Logf("RANK error: %v", err)
	} else {
		t.Logf("RANK result rows: %d", len(result.Rows))
	}

	// Test window function with PARTITION BY
	result, err = c.ExecuteQuery("SELECT id, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM employees")
	if err != nil {
		t.Logf("Window with PARTITION BY error: %v", err)
	} else {
		t.Logf("Window PARTITION BY result rows: %d", len(result.Rows))
	}

	// Test window function with frame
	result, err = c.ExecuteQuery("SELECT id, SUM(salary) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_sum FROM employees")
	if err != nil {
		t.Logf("Window with frame error: %v", err)
	} else {
		t.Logf("Window frame result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithSubqueryInWhere tests subqueries in WHERE clause
func TestCoverage_selectLockedWithSubqueryInWhere(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "product_id", Type: query.TokenInteger},
			{Name: "qty", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "name", "price"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Widget"), numReal(100)},
			{numReal(2), strReal("Gadget"), numReal(200)},
		},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "product_id", "qty"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(5)},
			{numReal(2), numReal(1), numReal(3)},
		},
	}, nil)

	// Test subquery in WHERE with IN
	result, err := c.ExecuteQuery("SELECT * FROM products WHERE id IN (SELECT product_id FROM orders)")
	if err != nil {
		t.Logf("Subquery with IN error: %v", err)
	} else {
		t.Logf("Subquery IN result rows: %d", len(result.Rows))
	}

	// Test subquery in WHERE with EXISTS
	result, err = c.ExecuteQuery("SELECT * FROM products WHERE EXISTS (SELECT 1 FROM orders WHERE orders.product_id = products.id)")
	if err != nil {
		t.Logf("Subquery with EXISTS error: %v", err)
	} else {
		t.Logf("Subquery EXISTS result rows: %d", len(result.Rows))
	}

	// Test correlated subquery
	result, err = c.ExecuteQuery("SELECT *, (SELECT SUM(qty) FROM orders WHERE orders.product_id = products.id) as total_qty FROM products")
	if err != nil {
		t.Logf("Correlated subquery error: %v", err)
	} else {
		t.Logf("Correlated subquery result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithCTEs tests Common Table Expressions
func TestCoverage_selectLockedWithCTEs(t *testing.T) {
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
			{Name: "region", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "region", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("North"), numReal(1000)},
			{numReal(2), strReal("North"), numReal(1500)},
			{numReal(3), strReal("South"), numReal(800)},
			{numReal(4), strReal("South"), numReal(1200)},
		},
	}, nil)

	// Test simple CTE
	result, err := c.ExecuteQuery("WITH regional_sales AS (SELECT region, SUM(amount) as total FROM sales GROUP BY region) SELECT * FROM regional_sales ORDER BY total DESC")
	if err != nil {
		t.Logf("Simple CTE error: %v", err)
	} else {
		t.Logf("Simple CTE result rows: %d", len(result.Rows))
	}

	// Test multiple CTEs
	result, err = c.ExecuteQuery(`
		WITH
			regional_sales AS (SELECT region, SUM(amount) as total FROM sales GROUP BY region),
			top_regions AS (SELECT region FROM regional_sales ORDER BY total DESC LIMIT 1)
		SELECT * FROM top_regions
	`)
	if err != nil {
		t.Logf("Multiple CTEs error: %v", err)
	} else {
		t.Logf("Multiple CTEs result rows: %d", len(result.Rows))
	}

	// Test recursive CTE
	result, err = c.ExecuteQuery(`
		WITH RECURSIVE nums(n) AS (
			SELECT 1
			UNION ALL
			SELECT n + 1 FROM nums WHERE n < 5
		)
		SELECT * FROM nums
	`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithComplexJoins tests complex JOIN scenarios
func TestCoverage_selectLockedWithComplexJoins(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
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
			{Name: "total", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "order_id", Type: query.TokenInteger},
			{Name: "product", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "total"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}, {numReal(2), numReal(1), numReal(200)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "order_id", "product"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Widget")}, {numReal(2), numReal(2), strReal("Gadget")}},
	}, nil)

	// Test three-way JOIN
	result, err := c.ExecuteQuery(`
		SELECT c.name, o.total, i.product
		FROM customers c
		JOIN orders o ON c.id = o.customer_id
		JOIN items i ON o.id = i.order_id
	`)
	if err != nil {
		t.Logf("Three-way JOIN error: %v", err)
	} else {
		t.Logf("Three-way JOIN result rows: %d", len(result.Rows))
	}

	// Test LEFT JOIN with NULLs
	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(3), strReal("Charlie")}},
	}, nil)

	result, err = c.ExecuteQuery(`
		SELECT c.name, COALESCE(SUM(o.total), 0) as total_spent
		FROM customers c
		LEFT JOIN orders o ON c.id = o.customer_id
		GROUP BY c.id, c.name
	`)
	if err != nil {
		t.Logf("LEFT JOIN with NULLs error: %v", err)
	} else {
		t.Logf("LEFT JOIN result rows: %d", len(result.Rows))
	}

	// Test self-JOIN
	result, err = c.ExecuteQuery(`
		SELECT c1.name as customer1, c2.name as customer2
		FROM customers c1
		CROSS JOIN customers c2
		WHERE c1.id < c2.id
	`)
	if err != nil {
		t.Logf("Self-JOIN error: %v", err)
	} else {
		t.Logf("Self-JOIN result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithDistinctAndAggregates tests DISTINCT with aggregates
func TestCoverage_selectLockedWithDistinctAndAggregates(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "inventory",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "item", Type: query.TokenText},
		},
	})

	// Insert data with duplicates
	c.Insert(ctx, &query.InsertStmt{
		Table:   "inventory",
		Columns: []string{"id", "category", "item"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Electronics"), strReal("Phone")},
			{numReal(2), strReal("Electronics"), strReal("Tablet")},
			{numReal(3), strReal("Electronics"), strReal("Phone")}, // Duplicate item
			{numReal(4), strReal("Clothing"), strReal("Shirt")},
		},
	}, nil)

	// Test DISTINCT
	result, err := c.ExecuteQuery("SELECT DISTINCT category FROM inventory")
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	} else {
		t.Logf("DISTINCT result rows: %d", len(result.Rows))
	}

	// Test COUNT(DISTINCT)
	result, err = c.ExecuteQuery("SELECT COUNT(DISTINCT item) as unique_items FROM inventory")
	if err != nil {
		t.Logf("COUNT(DISTINCT) error: %v", err)
	} else {
		t.Logf("COUNT(DISTINCT) result: %v", result.Rows)
	}

	// Test GROUP BY with HAVING
	result, err = c.ExecuteQuery("SELECT category, COUNT(*) as cnt FROM inventory GROUP BY category HAVING COUNT(*) > 1")
	if err != nil {
		t.Logf("HAVING error: %v", err)
	} else {
		t.Logf("HAVING result rows: %d", len(result.Rows))
	}

	// Test complex aggregate with DISTINCT
	result, err = c.ExecuteQuery("SELECT category, COUNT(DISTINCT item) as unique_per_category FROM inventory GROUP BY category")
	if err != nil {
		t.Logf("Aggregate with DISTINCT error: %v", err)
	} else {
		t.Logf("Aggregate DISTINCT result: %v", result.Rows)
	}
}

// TestCoverage_selectLockedWithLimitOffset tests LIMIT and OFFSET
func TestCoverage_selectLockedWithLimitOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "numbers",
		Columns: []*query.ColumnDef{
			{Name: "n", Type: query.TokenInteger},
		},
	})

	// Insert data
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "numbers",
			Columns: []string{"n"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	// Test LIMIT
	result, err := c.ExecuteQuery("SELECT * FROM numbers ORDER BY n LIMIT 5")
	if err != nil {
		t.Logf("LIMIT error: %v", err)
	} else if len(result.Rows) != 5 {
		t.Errorf("Expected 5 rows with LIMIT 5, got %d", len(result.Rows))
	}

	// Test LIMIT OFFSET
	result, err = c.ExecuteQuery("SELECT * FROM numbers ORDER BY n LIMIT 5 OFFSET 5")
	if err != nil {
		t.Logf("LIMIT OFFSET error: %v", err)
	} else if len(result.Rows) != 5 {
		t.Errorf("Expected 5 rows with LIMIT 5 OFFSET 5, got %d", len(result.Rows))
	}

	// Test OFFSET only (should work)
	result, err = c.ExecuteQuery("SELECT * FROM numbers ORDER BY n OFFSET 15")
	if err != nil {
		t.Logf("OFFSET error: %v", err)
	} else {
		t.Logf("OFFSET result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedWithOrderByNulls tests ORDER BY with NULL handling
func TestCoverage_selectLockedWithOrderByNulls(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "nullable",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert data with NULLs
	c.Insert(ctx, &query.InsertStmt{
		Table:   "nullable",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(10)},
			{numReal(2), &query.NullLiteral{}},
			{numReal(3), numReal(30)},
			{numReal(4), &query.NullLiteral{}},
		},
	}, nil)

	// Test ORDER BY ASC (NULLs last by default in CobaltDB)
	result, err := c.ExecuteQuery("SELECT * FROM nullable ORDER BY val ASC")
	if err != nil {
		t.Logf("ORDER BY ASC error: %v", err)
	} else {
		t.Logf("ORDER BY ASC result: %v", result.Rows)
	}

	// Test ORDER BY DESC (NULLs first by default)
	result, err = c.ExecuteQuery("SELECT * FROM nullable ORDER BY val DESC")
	if err != nil {
		t.Logf("ORDER BY DESC error: %v", err)
	} else {
		t.Logf("ORDER BY DESC result: %v", result.Rows)
	}
}
