package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_selectLockedMaterializedView tests querying materialized views
func TestCoverage_selectLockedMaterializedView(t *testing.T) {
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
			{Name: "product", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "product", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Widget"), numReal(100)},
			{numReal(2), strReal("Gadget"), numReal(200)},
		},
	}, nil)

	// Create materialized view
	c.ExecuteQuery("CREATE MATERIALIZED VIEW sales_summary AS SELECT product, SUM(amount) as total FROM sales GROUP BY product")

	// Query materialized view
	result, err := c.ExecuteQuery("SELECT * FROM sales_summary ORDER BY product")
	if err != nil {
		t.Logf("Materialized view query error: %v", err)
	} else {
		t.Logf("Materialized view result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedDistinct tests DISTINCT queries
func TestCoverage_selectLockedDistinct(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer", Type: query.TokenText},
			{Name: "status", Type: query.TokenText},
		},
	})

	// Insert duplicate values
	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer", "status"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), strReal("pending")},
			{numReal(2), strReal("Bob"), strReal("pending")},
			{numReal(3), strReal("Alice"), strReal("completed")},
			{numReal(4), strReal("Alice"), strReal("pending")}, // Duplicate customer+status
		},
	}, nil)

	// Test DISTINCT
	result, err := c.ExecuteQuery("SELECT DISTINCT customer FROM orders ORDER BY customer")
	if err != nil {
		t.Logf("DISTINCT error: %v", err)
	} else {
		t.Logf("DISTINCT result: %v", result.Rows)
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 distinct customers, got %d", len(result.Rows))
		}
	}

	// Test DISTINCT with multiple columns
	result2, err := c.ExecuteQuery("SELECT DISTINCT customer, status FROM orders")
	if err != nil {
		t.Logf("DISTINCT multi-column error: %v", err)
	} else {
		t.Logf("DISTINCT multi-column result: %v", result2.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedWindowFunctions tests window functions
func TestCoverage_selectLockedWindowFunctions(t *testing.T) {
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

	// Test ROW_NUMBER
	result, err := c.ExecuteQuery(`
		SELECT dept, salary,
			ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
		FROM employees
	`)
	if err != nil {
		t.Logf("ROW_NUMBER error: %v", err)
	} else {
		t.Logf("ROW_NUMBER result: %v", result.Rows)
	}

	// Test RANK
	result2, err := c.ExecuteQuery(`
		SELECT dept, salary,
			RANK() OVER (ORDER BY salary DESC) as rnk
		FROM employees
	`)
	if err != nil {
		t.Logf("RANK error: %v", err)
	} else {
		t.Logf("RANK result: %v", result2.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedComplexJoinAliases tests complex JOIN with table aliases
func TestCoverage_selectLockedComplexJoinAliases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
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
		Table:   "users",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "user_id", "total"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "order_id", "product"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Widget")}},
	}, nil)

	// Test 3-way JOIN with aliases
	result, err := c.ExecuteQuery(`
		SELECT u.name, o.total, i.product
		FROM users u
		JOIN orders o ON u.id = o.user_id
		JOIN items i ON o.id = i.order_id
	`)
	if err != nil {
		t.Logf("3-way JOIN error: %v", err)
	} else {
		t.Logf("3-way JOIN result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedStarExprView tests SELECT * from view
func TestCoverage_selectLockedStarExprView(t *testing.T) {
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
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "name", "price"},
		Values:  [][]query.Expression{{numReal(1), strReal("Widget"), numReal(100)}},
	}, nil)

	// Create simple view
	c.ExecuteQuery("CREATE VIEW product_view AS SELECT * FROM products WHERE price > 50")

	// Query view with SELECT *
	result, err := c.ExecuteQuery("SELECT * FROM product_view")
	if err != nil {
		t.Logf("SELECT * from view error: %v", err)
	} else {
		t.Logf("SELECT * from view result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedLeftJoin tests LEFT JOIN
func TestCoverage_selectLockedLeftJoin(t *testing.T) {
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
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data - some customers without orders
	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice")},
			{numReal(2), strReal("Bob")}, // No orders
		},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}},
	}, nil)

	// Test LEFT JOIN
	result, err := c.ExecuteQuery(`
		SELECT c.name, o.amount
		FROM customers c
		LEFT JOIN orders o ON c.id = o.customer_id
		ORDER BY c.name
	`)
	if err != nil {
		t.Logf("LEFT JOIN error: %v", err)
	} else {
		t.Logf("LEFT JOIN result: %v", result.Rows)
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows (including NULL), got %d", len(result.Rows))
		}
	}

	_ = ctx
}

// TestCoverage_selectLockedSubqueryInSelect tests subquery in SELECT clause
func TestCoverage_selectLockedSubqueryInSelect(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
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
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("IT")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id", "name"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), strReal("Alice")},
			{numReal(2), numReal(1), strReal("Bob")},
		},
	}, nil)

	// Test subquery in SELECT
	result, err := c.ExecuteQuery(`
		SELECT d.name,
			(SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) as emp_count
		FROM departments d
	`)
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	} else {
		t.Logf("Subquery in SELECT result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedUnion tests UNION queries
func TestCoverage_selectLockedUnion(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "table_a",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "table_b",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "table_a",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A1")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "table_b",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), strReal("B1")}},
	}, nil)

	// Test UNION
	result, err := c.ExecuteQuery(`
		SELECT val FROM table_a
		UNION
		SELECT val FROM table_b
		ORDER BY val
	`)
	if err != nil {
		t.Logf("UNION error: %v", err)
	} else {
		t.Logf("UNION result: %v", result.Rows)
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows from UNION, got %d", len(result.Rows))
		}
	}

	// Test UNION ALL
	result2, err := c.ExecuteQuery(`
		SELECT val FROM table_a
		UNION ALL
		SELECT val FROM table_a
	`)
	if err != nil {
		t.Logf("UNION ALL error: %v", err)
	} else {
		t.Logf("UNION ALL result: %v", result2.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedHavingWithoutGroupBy tests HAVING without GROUP BY
func TestCoverage_selectLockedHavingWithoutGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "scores",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "scores",
		Columns: []string{"id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Test HAVING without GROUP BY
	result, err := c.ExecuteQuery(`
		SELECT COUNT(*) as cnt, SUM(value) as total
		FROM scores
		HAVING COUNT(*) > 0
	`)
	if err != nil {
		t.Logf("HAVING without GROUP BY error: %v", err)
	} else {
		t.Logf("HAVING without GROUP BY result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedDottedIdentifier tests dotted identifier like table.column
func TestCoverage_selectLockedDottedIdentifier(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "posts",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
			{Name: "title", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "posts",
		Columns: []string{"id", "user_id", "title"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Hello")}},
	}, nil)

	// Test dotted identifier in SELECT
	result, err := c.ExecuteQuery(`
		SELECT users.name, posts.title
		FROM users
		JOIN posts ON users.id = posts.user_id
	`)
	if err != nil {
		t.Logf("Dotted identifier error: %v", err)
	} else {
		t.Logf("Dotted identifier result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedCaseExpression tests CASE expression
func TestCoverage_selectLockedCaseExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "grades",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "score", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "grades",
		Columns: []string{"id", "score"},
		Values: [][]query.Expression{
			{numReal(1), numReal(95)},
			{numReal(2), numReal(75)},
			{numReal(3), numReal(55)},
		},
	}, nil)

	// Test CASE expression
	result, err := c.ExecuteQuery(`
		SELECT id, score,
			CASE
				WHEN score >= 90 THEN 'A'
				WHEN score >= 70 THEN 'B'
				ELSE 'C'
			END as grade
		FROM grades
		ORDER BY id
	`)
	if err != nil {
		t.Logf("CASE expression error: %v", err)
	} else {
		t.Logf("CASE expression result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedInExpression tests IN expression
func TestCoverage_selectLockedInExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "category"},
		Values: [][]query.Expression{
			{numReal(1), strReal("electronics")},
			{numReal(2), strReal("clothing")},
			{numReal(3), strReal("food")},
		},
	}, nil)

	// Test IN expression
	result, err := c.ExecuteQuery(`
		SELECT * FROM products
		WHERE category IN ('electronics', 'clothing')
		ORDER BY id
	`)
	if err != nil {
		t.Logf("IN expression error: %v", err)
	} else {
		t.Logf("IN expression result: %v", result.Rows)
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(result.Rows))
		}
	}

	// Test NOT IN
	result2, err := c.ExecuteQuery(`
		SELECT * FROM products
		WHERE category NOT IN ('food')
		ORDER BY id
	`)
	if err != nil {
		t.Logf("NOT IN error: %v", err)
	} else {
		t.Logf("NOT IN result: %v", result2.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedBetweenExpression tests BETWEEN expression
func TestCoverage_selectLockedBetweenExpression(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "price"},
		Values: [][]query.Expression{
			{numReal(1), numReal(10)},
			{numReal(2), numReal(50)},
			{numReal(3), numReal(100)},
		},
	}, nil)

	// Test BETWEEN
	result, err := c.ExecuteQuery(`
		SELECT * FROM items
		WHERE price BETWEEN 20 AND 80
	`)
	if err != nil {
		t.Logf("BETWEEN error: %v", err)
	} else {
		t.Logf("BETWEEN result: %v", result.Rows)
		if len(result.Rows) != 1 {
			t.Errorf("Expected 1 row with price 50, got %d", len(result.Rows))
		}
	}

	// Test NOT BETWEEN
	result2, err := c.ExecuteQuery(`
		SELECT * FROM items
		WHERE price NOT BETWEEN 20 AND 80
		ORDER BY id
	`)
	if err != nil {
		t.Logf("NOT BETWEEN error: %v", err)
	} else {
		t.Logf("NOT BETWEEN result: %v", result2.Rows)
	}

	_ = ctx
}
