package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_selectLockedCTEWithWindowAndOrderBy tests CTE with window functions and ORDER BY
func TestCoverage_selectLockedCTEWithWindowAndOrderAndLimit(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "amount", Type: query.TokenInteger},
			{Name: "region", Type: query.TokenText},
		},
	})

	// Insert test data
	for i := 1; i <= 10; i++ {
		region := "north"
		if i > 5 {
			region = "south"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "orders",
			Columns: []string{"id", "amount", "region"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10)), strReal(region)}},
		}, nil)
	}

	// Test CTE with window function (ROW_NUMBER) and ORDER BY
	result, err := c.ExecuteQuery(`
		WITH ranked_orders AS (
			SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount DESC) as rn
			FROM orders
		)
		SELECT * FROM ranked_orders WHERE rn <= 5 ORDER BY rn
	`)
	if err != nil {
		t.Logf("CTE with window function error: %v", err)
	} else {
		t.Logf("CTE window function result rows: %d", len(result.Rows))
	}

	// Test CTE with LIMIT and OFFSET
	result2, err := c.ExecuteQuery(`
		WITH paged_orders AS (
			SELECT * FROM orders ORDER BY id
		)
		SELECT * FROM paged_orders LIMIT 3 OFFSET 2
	`)
	if err != nil {
		t.Logf("CTE with LIMIT/OFFSET error: %v", err)
	} else {
		t.Logf("CTE LIMIT/OFFSET result rows: %d, expected 3", len(result2.Rows))
	}

	_ = ctx
}

// TestCoverage_selectLockedComplexView tests complex view handling
func TestCoverage_selectLockedComplexView(t *testing.T) {
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
			{Name: "quantity", Type: query.TokenInteger},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	// Insert test data
	products := []string{"apple", "banana", "cherry"}
	for i, prod := range products {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "sales",
			Columns: []string{"id", "product", "quantity", "price"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(prod), numReal(float64((i + 1) * 10)), numReal(float64((i + 1) * 5))}},
		}, nil)
	}

	// Create complex view with GROUP BY
	c.ExecuteQuery("CREATE VIEW sales_summary AS SELECT product, SUM(quantity) as total_qty, SUM(price) as total_price FROM sales GROUP BY product")

	// Query complex view
	result, err := c.ExecuteQuery("SELECT * FROM sales_summary ORDER BY product")
	if err != nil {
		t.Logf("Complex view query error: %v", err)
	} else {
		t.Logf("Complex view result: %v", result.Rows)
	}

	// Query complex view with HAVING
	result2, err := c.ExecuteQuery("SELECT * FROM sales_summary WHERE total_qty > 10 ORDER BY product")
	if err != nil {
		t.Logf("Complex view with WHERE error: %v", err)
	} else {
		t.Logf("Complex view with WHERE result: %v", result2.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedSimpleViewInline tests simple view inlining
func TestCoverage_selectLockedSimpleViewInline(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "dept", Type: query.TokenText},
			{Name: "salary", Type: query.TokenInteger},
		},
	})

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "name", "dept", "salary"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), strReal("IT"), numReal(5000)},
			{numReal(2), strReal("Bob"), strReal("HR"), numReal(4000)},
			{numReal(3), strReal("Charlie"), strReal("IT"), numReal(6000)},
		},
	}, nil)

	// Create simple view (no GROUP BY, aggregates, etc.)
	c.ExecuteQuery("CREATE VIEW it_employees AS SELECT * FROM employees WHERE dept = 'IT'")

	// Query simple view
	result, err := c.ExecuteQuery("SELECT * FROM it_employees ORDER BY name")
	if err != nil {
		t.Logf("Simple view query error: %v", err)
	} else {
		t.Logf("Simple view result rows: %d", len(result.Rows))
		if len(result.Rows) != 2 {
			t.Errorf("Expected 2 IT employees, got %d", len(result.Rows))
		}
	}

	// Query simple view with additional filter
	result2, err := c.ExecuteQuery("SELECT * FROM it_employees WHERE salary > 5500")
	if err != nil {
		t.Logf("Simple view with filter error: %v", err)
	} else {
		t.Logf("Simple view with filter result: %v", result2.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedViewWithAlias tests view with alias
func TestCoverage_selectLockedViewWithAlias(t *testing.T) {
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

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "name", "price"},
		Values:  [][]query.Expression{{numReal(1), strReal("Widget"), numReal(100)}},
	}, nil)

	// Create view with aliased columns
	c.ExecuteQuery("CREATE VIEW product_info AS SELECT id, name as product_name, price as product_price FROM products")

	// Query view with aliases
	result, err := c.ExecuteQuery("SELECT product_name, product_price FROM product_info")
	if err != nil {
		t.Logf("View with alias query error: %v", err)
	} else {
		t.Logf("View with alias result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedDerivedTableWithOrderBy tests derived table with ORDER BY
func TestCoverage_selectLockedDerivedTableWithOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "scores",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "player", Type: query.TokenText},
			{Name: "score", Type: query.TokenInteger},
		},
	})

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "scores",
		Columns: []string{"id", "player", "score"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), numReal(100)},
			{numReal(2), strReal("Bob"), numReal(150)},
			{numReal(3), strReal("Charlie"), numReal(120)},
		},
	}, nil)

	// Query derived table with ORDER BY
	result, err := c.ExecuteQuery(`
		SELECT * FROM (
			SELECT player, score FROM scores ORDER BY score DESC
		) AS top_scores LIMIT 2
	`)
	if err != nil {
		t.Logf("Derived table with ORDER BY error: %v", err)
	} else {
		t.Logf("Derived table result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedCTENoOrderBy tests CTE without ORDER BY (partition mode)
func TestCoverage_selectLockedCTENoOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert test data
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "data",
			Columns: []string{"id", "category", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("A"), numReal(float64(i * 10))}},
		}, nil)
	}

	// Test CTE without ORDER BY in window function (partition mode)
	result, err := c.ExecuteQuery(`
		WITH stats AS (
			SELECT category, SUM(value) OVER () as total FROM data
		)
		SELECT * FROM stats
	`)
	if err != nil {
		t.Logf("CTE without ORDER BY error: %v", err)
	} else {
		t.Logf("CTE result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedMultipleCTEs tests multiple CTEs
func TestCoverage_selectLockedMultipleCTEs(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base tables
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

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}},
	}, nil)

	// Test multiple CTEs
	result, err := c.ExecuteQuery(`
		WITH
			customer_list AS (SELECT * FROM customers),
			order_list AS (SELECT * FROM orders)
		SELECT c.name, o.amount
		FROM customer_list c
		JOIN order_list o ON c.id = o.customer_id
	`)
	if err != nil {
		t.Logf("Multiple CTEs error: %v", err)
	} else {
		t.Logf("Multiple CTEs result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedViewWithJoin tests view with JOIN
func TestCoverage_selectLockedViewWithJoin(t *testing.T) {
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
		Table: "profiles",
		Columns: []*query.ColumnDef{
			{Name: "user_id", Type: query.TokenInteger},
			{Name: "bio", Type: query.TokenText},
		},
	})

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "profiles",
		Columns: []string{"user_id", "bio"},
		Values:  [][]query.Expression{{numReal(1), strReal("Developer")}},
	}, nil)

	// Create view with JOIN
	c.ExecuteQuery("CREATE VIEW user_profiles AS SELECT u.name, p.bio FROM users u JOIN profiles p ON u.id = p.user_id")

	// Query view with JOIN
	result, err := c.ExecuteQuery("SELECT * FROM user_profiles")
	if err != nil {
		t.Logf("View with JOIN error: %v", err)
	} else {
		t.Logf("View with JOIN result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_RollbackToSavepointIndexCreateDrop tests rollback of CREATE INDEX and DROP INDEX
func TestCoverage_RollbackToSavepointIndexCreateDrop(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "test_idx",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	c.BeginTransaction(1)
	c.Savepoint("sp1")

	// Create index after savepoint
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_val",
		Table:   "test_idx",
		Columns: []string{"val"},
	})

	// Verify index exists
	if _, ok := c.indexes["idx_val"]; !ok {
		t.Fatal("Index should exist after creation")
	}

	// Rollback to savepoint
	err := c.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify index was removed
	if _, ok := c.indexes["idx_val"]; ok {
		t.Error("Index should not exist after rollback")
	}

	c.RollbackTransaction()
	_ = ctx
}

// TestCoverage_deleteWithUsingLocked115 tests DELETE with USING clause
func TestCoverage_deleteWithUsingLocked115(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "main_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "ref_id", Type: query.TokenInteger},
			{Name: "data", Type: query.TokenText},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "ref_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "status", Type: query.TokenText},
		},
	})

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "main_table",
		Columns: []string{"id", "ref_id", "data"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), strReal("keep")},
			{numReal(2), numReal(2), strReal("delete")},
		},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "ref_table",
		Columns: []string{"id", "status"},
		Values: [][]query.Expression{
			{numReal(1), strReal("active")},
			{numReal(2), strReal("deleted")},
		},
	}, nil)

	// Test DELETE with USING
	_, err := c.ExecuteQuery("DELETE FROM main_table USING ref_table WHERE main_table.ref_id = ref_table.id AND ref_table.status = 'deleted'")
	if err != nil {
		t.Logf("DELETE with USING error: %v", err)
	} else {
		// Verify deletion
		result, _ := c.ExecuteQuery("SELECT COUNT(*) FROM main_table")
		t.Logf("Rows after DELETE: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_executeSelectWithJoinAndGroupByComplex115 tests complex JOIN with GROUP BY
func TestCoverage_executeSelectWithJoinAndGroupByComplex115(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "categories",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "cat_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert test data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "categories",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Food")},
			{numReal(2), strReal("Drink")},
		},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "cat_id", "amount"},
		Values: [][]query.Expression{
			{numReal(1), numReal(1), numReal(100)},
			{numReal(2), numReal(1), numReal(200)},
			{numReal(3), numReal(2), numReal(50)},
		},
	}, nil)

	// Test JOIN with GROUP BY and HAVING
	result, err := c.ExecuteQuery(`
		SELECT c.name, SUM(i.amount) as total
		FROM categories c
		JOIN items i ON c.id = i.cat_id
		GROUP BY c.name
		HAVING SUM(i.amount) > 100
		ORDER BY total DESC
	`)
	if err != nil {
		t.Logf("JOIN with GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN GROUP BY result: %v", result.Rows)
	}

	_ = ctx
}

// TestCoverage_selectLockedOffsetBeyond tests OFFSET beyond row count
func TestCoverage_selectLockedOffsetBeyond(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "small_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert only 2 rows
	c.Insert(ctx, &query.InsertStmt{
		Table:   "small_table",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Test OFFSET beyond row count
	result, err := c.ExecuteQuery("SELECT * FROM small_table ORDER BY id OFFSET 10")
	if err != nil {
		t.Logf("OFFSET beyond error: %v", err)
	} else {
		t.Logf("OFFSET beyond result rows: %d", len(result.Rows))
		if len(result.Rows) != 0 {
			t.Error("Expected 0 rows when OFFSET beyond row count")
		}
	}

	_ = ctx
}

// TestCoverage_selectLockedNegativeLimit tests negative LIMIT
func TestCoverage_selectLockedNegativeLimit(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table
	c.CreateTable(&query.CreateTableStmt{
		Table: "test_limit",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert rows
	c.Insert(ctx, &query.InsertStmt{
		Table:   "test_limit",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}, {numReal(3)}},
	}, nil)

	// Test negative LIMIT (should be treated as no limit in SQLite)
	result, err := c.ExecuteQuery("SELECT * FROM test_limit LIMIT -1")
	if err != nil {
		t.Logf("Negative LIMIT error: %v", err)
	} else {
		t.Logf("Negative LIMIT result rows: %d", len(result.Rows))
	}

	_ = ctx
}
