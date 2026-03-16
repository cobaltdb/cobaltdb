package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_selectLockedComplexViewWithJoins tests complex views with JOINs
func TestCoverage_selectLockedComplexViewWithJoins(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base tables
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
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "user_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}, {numReal(2), numReal(1), numReal(200)}},
	}, nil)

	// Create a view with GROUP BY (complex view)
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "user_id"},
			&query.AliasExpr{
				Alias: "total_amount",
				Expr:  &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
		},
		From:    &query.TableRef{Name: "orders"},
		GroupBy: []query.Expression{&query.Identifier{Name: "user_id"}},
	}
	c.CreateView("user_order_totals", viewQuery)

	// Test: Complex view with JOIN to another table
	result, err := c.ExecuteQuery(`
		SELECT u.name, v.total_amount
		FROM users u
		JOIN user_order_totals v ON u.id = v.user_id
	`)
	if err != nil {
		t.Logf("Complex view with JOIN error: %v", err)
	} else {
		t.Logf("Complex view with JOIN result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedViewInlining tests simple view inlining
func TestCoverage_selectLockedViewInlining(t *testing.T) {
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
			{Name: "category", Type: query.TokenText},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "name", "category", "price"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Widget"), strReal("A"), numReal(10)},
			{numReal(2), strReal("Gadget"), strReal("A"), numReal(20)},
			{numReal(3), strReal("Tool"), strReal("B"), numReal(30)},
		},
	}, nil)

	// Create simple view (no GROUP BY, aggregates, etc.)
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
			&query.Identifier{Name: "price"},
		},
		From: &query.TableRef{Name: "products"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "category"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "A"},
		},
	}
	c.CreateView("category_a_products", viewQuery)

	// Test: Simple view with outer query filter
	result, err := c.ExecuteQuery(`
		SELECT * FROM category_a_products WHERE price > 15
	`)
	if err != nil {
		t.Logf("Simple view with WHERE error: %v", err)
	} else {
		t.Logf("Simple view with WHERE result rows: %d", len(result.Rows))
	}

	// Test: Simple view with alias
	result, err = c.ExecuteQuery(`
		SELECT ap.name, ap.price
		FROM category_a_products ap
		WHERE ap.price > 5
	`)
	if err != nil {
		t.Logf("Simple view with alias error: %v", err)
	} else {
		t.Logf("Simple view with alias result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedDerivedTableWithJoins tests derived tables with JOINs
func TestCoverage_selectLockedDerivedTableWithJoins(t *testing.T) {
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
			{Name: "total", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "total"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Widget")}},
	}, nil)

	// Test: Derived table with JOIN to another table
	result, err := c.ExecuteQuery(`
		SELECT d.name, d.order_count, p.name as product_name
		FROM (
			SELECT c.name, COUNT(*) as order_count
			FROM customers c
			JOIN orders o ON c.id = o.customer_id
			GROUP BY c.id, c.name
		) d
		CROSS JOIN products p
	`)
	if err != nil {
		t.Logf("Derived table with CROSS JOIN error: %v", err)
	} else {
		t.Logf("Derived table with CROSS JOIN result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedCTEWithJoins tests CTEs with JOINs
func TestCoverage_selectLockedCTEWithJoins(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "dept_id", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_name", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "name", "dept_id"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice"), numReal(1)}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "dept_name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Engineering")}},
	}, nil)

	// Test: CTE with JOIN to regular table
	result, err := c.ExecuteQuery(`
		WITH dept_employees AS (
			SELECT id, name, dept_id FROM employees WHERE dept_id = 1
		)
		SELECT d.dept_name, e.name
		FROM dept_employees e
		JOIN departments d ON e.dept_id = d.id
	`)
	if err != nil {
		t.Logf("CTE with JOIN error: %v", err)
	} else {
		t.Logf("CTE with JOIN result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedWindowFuncsFromCTE tests window functions on CTE results
func TestCoverage_selectLockedWindowFuncsFromCTE(t *testing.T) {
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
			{Name: "points", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "scores",
		Columns: []string{"id", "player", "points"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), numReal(100)},
			{numReal(2), strReal("Bob"), numReal(200)},
			{numReal(3), strReal("Carol"), numReal(150)},
		},
	}, nil)

	// Test: Window function on CTE result
	result, err := c.ExecuteQuery(`
		WITH ranked_scores AS (
			SELECT player, points FROM scores
		)
		SELECT player, points, ROW_NUMBER() OVER (ORDER BY points DESC) as rank
		FROM ranked_scores
	`)
	if err != nil {
		t.Logf("Window function on CTE error: %v", err)
	} else {
		t.Logf("Window function on CTE result rows: %d", len(result.Rows))
	}

	// Test: Window function with PARTITION BY on CTE
	result, err = c.ExecuteQuery(`
		WITH categorized AS (
			SELECT player, points, CASE WHEN points > 150 THEN 'high' ELSE 'low' END as category
			FROM scores
		)
		SELECT player, points, category,
		       RANK() OVER (PARTITION BY category ORDER BY points DESC) as category_rank
		FROM categorized
	`)
	if err != nil {
		t.Logf("Window function with PARTITION BY on CTE error: %v", err)
	} else {
		t.Logf("Window function with PARTITION BY on CTE result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedSimpleViewWithStar tests simple view with SELECT *
func TestCoverage_selectLockedSimpleViewWithStar(t *testing.T) {
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
		Values:  [][]query.Expression{{numReal(1), strReal("item1"), numReal(100)}},
	}, nil)

	// Create simple view
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
			&query.Identifier{Name: "value"},
		},
		From: &query.TableRef{Name: "items"},
	}
	c.CreateView("all_items", viewQuery)

	// Test: SELECT * on simple view
	result, err := c.ExecuteQuery(`SELECT * FROM all_items`)
	if err != nil {
		t.Logf("SELECT * on simple view error: %v", err)
	} else {
		t.Logf("SELECT * on simple view result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedViewWithWhereMerging tests WHERE clause merging
func TestCoverage_selectLockedViewWithWhereMerging(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "inventory",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "item", Type: query.TokenText},
			{Name: "quantity", Type: query.TokenInteger},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "inventory",
		Columns: []string{"id", "item", "quantity", "price"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(10), numReal(5)},
			{numReal(2), strReal("B"), numReal(5), numReal(10)},
			{numReal(3), strReal("C"), numReal(20), numReal(3)},
		},
	}, nil)

	// Create simple view with WHERE
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "item"},
			&query.Identifier{Name: "quantity"},
			&query.Identifier{Name: "price"},
		},
		From: &query.TableRef{Name: "inventory"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "quantity"},
			Operator: query.TokenLt,
			Right:    &query.NumberLiteral{Value: 15},
		},
	}
	c.CreateView("low_stock", viewQuery)

	// Test: View with additional WHERE (should merge conditions with AND)
	result, err := c.ExecuteQuery(`SELECT * FROM low_stock WHERE price < 10`)
	if err != nil {
		t.Logf("View with merged WHERE error: %v", err)
	} else {
		t.Logf("View with merged WHERE result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedComplexViewWithAggregate tests complex view with aggregate
func TestCoverage_selectLockedComplexViewWithAggregate(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "transactions",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "account", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "transactions",
		Columns: []string{"id", "account", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), numReal(100)},
			{numReal(2), strReal("A"), numReal(200)},
			{numReal(3), strReal("B"), numReal(300)},
		},
	}, nil)

	// Create view with aggregate (complex view)
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "account"},
			&query.AliasExpr{
				Alias: "total",
				Expr:  &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
		},
		From:    &query.TableRef{Name: "transactions"},
		GroupBy: []query.Expression{&query.Identifier{Name: "account"}},
	}
	c.CreateView("account_summary", viewQuery)

	// Test: Complex view with outer aggregate
	result, err := c.ExecuteQuery(`
		SELECT COUNT(*) as num_accounts, AVG(total) as avg_total
		FROM account_summary
	`)
	if err != nil {
		t.Logf("Complex view with outer aggregate error: %v", err)
	} else {
		t.Logf("Complex view with outer aggregate result rows: %d", len(result.Rows))
		if len(result.Rows) > 0 {
			t.Logf("Result: %v", result.Rows[0])
		}
	}
}

// TestCoverage_selectLockedViewWithAliasPreservation tests alias preservation in view
func TestCoverage_selectLockedViewWithAliasPreservation(t *testing.T) {
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
			{Name: "value", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "data",
		Columns: []string{"id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)

	// Create simple view
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "value"},
		},
		From: &query.TableRef{Name: "data"},
	}
	c.CreateView("data_view", viewQuery)

	// Test: Query view with different alias
	result, err := c.ExecuteQuery(`
		SELECT dv.id, dv.value FROM data_view dv
	`)
	if err != nil {
		t.Logf("View with different alias error: %v", err)
	} else {
		t.Logf("View with different alias result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedViewWithJoins tests view with its own JOINs
func TestCoverage_selectLockedViewWithJoins(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "authors",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "books",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "author_id", Type: query.TokenInteger},
			{Name: "title", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "authors",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Author1")}},
	}, nil)

	c.Insert(ctx, &query.InsertStmt{
		Table:   "books",
		Columns: []string{"id", "author_id", "title"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Book1")}},
	}, nil)

	// Create simple view with JOIN
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "name"},
			&query.Identifier{Name: "title"},
		},
		From: &query.TableRef{Name: "authors"},
		Joins: []*query.JoinClause{
			{
				Table: &query.TableRef{Name: "books"},
				Type:  query.TokenInner,
				Condition: &query.BinaryExpr{
					Left:     &query.QualifiedIdentifier{Table: "authors", Column: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "books", Column: "author_id"},
				},
			},
		},
	}
	c.CreateView("author_books", viewQuery)

	// Test: Query view that has its own JOINs
	result, err := c.ExecuteQuery(`SELECT * FROM author_books`)
	if err != nil {
		t.Logf("View with JOINs error: %v", err)
	} else {
		t.Logf("View with JOINs result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedViewWithWhereMergingOr tests WHERE merging with OR case
func TestCoverage_selectLockedViewWithWhereMergingOr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "records",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "type", Type: query.TokenText},
			{Name: "status", Type: query.TokenText},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "records",
		Columns: []string{"id", "type", "status"},
		Values: [][]query.Expression{
			{numReal(1), strReal("A"), strReal("active")},
			{numReal(2), strReal("A"), strReal("inactive")},
			{numReal(3), strReal("B"), strReal("active")},
		},
	}, nil)

	// Create simple view without WHERE (tests view.Where == nil case)
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "type"},
			&query.Identifier{Name: "status"},
		},
		From: &query.TableRef{Name: "records"},
	}
	c.CreateView("all_records", viewQuery)

	// Test: View without WHERE but outer query has WHERE (uses stmt.Where only)
	result, err := c.ExecuteQuery(`SELECT * FROM all_records WHERE type = 'A'`)
	if err != nil {
		t.Logf("View without WHERE, outer with WHERE error: %v", err)
	} else {
		t.Logf("View without WHERE, outer with WHERE result rows: %d", len(result.Rows))
	}
}

// TestCoverage_selectLockedComplexViewOnly tests complex view without JOIN
func TestCoverage_selectLockedComplexViewOnly(t *testing.T) {
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
			{Name: "region", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	c.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "region", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("North"), numReal(100)},
			{numReal(2), strReal("North"), numReal(200)},
			{numReal(3), strReal("South"), numReal(150)},
		},
	}, nil)

	// Create complex view (has GROUP BY)
	viewQuery := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "region"},
			&query.AliasExpr{
				Alias: "total_sales",
				Expr:  &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
		},
		From:    &query.TableRef{Name: "sales"},
		GroupBy: []query.Expression{&query.Identifier{Name: "region"}},
	}
	c.CreateView("sales_by_region", viewQuery)

	// Test: Complex view without JOIN (uses applyOuterQuery path)
	result, err := c.ExecuteQuery(`SELECT * FROM sales_by_region WHERE total_sales > 150`)
	if err != nil {
		t.Logf("Complex view without JOIN error: %v", err)
	} else {
		t.Logf("Complex view without JOIN result rows: %d", len(result.Rows))
	}
}
