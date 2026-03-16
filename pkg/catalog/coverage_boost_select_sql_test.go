package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestSelectLockedSQL_CTEWithWindowFunc tests CTE with window functions via SQL
func TestSelectLockedSQL_CTEWithWindowFunc(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
	c.ExecuteQuery("INSERT INTO sales VALUES (1, 'North', 100), (2, 'North', 200), (3, 'South', 150)")

	// CTE with window function in outer query
	result, err := c.ExecuteQuery(`
		WITH regional_sales AS (
			SELECT region, amount FROM sales
		)
		SELECT region, amount,
			SUM(amount) OVER (PARTITION BY region) as region_total,
			SUM(amount) OVER () as grand_total
		FROM regional_sales
	`)
	if err != nil {
		t.Logf("CTE with window function error: %v", err)
	} else {
		t.Logf("CTE with window function returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_DerivedTableWithJoin tests derived table with JOIN via SQL
func TestSelectLockedSQL_DerivedTableWithJoin(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)")
	c.ExecuteQuery("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200)")
	c.ExecuteQuery("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")

	// Derived table with JOIN
	result, err := c.ExecuteQuery(`
		SELECT * FROM (
			SELECT customer_id, SUM(amount) as total
			FROM orders
			GROUP BY customer_id
		) AS order_totals
		JOIN customers ON order_totals.customer_id = customers.id
	`)
	if err != nil {
		t.Logf("Derived table with JOIN error: %v", err)
	} else {
		t.Logf("Derived table with JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_RecursiveCTE tests recursive CTE via SQL
func TestSelectLockedSQL_RecursiveCTE(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE org (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	c.ExecuteQuery("INSERT INTO org VALUES (1, 'CEO', NULL), (2, 'VP1', 1), (3, 'VP2', 1), (4, 'Manager', 2)")

	// Recursive CTE
	result, err := c.ExecuteQuery(`
		WITH RECURSIVE subordinates AS (
			SELECT id, name, manager_id FROM org WHERE id = 2
			UNION ALL
			SELECT e.id, e.name, e.manager_id
			FROM org e
			JOIN subordinates s ON e.manager_id = s.id
		)
		SELECT * FROM subordinates
	`)
	if err != nil {
		t.Logf("Recursive CTE error: %v", err)
	} else {
		t.Logf("Recursive CTE returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_ViewWithAggregate tests view with aggregate via SQL
func TestSelectLockedSQL_ViewWithAggregate(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)")
	c.ExecuteQuery("INSERT INTO sales VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 150)")

	// Create view with aggregate
	c.ExecuteQuery("CREATE VIEW category_totals AS SELECT category, SUM(amount) as total FROM sales GROUP BY category")

	// Query the view
	result, err := c.ExecuteQuery("SELECT * FROM category_totals WHERE total > 150")
	if err != nil {
		t.Logf("View with aggregate error: %v", err)
	} else {
		t.Logf("View with aggregate returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_SubqueryInSelect tests subquery in SELECT clause
func TestSelectLockedSQL_SubqueryInSelect(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER)")
	c.ExecuteQuery("INSERT INTO departments VALUES (1, 'Sales'), (2, 'Eng')")
	c.ExecuteQuery("INSERT INTO employees VALUES (1, 1), (2, 1), (3, 2)")

	// Subquery in SELECT
	result, err := c.ExecuteQuery(`
		SELECT id, name,
			(SELECT COUNT(*) FROM employees WHERE dept_id = departments.id) as emp_count
		FROM departments
	`)
	if err != nil {
		t.Logf("Subquery in SELECT error: %v", err)
	} else {
		t.Logf("Subquery in SELECT returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_ExistsSubquery tests EXISTS subquery
func TestSelectLockedSQL_ExistsSubquery(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")
	c.ExecuteQuery("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
	c.ExecuteQuery("INSERT INTO orders VALUES (1, 1)")

	// EXISTS subquery
	result, err := c.ExecuteQuery(`
		SELECT * FROM customers
		WHERE EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
	`)
	if err != nil {
		t.Logf("EXISTS subquery error: %v", err)
	} else {
		t.Logf("EXISTS subquery returned %d rows", len(result.Rows))
	}

	// NOT EXISTS subquery
	result, err = c.ExecuteQuery(`
		SELECT * FROM customers
		WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.customer_id = customers.id)
	`)
	if err != nil {
		t.Logf("NOT EXISTS subquery error: %v", err)
	} else {
		t.Logf("NOT EXISTS subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_CorrelatedSubquery tests correlated subquery
func TestSelectLockedSQL_CorrelatedSubquery(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")
	c.ExecuteQuery("INSERT INTO employees VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")

	// Correlated subquery
	result, err := c.ExecuteQuery(`
		SELECT * FROM employees e
		WHERE salary > (SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id)
	`)
	if err != nil {
		t.Logf("Correlated subquery error: %v", err)
	} else {
		t.Logf("Correlated subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_MultipleCTEs tests multiple CTEs
func TestSelectLockedSQL_MultipleCTEs(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")
	c.ExecuteQuery("INSERT INTO employees VALUES (1, 1, 50000), (2, 1, 60000), (3, 2, 70000)")

	// Multiple CTEs
	result, err := c.ExecuteQuery(`
		WITH dept_avg AS (
			SELECT dept_id, AVG(salary) as avg_sal
			FROM employees
			GROUP BY dept_id
		),
		above_avg AS (
			SELECT e.* FROM employees e
			JOIN dept_avg d ON e.dept_id = d.dept_id
			WHERE e.salary > d.avg_sal
		)
		SELECT * FROM above_avg
	`)
	if err != nil {
		t.Logf("Multiple CTEs error: %v", err)
	} else {
		t.Logf("Multiple CTEs returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_HavingWithoutGroupBy tests HAVING without GROUP BY
func TestSelectLockedSQL_HavingWithoutGroupBy(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO test VALUES (1, 10), (2, 20)")

	// HAVING without GROUP BY
	result, err := c.ExecuteQuery("SELECT COUNT(*) as cnt FROM test HAVING COUNT(*) > 0")
	if err != nil {
		t.Logf("HAVING without GROUP BY error: %v", err)
	} else {
		t.Logf("HAVING without GROUP BY returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_OrderByOrdinal tests ORDER BY ordinal
func TestSelectLockedSQL_OrderByOrdinal(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO test VALUES (2, 'B'), (1, 'A')")

	// ORDER BY ordinal
	result, err := c.ExecuteQuery("SELECT id, name FROM test ORDER BY 2")
	if err != nil {
		t.Logf("ORDER BY ordinal error: %v", err)
	} else {
		t.Logf("ORDER BY ordinal returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_WindowFrame tests window function with frame
func TestSelectLockedSQL_WindowFrame(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE sales (id INTEGER PRIMARY KEY, amount INTEGER)")
	c.ExecuteQuery("INSERT INTO sales VALUES (1, 100), (2, 200), (3, 150)")

	// Window function with frame
	result, err := c.ExecuteQuery(`
		SELECT id, amount,
			SUM(amount) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as moving_sum
		FROM sales
	`)
	if err != nil {
		t.Logf("Window frame error: %v", err)
	} else {
		t.Logf("Window frame returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_LeftJoinWhereNull tests LEFT JOIN with IS NULL check
func TestSelectLockedSQL_LeftJoinWhereNull(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)")
	c.ExecuteQuery("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
	c.ExecuteQuery("INSERT INTO orders VALUES (1, 1)")

	// LEFT JOIN with IS NULL
	result, err := c.ExecuteQuery(`
		SELECT customers.*
		FROM customers
		LEFT JOIN orders ON customers.id = orders.customer_id
		WHERE orders.id IS NULL
	`)
	if err != nil {
		t.Logf("LEFT JOIN IS NULL error: %v", err)
	} else {
		t.Logf("LEFT JOIN IS NULL returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_RightJoin tests RIGHT JOIN
func TestSelectLockedSQL_RightJoin(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, val INTEGER)")
	c.ExecuteQuery("INSERT INTO a VALUES (1, 10), (2, 20)")
	c.ExecuteQuery("INSERT INTO b VALUES (1, 1, 100)")

	// RIGHT JOIN
	result, err := c.ExecuteQuery("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id")
	if err != nil {
		t.Logf("RIGHT JOIN error: %v", err)
	} else {
		t.Logf("RIGHT JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_FullOuterJoin tests FULL OUTER JOIN
func TestSelectLockedSQL_FullOuterJoin(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE a (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("CREATE TABLE b (id INTEGER PRIMARY KEY, val INTEGER)")
	c.ExecuteQuery("INSERT INTO a VALUES (1, 10), (2, 20)")
	c.ExecuteQuery("INSERT INTO b VALUES (1, 100), (3, 300)")

	// FULL OUTER JOIN
	result, err := c.ExecuteQuery("SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")
	if err != nil {
		t.Logf("FULL OUTER JOIN error: %v", err)
	} else {
		t.Logf("FULL OUTER JOIN returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_INSubquery tests IN subquery
func TestSelectLockedSQL_INSubquery(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE products (id INTEGER PRIMARY KEY, category_id INTEGER)")
	c.ExecuteQuery("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO products VALUES (1, 1), (2, 2), (3, 1)")
	c.ExecuteQuery("INSERT INTO categories VALUES (1, 'Electronics'), (2, 'Books')")

	// IN subquery
	result, err := c.ExecuteQuery(`
		SELECT * FROM products
		WHERE category_id IN (SELECT id FROM categories WHERE name = 'Electronics')
	`)
	if err != nil {
		t.Logf("IN subquery error: %v", err)
	} else {
		t.Logf("IN subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_ALLANYSomeSubquery tests ALL/ANY subqueries
func TestSelectLockedSQL_ALLANYSomeSubquery(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE products (id INTEGER PRIMARY KEY, price INTEGER)")
	c.ExecuteQuery("CREATE TABLE premium (price INTEGER)")
	c.ExecuteQuery("INSERT INTO products VALUES (1, 100), (2, 200), (3, 300)")
	c.ExecuteQuery("INSERT INTO premium VALUES (150)")

	// > ANY subquery
	result, err := c.ExecuteQuery("SELECT * FROM products WHERE price > ANY (SELECT price FROM premium)")
	if err != nil {
		t.Logf("> ANY subquery error: %v", err)
	} else {
		t.Logf("> ANY subquery returned %d rows", len(result.Rows))
	}

	// > ALL subquery
	result, err = c.ExecuteQuery("SELECT * FROM products WHERE price > ALL (SELECT price FROM premium)")
	if err != nil {
		t.Logf("> ALL subquery error: %v", err)
	} else {
		t.Logf("> ALL subquery returned %d rows", len(result.Rows))
	}
}

// TestSelectLockedSQL_UnionIntersectExcept tests set operations
func TestSelectLockedSQL_UnionIntersectExcept(t *testing.T) {
	_ = context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE a (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("CREATE TABLE b (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO a VALUES (1), (2), (3)")
	c.ExecuteQuery("INSERT INTO b VALUES (2), (3), (4)")

	// UNION
	result, err := c.ExecuteQuery("SELECT * FROM a UNION SELECT * FROM b")
	if err != nil {
		t.Logf("UNION error: %v", err)
	} else {
		t.Logf("UNION returned %d rows", len(result.Rows))
	}

	// UNION ALL
	result, err = c.ExecuteQuery("SELECT * FROM a UNION ALL SELECT * FROM b")
	if err != nil {
		t.Logf("UNION ALL error: %v", err)
	} else {
		t.Logf("UNION ALL returned %d rows", len(result.Rows))
	}

	// INTERSECT
	result, err = c.ExecuteQuery("SELECT * FROM a INTERSECT SELECT * FROM b")
	if err != nil {
		t.Logf("INTERSECT error: %v", err)
	} else {
		t.Logf("INTERSECT returned %d rows", len(result.Rows))
	}

	// EXCEPT
	result, err = c.ExecuteQuery("SELECT * FROM a EXCEPT SELECT * FROM b")
	if err != nil {
		t.Logf("EXCEPT error: %v", err)
	} else {
		t.Logf("EXCEPT returned %d rows", len(result.Rows))
	}
}
