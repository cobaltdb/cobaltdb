package test

import (
	"fmt"
	"strings"
	"testing"
)

// ============================================================
// FK ON UPDATE CASCADE - Update parent PK, child FK should cascade
// ============================================================

func TestV88_FKOnUpdateCascade(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
	afExec(t, db, ctx, "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id) ON UPDATE CASCADE ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO departments VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO departments VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (100, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (101, 'Bob', 1)")
	afExec(t, db, ctx, "INSERT INTO employees VALUES (102, 'Carol', 2)")

	// Update parent PK - should cascade to children
	afExec(t, db, ctx, "UPDATE departments SET id = 10 WHERE id = 1")

	// Verify cascade: employees dept_id should now be 10
	afExpectVal(t, db, ctx, "SELECT dept_id FROM employees WHERE name = 'Alice'", float64(10))
	afExpectVal(t, db, ctx, "SELECT dept_id FROM employees WHERE name = 'Bob'", float64(10))
	// Carol should still reference dept 2
	afExpectVal(t, db, ctx, "SELECT dept_id FROM employees WHERE name = 'Carol'", float64(2))
}

func TestV88_FKOnUpdateCascadeMultiLevel(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, cat_id INTEGER, FOREIGN KEY (cat_id) REFERENCES categories(id) ON UPDATE CASCADE)")
	afExec(t, db, ctx, "CREATE TABLE reviews (id INTEGER PRIMARY KEY, prod_id INTEGER, rating INTEGER, FOREIGN KEY (prod_id) REFERENCES products(id) ON UPDATE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO categories VALUES (1, 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO products VALUES (10, 'Laptop', 1)")
	afExec(t, db, ctx, "INSERT INTO reviews VALUES (100, 10, 5)")

	// Update category PK - should cascade to products
	afExec(t, db, ctx, "UPDATE categories SET id = 5 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT cat_id FROM products WHERE name = 'Laptop'", float64(5))
	// Reviews should still reference product 10 (products PK didn't change)
	afExpectVal(t, db, ctx, "SELECT prod_id FROM reviews WHERE id = 100", float64(10))
}

// ============================================================
// FK ON UPDATE SET NULL
// ============================================================

func TestV88_FKOnUpdateSetNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE teams (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE players (id INTEGER PRIMARY KEY, name TEXT, team_id INTEGER, FOREIGN KEY (team_id) REFERENCES teams(id) ON UPDATE SET NULL)")

	afExec(t, db, ctx, "INSERT INTO teams VALUES (1, 'Red')")
	afExec(t, db, ctx, "INSERT INTO teams VALUES (2, 'Blue')")
	afExec(t, db, ctx, "INSERT INTO players VALUES (10, 'Alice', 1)")
	afExec(t, db, ctx, "INSERT INTO players VALUES (11, 'Bob', 2)")

	// Update parent PK - child FK should become NULL
	afExec(t, db, ctx, "UPDATE teams SET id = 100 WHERE id = 1")

	// Alice's team_id should be NULL now
	afExpectVal(t, db, ctx, "SELECT team_id FROM players WHERE name = 'Alice'", nil)
	// Bob should still reference team 2
	afExpectVal(t, db, ctx, "SELECT team_id FROM players WHERE name = 'Bob'", float64(2))
}

// ============================================================
// FK ON UPDATE RESTRICT
// ============================================================

func TestV88_FKOnUpdateRestrict(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON UPDATE RESTRICT)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'Parent1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Should fail: RESTRICT prevents update when child references
	_, err := db.Exec(ctx, "UPDATE parents SET id = 99 WHERE id = 1")
	if err == nil {
		t.Fatal("expected RESTRICT error on FK update")
	}
	if !strings.Contains(err.Error(), "foreign key") {
		t.Logf("got error: %v", err)
	}

	// Original data should be unchanged
	afExpectVal(t, db, ctx, "SELECT id FROM parents WHERE name = 'Parent1'", float64(1))
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", float64(1))
}

func TestV88_FKOnUpdateRestrictNoReference(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON UPDATE RESTRICT)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'Parent1')")
	afExec(t, db, ctx, "INSERT INTO parents VALUES (2, 'Parent2')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Should succeed: parent 2 has no children referencing it
	afExec(t, db, ctx, "UPDATE parents SET id = 99 WHERE id = 2")
	afExpectVal(t, db, ctx, "SELECT id FROM parents WHERE name = 'Parent2'", float64(99))
}

func TestV88_FKOnUpdateDefaultBehavior(t *testing.T) {
	// Default FK behavior (no ON UPDATE specified) should be RESTRICT
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE masters (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE details (id INTEGER PRIMARY KEY, master_id INTEGER, FOREIGN KEY (master_id) REFERENCES masters(id))")

	afExec(t, db, ctx, "INSERT INTO masters VALUES (1, 'M1')")
	afExec(t, db, ctx, "INSERT INTO details VALUES (10, 1)")

	// Default behavior: should restrict
	_, err := db.Exec(ctx, "UPDATE masters SET id = 99 WHERE id = 1")
	if err == nil {
		t.Fatal("expected default RESTRICT error on FK update")
	}
}

func TestV88_FKOnUpdateNullChild(t *testing.T) {
	// Child with NULL FK value should not block parent update
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON UPDATE RESTRICT)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, NULL)") // NULL FK

	// Should succeed: no child references parent 1
	afExec(t, db, ctx, "UPDATE parents SET id = 99 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT id FROM parents WHERE name = 'P1'", float64(99))
}

// ============================================================
// FK ON UPDATE CASCADE with non-PK update (FK column unchanged)
// ============================================================

func TestV88_FKUpdateNonPKColumnNoCascade(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON UPDATE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'OldName')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Update non-PK column - should NOT trigger cascade
	afExec(t, db, ctx, "UPDATE parents SET name = 'NewName' WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT parent_id FROM children WHERE id = 10", float64(1))
	afExpectVal(t, db, ctx, "SELECT name FROM parents WHERE id = 1", "NewName")
}

// ============================================================
// HAVING deeper coverage paths
// ============================================================

func TestV88_HavingWithCASEExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE sales (id INTEGER PRIMARY KEY, region TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (1, 'north', 100)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (2, 'north', 200)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (3, 'south', 50)")
	afExec(t, db, ctx, "INSERT INTO sales VALUES (4, 'south', 60)")

	// HAVING with CASE expression
	rows := afQuery(t, db, ctx, "SELECT region, SUM(amount) as total FROM sales GROUP BY region HAVING CASE WHEN SUM(amount) > 200 THEN 1 ELSE 0 END = 1")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "north" {
		t.Fatalf("expected north, got %v", rows[0][0])
	}
}

func TestV88_HavingWithFunctionOnAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, grp TEXT, val REAL)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (1, 'a', 1.5)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (2, 'a', 2.5)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (3, 'b', 10.1)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (4, 'b', 10.9)")

	// HAVING with function on aggregate
	rows := afQuery(t, db, ctx, "SELECT grp, AVG(val) as avg_val FROM data GROUP BY grp HAVING AVG(val) > 5")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestV88_HavingWithSubExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, cat TEXT, price INTEGER)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (3, 'B', 5)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (4, 'B', 15)")

	// HAVING with arithmetic on aggregates: A=30/2=15, B=20/2=10
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(price) as total, COUNT(*) as cnt FROM items GROUP BY cat HAVING SUM(price) / COUNT(*) > 12")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "A" {
		t.Fatalf("expected A, got %v", rows[0][0])
	}
}

// ============================================================
// LIKE deeper coverage paths
// ============================================================

func TestV88_LikeWithEscapeInPattern(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE texts (id INTEGER PRIMARY KEY, content TEXT)")
	afExec(t, db, ctx, "INSERT INTO texts VALUES (1, 'hello world')")
	afExec(t, db, ctx, "INSERT INTO texts VALUES (2, 'hello')")
	afExec(t, db, ctx, "INSERT INTO texts VALUES (3, 'world')")
	afExec(t, db, ctx, "INSERT INTO texts VALUES (4, '')")

	// Pattern with % at start and end
	rows := afQuery(t, db, ctx, "SELECT id FROM texts WHERE content LIKE '%llo%' ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Pattern with single underscore
	rows = afQuery(t, db, ctx, "SELECT id FROM texts WHERE content LIKE 'hell_' ORDER BY id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// Empty string LIKE empty pattern
	rows = afQuery(t, db, ctx, "SELECT id FROM texts WHERE content LIKE ''")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestV88_LikeWithNumberConversion(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO nums VALUES (1, 123)")
	afExec(t, db, ctx, "INSERT INTO nums VALUES (2, 456)")
	afExec(t, db, ctx, "INSERT INTO nums VALUES (3, 1230)")

	// LIKE on numeric column - should convert to string
	rows := afQuery(t, db, ctx, "SELECT id FROM nums WHERE val LIKE '123%' ORDER BY id")
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}
}

// ============================================================
// evaluateExprWithGroupAggregatesJoin - JOIN + GROUP BY with complex aggregates
// ============================================================

func TestV88_JoinGroupByComplexAgg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, total INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, qty INTEGER, price INTEGER)")

	afExec(t, db, ctx, "INSERT INTO orders VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO orders VALUES (3, 'Alice', 150)")
	afExec(t, db, ctx, "INSERT INTO order_items VALUES (10, 1, 2, 50)")
	afExec(t, db, ctx, "INSERT INTO order_items VALUES (11, 1, 1, 50)")
	afExec(t, db, ctx, "INSERT INTO order_items VALUES (12, 2, 4, 50)")
	afExec(t, db, ctx, "INSERT INTO order_items VALUES (13, 3, 3, 50)")

	// JOIN + GROUP BY + aggregate expression
	rows := afQuery(t, db, ctx, "SELECT orders.customer, SUM(order_items.qty) as total_qty FROM orders INNER JOIN order_items ON orders.id = order_items.order_id GROUP BY orders.customer ORDER BY total_qty DESC")
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}
}

func TestV88_JoinGroupByHavingAgg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE depts (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE emps (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary INTEGER)")

	afExec(t, db, ctx, "INSERT INTO depts VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO depts VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (10, 'A', 1, 100)")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (11, 'B', 1, 200)")
	afExec(t, db, ctx, "INSERT INTO emps VALUES (12, 'C', 2, 50)")

	// JOIN + GROUP BY + HAVING on aggregate
	rows := afQuery(t, db, ctx, "SELECT depts.name, SUM(emps.salary) as total FROM depts INNER JOIN emps ON depts.id = emps.dept_id GROUP BY depts.name HAVING SUM(emps.salary) > 100")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "Engineering" {
		t.Fatalf("expected Engineering, got %v", rows[0][0])
	}
}

func TestV88_JoinGroupByMaxMin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, cat_id INTEGER, price INTEGER)")

	afExec(t, db, ctx, "INSERT INTO categories VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO categories VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO products VALUES (10, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (11, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (12, 2, 50)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (13, 2, 300)")

	// JOIN + GROUP BY with MAX and MIN
	rows := afQuery(t, db, ctx, "SELECT categories.name, MAX(products.price) as mx, MIN(products.price) as mn FROM categories INNER JOIN products ON categories.id = products.cat_id GROUP BY categories.name ORDER BY categories.name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// applyGroupByOrderBy deeper coverage
// ============================================================

func TestV88_GroupByOrderByAggDesc(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE scores (id INTEGER PRIMARY KEY, team TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (3, 'B', 50)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (4, 'C', 5)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (5, 'C', 15)")

	// ORDER BY aggregate DESC
	rows := afQuery(t, db, ctx, "SELECT team, SUM(score) as total FROM scores GROUP BY team ORDER BY SUM(score) DESC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0][0] != "B" {
		t.Fatalf("expected B first (50), got %v", rows[0][0])
	}
}

func TestV88_GroupByOrderByMultipleAggs(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE data (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (1, 'x', 10)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (2, 'x', 20)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (3, 'y', 10)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (4, 'y', 10)")
	afExec(t, db, ctx, "INSERT INTO data VALUES (5, 'z', 100)")

	// ORDER BY count, then sum
	rows := afQuery(t, db, ctx, "SELECT grp, COUNT(*) as cnt, SUM(val) as total FROM data GROUP BY grp ORDER BY COUNT(*) DESC, SUM(val) DESC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV88_GroupByOrderByPositional(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'b', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'c', 20)")

	// ORDER BY positional reference on aggregate
	rows := afQuery(t, db, ctx, "SELECT grp, SUM(val) as total FROM t GROUP BY grp ORDER BY 2 ASC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0][0] != "b" {
		t.Fatalf("expected b first (10), got %v", rows[0][0])
	}
}

// ============================================================
// toNumber coverage (44.4%)
// ============================================================

func TestV88_StringToNumberInExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, '42')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, '0')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'abc')")

	// String-to-number conversion in comparison
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val > '10' ORDER BY id")
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}
}

func TestV88_CastToNumberTypes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	// CAST text to INTEGER
	afExpectVal(t, db, ctx, "SELECT CAST('123' AS INTEGER)", float64(123))
	// CAST text to REAL
	afExpectVal(t, db, ctx, "SELECT CAST('3.14' AS REAL)", float64(3.14))
	// CAST bool to INTEGER
	afExpectVal(t, db, ctx, "SELECT CAST(1 = 1 AS INTEGER)", float64(1))
	// CAST NULL preserved
	afExpectVal(t, db, ctx, "SELECT CAST(NULL AS INTEGER)", nil)
	// CAST real to TEXT
	afExpectVal(t, db, ctx, "SELECT CAST(3.14 AS TEXT)", "3.14")
}

// ============================================================
// Analyze deeper coverage (60.4%)
// ============================================================

func TestV88_AnalyzeWithIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE indexed_t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	afExec(t, db, ctx, "CREATE INDEX idx_name ON indexed_t (name)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO indexed_t VALUES (%d, 'name%d', %d)", i, i%5, 20+i))
	}

	afExec(t, db, ctx, "ANALYZE indexed_t")

	// Verify table is still queryable after ANALYZE
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM indexed_t", float64(20))
}

func TestV88_AnalyzeWithNulls(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE nullable_t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO nullable_t VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO nullable_t VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO nullable_t VALUES (3, 'b')")
	afExec(t, db, ctx, "INSERT INTO nullable_t VALUES (4, NULL)")
	afExec(t, db, ctx, "INSERT INTO nullable_t VALUES (5, 'a')")

	afExec(t, db, ctx, "ANALYZE nullable_t")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM nullable_t", float64(5))
}

func TestV88_AnalyzeMultipleTables(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t1 VALUES (%d, %d)", i, i*10))
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t2 VALUES (%d, 'val%d')", i, i))
	}

	afExec(t, db, ctx, "ANALYZE t1")
	afExec(t, db, ctx, "ANALYZE t2")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t1", float64(10))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t2", float64(10))
}

// ============================================================
// CAST deeper coverage (77.8%)
// ============================================================

func TestV88_CastBoolToText(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	// Boolean expressions cast to text
	val := afQuery(t, db, ctx, "SELECT CAST(1 = 1 AS TEXT)")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	// Result should be "true" or "1"
	t.Logf("CAST(true AS TEXT) = %v", val[0][0])
}

func TestV88_CastIntToReal(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT CAST(42 AS REAL)", float64(42))
}

func TestV88_CastTextToBlob(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	// CAST to BLOB - should return the text as-is or bytes
	val := afQuery(t, db, ctx, "SELECT CAST('hello' AS BLOB)")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	t.Logf("CAST('hello' AS BLOB) = %v", val[0][0])
}

// ============================================================
// Correlated subqueries with various expression types (resolveOuterRefsInExpr)
// ============================================================

func TestV88_CorrelatedSubqueryWithBetween(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE ranges (id INTEGER PRIMARY KEY, low INTEGER, high INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE vals (id INTEGER PRIMARY KEY, val INTEGER)")

	afExec(t, db, ctx, "INSERT INTO ranges VALUES (1, 10, 20)")
	afExec(t, db, ctx, "INSERT INTO ranges VALUES (2, 30, 40)")
	afExec(t, db, ctx, "INSERT INTO vals VALUES (1, 15)")
	afExec(t, db, ctx, "INSERT INTO vals VALUES (2, 25)")
	afExec(t, db, ctx, "INSERT INTO vals VALUES (3, 35)")

	// Correlated subquery with EXISTS and outer ref
	rows := afQuery(t, db, ctx, "SELECT v.val FROM vals v WHERE EXISTS (SELECT 1 FROM ranges r WHERE v.val >= r.low AND v.val <= r.high) ORDER BY v.val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (15, 35), got %d", len(rows))
	}
}

func TestV88_CorrelatedSubqueryWithLike(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE patterns (id INTEGER PRIMARY KEY, pat TEXT)")
	afExec(t, db, ctx, "CREATE TABLE strings (id INTEGER PRIMARY KEY, val TEXT)")

	afExec(t, db, ctx, "INSERT INTO patterns VALUES (1, '%hello%')")
	afExec(t, db, ctx, "INSERT INTO strings VALUES (1, 'say hello world')")
	afExec(t, db, ctx, "INSERT INTO strings VALUES (2, 'goodbye')")

	// Correlated subquery with LIKE
	rows := afQuery(t, db, ctx, "SELECT s.val FROM strings s WHERE EXISTS (SELECT 1 FROM patterns p WHERE s.val LIKE p.pat)")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestV88_CorrelatedSubqueryWithFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, name TEXT)")

	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 'world')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 'HELLO')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (2, 'bye')")

	// Correlated subquery with UPPER function
	rows := afQuery(t, db, ctx, "SELECT t1.name FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE UPPER(t1.name) = UPPER(t2.name))")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "hello" {
		t.Fatalf("expected 'hello', got %v", rows[0][0])
	}
}

// ============================================================
// evaluateExpression deeper paths
// ============================================================

func TestV88_NestedCaseWhenExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	// Nested CASE WHEN
	rows := afQuery(t, db, ctx, `SELECT id,
		CASE WHEN val < 15 THEN
			CASE WHEN val < 5 THEN 'very low' ELSE 'low' END
		WHEN val < 25 THEN 'medium'
		ELSE 'high' END as label
		FROM t ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0][1] != "low" {
		t.Fatalf("expected 'low', got %v", rows[0][1])
	}
	if rows[1][1] != "medium" {
		t.Fatalf("expected 'medium', got %v", rows[1][1])
	}
	if rows[2][1] != "high" {
		t.Fatalf("expected 'high', got %v", rows[2][1])
	}
}

func TestV88_ComplexBinaryExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10, 3)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20, 7)")

	// Complex arithmetic expressions
	afExpectVal(t, db, ctx, "SELECT (a * b) + (a - b) FROM t WHERE id = 1", float64(37)) // (10*3) + (10-3) = 30 + 7 = 37
	afExpectVal(t, db, ctx, "SELECT a % b FROM t WHERE id = 1", float64(1))               // 10 % 3 = 1
}

// ============================================================
// selectLocked edge cases (78.5%)
// ============================================================

func TestV88_SelectWithAllExprTypes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'alice', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'bob', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'carol', 30)")

	// Select with CASE, function, arithmetic, CAST all in one
	rows := afQuery(t, db, ctx, `SELECT id,
		CASE WHEN val > 15 THEN 'high' ELSE 'low' END,
		UPPER(name),
		val * 2 + 1,
		CAST(val AS TEXT)
		FROM t ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0][1] != "low" {
		t.Fatalf("expected 'low', got %v", rows[0][1])
	}
	if rows[0][2] != "ALICE" {
		t.Fatalf("expected 'ALICE', got %v", rows[0][2])
	}
}

func TestV88_SelectDistinctWithOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'a')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'c')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (5, 'b')")

	rows := afQuery(t, db, ctx, "SELECT DISTINCT val FROM t ORDER BY val")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV88_SelectWithMultipleSubqueries(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	// Scalar subqueries in SELECT list
	rows := afQuery(t, db, ctx, "SELECT id, val, (SELECT MAX(val) FROM t) as mx, (SELECT MIN(val) FROM t) as mn FROM t ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// ============================================================
// evaluateFunctionCall deeper paths (69.4%)
// ============================================================

func TestV88_FunctionABS(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT ABS(-42)", float64(42))
	afExpectVal(t, db, ctx, "SELECT ABS(42)", float64(42))
	afExpectVal(t, db, ctx, "SELECT ABS(0)", float64(0))
}

func TestV88_FunctionCOALESCEChain(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, NULL, 'third')", "third")
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, 'second', 'third')", "second")
	afExpectVal(t, db, ctx, "SELECT COALESCE('first', NULL, 'third')", "first")
}

func TestV88_FunctionIIF(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT IIF(1 = 1, 'yes', 'no')", "yes")
	afExpectVal(t, db, ctx, "SELECT IIF(1 = 2, 'yes', 'no')", "no")
}

func TestV88_FunctionNULLIF(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT NULLIF(1, 1)", nil)
	afExpectVal(t, db, ctx, "SELECT NULLIF(1, 2)", float64(1))
	afExpectVal(t, db, ctx, "SELECT NULLIF('a', 'b')", "a")
}

func TestV88_FunctionTRIM(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT TRIM('  hello  ')", "hello")
	afExpectVal(t, db, ctx, "SELECT LTRIM('  hello  ')", "hello  ")
	afExpectVal(t, db, ctx, "SELECT RTRIM('  hello  ')", "  hello")
}

func TestV88_FunctionLENGTH(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT LENGTH('hello')", float64(5))
	afExpectVal(t, db, ctx, "SELECT LENGTH('')", float64(0))
}

func TestV88_FunctionSUBSTR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT SUBSTR('hello', 2, 3)", "ell")
	afExpectVal(t, db, ctx, "SELECT SUBSTR('hello', 1)", "hello")
}

func TestV88_FunctionREPLACE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT REPLACE('hello world', 'world', 'earth')", "hello earth")
	afExpectVal(t, db, ctx, "SELECT REPLACE('aaa', 'a', 'bb')", "bbbbbb")
}

func TestV88_FunctionINSTR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT INSTR('hello world', 'world')", float64(7))
	afExpectVal(t, db, ctx, "SELECT INSTR('hello', 'xyz')", float64(0))
}

func TestV88_FunctionCHARandUNICODE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT CHAR(65)", "A")
	afExpectVal(t, db, ctx, "SELECT UNICODE('A')", float64(65))
}

func TestV88_FunctionHEX(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT HEX('A')", "41")
}

// ============================================================
// evaluateBetween - deeper paths
// ============================================================

func TestV88_BetweenWithExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 15)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 25)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 5)")

	// BETWEEN with expressions
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val BETWEEN 10 AND 20 ORDER BY id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "1" {
		t.Fatalf("expected 1, got %v", rows[0][0])
	}

	// NOT BETWEEN
	rows = afQuery(t, db, ctx, "SELECT id FROM t WHERE val NOT BETWEEN 10 AND 20 ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV88_BetweenWithStrings(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'bob')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'dave')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'alice')")

	rows := afQuery(t, db, ctx, "SELECT name FROM t WHERE name BETWEEN 'b' AND 'd' ORDER BY name")
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}
}

// ============================================================
// evaluateIn deeper paths
// ============================================================

func TestV88_InWithSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (2, 30)")

	rows := afQuery(t, db, ctx, "SELECT id FROM t1 WHERE val IN (SELECT val FROM t2) ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV88_NotInWithValues(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'c')")

	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val NOT IN ('a', 'c') ORDER BY id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "2" {
		t.Fatalf("expected 2, got %v", rows[0][0])
	}
}

// ============================================================
// evaluateWhere deeper paths (71.4%)
// ============================================================

func TestV88_WhereWithComplexOR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, a INTEGER, b TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10, 'x')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20, 'y')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30, 'x')")

	// Complex OR with different types
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE (a > 15 AND b = 'y') OR (a < 15 AND b = 'x') ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV88_WhereWithNestedNOT(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE NOT (val > 25 OR val < 15) ORDER BY id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "2" {
		t.Fatalf("expected 2, got %v", rows[0][0])
	}
}

// ============================================================
// evaluateJSONFunction deeper paths (71.8%)
// ============================================================

func TestV88_JSONExtractNested(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"a": {"b": {"c": 42}}}')`)

	afExpectVal(t, db, ctx, "SELECT JSON_EXTRACT(data, '$.a.b.c') FROM t WHERE id = 1", float64(42))
}

func TestV88_JSONArrayLength(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '[1, 2, 3, 4]')`)
	afExec(t, db, ctx, `INSERT INTO t VALUES (2, '[]')`)

	afExpectVal(t, db, ctx, "SELECT JSON_ARRAY_LENGTH(data) FROM t WHERE id = 1", float64(4))
	afExpectVal(t, db, ctx, "SELECT JSON_ARRAY_LENGTH(data) FROM t WHERE id = 2", float64(0))
}

func TestV88_JSONKeys(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"name": "test", "age": 25}')`)

	val := afQuery(t, db, ctx, "SELECT JSON_KEYS(data) FROM t WHERE id = 1")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	t.Logf("JSON_KEYS result: %v", val[0][0])
}

func TestV88_JSONRemove(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO t VALUES (1, '{"a": 1, "b": 2, "c": 3}')`)

	val := afQuery(t, db, ctx, "SELECT JSON_REMOVE(data, '$.b') FROM t WHERE id = 1")
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	result := fmt.Sprintf("%v", val[0][0])
	if strings.Contains(result, "\"b\"") {
		t.Fatalf("expected 'b' removed, got %v", result)
	}
}

func TestV88_JSONMerge(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	val := afQuery(t, db, ctx, `SELECT JSON_MERGE('{"a": 1}', '{"b": 2}')`)
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	result := fmt.Sprintf("%v", val[0][0])
	if !strings.Contains(result, "a") || !strings.Contains(result, "b") {
		t.Fatalf("expected merged JSON, got %v", result)
	}
}

func TestV88_JSONQuoteUnquote(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	afExpectVal(t, db, ctx, `SELECT JSON_QUOTE('hello')`, `"hello"`)
	afExpectVal(t, db, ctx, `SELECT JSON_UNQUOTE('"hello"')`, "hello")
}

func TestV88_JSONPretty(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	val := afQuery(t, db, ctx, `SELECT JSON_PRETTY('{"a":1}')`)
	if len(val) != 1 {
		t.Fatalf("expected 1 row, got %d", len(val))
	}
	result := fmt.Sprintf("%v", val[0][0])
	if !strings.Contains(result, "\n") && !strings.Contains(result, "  ") {
		t.Logf("JSON_PRETTY result: %v (may not have indentation)", result)
	}
}

// ============================================================
// Derived table (executeDerivedTable) deeper coverage (62.5%)
// ============================================================

func TestV88_DerivedTableWithAggregate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'a', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'b', 30)")

	// Derived table with aggregate
	rows := afQuery(t, db, ctx, "SELECT sub.grp, sub.total FROM (SELECT grp, SUM(val) as total FROM t GROUP BY grp) sub ORDER BY sub.total DESC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV88_DerivedTableWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	// Derived table with WHERE in outer query
	rows := afQuery(t, db, ctx, "SELECT sub.val FROM (SELECT val FROM t) sub WHERE sub.val > 15 ORDER BY sub.val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// Window functions deeper coverage (evalWindowExprOnRow 76.7%)
// ============================================================

func TestV88_WindowROW_NUMBER(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'a', 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'b', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'b', 40)")

	rows := afQuery(t, db, ctx, "SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn FROM t ORDER BY grp, val")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestV88_WindowLAGLEAD(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	rows := afQuery(t, db, ctx, "SELECT id, val, LAG(val) OVER (ORDER BY id) as prev_val, LEAD(val) OVER (ORDER BY id) as next_val FROM t ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// First row LAG should be NULL
	if rows[0][2] != nil {
		t.Fatalf("expected NULL for first LAG, got %v", rows[0][2])
	}
	// Last row LEAD should be NULL
	if rows[2][3] != nil {
		t.Fatalf("expected NULL for last LEAD, got %v", rows[2][3])
	}
}

func TestV88_WindowSUMPartition(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'eng', 100)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'eng', 200)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'sales', 150)")

	rows := afQuery(t, db, ctx, "SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM t ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// ============================================================
// CTE deeper paths (ExecuteCTE 75.5%)
// ============================================================

func TestV88_CTEWithFilter(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	rows := afQuery(t, db, ctx, "WITH filtered AS (SELECT * FROM t WHERE val > 15) SELECT id FROM filtered ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV88_CTEChained(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 30)")

	rows := afQuery(t, db, ctx, "WITH cte1 AS (SELECT id, val FROM t WHERE val >= 20), cte2 AS (SELECT id, val * 2 as doubled FROM cte1) SELECT id, doubled FROM cte2 ORDER BY id")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ============================================================
// Vacuum deeper coverage (82.9%)
// ============================================================

func TestV88_VacuumAfterDeletes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 50; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t VALUES (%d, 'value_%d')", i, i))
	}
	// Delete half the rows
	afExec(t, db, ctx, "DELETE FROM t WHERE id > 25")
	afExec(t, db, ctx, "VACUUM")

	// Verify remaining data
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t", float64(25))
}

// ============================================================
// Trigger + FK combined scenarios
// ============================================================

func TestV88_TriggerWithFKCascade(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, detail TEXT)")
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE)")

	// Trigger to log deletions
	afExec(t, db, ctx, `CREATE TRIGGER log_parent_delete AFTER DELETE ON parents FOR EACH ROW BEGIN INSERT INTO audit_log VALUES (NULL, 'DELETE', OLD.name); END`)

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Delete parent - should cascade delete child AND fire trigger
	afExec(t, db, ctx, "DELETE FROM parents WHERE id = 1")

	// Verify cascade
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM children", float64(0))
	// Verify trigger fired
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM audit_log", float64(1))
	afExpectVal(t, db, ctx, "SELECT detail FROM audit_log WHERE action = 'DELETE'", "P1")
}

// ============================================================
// ALTER TABLE deeper coverage
// ============================================================

func TestV88_AlterTableRenameColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, old_name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")

	afExec(t, db, ctx, "ALTER TABLE t RENAME COLUMN old_name TO new_name")
	afExpectVal(t, db, ctx, "SELECT new_name FROM t WHERE id = 1", "hello")
}

func TestV88_AlterTableRenameTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE old_table (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO old_table VALUES (1, 'test')")

	afExec(t, db, ctx, "ALTER TABLE old_table RENAME TO new_table")
	afExpectVal(t, db, ctx, "SELECT val FROM new_table WHERE id = 1", "test")
}

// ============================================================
// Edge cases: operations on tables with various column types
// ============================================================

func TestV88_MixedTypeOperations(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, i INTEGER, r REAL, t TEXT, b BOOLEAN)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 42, 3.14, 'hello', 1)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, NULL, NULL, NULL, NULL)")

	// Various operations on mixed types
	afExpectVal(t, db, ctx, "SELECT TYPEOF(i) FROM t WHERE id = 1", "integer")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(t) FROM t WHERE id = 1", "text")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(i) FROM t WHERE id = 2", "null")
}

func TestV88_InsertSelectWithTransform(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE src (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE dst (id INTEGER PRIMARY KEY, doubled INTEGER)")

	afExec(t, db, ctx, "INSERT INTO src VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO src VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO src VALUES (3, 30)")

	afExec(t, db, ctx, "INSERT INTO dst SELECT id, val * 2 FROM src")
	afExpectVal(t, db, ctx, "SELECT doubled FROM dst WHERE id = 2", float64(40))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM dst", float64(3))
}

// ============================================================
// Transaction + FK interaction
// ============================================================

func TestV88_TransactionFKCascadeRollback(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE children (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO parents VALUES (1, 'P1')")
	afExec(t, db, ctx, "INSERT INTO children VALUES (10, 1)")

	// Begin transaction, delete parent (cascades), then rollback
	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "DELETE FROM parents WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM children", float64(0))
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback, both parent and child should be back
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM parents", float64(1))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM children", float64(1))
}

// ============================================================
// Complex real-world scenario
// ============================================================

func TestV88_ECommerceOrderFlow(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
	afExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, total INTEGER, FOREIGN KEY (customer_id) REFERENCES customers(id))")
	afExec(t, db, ctx, "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product TEXT, qty INTEGER, price INTEGER, FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE)")

	// Create customers and orders
	afExec(t, db, ctx, "INSERT INTO customers VALUES (1, 'Alice', 'alice@example.com')")
	afExec(t, db, ctx, "INSERT INTO customers VALUES (2, 'Bob', 'bob@example.com')")

	for i := 1; i <= 5; i++ {
		custID := (i % 2) + 1
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, 'completed', %d)", i, custID, i*100))
		for j := 1; j <= 3; j++ {
			itemID := (i-1)*3 + j
			afExec(t, db, ctx, fmt.Sprintf("INSERT INTO order_items VALUES (%d, %d, 'product_%d', %d, %d)", itemID, i, j, j, 50))
		}
	}

	// Complex query: customer order summary
	rows := afQuery(t, db, ctx, `
		SELECT c.name, COUNT(DISTINCT o.id) as order_count, SUM(o.total) as total_spent
		FROM customers c
		INNER JOIN orders o ON c.id = o.customer_id
		GROUP BY c.name
		HAVING COUNT(DISTINCT o.id) >= 2
		ORDER BY total_spent DESC
	`)
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}

	// Verify order items count
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM order_items", float64(15))

	// Delete an order - should cascade to items
	afExec(t, db, ctx, "DELETE FROM orders WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM order_items", float64(12))
}

// ============================================================
// evaluateExpression - UnaryExpr deeper (NOT on non-boolean)
// ============================================================

func TestV88_UnaryMinusOnExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 42)")

	afExpectVal(t, db, ctx, "SELECT -val FROM t WHERE id = 1", float64(-42))
	afExpectVal(t, db, ctx, "SELECT -(val + 8) FROM t WHERE id = 1", float64(-50))
}

// ============================================================
// matchLikeSimple deeper paths (81.8%)
// ============================================================

func TestV88_LikeComplexPatterns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'abcdef')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'abc')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (3, 'xyzabc')")
	afExec(t, db, ctx, "INSERT INTO t VALUES (4, 'abXYZef')")

	// % at both ends
	rows := afQuery(t, db, ctx, "SELECT id FROM t WHERE val LIKE '%abc%' ORDER BY id")
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}

	// Underscore wildcard
	rows = afQuery(t, db, ctx, "SELECT id FROM t WHERE val LIKE 'ab_' ORDER BY id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (abc), got %d", len(rows))
	}

	// Multiple underscores
	rows = afQuery(t, db, ctx, "SELECT id FROM t WHERE val LIKE 'a_c___' ORDER BY id")
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}
}

// ============================================================
// exprToSQL coverage (52.4%)
// ============================================================

func TestV88_CreateIndexOnExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'alice', 30)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (2, 'bob', 25)")

	// Create index - exercises storeIndexDef and exprToSQL
	afExec(t, db, ctx, "CREATE INDEX idx_age ON t (age)")
	afExec(t, db, ctx, "CREATE INDEX idx_name ON t (name)")

	// Query that could use the index
	afExpectVal(t, db, ctx, "SELECT name FROM t WHERE age = 30", "alice")
}

// ============================================================
// DropIndex coverage (81.8%)
// ============================================================

func TestV88_DropIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1, 'hello')")
	afExec(t, db, ctx, "CREATE INDEX idx_val ON t (val)")

	// Drop the index
	afExec(t, db, ctx, "DROP INDEX idx_val")

	// Table should still work
	afExpectVal(t, db, ctx, "SELECT val FROM t WHERE id = 1", "hello")
}

// ============================================================
// Recursive CTE deeper coverage (90.0%)
// ============================================================

func TestV88_RecursiveCTEWithFilter(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO t VALUES (1)")

	// Recursive CTE: generate numbers 1-10
	rows := afQuery(t, db, ctx, `
		WITH RECURSIVE nums AS (
			SELECT 1 as n
			UNION ALL
			SELECT n + 1 FROM nums WHERE n < 10
		)
		SELECT n FROM nums WHERE n > 5 ORDER BY n
	`)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows (6-10), got %d", len(rows))
	}
}

// ============================================================
// Set operations deeper: UNION ALL, INTERSECT, EXCEPT
// ============================================================

func TestV88_UnionAllWithDuplicates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 'b')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (2, 'c')")

	// UNION ALL preserves duplicates
	rows := afQuery(t, db, ctx, "SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	// UNION removes duplicates
	rows = afQuery(t, db, ctx, "SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV88_IntersectAndExcept(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (1, 20)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (2, 30)")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (3, 40)")

	// INTERSECT
	rows := afQuery(t, db, ctx, "SELECT val FROM t1 INTERSECT SELECT val FROM t2 ORDER BY val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (20, 30), got %d", len(rows))
	}

	// EXCEPT
	rows = afQuery(t, db, ctx, "SELECT val FROM t1 EXCEPT SELECT val FROM t2 ORDER BY val")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (10), got %d", len(rows))
	}
}
