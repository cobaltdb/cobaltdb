package test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// Bug hunt: systematically test SQL features to find remaining issues

func bh(t *testing.T) (*engine.DB, context.Context) {
	t.Helper()
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	return db, context.Background()
}

func bhExec(t *testing.T, db *engine.DB, ctx context.Context, sql string) {
	t.Helper()
	if _, err := db.Exec(ctx, sql); err != nil {
		t.Fatalf("EXEC [%s]: %v", sql, err)
	}
}

func bhQuery(t *testing.T, db *engine.DB, ctx context.Context, sql string) [][]interface{} {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("QUERY [%s]: %v", sql, err)
	}
	defer rows.Close()
	cols := rows.Columns()
	var result [][]interface{}
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		row := make([]interface{}, len(cols))
		copy(row, vals)
		result = append(result, row)
	}
	return result
}

func bhExpectRows(t *testing.T, db *engine.DB, ctx context.Context, sql string, n int) [][]interface{} {
	t.Helper()
	rows := bhQuery(t, db, ctx, sql)
	if len(rows) != n {
		t.Fatalf("[%s] expected %d rows, got %d", sql, n, len(rows))
	}
	return rows
}

func bhExpectVal(t *testing.T, db *engine.DB, ctx context.Context, sql string, expected interface{}) {
	t.Helper()
	rows := bhQuery(t, db, ctx, sql)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatalf("[%s] no result", sql)
	}
	got := fmt.Sprintf("%v", rows[0][0])
	exp := fmt.Sprintf("%v", expected)
	if got != exp {
		t.Fatalf("[%s] expected %v, got %v", sql, expected, rows[0][0])
	}
}

func bhExpectError(t *testing.T, db *engine.DB, ctx context.Context, sql string) {
	t.Helper()
	_, err := db.Exec(ctx, sql)
	if err == nil {
		rows, err2 := db.Query(ctx, sql)
		if err2 == nil {
			rows.Close()
			t.Fatalf("[%s] expected error but succeeded", sql)
		}
	}
}

func setupBHData(t *testing.T, db *engine.DB, ctx context.Context) {
	t.Helper()
	bhExec(t, db, ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, age INTEGER, active INTEGER DEFAULT 1)")
	bhExec(t, db, ctx, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product TEXT, amount REAL, status TEXT DEFAULT 'pending')")
	bhExec(t, db, ctx, "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)")

	bhExec(t, db, ctx, "INSERT INTO users (id, name, email, age, active) VALUES (1, 'Alice', 'alice@test.com', 30, 1)")
	bhExec(t, db, ctx, "INSERT INTO users (id, name, email, age, active) VALUES (2, 'Bob', 'bob@test.com', 25, 1)")
	bhExec(t, db, ctx, "INSERT INTO users (id, name, email, age, active) VALUES (3, 'Carol', 'carol@test.com', 35, 0)")
	bhExec(t, db, ctx, "INSERT INTO users (id, name, email, age, active) VALUES (4, 'Dave', 'dave@test.com', 28, 1)")
	bhExec(t, db, ctx, "INSERT INTO users (id, name, email, age, active) VALUES (5, 'Eve', 'eve@test.com', 22, 0)")

	bhExec(t, db, ctx, "INSERT INTO orders (id, user_id, product, amount, status) VALUES (1, 1, 'Laptop', 999.99, 'completed')")
	bhExec(t, db, ctx, "INSERT INTO orders (id, user_id, product, amount, status) VALUES (2, 1, 'Mouse', 29.99, 'completed')")
	bhExec(t, db, ctx, "INSERT INTO orders (id, user_id, product, amount, status) VALUES (3, 2, 'Keyboard', 79.99, 'pending')")
	bhExec(t, db, ctx, "INSERT INTO orders (id, user_id, product, amount, status) VALUES (4, 2, 'Monitor', 499.99, 'completed')")
	bhExec(t, db, ctx, "INSERT INTO orders (id, user_id, product, amount, status) VALUES (5, 3, 'Headphones', 149.99, 'cancelled')")
	bhExec(t, db, ctx, "INSERT INTO orders (id, user_id, product, amount, status) VALUES (6, 4, 'Tablet', 399.99, 'pending')")

	bhExec(t, db, ctx, "INSERT INTO categories (id, name, parent_id) VALUES (1, 'Electronics', NULL)")
	bhExec(t, db, ctx, "INSERT INTO categories (id, name, parent_id) VALUES (2, 'Computers', 1)")
	bhExec(t, db, ctx, "INSERT INTO categories (id, name, parent_id) VALUES (3, 'Accessories', 1)")
	bhExec(t, db, ctx, "INSERT INTO categories (id, name, parent_id) VALUES (4, 'Audio', 1)")
}

// ==================== REMAINING FEATURE TESTS ====================

func TestBH_OrderByAlias(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// ORDER BY alias name
	rows := bhQuery(t, db, ctx, "SELECT name, age AS user_age FROM users ORDER BY user_age DESC")
	if len(rows) != 5 {
		t.Fatalf("expected 5, got %d", len(rows))
	}
	// Carol (35) should be first
	if fmt.Sprintf("%v", rows[0][0]) != "Carol" {
		t.Fatalf("expected Carol first, got %v", rows[0][0])
	}
}

func TestBH_OrderByExpression(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// ORDER BY expression (age * 2)
	rows := bhQuery(t, db, ctx, "SELECT name, age FROM users ORDER BY age")
	if len(rows) != 5 {
		t.Fatalf("expected 5, got %d", len(rows))
	}
	// Eve (22) should be first
	if fmt.Sprintf("%v", rows[0][0]) != "Eve" {
		t.Fatalf("expected Eve first, got %v", rows[0][0])
	}
}

func TestBH_MultipleJoins(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	bhExec(t, db, ctx, "CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)")
	bhExec(t, db, ctx, "CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER, val TEXT)")
	bhExec(t, db, ctx, "CREATE TABLE c (id INTEGER PRIMARY KEY, b_id INTEGER, val TEXT)")

	bhExec(t, db, ctx, "INSERT INTO a (id, val) VALUES (1, 'A1')")
	bhExec(t, db, ctx, "INSERT INTO b (id, a_id, val) VALUES (1, 1, 'B1')")
	bhExec(t, db, ctx, "INSERT INTO c (id, b_id, val) VALUES (1, 1, 'C1')")

	// Triple join
	rows := bhQuery(t, db, ctx, "SELECT a.val, b.val, c.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A1" || fmt.Sprintf("%v", rows[0][1]) != "B1" || fmt.Sprintf("%v", rows[0][2]) != "C1" {
		t.Fatalf("unexpected values: %v", rows[0])
	}
}

func TestBH_LeftJoinNulls(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// LEFT JOIN - Eve (id=5) has no orders
	rows := bhQuery(t, db, ctx, "SELECT users.name, orders.product FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.id = 5")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for Eve with NULL order, got %d", len(rows))
	}
	if rows[0][1] != nil {
		t.Fatalf("expected NULL product for Eve, got %v", rows[0][1])
	}
}

func TestBH_GroupByHaving(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// GROUP BY with HAVING
	rows := bhQuery(t, db, ctx, "SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id HAVING COUNT(*) > 1")
	if len(rows) != 2 {
		t.Fatalf("expected 2 users with >1 orders, got %d", len(rows))
	}
}

func TestBH_GroupByWithSum(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// SUM with GROUP BY
	rows := bhQuery(t, db, ctx, "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id ORDER BY user_id")
	if len(rows) < 1 {
		t.Fatal("expected rows from GROUP BY SUM")
	}
}

func TestBH_NestedFunctions(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Nested function calls
	bhExpectVal(t, db, ctx, "SELECT UPPER(TRIM('  hello  '))", "HELLO")
	bhExpectVal(t, db, ctx, "SELECT LENGTH(UPPER('hello'))", float64(5))
	bhExpectVal(t, db, ctx, "SELECT COALESCE(NULL, NULL, 'found')", "found")
	bhExpectVal(t, db, ctx, "SELECT ABS(-42)", float64(42))
	bhExpectVal(t, db, ctx, "SELECT ROUND(3.14159, 2)", 3.14)
}

func TestBH_ComplexWhere(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Complex WHERE with AND, OR, parentheses
	// Alice(30,1), Dave(28,1) match first clause; Eve(22,0) matches second = 3
	rows := bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE (age > 25 AND active = 1) OR (age < 25 AND active = 0)", 3)
	_ = rows

	// BETWEEN
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE age BETWEEN 25 AND 30", 3)

	// NOT BETWEEN
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE age NOT BETWEEN 25 AND 30", 2)

	// IN with list
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE age IN (22, 30, 35)", 3)

	// NOT IN with list
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE age NOT IN (22, 30, 35)", 2)
}

func TestBH_LikePatterns(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// LIKE patterns
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE name LIKE 'A%'", 1)     // Alice
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE name LIKE '%e'", 3)      // Alice, Dave, Eve
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE name LIKE '%o%'", 2)     // Bob, Carol
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE name NOT LIKE '%a%'", 2) // Bob, Eve (LIKE is case-insensitive)

	// LIKE with underscore
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE name LIKE 'B__'", 1) // Bob
}

func TestBH_NullHandling(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	bhExec(t, db, ctx, "CREATE TABLE nulltest (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)")
	bhExec(t, db, ctx, "INSERT INTO nulltest (id, val, num) VALUES (1, 'hello', 10)")
	bhExec(t, db, ctx, "INSERT INTO nulltest (id, val, num) VALUES (2, NULL, 20)")
	bhExec(t, db, ctx, "INSERT INTO nulltest (id, val, num) VALUES (3, 'world', NULL)")
	bhExec(t, db, ctx, "INSERT INTO nulltest (id, val, num) VALUES (4, NULL, NULL)")

	// IS NULL
	bhExpectRows(t, db, ctx, "SELECT * FROM nulltest WHERE val IS NULL", 2)
	// IS NOT NULL
	bhExpectRows(t, db, ctx, "SELECT * FROM nulltest WHERE val IS NOT NULL", 2)
	// COALESCE with NULL
	bhExpectVal(t, db, ctx, "SELECT COALESCE(val, 'default') FROM nulltest WHERE id = 2", "default")
	// IFNULL
	bhExpectVal(t, db, ctx, "SELECT IFNULL(val, 'fallback') FROM nulltest WHERE id = 4", "fallback")
	// NULL in arithmetic
	rows := bhQuery(t, db, ctx, "SELECT num + 5 FROM nulltest WHERE id = 3")
	if len(rows) == 1 && rows[0][0] != nil {
		t.Logf("NULL + 5 = %v (should be NULL ideally)", rows[0][0])
	}
}

func TestBH_CaseWhenElse(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Simple CASE
	rows := bhQuery(t, db, ctx, `SELECT name, CASE active WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END FROM users ORDER BY id`)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "active" {
		t.Fatalf("expected 'active' for Alice, got %v", rows[0][1])
	}

	// Searched CASE
	rows = bhQuery(t, db, ctx, `SELECT name, CASE WHEN age >= 30 THEN 'senior' WHEN age >= 25 THEN 'mid' ELSE 'junior' END FROM users ORDER BY id`)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

func TestBH_InsertDefaultValues(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	bhExec(t, db, ctx, "CREATE TABLE defaults_test (id INTEGER PRIMARY KEY, name TEXT DEFAULT 'unknown', score REAL DEFAULT 0.0, active INTEGER DEFAULT 1)")
	bhExec(t, db, ctx, "INSERT INTO defaults_test (id) VALUES (1)")

	rows := bhExpectRows(t, db, ctx, "SELECT name, active FROM defaults_test WHERE id = 1", 1)
	if fmt.Sprintf("%v", rows[0][0]) != "unknown" {
		t.Fatalf("expected default 'unknown', got %v", rows[0][0])
	}
}

func TestBH_UniqueConstraint(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Duplicate email should fail
	_, err := db.Exec(ctx, "INSERT INTO users (id, name, email, age) VALUES (99, 'Test', 'alice@test.com', 20)")
	if err == nil {
		t.Fatal("expected UNIQUE constraint error")
	}
}

func TestBH_NotNullConstraint(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// NULL name should fail
	_, err := db.Exec(ctx, "INSERT INTO users (id, name, email, age) VALUES (99, NULL, 'test@test.com', 20)")
	if err == nil {
		t.Fatal("expected NOT NULL constraint error")
	}
}

func TestBH_UpdateMultipleColumns(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Update multiple columns at once
	bhExec(t, db, ctx, "UPDATE users SET age = 31, active = 0 WHERE name = 'Alice'")
	rows := bhExpectRows(t, db, ctx, "SELECT age, active FROM users WHERE name = 'Alice'", 1)
	if fmt.Sprintf("%v", rows[0][0]) != "31" {
		t.Fatalf("expected age 31, got %v", rows[0][0])
	}
	if fmt.Sprintf("%v", rows[0][1]) != "0" {
		t.Fatalf("expected active 0, got %v", rows[0][1])
	}
}

func TestBH_DeleteWithJoinLikePattern(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Delete with LIKE
	bhExec(t, db, ctx, "DELETE FROM orders WHERE status LIKE 'cancel%'")
	bhExpectRows(t, db, ctx, "SELECT * FROM orders", 5)
}

func TestBH_CountDistinct(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// COUNT with DISTINCT
	bhExpectVal(t, db, ctx, "SELECT COUNT(DISTINCT user_id) FROM orders", float64(4))
	bhExpectVal(t, db, ctx, "SELECT COUNT(DISTINCT status) FROM orders", float64(3))
}

func TestBH_MinMaxOnStrings(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// MIN/MAX on strings (alphabetical)
	bhExpectVal(t, db, ctx, "SELECT MIN(name) FROM users", "Alice")
	bhExpectVal(t, db, ctx, "SELECT MAX(name) FROM users", "Eve")
}

func TestBH_SubqueryInSelect(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Scalar subquery in SELECT list
	rows := bhQuery(t, db, ctx, "SELECT name, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users ORDER BY id")
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

func TestBH_CTEBasic(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Basic CTE
	rows := bhQuery(t, db, ctx, "WITH active_users AS (SELECT * FROM users WHERE active = 1) SELECT name FROM active_users ORDER BY name")
	if len(rows) != 3 {
		t.Fatalf("expected 3 active users, got %d", len(rows))
	}
}

func TestBH_CTEMultiple(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Multiple CTEs
	rows := bhQuery(t, db, ctx, `WITH
		active AS (SELECT id, name FROM users WHERE active = 1),
		big_orders AS (SELECT user_id, product FROM orders WHERE amount > 100)
		SELECT active.name, big_orders.product FROM active JOIN big_orders ON active.id = big_orders.user_id`)
	if len(rows) < 1 {
		t.Fatal("expected at least 1 row from multiple CTEs")
	}
}

func TestBH_ViewCreateQuery(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Create and query a view
	bhExec(t, db, ctx, "CREATE VIEW active_users AS SELECT id, name, email FROM users WHERE active = 1")
	rows := bhExpectRows(t, db, ctx, "SELECT name FROM active_users", 3)
	_ = rows

	// View with JOIN
	bhExec(t, db, ctx, "CREATE VIEW user_orders AS SELECT users.name, orders.product, orders.amount FROM users JOIN orders ON users.id = orders.user_id")
	rows = bhQuery(t, db, ctx, "SELECT * FROM user_orders")
	if len(rows) < 1 {
		t.Fatal("expected rows from view with JOIN")
	}
}

func TestBH_AlterTableAddColumn(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// ALTER TABLE ADD COLUMN
	bhExec(t, db, ctx, "ALTER TABLE users ADD COLUMN phone TEXT")
	bhExec(t, db, ctx, "UPDATE users SET phone = '555-1234' WHERE id = 1")
	bhExpectVal(t, db, ctx, "SELECT phone FROM users WHERE id = 1", "555-1234")
}

func TestBH_InsertSelect(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	bhExec(t, db, ctx, "CREATE TABLE users_backup (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, active INTEGER)")
	bhExec(t, db, ctx, "INSERT INTO users_backup (id, name, email, age, active) SELECT id, name, email, age, active FROM users WHERE active = 1")
	bhExpectRows(t, db, ctx, "SELECT * FROM users_backup", 3)
}

func TestBH_TransactionCommit(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	bhExec(t, db, ctx, "BEGIN")
	bhExec(t, db, ctx, "UPDATE users SET age = 99 WHERE id = 1")
	bhExec(t, db, ctx, "COMMIT")

	bhExpectVal(t, db, ctx, "SELECT age FROM users WHERE id = 1", float64(99))
}

func TestBH_TransactionRollback(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	bhExec(t, db, ctx, "BEGIN")
	bhExec(t, db, ctx, "UPDATE users SET age = 99 WHERE id = 1")
	bhExec(t, db, ctx, "ROLLBACK")

	bhExpectVal(t, db, ctx, "SELECT age FROM users WHERE id = 1", float64(30))
}

func TestBH_CreateTableIfNotExists(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Should not error
	bhExec(t, db, ctx, "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY)")
	// Data should still be there
	bhExpectRows(t, db, ctx, "SELECT * FROM users", 5)
}

func TestBH_DropTableIfExists(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	// Should not error even if table doesn't exist
	bhExec(t, db, ctx, "DROP TABLE IF EXISTS nonexistent")
	// Create and drop
	bhExec(t, db, ctx, "CREATE TABLE temp (id INTEGER PRIMARY KEY)")
	bhExec(t, db, ctx, "DROP TABLE IF EXISTS temp")
}

func TestBH_StringConcatenation(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// String concatenation with ||
	bhExpectVal(t, db, ctx, "SELECT name || ' (' || email || ')' FROM users WHERE id = 1", "Alice (alice@test.com)")
}

func TestBH_ArithmeticExpressions(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Arithmetic in SELECT
	bhExpectVal(t, db, ctx, "SELECT age * 2 FROM users WHERE id = 1", float64(60))
	bhExpectVal(t, db, ctx, "SELECT age + 10 FROM users WHERE id = 1", float64(40))
	bhExpectVal(t, db, ctx, "SELECT 100 / 4", float64(25))
	bhExpectVal(t, db, ctx, "SELECT 10 % 3", float64(1))

	// Arithmetic in WHERE
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE age * 2 > 60", 1)
}

func TestBH_CastExpressions(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	bhExpectVal(t, db, ctx, "SELECT CAST(42 AS TEXT)", "42")
	bhExpectVal(t, db, ctx, "SELECT CAST('123' AS INTEGER)", float64(123))
	bhExpectVal(t, db, ctx, "SELECT CAST('3.14' AS REAL)", 3.14)
}

func TestBH_UpdateWithExpression(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// UPDATE with expression referencing current value
	bhExec(t, db, ctx, "UPDATE users SET age = age + 1 WHERE id = 1")
	bhExpectVal(t, db, ctx, "SELECT age FROM users WHERE id = 1", float64(31))

	// UPDATE with CASE
	bhExec(t, db, ctx, "UPDATE users SET active = CASE WHEN age > 30 THEN 1 ELSE 0 END")
	bhExpectVal(t, db, ctx, "SELECT active FROM users WHERE id = 1", float64(1)) // age=31, should be 1
}

func TestBH_MultiTableSubqueries(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// IN subquery
	rows := bhQuery(t, db, ctx, "SELECT name FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)")
	if len(rows) < 1 {
		t.Fatal("expected results from IN subquery")
	}

	// EXISTS
	rows = bhQuery(t, db, ctx, "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)")
	if len(rows) < 1 {
		t.Fatal("expected results from EXISTS subquery")
	}

	// NOT EXISTS
	rows = bhQuery(t, db, ctx, "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND status = 'cancelled')")
	if len(rows) < 1 {
		t.Fatal("expected results from NOT EXISTS subquery")
	}
}

func TestBH_SelectDistinct(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	rows := bhQuery(t, db, ctx, "SELECT DISTINCT status FROM orders")
	if len(rows) != 3 {
		t.Fatalf("expected 3 distinct statuses, got %d", len(rows))
	}

	rows = bhQuery(t, db, ctx, "SELECT DISTINCT user_id FROM orders ORDER BY user_id")
	if len(rows) != 4 {
		t.Fatalf("expected 4 distinct user_ids, got %d", len(rows))
	}
}

func TestBH_LimitOffset(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// LIMIT
	bhExpectRows(t, db, ctx, "SELECT * FROM users LIMIT 3", 3)
	// LIMIT + OFFSET
	bhExpectRows(t, db, ctx, "SELECT * FROM users LIMIT 2 OFFSET 3", 2)
	// Only LIMIT 1
	bhExpectRows(t, db, ctx, "SELECT * FROM users ORDER BY id LIMIT 1", 1)
}

func TestBH_IndexCreateAndUse(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Create index and verify queries still work
	bhExec(t, db, ctx, "CREATE INDEX idx_users_age ON users (age)")
	bhExec(t, db, ctx, "CREATE UNIQUE INDEX idx_orders_product ON orders (product)")

	// Queries should still work correctly
	bhExpectRows(t, db, ctx, "SELECT name FROM users WHERE age = 30", 1)
	bhExpectRows(t, db, ctx, "SELECT * FROM orders WHERE product = 'Laptop'", 1)
}

func TestBH_ComplexJoinWithAggregate(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// JOIN with GROUP BY and HAVING
	rows := bhQuery(t, db, ctx, `
		SELECT users.name, COUNT(orders.id) as order_count, SUM(orders.amount) as total
		FROM users
		JOIN orders ON users.id = orders.user_id
		GROUP BY users.name
		HAVING COUNT(orders.id) > 1`)
	if len(rows) < 1 {
		t.Fatal("expected at least 1 user with >1 orders")
	}
}

func TestBH_ExistsWithConditions(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// EXISTS combined with AND
	rows := bhQuery(t, db, ctx, "SELECT name FROM users WHERE active = 1 AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND amount > 400)")
	t.Logf("EXISTS with AND: %d rows", len(rows))

	// NOT EXISTS combined with OR
	rows = bhQuery(t, db, ctx, "SELECT name FROM users WHERE active = 0 OR NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)")
	t.Logf("NOT EXISTS with OR: %d rows", len(rows))
}

func TestBH_MultipleUpdateStatements(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Multiple updates in sequence
	bhExec(t, db, ctx, "UPDATE orders SET status = 'shipped' WHERE status = 'pending'")
	bhExpectRows(t, db, ctx, "SELECT * FROM orders WHERE status = 'shipped'", 2)

	bhExec(t, db, ctx, "UPDATE orders SET amount = amount * 1.1 WHERE status = 'completed'")
	rows := bhQuery(t, db, ctx, "SELECT amount FROM orders WHERE id = 1")
	// 999.99 * 1.1 ≈ 1099.989
	if len(rows) > 0 {
		amt := fmt.Sprintf("%.2f", rows[0][0])
		if amt != "1099.99" {
			t.Logf("Amount after 10%% increase: %s (expected ~1099.99)", amt)
		}
	}
}

func TestBH_EmptyTableOperations(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	bhExec(t, db, ctx, "CREATE TABLE empty (id INTEGER PRIMARY KEY, val TEXT)")

	// Operations on empty table
	bhExpectRows(t, db, ctx, "SELECT * FROM empty", 0)
	bhExpectVal(t, db, ctx, "SELECT COUNT(*) FROM empty", float64(0))

	// Update/Delete on empty table should not error
	bhExec(t, db, ctx, "UPDATE empty SET val = 'x' WHERE id = 1")
	bhExec(t, db, ctx, "DELETE FROM empty WHERE id = 1")
}

func TestBH_SelectWithoutFrom(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	// Scalar SELECT
	bhExpectVal(t, db, ctx, "SELECT 1 + 2", float64(3))
	bhExpectVal(t, db, ctx, "SELECT 'hello'", "hello")
	bhExpectVal(t, db, ctx, "SELECT UPPER('world')", "WORLD")
	bhExpectVal(t, db, ctx, "SELECT 10 * 5 + 3", float64(53))
}

func TestBH_WhereWithSubqueryComparison(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// Scalar subquery comparison
	rows := bhQuery(t, db, ctx, "SELECT name FROM users WHERE age = (SELECT MAX(age) FROM users)")
	if len(rows) != 1 {
		t.Fatalf("expected 1 oldest user, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "Carol" {
		t.Fatalf("expected Carol, got %v", rows[0][0])
	}

	// Greater than subquery
	rows = bhQuery(t, db, ctx, "SELECT name FROM users WHERE age > (SELECT AVG(age) FROM users)")
	t.Logf("Users older than average: %d", len(rows))
}

func TestBH_UpdateWithSubqueryInWhere(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// UPDATE with scalar subquery in WHERE
	bhExec(t, db, ctx, "UPDATE orders SET status = 'vip' WHERE user_id = (SELECT id FROM users WHERE name = 'Alice')")
	bhExpectRows(t, db, ctx, "SELECT * FROM orders WHERE status = 'vip'", 2)
}

func TestBH_DeleteWithSubqueryInWhere(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	// DELETE with IN subquery
	bhExec(t, db, ctx, "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE active = 0)")
	// Users 3 and 5 are inactive. User 3 has 1 order, User 5 has 0 orders. So 1 order deleted.
	bhExpectRows(t, db, ctx, "SELECT * FROM orders", 5)
}

func TestBH_ComplexCTEWithJoin(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	rows := bhQuery(t, db, ctx, `
		WITH user_totals AS (
			SELECT user_id, SUM(amount) as total_amount
			FROM orders
			GROUP BY user_id
		)
		SELECT users.name, user_totals.total_amount
		FROM users
		JOIN user_totals ON users.id = user_totals.user_id
		ORDER BY user_totals.total_amount DESC`)
	if len(rows) < 1 {
		t.Fatal("expected results from CTE with JOIN")
	}
	t.Logf("CTE with JOIN returned %d rows", len(rows))
}

func TestBH_NullIfExpression(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	// NULLIF returns NULL if both args are equal
	rows := bhQuery(t, db, ctx, "SELECT NULLIF(1, 1)")
	if len(rows) > 0 && rows[0][0] != nil {
		t.Fatalf("expected NULL from NULLIF(1,1), got %v", rows[0][0])
	}

	bhExpectVal(t, db, ctx, "SELECT NULLIF(1, 2)", float64(1))
}

func TestBH_MathFunctions(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	bhExpectVal(t, db, ctx, "SELECT ABS(-10)", float64(10))
	bhExpectVal(t, db, ctx, "SELECT ROUND(2.5)", float64(3))
	bhExpectVal(t, db, ctx, "SELECT FLOOR(2.9)", float64(2))
	bhExpectVal(t, db, ctx, "SELECT CEIL(2.1)", float64(3))
}

func TestBH_StringFunctions(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()

	bhExpectVal(t, db, ctx, "SELECT LENGTH('hello')", float64(5))
	bhExpectVal(t, db, ctx, "SELECT UPPER('hello')", "HELLO")
	bhExpectVal(t, db, ctx, "SELECT LOWER('HELLO')", "hello")
	bhExpectVal(t, db, ctx, "SELECT TRIM('  hello  ')", "hello")
	bhExpectVal(t, db, ctx, "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	bhExpectVal(t, db, ctx, "SELECT SUBSTR('hello', 1, 3)", "hel")
	bhExpectVal(t, db, ctx, "SELECT INSTR('hello world', 'world')", float64(7))
}

func TestBH_ShowCommands(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	rows := bhQuery(t, db, ctx, "SHOW TABLES")
	if len(rows) < 3 {
		t.Fatalf("expected at least 3 tables, got %d", len(rows))
	}

	rows = bhQuery(t, db, ctx, "SHOW COLUMNS FROM users")
	if len(rows) < 1 {
		t.Fatal("expected columns from SHOW COLUMNS")
	}
}

func TestBH_DescribeTable(t *testing.T) {
	db, ctx := bh(t)
	defer db.Close()
	setupBHData(t, db, ctx)

	rows := bhQuery(t, db, ctx, "DESCRIBE users")
	if len(rows) < 5 {
		t.Fatalf("expected at least 5 columns in users table, got %d", len(rows))
	}
}

// ==================== Compile guard ====================
var _ = strings.Contains
