package test

import (
	"fmt"
	"testing"
)

// =============================================================================
// v94: Tests for INSERT OR REPLACE bugs and edge cases
// =============================================================================

// Bug #3: INSERT OR REPLACE with UNIQUE index should delete old row
func TestV94_InsertOrReplaceUniqueIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94a (id INTEGER PRIMARY KEY, email TEXT, name TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t94a_email ON t94a(email)")

	// Insert initial rows
	afExec(t, db, ctx, "INSERT INTO t94a VALUES (1, 'alice@test.com', 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t94a VALUES (2, 'bob@test.com', 'Bob')")

	// INSERT OR REPLACE with same PK - should replace
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t94a VALUES (1, 'alice@test.com', 'Alice Updated')")
	afExpectVal(t, db, ctx, "SELECT name FROM t94a WHERE id = 1", "Alice Updated")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94a", float64(2))

	// INSERT OR REPLACE with different PK but same UNIQUE email - should delete old row
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t94a VALUES (3, 'bob@test.com', 'Bob New')")
	afExpectVal(t, db, ctx, "SELECT name FROM t94a WHERE id = 3", "Bob New")

	// Old row with id=2 should be gone since it conflicted on email
	rows := afQuery(t, db, ctx, "SELECT id FROM t94a WHERE id = 2")
	if len(rows) != 0 {
		t.Fatalf("expected row id=2 to be deleted (UNIQUE conflict on email), but found it")
	}

	// Should have exactly 2 rows now
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94a", float64(2))
}

// INSERT OR REPLACE with column UNIQUE constraint (not index)
func TestV94_InsertOrReplaceUniqueColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94b (id INTEGER PRIMARY KEY, code TEXT UNIQUE, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94b VALUES (1, 'AAA', 10)")
	afExec(t, db, ctx, "INSERT INTO t94b VALUES (2, 'BBB', 20)")

	// Replace by PK conflict
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t94b VALUES (1, 'AAA', 100)")
	afExpectVal(t, db, ctx, "SELECT val FROM t94b WHERE id = 1", float64(100))

	// Replace by UNIQUE column conflict (different PK)
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t94b VALUES (3, 'BBB', 200)")
	afExpectVal(t, db, ctx, "SELECT val FROM t94b WHERE code = 'BBB'", float64(200))

	// Row with id=2 should be gone
	rows := afQuery(t, db, ctx, "SELECT id FROM t94b WHERE id = 2")
	if len(rows) != 0 {
		t.Fatalf("old row id=2 should be deleted on UNIQUE column conflict")
	}
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94b", float64(2))
}

// INSERT OR IGNORE with UNIQUE index
func TestV94_InsertOrIgnoreUniqueIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94c (id INTEGER PRIMARY KEY, email TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_t94c_email ON t94c(email)")

	afExec(t, db, ctx, "INSERT INTO t94c VALUES (1, 'alice@test.com')")

	// INSERT OR IGNORE with conflicting UNIQUE index - should be silently ignored
	afExec(t, db, ctx, "INSERT OR IGNORE INTO t94c VALUES (2, 'alice@test.com')")

	// Should still have only 1 row
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94c", float64(1))
	afExpectVal(t, db, ctx, "SELECT id FROM t94c WHERE email = 'alice@test.com'", float64(1))
}

// INSERT OR REPLACE with multiple UNIQUE columns
func TestV94_InsertOrReplaceMultipleUnique(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94d (id INTEGER PRIMARY KEY, email TEXT UNIQUE, phone TEXT UNIQUE, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t94d VALUES (1, 'a@test.com', '111', 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t94d VALUES (2, 'b@test.com', '222', 'Bob')")

	// Replace with PK conflict
	afExec(t, db, ctx, "INSERT OR REPLACE INTO t94d VALUES (1, 'a@new.com', '333', 'Alice New')")
	afExpectVal(t, db, ctx, "SELECT name FROM t94d WHERE id = 1", "Alice New")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94d", float64(2))
}

// INSERT with prepared statement placeholders in function calls
func TestV94_PreparedInsertNestedPlaceholder(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94e (id INTEGER PRIMARY KEY, val TEXT)")

	// Simple INSERT with placeholder
	_, err := db.Exec(ctx, "INSERT INTO t94e VALUES (?, ?)", 1, "hello")
	if err != nil {
		t.Fatalf("INSERT with placeholders failed: %v", err)
	}
	afExpectVal(t, db, ctx, "SELECT val FROM t94e WHERE id = 1", "hello")
}

// UPDATE with prepared statement placeholders
func TestV94_PreparedUpdateNestedPlaceholder(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94f (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94f VALUES (1, 'Alice', 10)")
	afExec(t, db, ctx, "INSERT INTO t94f VALUES (2, 'Bob', 20)")

	// UPDATE with placeholder in SET and WHERE
	_, err := db.Exec(ctx, "UPDATE t94f SET val = ? WHERE id = ?", 100, 1)
	if err != nil {
		t.Fatalf("UPDATE with placeholders failed: %v", err)
	}
	afExpectVal(t, db, ctx, "SELECT val FROM t94f WHERE id = 1", float64(100))

	// UPDATE with multiple SET placeholders
	_, err = db.Exec(ctx, "UPDATE t94f SET name = ?, val = ? WHERE id = ?", "Alice Updated", 200, 1)
	if err != nil {
		t.Fatalf("UPDATE with multiple SET placeholders failed: %v", err)
	}
	afExpectVal(t, db, ctx, "SELECT name FROM t94f WHERE id = 1", "Alice Updated")
	afExpectVal(t, db, ctx, "SELECT val FROM t94f WHERE id = 1", float64(200))
}

// Test UPDATE SET a=b, b=a (column swap)
func TestV94_UpdateColumnSwap(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94g (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94g VALUES (1, 10, 20)")

	afExec(t, db, ctx, "UPDATE t94g SET a = b, b = a WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT a FROM t94g WHERE id = 1", float64(20))
	afExpectVal(t, db, ctx, "SELECT b FROM t94g WHERE id = 1", float64(10))
}

// Test DEFAULT expressions in INSERT
func TestV94_InsertWithDefault(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94h (id INTEGER PRIMARY KEY, val INTEGER DEFAULT 42, name TEXT DEFAULT 'unknown')")
	afExec(t, db, ctx, "INSERT INTO t94h (id) VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT val FROM t94h WHERE id = 1", float64(42))
	afExpectVal(t, db, ctx, "SELECT name FROM t94h WHERE id = 1", "unknown")
}

// Test subquery in INSERT
func TestV94_InsertWithSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94_src (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t94_dst (id INTEGER PRIMARY KEY, total INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94_src VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t94_src VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t94_src VALUES (3, 30)")

	// INSERT with subquery value
	afExec(t, db, ctx, "INSERT INTO t94_dst VALUES (1, (SELECT SUM(val) FROM t94_src))")
	afExpectVal(t, db, ctx, "SELECT total FROM t94_dst WHERE id = 1", float64(60))
}

// Test NULL handling in UNIQUE constraints
func TestV94_UniqueConstraintWithNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94i (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
	// Multiple NULLs should be allowed in UNIQUE column (SQL standard)
	afExec(t, db, ctx, "INSERT INTO t94i VALUES (1, NULL)")
	afExec(t, db, ctx, "INSERT INTO t94i VALUES (2, NULL)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94i", float64(2))
}

// Test complex WHERE with multiple conditions and placeholders
func TestV94_ComplexWhereWithPlaceholders(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94j (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER, active BOOLEAN)")
	afExec(t, db, ctx, "INSERT INTO t94j VALUES (1, 'A', 10, TRUE)")
	afExec(t, db, ctx, "INSERT INTO t94j VALUES (2, 'B', 20, TRUE)")
	afExec(t, db, ctx, "INSERT INTO t94j VALUES (3, 'A', 30, FALSE)")
	afExec(t, db, ctx, "INSERT INTO t94j VALUES (4, 'B', 40, FALSE)")

	// Complex WHERE with placeholders
	rows, err := db.Query(ctx, "SELECT id FROM t94j WHERE cat = ? AND val > ?", "A", 5)
	if err != nil {
		t.Fatalf("query with placeholders failed: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 rows for cat='A' AND val>5, got %d", count)
	}
}

// Test DELETE with subquery in WHERE
func TestV94_DeleteWithSubqueryWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94k (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94k VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t94k VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t94k VALUES (3, 30)")

	// Delete rows where val > avg
	afExec(t, db, ctx, "DELETE FROM t94k WHERE val > (SELECT AVG(val) FROM t94k)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94k", float64(2))
}

// Test UPDATE with CASE expression
func TestV94_UpdateWithCase(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94l (id INTEGER PRIMARY KEY, score INTEGER, grade TEXT)")
	afExec(t, db, ctx, "INSERT INTO t94l VALUES (1, 95, '')")
	afExec(t, db, ctx, "INSERT INTO t94l VALUES (2, 75, '')")
	afExec(t, db, ctx, "INSERT INTO t94l VALUES (3, 55, '')")

	afExec(t, db, ctx, "UPDATE t94l SET grade = CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' ELSE 'C' END")
	afExpectVal(t, db, ctx, "SELECT grade FROM t94l WHERE id = 1", "A")
	afExpectVal(t, db, ctx, "SELECT grade FROM t94l WHERE id = 2", "B")
	afExpectVal(t, db, ctx, "SELECT grade FROM t94l WHERE id = 3", "C")
}

// Test edge: very long string values
func TestV94_LongStringValues(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94m (id INTEGER PRIMARY KEY, data TEXT)")
	longStr := ""
	for i := 0; i < 1000; i++ {
		longStr += "abcdefghij"
	}
	_, err := db.Exec(ctx, "INSERT INTO t94m VALUES (1, ?)", longStr)
	if err != nil {
		t.Fatalf("INSERT long string failed: %v", err)
	}

	rows := afQuery(t, db, ctx, "SELECT LENGTH(data) FROM t94m WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "10000" {
		t.Fatalf("expected length 10000, got %v", got)
	}
}

// Test edge: INSERT with expression values
func TestV94_InsertWithExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94n (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94n VALUES (1, 2 + 3)")
	afExpectVal(t, db, ctx, "SELECT val FROM t94n WHERE id = 1", float64(5))

	afExec(t, db, ctx, "INSERT INTO t94n VALUES (2, 10 * 5 - 3)")
	afExpectVal(t, db, ctx, "SELECT val FROM t94n WHERE id = 2", float64(47))
}

// Test: SELECT with expression columns
func TestV94_SelectWithExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94o (id INTEGER PRIMARY KEY, price INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94o VALUES (1, 10, 5)")
	afExec(t, db, ctx, "INSERT INTO t94o VALUES (2, 20, 3)")

	afExpectVal(t, db, ctx, "SELECT price * qty FROM t94o WHERE id = 1", float64(50))
	afExpectVal(t, db, ctx, "SELECT SUM(price * qty) FROM t94o", float64(110))
}

// Test: Multiple INSERT in single transaction
func TestV94_MultiInsertTransaction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94p (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "BEGIN")
	for i := 1; i <= 100; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t94p VALUES (%d, %d)", i, i*10))
	}
	afExec(t, db, ctx, "COMMIT")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t94p", float64(100))
	afExpectVal(t, db, ctx, "SELECT SUM(val) FROM t94p", float64(50500))
}

// Test: UNION with ORDER BY
func TestV94_UnionWithOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94q1 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t94q2 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t94q1 VALUES (1, 'Charlie')")
	afExec(t, db, ctx, "INSERT INTO t94q1 VALUES (2, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t94q2 VALUES (1, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO t94q2 VALUES (2, 'David')")

	rows := afQuery(t, db, ctx,
		"SELECT name FROM t94q1 UNION SELECT name FROM t94q2 ORDER BY name")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Alice" {
		t.Fatalf("expected 'Alice' first, got %v", first)
	}
}

// Test: EXCEPT operation
func TestV94_ExceptOperation(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94r1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t94r2 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94r1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t94r1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t94r1 VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t94r2 VALUES (1, 20)")

	rows := afQuery(t, db, ctx,
		"SELECT val FROM t94r1 EXCEPT SELECT val FROM t94r2")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from EXCEPT, got %d", len(rows))
	}
}

// Test: INTERSECT operation
func TestV94_IntersectOperation(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94s1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE t94s2 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94s1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t94s1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t94s1 VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t94s2 VALUES (1, 20)")
	afExec(t, db, ctx, "INSERT INTO t94s2 VALUES (2, 30)")

	rows := afQuery(t, db, ctx,
		"SELECT val FROM t94s1 INTERSECT SELECT val FROM t94s2")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from INTERSECT, got %d", len(rows))
	}
}

// Test: Self-join
func TestV94_SelfJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94t (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94t VALUES (1, 'CEO', NULL)")
	afExec(t, db, ctx, "INSERT INTO t94t VALUES (2, 'VP', 1)")
	afExec(t, db, ctx, "INSERT INTO t94t VALUES (3, 'Manager', 2)")
	afExec(t, db, ctx, "INSERT INTO t94t VALUES (4, 'Dev', 3)")

	// Self-join to find employees with their managers
	rows := afQuery(t, db, ctx,
		"SELECT e.name, m.name FROM t94t e JOIN t94t m ON e.manager_id = m.id ORDER BY e.id")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows (excluding CEO), got %d", len(rows))
	}
	emp := fmt.Sprintf("%v", rows[0][0])
	mgr := fmt.Sprintf("%v", rows[0][1])
	if emp != "VP" || mgr != "CEO" {
		t.Fatalf("expected VP->CEO, got %v->%v", emp, mgr)
	}
}

// Test: Nested subqueries
func TestV94_NestedSubqueries(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t94u (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t94u VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t94u VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t94u VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO t94u VALUES (4, 40)")
	afExec(t, db, ctx, "INSERT INTO t94u VALUES (5, 50)")

	// Select rows above average
	rows := afQuery(t, db, ctx,
		"SELECT val FROM t94u WHERE val > (SELECT AVG(val) FROM t94u) ORDER BY val")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows above avg(30), got %d", len(rows))
	}
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "40" {
		t.Fatalf("expected 40 first, got %v", first)
	}
}
