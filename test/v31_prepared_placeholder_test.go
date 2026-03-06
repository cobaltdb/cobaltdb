package test

import (
	"fmt"
	"testing"
)

func TestV31PreparedAndPlaceholder(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	// ============================================================
	// === PLACEHOLDER QUERIES WITH ? ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE ph_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ph_test VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO ph_test VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO ph_test VALUES (3, 'Charlie', 300)")

	// Query with placeholder
	total++
	rows, err := db.Query(ctx, "SELECT name FROM ph_test WHERE id = ?", 1)
	if err != nil {
		t.Errorf("[FAIL] placeholder query: %v", err)
	} else {
		if rows.Next() {
			var name string
			if err := rows.Scan(&name); err == nil && name == "Alice" {
				pass++
			} else {
				t.Errorf("[FAIL] placeholder query: got name=%v, err=%v", name, err)
			}
		} else {
			t.Errorf("[FAIL] placeholder query: no rows")
		}
		rows.Close()
	}

	// Exec with placeholder
	total++
	_, err = db.Exec(ctx, "INSERT INTO ph_test VALUES (?, ?, ?)", 4, "Diana", 400)
	if err != nil {
		t.Errorf("[FAIL] placeholder insert: %v", err)
	} else {
		pass++
	}
	check("Placeholder insert result", "SELECT name FROM ph_test WHERE id = 4", "Diana")
	check("Placeholder insert val", "SELECT val FROM ph_test WHERE id = 4", 400)

	// Update with placeholder
	total++
	_, err = db.Exec(ctx, "UPDATE ph_test SET val = ? WHERE id = ?", 999, 1)
	if err != nil {
		t.Errorf("[FAIL] placeholder update: %v", err)
	} else {
		pass++
	}
	check("Placeholder update result", "SELECT val FROM ph_test WHERE id = 1", 999)

	// Delete with placeholder
	total++
	_, err = db.Exec(ctx, "DELETE FROM ph_test WHERE id = ?", 4)
	if err != nil {
		t.Errorf("[FAIL] placeholder delete: %v", err)
	} else {
		pass++
	}
	checkRowCount("After placeholder delete", "SELECT * FROM ph_test", 3)

	// Multiple placeholders in WHERE
	total++
	rows, err = db.Query(ctx, "SELECT name FROM ph_test WHERE val > ? AND val < ?", 100, 300)
	if err != nil {
		t.Errorf("[FAIL] multi-placeholder query: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
			var name string
			rows.Scan(&name)
		}
		rows.Close()
		if count == 1 {
			pass++
		} else {
			t.Errorf("[FAIL] multi-placeholder query: expected 1 row, got %d", count)
		}
	}

	// ============================================================
	// === LARGE DATASET OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE large_ops (id INTEGER PRIMARY KEY AUTO_INCREMENT, cat TEXT, val INTEGER)")

	// Insert 200 rows
	for i := 0; i < 200; i++ {
		cats := []string{"A", "B", "C", "D", "E"}
		cat := cats[i%5]
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO large_ops (cat, val) VALUES ('%s', %d)", cat, i+1))
	}

	check("200 rows inserted", "SELECT COUNT(*) FROM large_ops", 200)
	check("Category A count", "SELECT COUNT(*) FROM large_ops WHERE cat = 'A'", 40)
	check("Category E count", "SELECT COUNT(*) FROM large_ops WHERE cat = 'E'", 40)

	// GROUP BY on large dataset
	check("Group count", "SELECT COUNT(DISTINCT cat) FROM large_ops", 5)
	check("Max val in cat A",
		"SELECT MAX(val) FROM large_ops WHERE cat = 'A'", 196) // Last A: 200-4=196

	// ORDER BY on large dataset
	check("ORDER BY large ASC",
		"SELECT val FROM large_ops ORDER BY val ASC LIMIT 1", 1)
	check("ORDER BY large DESC",
		"SELECT val FROM large_ops ORDER BY val DESC LIMIT 1", 200)

	// OFFSET on large dataset
	check("OFFSET on large",
		"SELECT val FROM large_ops ORDER BY val ASC LIMIT 1 OFFSET 10", 11)

	// ============================================================
	// === COMPLEX AGGREGATE QUERIES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE complex_agg (id INTEGER PRIMARY KEY, dept TEXT, role TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO complex_agg VALUES (1, 'Eng', 'Senior', 120)")
	afExec(t, db, ctx, "INSERT INTO complex_agg VALUES (2, 'Eng', 'Junior', 80)")
	afExec(t, db, ctx, "INSERT INTO complex_agg VALUES (3, 'Eng', 'Senior', 130)")
	afExec(t, db, ctx, "INSERT INTO complex_agg VALUES (4, 'Sales', 'Senior', 100)")
	afExec(t, db, ctx, "INSERT INTO complex_agg VALUES (5, 'Sales', 'Junior', 70)")
	afExec(t, db, ctx, "INSERT INTO complex_agg VALUES (6, 'HR', 'Senior', 90)")

	// GROUP BY with multiple columns
	check("GROUP BY dept+role top",
		"SELECT dept FROM complex_agg GROUP BY dept, role ORDER BY SUM(salary) DESC LIMIT 1", "Eng") // Eng Senior: 250

	checkRowCount("GROUP BY dept+role count",
		"SELECT dept, role, SUM(salary) FROM complex_agg GROUP BY dept, role", 5)

	// HAVING with compound condition
	checkRowCount("HAVING compound",
		"SELECT dept, SUM(salary) FROM complex_agg GROUP BY dept HAVING SUM(salary) > 100 AND COUNT(*) >= 2", 2) // Eng:330,3ppl; Sales:170,2ppl

	// ============================================================
	// === VIEW OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE vw_base (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO vw_base VALUES (1, 'active', 100)")
	afExec(t, db, ctx, "INSERT INTO vw_base VALUES (2, 'inactive', 200)")
	afExec(t, db, ctx, "INSERT INTO vw_base VALUES (3, 'active', 300)")
	afExec(t, db, ctx, "INSERT INTO vw_base VALUES (4, 'active', 400)")

	checkNoError("Create filtered view", "CREATE VIEW active_items AS SELECT * FROM vw_base WHERE status = 'active'")
	checkRowCount("View shows filtered", "SELECT * FROM active_items", 3)
	check("View SUM", "SELECT SUM(amount) FROM active_items", 800)

	// Insert into base table, view should reflect
	afExec(t, db, ctx, "INSERT INTO vw_base VALUES (5, 'active', 500)")
	checkRowCount("View reflects insert", "SELECT * FROM active_items", 4)

	// Drop view
	checkNoError("DROP VIEW", "DROP VIEW active_items")

	// ============================================================
	// === TRANSACTION WITH INDEX ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_idx (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	checkNoError("Create index for txn test", "CREATE INDEX txn_idx_score ON txn_idx(score)")

	afExec(t, db, ctx, "INSERT INTO txn_idx VALUES (1, 'Alice', 90)")
	afExec(t, db, ctx, "INSERT INTO txn_idx VALUES (2, 'Bob', 80)")

	checkNoError("BEGIN txn idx", "BEGIN")
	checkNoError("Insert in txn", "INSERT INTO txn_idx VALUES (3, 'Charlie', 85)")
	check("See in txn via index", "SELECT name FROM txn_idx WHERE score = 85", "Charlie")
	checkNoError("ROLLBACK txn idx", "ROLLBACK")

	checkRowCount("Index consistent after rollback", "SELECT * FROM txn_idx WHERE score = 85", 0)
	check("Original data intact via index", "SELECT name FROM txn_idx WHERE score = 90", "Alice")

	// ============================================================
	// === MULTIPLE INDEX OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_idx (id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c TEXT)")
	checkNoError("Create index a", "CREATE INDEX idx_a ON multi_idx(a)")
	checkNoError("Create index b", "CREATE INDEX idx_b ON multi_idx(b)")

	afExec(t, db, ctx, "INSERT INTO multi_idx VALUES (1, 'foo', 10, 'x')")
	afExec(t, db, ctx, "INSERT INTO multi_idx VALUES (2, 'bar', 20, 'y')")
	afExec(t, db, ctx, "INSERT INTO multi_idx VALUES (3, 'foo', 30, 'z')")

	checkRowCount("Index a query", "SELECT * FROM multi_idx WHERE a = 'foo'", 2)
	check("Index b query", "SELECT a FROM multi_idx WHERE b = 20", "bar")

	// ============================================================
	// === SHOW TABLES ===
	// ============================================================
	checkNoError("SHOW TABLES", "SHOW TABLES")

	// ============================================================
	// === CASCADE UPDATE VIA FK ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE fk_p (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE fk_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_p(id) ON DELETE CASCADE)")

	afExec(t, db, ctx, "INSERT INTO fk_p VALUES (1, 'Parent1')")
	afExec(t, db, ctx, "INSERT INTO fk_p VALUES (2, 'Parent2')")
	afExec(t, db, ctx, "INSERT INTO fk_c VALUES (1, 1)")
	afExec(t, db, ctx, "INSERT INTO fk_c VALUES (2, 1)")
	afExec(t, db, ctx, "INSERT INTO fk_c VALUES (3, 2)")

	checkNoError("Delete parent cascade", "DELETE FROM fk_p WHERE id = 1")
	checkRowCount("Children cascaded", "SELECT * FROM fk_c WHERE pid = 1", 0)
	checkRowCount("Other children ok", "SELECT * FROM fk_c WHERE pid = 2", 1)

	// ============================================================
	// === RECURSIVE CTE WITH REAL TABLE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE tree (id INTEGER PRIMARY KEY, name TEXT, parent_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (1, 'Root', NULL)")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (2, 'Child1', 1)")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (3, 'Child2', 1)")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (4, 'Grandchild1', 2)")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (5, 'Grandchild2', 3)")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (6, 'GreatGC', 4)")

	check("Recursive tree traversal count",
		"WITH RECURSIVE hier(id, name, lvl) AS (SELECT id, name, 0 FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.name, h.lvl+1 FROM tree t JOIN hier h ON t.parent_id = h.id) SELECT COUNT(*) FROM hier",
		6) // All 6 nodes

	check("Recursive tree max depth",
		"WITH RECURSIVE hier(id, name, lvl) AS (SELECT id, name, 0 FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.name, h.lvl+1 FROM tree t JOIN hier h ON t.parent_id = h.id) SELECT MAX(lvl) FROM hier",
		3) // Root(0)->Child(1)->GrandChild(2)->GreatGC(3)

	t.Logf("\n=== V31 PREPARED & PLACEHOLDER: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
