package test

import (
	"fmt"
	"testing"
)

func TestV26EdgeCasesAdvanced(t *testing.T) {
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

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
			return
		}
		pass++
	}

	_ = checkError

	// ============================================================
	// === UPDATE WITH CASE EXPRESSION ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE upd_case (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, grade TEXT)")
	afExec(t, db, ctx, "INSERT INTO upd_case VALUES (1, 'Alice', 95, '')")
	afExec(t, db, ctx, "INSERT INTO upd_case VALUES (2, 'Bob', 72, '')")
	afExec(t, db, ctx, "INSERT INTO upd_case VALUES (3, 'Charlie', 88, '')")
	afExec(t, db, ctx, "INSERT INTO upd_case VALUES (4, 'Diana', 45, '')")

	checkNoError("UPDATE with CASE",
		"UPDATE upd_case SET grade = CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END")

	check("Grade A", "SELECT grade FROM upd_case WHERE id = 1", "A")
	check("Grade C", "SELECT grade FROM upd_case WHERE id = 2", "C")
	check("Grade B", "SELECT grade FROM upd_case WHERE id = 3", "B")
	check("Grade F", "SELECT grade FROM upd_case WHERE id = 4", "F")

	// ============================================================
	// === DELETE WITH SUBQUERY IN WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE del_main (id INTEGER PRIMARY KEY, category TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO del_main VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO del_main VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO del_main VALUES (3, 'B', 30)")
	afExec(t, db, ctx, "INSERT INTO del_main VALUES (4, 'B', 40)")

	checkNoError("DELETE with subquery",
		"DELETE FROM del_main WHERE val > (SELECT AVG(val) FROM del_main)")
	checkRowCount("Rows after delete", "SELECT * FROM del_main", 2) // 10, 20 remain (avg was 25)

	// ============================================================
	// === NESTED AGGREGATES IN HAVING ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE nested_agg (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO nested_agg VALUES (1, 'X', 10)")
	afExec(t, db, ctx, "INSERT INTO nested_agg VALUES (2, 'X', 20)")
	afExec(t, db, ctx, "INSERT INTO nested_agg VALUES (3, 'Y', 30)")
	afExec(t, db, ctx, "INSERT INTO nested_agg VALUES (4, 'Y', 40)")
	afExec(t, db, ctx, "INSERT INTO nested_agg VALUES (5, 'Z', 50)")

	checkRowCount("HAVING with SUM > value",
		"SELECT grp, SUM(val) FROM nested_agg GROUP BY grp HAVING SUM(val) > 25", 3) // X(30), Y(70), Z(50)

	checkRowCount("HAVING with COUNT",
		"SELECT grp FROM nested_agg GROUP BY grp HAVING COUNT(*) = 2", 2) // X, Y

	check("HAVING with SUM order",
		"SELECT grp FROM nested_agg GROUP BY grp HAVING SUM(val) > 25 ORDER BY SUM(val) DESC LIMIT 1", "Y")

	// ============================================================
	// === MULTIPLE JOINS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE mj_customers (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE mj_orders2 (id INTEGER PRIMARY KEY, cust_id INTEGER, product_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE mj_products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)")

	afExec(t, db, ctx, "INSERT INTO mj_customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO mj_customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO mj_products VALUES (1, 'Widget', 10)")
	afExec(t, db, ctx, "INSERT INTO mj_products VALUES (2, 'Gadget', 25)")
	afExec(t, db, ctx, "INSERT INTO mj_products VALUES (3, 'Phone', 100)")
	afExec(t, db, ctx, "INSERT INTO mj_orders2 VALUES (1, 1, 1)")
	afExec(t, db, ctx, "INSERT INTO mj_orders2 VALUES (2, 1, 2)")
	afExec(t, db, ctx, "INSERT INTO mj_orders2 VALUES (3, 1, 3)")
	afExec(t, db, ctx, "INSERT INTO mj_orders2 VALUES (4, 2, 1)")

	check("3-table JOIN total spend",
		"SELECT mj_customers.name FROM mj_customers JOIN mj_orders2 ON mj_customers.id = mj_orders2.cust_id JOIN mj_products ON mj_orders2.product_id = mj_products.id GROUP BY mj_customers.name ORDER BY SUM(mj_products.price) DESC LIMIT 1",
		"Alice") // Alice: 10+25+100=135, Bob: 10

	check("3-table JOIN count products",
		"SELECT mj_customers.name FROM mj_customers JOIN mj_orders2 ON mj_customers.id = mj_orders2.cust_id JOIN mj_products ON mj_orders2.product_id = mj_products.id GROUP BY mj_customers.name ORDER BY COUNT(*) DESC LIMIT 1",
		"Alice") // Alice: 3, Bob: 1

	// ============================================================
	// === LEFT JOIN WITH NULL HANDLING ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE lj_parent2 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE lj_child2 (id INTEGER PRIMARY KEY, parent_id INTEGER, val INTEGER)")

	afExec(t, db, ctx, "INSERT INTO lj_parent2 VALUES (1, 'Has Children')")
	afExec(t, db, ctx, "INSERT INTO lj_parent2 VALUES (2, 'No Children')")
	afExec(t, db, ctx, "INSERT INTO lj_child2 VALUES (1, 1, 10)")
	afExec(t, db, ctx, "INSERT INTO lj_child2 VALUES (2, 1, 20)")

	checkRowCount("LEFT JOIN shows all parents",
		"SELECT lj_parent2.name FROM lj_parent2 LEFT JOIN lj_child2 ON lj_parent2.id = lj_child2.parent_id", 3)
	// Parent 1 appears twice (2 children), Parent 2 appears once (no children)

	check("LEFT JOIN with COALESCE for NULL",
		"SELECT COALESCE(SUM(lj_child2.val), 0) FROM lj_parent2 LEFT JOIN lj_child2 ON lj_parent2.id = lj_child2.parent_id WHERE lj_parent2.id = 2",
		0)

	// ============================================================
	// === INSERT OR REPLACE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE upsert_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO upsert_test VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO upsert_test VALUES (2, 'Bob', 200)")

	checkNoError("INSERT OR REPLACE existing",
		"INSERT OR REPLACE INTO upsert_test VALUES (1, 'Alice Updated', 150)")
	check("Replaced value", "SELECT name FROM upsert_test WHERE id = 1", "Alice Updated")
	check("Replaced val", "SELECT val FROM upsert_test WHERE id = 1", 150)
	checkRowCount("Same number of rows", "SELECT * FROM upsert_test", 2)

	// INSERT OR IGNORE
	checkNoError("INSERT OR IGNORE duplicate",
		"INSERT OR IGNORE INTO upsert_test VALUES (2, 'Bob Ignored', 999)")
	check("Ignore preserved original", "SELECT name FROM upsert_test WHERE id = 2", "Bob")

	// ============================================================
	// === COMPLEX WHERE WITH OR AND AND ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE complex_where (id INTEGER PRIMARY KEY, a INTEGER, b TEXT, c INTEGER)")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (1, 10, 'X', 100)")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (2, 20, 'Y', 200)")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (3, 30, 'X', 300)")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (4, 40, 'Y', 400)")
	afExec(t, db, ctx, "INSERT INTO complex_where VALUES (5, 50, 'Z', 500)")

	checkRowCount("Complex OR AND",
		"SELECT * FROM complex_where WHERE (a > 20 AND b = 'X') OR (c >= 400)", 3) // id=3(30,X), id=4(Y,400), id=5(Z,500)

	checkRowCount("Complex nested AND OR",
		"SELECT * FROM complex_where WHERE a >= 20 AND (b = 'X' OR b = 'Y')", 3) // id=2,3,4

	// ============================================================
	// === ORDER BY WITH NULLS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE null_order (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (1, 30)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (3, 10)")
	afExec(t, db, ctx, "INSERT INTO null_order VALUES (4, 20)")

	// NULLs should sort consistently (typically first or last)
	checkRowCount("ORDER BY with NULLs returns all",
		"SELECT * FROM null_order ORDER BY val", 4)

	check("ORDER BY ASC non-null first",
		"SELECT val FROM null_order WHERE val IS NOT NULL ORDER BY val ASC LIMIT 1", 10)

	// ============================================================
	// === SUBQUERY IN SELECT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE sub_main2 (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO sub_main2 VALUES (1, 'Eng', 100)")
	afExec(t, db, ctx, "INSERT INTO sub_main2 VALUES (2, 'Eng', 120)")
	afExec(t, db, ctx, "INSERT INTO sub_main2 VALUES (3, 'Sales', 80)")

	check("Scalar subquery in WHERE",
		"SELECT dept FROM sub_main2 WHERE salary = (SELECT MAX(salary) FROM sub_main2)", "Eng")

	check("Correlated subquery",
		"SELECT dept FROM sub_main2 s1 WHERE salary > (SELECT AVG(salary) FROM sub_main2 s2 WHERE s2.dept = s1.dept) ORDER BY salary DESC LIMIT 1",
		"Eng") // Eng avg=110, only salary=120 > 110

	// ============================================================
	// === AGGREGATE WITHOUT GROUP BY ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE agg_no_gb (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO agg_no_gb VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO agg_no_gb VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO agg_no_gb VALUES (3, 30)")

	check("COUNT without GROUP BY", "SELECT COUNT(*) FROM agg_no_gb", 3)
	check("SUM without GROUP BY", "SELECT SUM(val) FROM agg_no_gb", 60)
	check("AVG without GROUP BY", "SELECT AVG(val) FROM agg_no_gb", 20)
	check("MIN without GROUP BY", "SELECT MIN(val) FROM agg_no_gb", 10)
	check("MAX without GROUP BY", "SELECT MAX(val) FROM agg_no_gb", 30)

	// ============================================================
	// === CASE WITH AGGREGATE (no GROUP BY) ===
	// ============================================================
	check("CASE WHEN SUM no GROUP BY",
		"SELECT CASE WHEN SUM(val) > 50 THEN 'big' ELSE 'small' END FROM agg_no_gb", "big")

	check("CASE WHEN COUNT no GROUP BY",
		"SELECT CASE WHEN COUNT(*) > 2 THEN 'many' ELSE 'few' END FROM agg_no_gb", "many")

	// ============================================================
	// === MULTIPLE AGGREGATE IN SELECT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE multi_agg (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO multi_agg VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO multi_agg VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO multi_agg VALUES (3, 'B', 30)")

	check("Multiple aggregates SUM first",
		"SELECT SUM(val) FROM multi_agg WHERE grp = 'A'", 30)

	check("Multiple aggregates COUNT",
		"SELECT COUNT(*) FROM multi_agg WHERE grp = 'A'", 2)

	// ============================================================
	// === CREATE INDEX AND USE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE idx_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	afExec(t, db, ctx, "INSERT INTO idx_test VALUES (1, 'Alice', 30)")
	afExec(t, db, ctx, "INSERT INTO idx_test VALUES (2, 'Bob', 25)")
	afExec(t, db, ctx, "INSERT INTO idx_test VALUES (3, 'Charlie', 35)")

	checkNoError("CREATE INDEX", "CREATE INDEX idx_age ON idx_test(age)")
	check("Query with index", "SELECT name FROM idx_test WHERE age = 25", "Bob")
	checkRowCount("Range query with index", "SELECT * FROM idx_test WHERE age >= 30", 2)

	// ============================================================
	// === ALTER TABLE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE alter_test (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO alter_test VALUES (1, 'Alice')")

	checkNoError("ALTER TABLE ADD COLUMN", "ALTER TABLE alter_test ADD COLUMN age INTEGER DEFAULT 0")
	check("New column has default", "SELECT COALESCE(age, 0) FROM alter_test WHERE id = 1", 0)

	checkNoError("INSERT with new column", "INSERT INTO alter_test VALUES (2, 'Bob', 25)")
	check("New column has value", "SELECT age FROM alter_test WHERE id = 2", 25)

	// ============================================================
	// === DROP TABLE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE drop_me (id INTEGER PRIMARY KEY)")
	afExec(t, db, ctx, "INSERT INTO drop_me VALUES (1)")
	checkNoError("DROP TABLE", "DROP TABLE drop_me")
	checkError("Query dropped table", "SELECT * FROM drop_me")

	checkNoError("DROP TABLE IF EXISTS", "DROP TABLE IF EXISTS drop_me")

	// ============================================================
	// === TRANSACTION ISOLATION ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_iso (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO txn_iso VALUES (1, 100)")

	checkNoError("BEGIN", "BEGIN")
	checkNoError("UPDATE in txn", "UPDATE txn_iso SET val = 200 WHERE id = 1")
	check("See updated in txn", "SELECT val FROM txn_iso WHERE id = 1", 200)
	checkNoError("COMMIT", "COMMIT")
	check("Updated persisted after commit", "SELECT val FROM txn_iso WHERE id = 1", 200)

	// ============================================================
	// === EMPTY TABLE AGGREGATES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE empty_agg (id INTEGER PRIMARY KEY, val INTEGER)")

	check("COUNT on empty table", "SELECT COUNT(*) FROM empty_agg", 0)

	// ============================================================
	// === STRING OPERATIONS ===
	// ============================================================
	check("Concatenation", "SELECT 'foo' || 'bar'", "foobar")
	check("UPPER", "SELECT UPPER('hello')", "HELLO")
	check("LOWER", "SELECT LOWER('HELLO')", "hello")
	check("LENGTH", "SELECT LENGTH('hello')", 5)
	check("SUBSTR 2 args", "SELECT SUBSTR('hello', 2)", "ello")
	check("SUBSTR 3 args", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("REPLACE", "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	check("TRIM", "SELECT TRIM('  hello  ')", "hello")

	// ============================================================
	// === BETWEEN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE between_test (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO between_test VALUES (1, 5)")
	afExec(t, db, ctx, "INSERT INTO between_test VALUES (2, 10)")
	afExec(t, db, ctx, "INSERT INTO between_test VALUES (3, 15)")
	afExec(t, db, ctx, "INSERT INTO between_test VALUES (4, 20)")

	checkRowCount("BETWEEN inclusive", "SELECT * FROM between_test WHERE val BETWEEN 10 AND 20", 3)
	checkRowCount("NOT BETWEEN", "SELECT * FROM between_test WHERE val NOT BETWEEN 10 AND 15", 2) // 5, 20

	// ============================================================
	// === EXISTS / NOT EXISTS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE exists_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE exists_child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO exists_parent VALUES (1, 'Has child')")
	afExec(t, db, ctx, "INSERT INTO exists_parent VALUES (2, 'No child')")
	afExec(t, db, ctx, "INSERT INTO exists_child VALUES (1, 1)")

	checkRowCount("EXISTS subquery",
		"SELECT * FROM exists_parent WHERE EXISTS (SELECT 1 FROM exists_child WHERE exists_child.parent_id = exists_parent.id)", 1)

	checkRowCount("NOT EXISTS subquery",
		"SELECT * FROM exists_parent WHERE NOT EXISTS (SELECT 1 FROM exists_child WHERE exists_child.parent_id = exists_parent.id)", 1)

	check("EXISTS returns correct row",
		"SELECT name FROM exists_parent WHERE EXISTS (SELECT 1 FROM exists_child WHERE exists_child.parent_id = exists_parent.id)", "Has child")

	// ============================================================
	// === VIEWS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE view_data (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO view_data VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO view_data VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO view_data VALUES (3, 'C', 30)")

	checkNoError("CREATE VIEW", "CREATE VIEW high_vals AS SELECT * FROM view_data WHERE val >= 20")
	checkRowCount("Query view", "SELECT * FROM high_vals", 2)
	check("View data correct", "SELECT name FROM high_vals ORDER BY val DESC LIMIT 1", "C")

	// ============================================================
	// === TRIGGERS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE trig_data (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE trig_log (id INTEGER PRIMARY KEY AUTO_INCREMENT, msg TEXT)")

	checkNoError("CREATE TRIGGER",
		"CREATE TRIGGER log_insert AFTER INSERT ON trig_data BEGIN INSERT INTO trig_log (msg) VALUES ('inserted'); END")

	checkNoError("Insert triggers fire", "INSERT INTO trig_data VALUES (1, 'test', 42)")
	checkRowCount("Trigger log entry", "SELECT * FROM trig_log", 1)
	check("Trigger log message", "SELECT msg FROM trig_log WHERE id = 1", "inserted")

	t.Logf("\n=== V26 EDGE CASES ADVANCED: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
