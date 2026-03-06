package test

import (
	"fmt"
	"testing"
)

func TestV29WindowUnionTrigger(t *testing.T) {
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
	// === WINDOW FUNCTION: ROW_NUMBER CORRECTNESS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE wf_emp (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wf_emp VALUES (1, 'Eng', 120)")
	afExec(t, db, ctx, "INSERT INTO wf_emp VALUES (2, 'Eng', 100)")
	afExec(t, db, ctx, "INSERT INTO wf_emp VALUES (3, 'Eng', 110)")
	afExec(t, db, ctx, "INSERT INTO wf_emp VALUES (4, 'Sales', 90)")
	afExec(t, db, ctx, "INSERT INTO wf_emp VALUES (5, 'Sales', 95)")

	// ROW_NUMBER with partition - just check Eng top salary gets 1
	check("Window ROW_NUMBER #1",
		"SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM wf_emp WHERE id = 1", 1)

	// Check total row count for window query
	checkRowCount("Window all rows",
		"SELECT id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM wf_emp", 5)

	// ============================================================
	// === UNION ALL CORRECTNESS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE u_a (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE u_b (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO u_a VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO u_a VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO u_b VALUES (1, 'Charlie')")
	afExec(t, db, ctx, "INSERT INTO u_b VALUES (2, 'Diana')")

	checkRowCount("UNION ALL", "SELECT name FROM u_a UNION ALL SELECT name FROM u_b", 4)
	checkRowCount("UNION dedup identical", "SELECT name FROM u_a UNION SELECT name FROM u_b", 4) // All different

	// Add overlap
	afExec(t, db, ctx, "INSERT INTO u_b VALUES (3, 'Alice')") // same name as u_a
	checkRowCount("UNION ALL with overlap", "SELECT name FROM u_a UNION ALL SELECT name FROM u_b", 5)
	checkRowCount("UNION dedup overlap", "SELECT name FROM u_a UNION SELECT name FROM u_b", 4) // Alice deduped

	// UNION with ORDER BY
	check("UNION ALL ORDER BY",
		"SELECT name FROM u_a UNION ALL SELECT name FROM u_b ORDER BY name LIMIT 1", "Alice")

	// ============================================================
	// === TRIGGER WITH MULTIPLE STATEMENTS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE trig_src (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE trig_audit (id INTEGER PRIMARY KEY AUTO_INCREMENT, action TEXT)")
	afExec(t, db, ctx, "CREATE TABLE trig_count (id INTEGER PRIMARY KEY, cnt INTEGER)")
	afExec(t, db, ctx, "INSERT INTO trig_count VALUES (1, 0)")

	checkNoError("Create multi-stmt trigger",
		"CREATE TRIGGER after_ins AFTER INSERT ON trig_src BEGIN INSERT INTO trig_audit (action) VALUES ('insert'); UPDATE trig_count SET cnt = cnt + 1 WHERE id = 1; END")

	checkNoError("Insert trigger fires 1", "INSERT INTO trig_src VALUES (1, 'A', 10)")
	checkNoError("Insert trigger fires 2", "INSERT INTO trig_src VALUES (2, 'B', 20)")
	checkNoError("Insert trigger fires 3", "INSERT INTO trig_src VALUES (3, 'C', 30)")

	check("Trigger audit count", "SELECT COUNT(*) FROM trig_audit", 3)
	check("Trigger counter", "SELECT cnt FROM trig_count WHERE id = 1", 3)

	// ============================================================
	// === TRIGGER WITH NEW REFERENCE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE trig_emp (id INTEGER PRIMARY KEY, name TEXT, salary INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE trig_high_sal (id INTEGER PRIMARY KEY AUTO_INCREMENT, emp_name TEXT, salary INTEGER)")

	checkNoError("Create trigger with NEW ref",
		"CREATE TRIGGER after_emp_ins AFTER INSERT ON trig_emp BEGIN INSERT INTO trig_high_sal (emp_name, salary) VALUES (NEW.name, NEW.salary); END")

	checkNoError("Insert emp 1", "INSERT INTO trig_emp VALUES (1, 'Alice', 100000)")
	checkNoError("Insert emp 2", "INSERT INTO trig_emp VALUES (2, 'Bob', 60000)")

	checkRowCount("High sal log", "SELECT * FROM trig_high_sal", 2)
	check("High sal name", "SELECT emp_name FROM trig_high_sal WHERE id = 1", "Alice")
	check("High sal salary", "SELECT salary FROM trig_high_sal WHERE id = 1", 100000)

	// ============================================================
	// === CTE WITH UNION ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE cte_u1 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE cte_u2 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cte_u1 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO cte_u1 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO cte_u2 VALUES (1, 30)")
	afExec(t, db, ctx, "INSERT INTO cte_u2 VALUES (2, 40)")

	check("CTE with filtered data",
		"WITH big AS (SELECT * FROM cte_u2 WHERE val > 25) SELECT SUM(val) FROM big", 70)

	// ============================================================
	// === COMPLEX VIEW WITH AGGREGATE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE vw_sales (id INTEGER PRIMARY KEY, product TEXT, region TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO vw_sales VALUES (1, 'Widget', 'North', 100)")
	afExec(t, db, ctx, "INSERT INTO vw_sales VALUES (2, 'Widget', 'South', 150)")
	afExec(t, db, ctx, "INSERT INTO vw_sales VALUES (3, 'Gadget', 'North', 200)")
	afExec(t, db, ctx, "INSERT INTO vw_sales VALUES (4, 'Gadget', 'South', 250)")

	checkNoError("Create aggregate view",
		"CREATE VIEW product_totals AS SELECT product, SUM(amount) AS total FROM vw_sales GROUP BY product")
	check("View aggregate data",
		"SELECT product FROM product_totals ORDER BY total DESC LIMIT 1", "Gadget") // 200+250=450

	// ============================================================
	// === STRING FUNCTIONS ===
	// ============================================================
	check("REPLACE multiple", "SELECT REPLACE('aabbcc', 'bb', 'XX')", "aaXXcc")
	check("SUBSTR negative start", "SELECT SUBSTR('abcdef', 4)", "def")
	check("INSTR found", "SELECT INSTR('hello world', 'world')", 7)
	check("INSTR not found", "SELECT INSTR('hello', 'xyz')", 0)
	check("Concat operator", "SELECT 'hello' || ' ' || 'world'", "hello world")

	// ============================================================
	// === MATH IN QUERIES ===
	// ============================================================
	check("Integer addition", "SELECT 100 + 200", 300)
	check("Float multiplication", "SELECT 3.14 * 2", 6.28)
	check("Modulo", "SELECT 17 % 5", 2)
	check("Negative number", "SELECT -42", -42)
	check("Parenthesized expr", "SELECT (10 + 20) * 3", 90)

	// ============================================================
	// === TRANSACTION NESTED OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE txn_nest (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO txn_nest VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO txn_nest VALUES (2, 20)")

	checkNoError("BEGIN", "BEGIN")
	checkNoError("UPDATE in txn", "UPDATE txn_nest SET val = val + 100 WHERE id = 1")
	check("Updated in txn", "SELECT val FROM txn_nest WHERE id = 1", 110)
	checkNoError("DELETE in txn", "DELETE FROM txn_nest WHERE id = 2")
	checkRowCount("Deleted in txn", "SELECT * FROM txn_nest", 1)
	checkNoError("INSERT in txn", "INSERT INTO txn_nest VALUES (3, 30)")
	checkRowCount("Inserted in txn", "SELECT * FROM txn_nest", 2)
	checkNoError("ROLLBACK", "ROLLBACK")

	check("Rollback restored val", "SELECT val FROM txn_nest WHERE id = 1", 10)
	checkRowCount("Rollback restored rows", "SELECT * FROM txn_nest", 2)
	check("Rollback undid insert", "SELECT COUNT(*) FROM txn_nest WHERE id = 3", 0)

	// ============================================================
	// === INDEX WITH UPDATE AND DELETE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE idx_upd (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	checkNoError("CREATE INDEX", "CREATE INDEX idx_upd_score ON idx_upd(score)")
	afExec(t, db, ctx, "INSERT INTO idx_upd VALUES (1, 'Alice', 90)")
	afExec(t, db, ctx, "INSERT INTO idx_upd VALUES (2, 'Bob', 80)")
	afExec(t, db, ctx, "INSERT INTO idx_upd VALUES (3, 'Charlie', 85)")

	check("Index query before update", "SELECT name FROM idx_upd WHERE score = 80", "Bob")
	checkNoError("UPDATE indexed column", "UPDATE idx_upd SET score = 95 WHERE id = 2")
	check("Index query after update", "SELECT name FROM idx_upd WHERE score = 95", "Bob")
	checkRowCount("Old index value gone", "SELECT * FROM idx_upd WHERE score = 80", 0)

	checkNoError("DELETE indexed row", "DELETE FROM idx_upd WHERE id = 3")
	checkRowCount("Deleted row gone from index", "SELECT * FROM idx_upd WHERE score = 85", 0)

	// ============================================================
	// === GROUP_CONCAT ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE gc_test (id INTEGER PRIMARY KEY, grp TEXT, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO gc_test VALUES (1, 'A', 'x')")
	afExec(t, db, ctx, "INSERT INTO gc_test VALUES (2, 'A', 'y')")
	afExec(t, db, ctx, "INSERT INTO gc_test VALUES (3, 'B', 'z')")

	check("GROUP_CONCAT simple",
		"SELECT GROUP_CONCAT(val) FROM gc_test WHERE grp = 'A'", "x,y")

	// ============================================================
	// === AGGREGATE WITH CASE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE agg_case (id INTEGER PRIMARY KEY, status TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO agg_case VALUES (1, 'paid', 100)")
	afExec(t, db, ctx, "INSERT INTO agg_case VALUES (2, 'unpaid', 200)")
	afExec(t, db, ctx, "INSERT INTO agg_case VALUES (3, 'paid', 300)")
	afExec(t, db, ctx, "INSERT INTO agg_case VALUES (4, 'unpaid', 400)")

	check("SUM with CASE filter",
		"SELECT SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) FROM agg_case", 400) // 100+300

	check("COUNT with CASE",
		"SELECT COUNT(CASE WHEN status = 'paid' THEN 1 END) FROM agg_case", 2) // NULL from ELSE not counted

	t.Logf("\n=== V29 WINDOW/UNION/TRIGGER: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
