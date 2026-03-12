package test

import (
	"fmt"
	"testing"
)

func TestV20AdvancedFeatures(t *testing.T) {
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
	// === TRIGGERS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTO_INCREMENT, action TEXT, record_id INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE trigger_test (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")

	checkNoError("CREATE TRIGGER AFTER INSERT",
		"CREATE TRIGGER trg_insert AFTER INSERT ON trigger_test BEGIN INSERT INTO audit_log (action, record_id) VALUES ('INSERT', NEW.id); END")
	checkNoError("Insert triggers audit",
		"INSERT INTO trigger_test VALUES (1, 'Alice', 100)")
	checkRowCount("Trigger fired on insert", "SELECT * FROM audit_log", 1)
	check("Trigger captured action", "SELECT action FROM audit_log WHERE record_id = 1", "INSERT")

	checkNoError("Second insert",
		"INSERT INTO trigger_test VALUES (2, 'Bob', 200)")
	checkRowCount("Trigger fired again", "SELECT * FROM audit_log", 2)

	// ============================================================
	// === VIEWS WITH COMPLEX QUERIES ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE v_products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER, category TEXT)")
	afExec(t, db, ctx, "INSERT INTO v_products VALUES (1, 'Phone', 800, 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO v_products VALUES (2, 'Laptop', 1200, 'Electronics')")
	afExec(t, db, ctx, "INSERT INTO v_products VALUES (3, 'Book', 20, 'Books')")
	afExec(t, db, ctx, "INSERT INTO v_products VALUES (4, 'Pen', 5, 'Stationery')")

	checkNoError("CREATE VIEW with WHERE",
		"CREATE VIEW expensive_products AS SELECT name, price FROM v_products WHERE price > 100")
	checkRowCount("View filters correctly", "SELECT * FROM expensive_products", 2)
	check("View data", "SELECT name FROM expensive_products ORDER BY price DESC LIMIT 1", "Laptop")

	// View of aggregates
	checkNoError("CREATE VIEW with aggregate",
		"CREATE VIEW category_stats AS SELECT category, COUNT(*) AS cnt, SUM(price) AS total FROM v_products GROUP BY category")
	check("Aggregate view",
		"SELECT category FROM category_stats ORDER BY total DESC LIMIT 1", "Electronics")

	// ============================================================
	// === RECURSIVE CTE ===
	// ============================================================
	// Generate numbers 1 to 5
	check("Recursive CTE count",
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT COUNT(*) FROM cnt",
		5)

	check("Recursive CTE max",
		"WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 10) SELECT MAX(x) FROM cnt",
		10)

	// Hierarchical query
	afExec(t, db, ctx, "CREATE TABLE tree (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (1, NULL, 'root')")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (2, 1, 'child1')")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (3, 1, 'child2')")
	afExec(t, db, ctx, "INSERT INTO tree VALUES (4, 2, 'grandchild1')")

	check("Recursive CTE hierarchy",
		"WITH RECURSIVE descendants(id, name, depth) AS (SELECT id, name, 0 FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.name, d.depth + 1 FROM tree t JOIN descendants d ON t.parent_id = d.id) SELECT COUNT(*) FROM descendants",
		4) // root + child1 + child2 + grandchild1

	check("Recursive CTE max depth",
		"WITH RECURSIVE descendants(id, name, depth) AS (SELECT id, name, 0 FROM tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.name, d.depth + 1 FROM tree t JOIN descendants d ON t.parent_id = d.id) SELECT MAX(depth) FROM descendants",
		2) // grandchild is depth 2

	// ============================================================
	// === WINDOW FUNCTIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE wf_test (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wf_test VALUES (1, 'Eng', 100)")
	afExec(t, db, ctx, "INSERT INTO wf_test VALUES (2, 'Eng', 120)")
	afExec(t, db, ctx, "INSERT INTO wf_test VALUES (3, 'Eng', 110)")
	afExec(t, db, ctx, "INSERT INTO wf_test VALUES (4, 'Sales', 90)")
	afExec(t, db, ctx, "INSERT INTO wf_test VALUES (5, 'Sales', 95)")

	check("ROW_NUMBER basic",
		"SELECT ROW_NUMBER() OVER (ORDER BY salary DESC) FROM wf_test LIMIT 1", 3) // First physical row (salary=100) is 3rd when sorted by salary DESC (120,110,100)

	check("ROW_NUMBER partition",
		"SELECT ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) FROM wf_test WHERE dept = 'Eng' ORDER BY salary DESC LIMIT 1", 1)

	check("RANK basic",
		"SELECT RANK() OVER (ORDER BY salary DESC) FROM wf_test ORDER BY salary DESC LIMIT 1", 1)

	// ============================================================
	// === JSON OPERATIONS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE json_test (id INTEGER PRIMARY KEY, data TEXT)")
	afExec(t, db, ctx, `INSERT INTO json_test VALUES (1, '{"name":"Alice","age":30}')`)
	afExec(t, db, ctx, `INSERT INTO json_test VALUES (2, '{"name":"Bob","age":25}')`)

	check("JSON_EXTRACT",
		`SELECT JSON_EXTRACT(data, '$.name') FROM json_test WHERE id = 1`, "Alice")
	check("JSON_EXTRACT number",
		`SELECT JSON_EXTRACT(data, '$.age') FROM json_test WHERE id = 2`, 25)

	// ============================================================
	// === CTE WITH AGGREGATION ===
	// ============================================================
	check("CTE with real table",
		"WITH high_price AS (SELECT name, price FROM v_products WHERE price > 100) SELECT name FROM high_price ORDER BY price DESC LIMIT 1",
		"Laptop")

	// ============================================================
	// === SUBQUERY IN WHERE WITH AGGREGATE ===
	// ============================================================
	check("Subquery with aggregate in WHERE",
		"SELECT name FROM v_products WHERE price = (SELECT MAX(price) FROM v_products)",
		"Laptop")

	// ============================================================
	// === UPDATE WITH CASE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE case_update (id INTEGER PRIMARY KEY, val INTEGER, tier TEXT)")
	afExec(t, db, ctx, "INSERT INTO case_update VALUES (1, 10, NULL)")
	afExec(t, db, ctx, "INSERT INTO case_update VALUES (2, 50, NULL)")
	afExec(t, db, ctx, "INSERT INTO case_update VALUES (3, 100, NULL)")

	checkNoError("UPDATE with CASE",
		"UPDATE case_update SET tier = CASE WHEN val < 25 THEN 'low' WHEN val < 75 THEN 'mid' ELSE 'high' END")
	check("CASE update low", "SELECT tier FROM case_update WHERE id = 1", "low")
	check("CASE update mid", "SELECT tier FROM case_update WHERE id = 2", "mid")
	check("CASE update high", "SELECT tier FROM case_update WHERE id = 3", "high")

	// ============================================================
	// === MULTIPLE ORDER BY WITH AGGREGATE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE mo_test (id INTEGER PRIMARY KEY, grp TEXT, sub_grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO mo_test VALUES (1, 'A', 'x', 10)")
	afExec(t, db, ctx, "INSERT INTO mo_test VALUES (2, 'A', 'y', 20)")
	afExec(t, db, ctx, "INSERT INTO mo_test VALUES (3, 'B', 'x', 30)")
	afExec(t, db, ctx, "INSERT INTO mo_test VALUES (4, 'B', 'y', 40)")

	check("ORDER BY aggregate then column",
		"SELECT grp, SUM(val) FROM mo_test GROUP BY grp ORDER BY SUM(val) ASC, grp LIMIT 1", "A")

	// ============================================================
	// === EXISTS / NOT EXISTS ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE ex_main (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE ex_detail (id INTEGER PRIMARY KEY, main_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ex_main VALUES (1, 'Has detail')")
	afExec(t, db, ctx, "INSERT INTO ex_main VALUES (2, 'No detail')")
	afExec(t, db, ctx, "INSERT INTO ex_detail VALUES (1, 1)")

	check("EXISTS",
		"SELECT name FROM ex_main WHERE EXISTS (SELECT 1 FROM ex_detail WHERE ex_detail.main_id = ex_main.id)",
		"Has detail")

	check("NOT EXISTS",
		"SELECT name FROM ex_main WHERE NOT EXISTS (SELECT 1 FROM ex_detail WHERE ex_detail.main_id = ex_main.id)",
		"No detail")

	// ============================================================
	// === CROSS JOIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE cj_a (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE cj_b (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO cj_a VALUES (1, 'x')")
	afExec(t, db, ctx, "INSERT INTO cj_a VALUES (2, 'y')")
	afExec(t, db, ctx, "INSERT INTO cj_b VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO cj_b VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO cj_b VALUES (3, 'c')")

	checkRowCount("CROSS JOIN produces cartesian product",
		"SELECT * FROM cj_a CROSS JOIN cj_b", 6) // 2 * 3 = 6

	// ============================================================
	// === RIGHT JOIN ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE rj_left (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE rj_right (id INTEGER PRIMARY KEY, left_id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO rj_left VALUES (1, 'matched')")
	afExec(t, db, ctx, "INSERT INTO rj_right VALUES (1, 1, 'exists')")
	afExec(t, db, ctx, "INSERT INTO rj_right VALUES (2, 99, 'orphan')")

	checkRowCount("RIGHT JOIN includes unmatched right",
		"SELECT * FROM rj_left RIGHT JOIN rj_right ON rj_left.id = rj_right.left_id", 2)

	// ============================================================
	// === INSERT INTO...SELECT with WHERE ===
	// ============================================================
	afExec(t, db, ctx, "CREATE TABLE is_src (id INTEGER PRIMARY KEY, val INTEGER, active INTEGER)")
	afExec(t, db, ctx, "INSERT INTO is_src VALUES (1, 100, 1)")
	afExec(t, db, ctx, "INSERT INTO is_src VALUES (2, 200, 0)")
	afExec(t, db, ctx, "INSERT INTO is_src VALUES (3, 300, 1)")

	afExec(t, db, ctx, "CREATE TABLE is_dst (id INTEGER PRIMARY KEY, val INTEGER)")
	checkNoError("INSERT...SELECT with WHERE",
		"INSERT INTO is_dst SELECT id, val FROM is_src WHERE active = 1")
	checkRowCount("Only active rows copied", "SELECT * FROM is_dst", 2)

	// ============================================================
	// === VIEW WITH ORDER BY ===
	// ============================================================
	checkNoError("CREATE VIEW with aggregate",
		"CREATE VIEW IF NOT EXISTS category_totals AS SELECT category, SUM(price) AS total FROM v_products GROUP BY category")
	check("View with aggregate query",
		"SELECT category FROM category_totals ORDER BY total DESC LIMIT 1", "Electronics")

	// ============================================================
	// === SHOW TABLES ===
	// ============================================================
	// SHOW TABLES must use Query() not Exec()
	total++
	if _, err := db.Query(ctx, "SHOW TABLES"); err != nil {
		t.Errorf("[FAIL] SHOW TABLES: %v", err)
	} else {
		pass++
	}

	// ============================================================
	// === VACUUM / ANALYZE ===
	// ============================================================
	checkNoError("VACUUM", "VACUUM")
	checkNoError("ANALYZE", "ANALYZE")

	t.Logf("\n=== V20 ADVANCED FEATURES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
