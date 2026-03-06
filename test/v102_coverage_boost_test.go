package test

import (
	"fmt"
	"strings"
	"testing"
)

func TestV102CoverageBoost(t *testing.T) {
	_ = fmt.Sprintf
	_ = strings.Contains

	t.Run("GroupByOrderBy", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v102_sales (id INTEGER PRIMARY KEY, dept TEXT, price REAL, quantity INTEGER, rep TEXT)")
		afExec(t, db, ctx, "INSERT INTO v102_sales VALUES (1, 'east', 10.0, 5, 'Alice')")
		afExec(t, db, ctx, "INSERT INTO v102_sales VALUES (2, 'east', 20.0, 3, 'Bob')")
		afExec(t, db, ctx, "INSERT INTO v102_sales VALUES (3, 'west', 15.0, 2, 'Carol')")
		afExec(t, db, ctx, "INSERT INTO v102_sales VALUES (4, 'west', 25.0, 4, 'Dave')")
		afExec(t, db, ctx, "INSERT INTO v102_sales VALUES (5, 'north', 5.0, 10, NULL)")
		afExec(t, db, ctx, "INSERT INTO v102_sales VALUES (6, 'north', 8.0, NULL, 'Eve')")
		t.Run("OrderBySumExprArg", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, SUM(price * quantity) AS rev FROM v102_sales GROUP BY dept ORDER BY SUM(price * quantity) DESC")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
			if fmt.Sprintf("%v", rows[0][0]) != "west" { t.Fatalf("expected west, got %v", rows[0][0]) }
		})
		t.Run("PositionalOrderBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, COUNT(*) AS cnt FROM v102_sales GROUP BY dept ORDER BY 2 DESC")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		})
		t.Run("StringComparisonOrderBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, MAX(rep) AS top_rep FROM v102_sales GROUP BY dept ORDER BY dept ASC")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
			if fmt.Sprintf("%v", rows[0][0]) != "east" { t.Fatalf("expected east, got %v", rows[0][0]) }
		})
		t.Run("NullInSortedResults", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT dept, AVG(quantity) AS avg_qty FROM v102_sales GROUP BY dept ORDER BY AVG(quantity)")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		})
		t.Run("QualifiedIdentOrderBy", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT v102_sales.dept, SUM(price) AS total FROM v102_sales GROUP BY v102_sales.dept ORDER BY v102_sales.dept")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
			if fmt.Sprintf("%v", rows[0][0]) != "east" { t.Fatalf("expected east, got %v", rows[0][0]) }
		})
	})

	t.Run("JoinGroupByAggregates", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v102_dept (id INTEGER PRIMARY KEY, dname TEXT)")
		afExec(t, db, ctx, "INSERT INTO v102_dept VALUES (1, 'Engineering')")
		afExec(t, db, ctx, "INSERT INTO v102_dept VALUES (2, 'Sales')")
		afExec(t, db, ctx, "INSERT INTO v102_dept VALUES (3, 'HR')")
		afExec(t, db, ctx, "CREATE TABLE v102_emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary REAL, bonus REAL)")
		afExec(t, db, ctx, "INSERT INTO v102_emp VALUES (1, 1, 100, 10)")
		afExec(t, db, ctx, "INSERT INTO v102_emp VALUES (2, 1, 120, 15)")
		afExec(t, db, ctx, "INSERT INTO v102_emp VALUES (3, 2, 90, NULL)")
		afExec(t, db, ctx, "INSERT INTO v102_emp VALUES (4, 2, 80, 5)")
		afExec(t, db, ctx, "INSERT INTO v102_emp VALUES (5, 3, 70, NULL)")
		t.Run("SUM_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT d.dname, SUM(e.salary) FROM v102_dept d JOIN v102_emp e ON d.id = e.dept_id GROUP BY d.dname ORDER BY SUM(e.salary) DESC")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
			if fmt.Sprintf("%.0f", rows[0][1]) != "220" { t.Fatalf("expected 220, got %v", rows[0][1]) }
		})
		t.Run("AVG_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT d.dname, AVG(e.salary) FROM v102_dept d JOIN v102_emp e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		})
		t.Run("MINMAX_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT d.dname, MIN(e.salary), MAX(e.salary) FROM v102_dept d JOIN v102_emp e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		})
		t.Run("COUNT_Column_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT d.dname, COUNT(e.bonus) FROM v102_dept d JOIN v102_emp e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
			for _, r := range rows {
				switch fmt.Sprintf("%v", r[0]) {
				case "Engineering":
					if fmt.Sprintf("%v", r[1]) != "2" { t.Fatalf("expected 2, got %v", r[1]) }
				case "Sales":
					if fmt.Sprintf("%v", r[1]) != "1" { t.Fatalf("expected 1, got %v", r[1]) }
				case "HR":
					if fmt.Sprintf("%v", r[1]) != "0" { t.Fatalf("expected 0, got %v", r[1]) }
				}
			}
		})
		t.Run("NULL_Agg_Join", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT d.dname, SUM(e.bonus) FROM v102_dept d JOIN v102_emp e ON d.id = e.dept_id GROUP BY d.dname ORDER BY d.dname")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		})
	})

	t.Run("Analyze", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v102_analyze_t (id INTEGER PRIMARY KEY, name TEXT, score REAL, tag TEXT)")
		afExec(t, db, ctx, "INSERT INTO v102_analyze_t VALUES (1, 'Alice', 95.5, 'A')")
		afExec(t, db, ctx, "INSERT INTO v102_analyze_t VALUES (2, 'Bob', 85.0, 'B')")
		afExec(t, db, ctx, "INSERT INTO v102_analyze_t VALUES (3, 'Carol', NULL, 'A')")
		afExec(t, db, ctx, "INSERT INTO v102_analyze_t VALUES (4, 'Dave', 70.0, NULL)")
		afExec(t, db, ctx, "INSERT INTO v102_analyze_t VALUES (5, 'Eve', 95.5, 'A')")
		afExec(t, db, ctx, "ANALYZE v102_analyze_t")
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v102_analyze_t", 5)
		afExpectVal(t, db, ctx, "SELECT MIN(score) FROM v102_analyze_t", 70)
		afExec(t, db, ctx, "CREATE INDEX idx_v102_an_score ON v102_analyze_t (score)")
		afExec(t, db, ctx, "ANALYZE v102_analyze_t")
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v102_analyze_t", 5)
	})

	t.Run("DerivedTable", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		t.Run("UnionInDerivedTable", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT * FROM (SELECT 1 AS v UNION SELECT 2 UNION SELECT 3) AS sub ORDER BY v")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
			if fmt.Sprintf("%v", rows[0][0]) != "1" { t.Fatalf("expected 1, got %v", rows[0][0]) }
		})
		t.Run("SubqueryInFrom", func(t *testing.T) {
			afExec(t, db, ctx, "CREATE TABLE v102_dt_src (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
			afExec(t, db, ctx, "INSERT INTO v102_dt_src VALUES (1, 'alpha', 10)")
			afExec(t, db, ctx, "INSERT INTO v102_dt_src VALUES (2, 'beta', 20)")
			afExec(t, db, ctx, "INSERT INTO v102_dt_src VALUES (3, 'gamma', 30)")
			rows := afQuery(t, db, ctx, "SELECT sub.val, sub.score FROM (SELECT val, score FROM v102_dt_src WHERE score > 10) AS sub ORDER BY sub.score")
			if len(rows) != 2 { t.Fatalf("expected 2, got %d", len(rows)) }
			if fmt.Sprintf("%v", rows[0][0]) != "beta" { t.Fatalf("expected beta, got %v", rows[0][0]) }
		})
		t.Run("DerivedTableAggregate", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT sub.total FROM (SELECT SUM(score) AS total FROM v102_dt_src) AS sub")
			if len(rows) != 1 { t.Fatalf("expected 1, got %d", len(rows)) }
			if fmt.Sprintf("%v", rows[0][0]) != "60" { t.Fatalf("expected 60, got %v", rows[0][0]) }
		})
		t.Run("DerivedTableUnionAll", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT * FROM (SELECT 'a' AS letter UNION ALL SELECT 'b' UNION ALL SELECT 'c') AS sub ORDER BY letter")
			if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		})
	})

	t.Run("FunctionCalls", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		t.Run("CONCAT_WS", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c") })
		t.Run("LEFT", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT LEFT('hello', 3)", "hel") })
		t.Run("RIGHT", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT RIGHT('hello', 3)", "llo") })
		t.Run("LPAD", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT LPAD('hi', 5, '0')", "000hi") })
		t.Run("RPAD", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT RPAD('hi', 5, '0')", "hi000") })
		t.Run("HEX_Number", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT HEX(255)", "FF") })
		t.Run("HEX_String", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT HEX('abc')")
			if len(rows) == 0 { t.Fatal("no result") }
			if len(fmt.Sprintf("%v", rows[0][0])) == 0 { t.Fatal("empty") }
		})
		t.Run("CHAR", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT CHAR(65, 66, 67)", "ABC") })
		t.Run("ZEROBLOB", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT ZEROBLOB(4)")
			if len(rows) == 0 { t.Fatal("no result") }
		})
		t.Run("QUOTE_String", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT QUOTE('hello')")
			if len(rows) == 0 { t.Fatal("no result") }
			if !strings.Contains(fmt.Sprintf("%v", rows[0][0]), "hello") { t.Fatal("bad QUOTE") }
		})
		t.Run("QUOTE_NULL", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT QUOTE(NULL)", "NULL") })
		t.Run("QUOTE_Number", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT QUOTE(42)", 42) })
		t.Run("GLOB_Match", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT GLOB('*.txt', 'file.txt')", true) })
		t.Run("GLOB_NoMatch", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT GLOB('*.txt', 'file.csv')", false) })
		t.Run("UNICODE", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT UNICODE('A')", 65) })
		t.Run("RANDOM", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT RANDOM()")
			if len(rows) == 0 { t.Fatal("no result") }
		})
		t.Run("PRINTF", func(t *testing.T) {
			sql := "SELECT PRINTF('%s has %d items', 'Alice', 5)"
			rows := afQuery(t, db, ctx, sql)
			if len(rows) == 0 { t.Fatal("no result") }
			got := fmt.Sprintf("%v", rows[0][0])
			if got != "Alice has 5 items" { t.Fatalf("expected 'Alice has 5 items', got '%v'", got) }
		})
		t.Run("STRFTIME", func(t *testing.T) {
			sql := "SELECT STRFTIME('%Y', '2024-01-15')"
			rows := afQuery(t, db, ctx, sql)
			if len(rows) == 0 { t.Fatal("no result") }
			got := fmt.Sprintf("%v", rows[0][0])
			if !strings.Contains(got, "2024") { t.Fatalf("expected to contain 2024, got %v", got) }
		})
		t.Run("DATE", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT DATE('2024-01-15')")
			if len(rows) == 0 { t.Fatal("no result") }
			if !strings.Contains(fmt.Sprintf("%v", rows[0][0]), "2024") { t.Fatal("bad DATE") }
		})
	})

	t.Run("JSONFunctions", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		t.Run("JSON_PRETTY", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT JSON_PRETTY('{\"a\":1}')")
			if len(rows) == 0 { t.Fatal("no result") }
			if !strings.Contains(fmt.Sprintf("%v", rows[0][0]), "a") { t.Fatal("bad JSON_PRETTY") }
		})
		t.Run("JSON_MINIFY", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT JSON_MINIFY('{ \"a\" : 1 }')")
			if len(rows) == 0 { t.Fatal("no result") }
		})
		t.Run("JSON_QUOTE", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT JSON_QUOTE('hello')")
			if len(rows) == 0 { t.Fatal("no result") }
			if !strings.Contains(fmt.Sprintf("%v", rows[0][0]), "hello") { t.Fatal("bad JSON_QUOTE") }
		})
		t.Run("JSON_TYPE_Array", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT JSON_TYPE('[1,2]')")
			if len(rows) == 0 { t.Fatal("no result") }
			if !strings.Contains(strings.ToLower(fmt.Sprintf("%v", rows[0][0])), "array") { t.Fatal("bad JSON_TYPE") }
		})
		t.Run("JSON_TYPE_Object", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT JSON_TYPE('{\"a\":1}')")
			if len(rows) == 0 { t.Fatal("no result") }
			if !strings.Contains(strings.ToLower(fmt.Sprintf("%v", rows[0][0])), "object") { t.Fatal("bad JSON_TYPE") }
		})
		t.Run("JSON_KEYS", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT JSON_KEYS('{\"a\":1,\"b\":2}')")
			if len(rows) == 0 { t.Fatal("no result") }
			got := fmt.Sprintf("%v", rows[0][0])
			if !strings.Contains(got, "a") || !strings.Contains(got, "b") { t.Fatal("bad JSON_KEYS") }
		})
		t.Run("JSON_MERGE", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT JSON_MERGE('{\"a\":1}', '{\"b\":2}')")
			if len(rows) == 0 { t.Fatal("no result") }
			got := fmt.Sprintf("%v", rows[0][0])
			if !strings.Contains(got, "a") || !strings.Contains(got, "b") { t.Fatal("bad JSON_MERGE") }
		})
	})

	t.Run("CTE", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		t.Run("RecursiveCTE", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x < 5) SELECT x FROM cnt")
			if len(rows) != 5 { t.Fatalf("expected 5, got %d", len(rows)) }
			if fmt.Sprintf("%v", rows[0][0]) != "1" { t.Fatalf("expected 1, got %v", rows[0][0]) }
			if fmt.Sprintf("%v", rows[4][0]) != "5" { t.Fatalf("expected 5, got %v", rows[4][0]) }
		})
		t.Run("CTEWithUnion", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "WITH combined AS (SELECT 1 AS v UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) SELECT * FROM combined ORDER BY v")
			if len(rows) != 4 { t.Fatalf("expected 4, got %d", len(rows)) }
		})
		t.Run("DuplicateCTEName", func(t *testing.T) {
			_, err := db.Exec(ctx, "WITH a AS (SELECT 1), a AS (SELECT 2) SELECT * FROM a")
			if err == nil { t.Fatal("expected error for duplicate CTE name") }
		})
	})

	t.Run("SaveLoad", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v102_sl_users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
		afExec(t, db, ctx, "CREATE TABLE v102_sl_orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")
		afExec(t, db, ctx, "CREATE INDEX idx_v102_sl_name ON v102_sl_users (name)")
		afExec(t, db, ctx, "INSERT INTO v102_sl_users VALUES (1, 'Alice', 'alice@test.com')")
		afExec(t, db, ctx, "INSERT INTO v102_sl_users VALUES (2, 'Bob', 'bob@test.com')")
		afExec(t, db, ctx, "INSERT INTO v102_sl_orders VALUES (1, 1, 100.50)")
		afExec(t, db, ctx, "INSERT INTO v102_sl_orders VALUES (2, 2, 200.75)")
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v102_sl_users", 2)
		afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM v102_sl_orders", 2)
		afExpectVal(t, db, ctx, "SELECT name FROM v102_sl_users WHERE id = 1", "Alice")
		rows := afQuery(t, db, ctx, "SELECT u.name, o.amount FROM v102_sl_users u JOIN v102_sl_orders o ON u.id = o.user_id ORDER BY u.name")
		if len(rows) != 2 { t.Fatalf("expected 2, got %d", len(rows)) }
	})

	t.Run("GroupByOrderByMultiAgg", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v102_magg (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v102_magg VALUES (1, 'X', 10)")
		afExec(t, db, ctx, "INSERT INTO v102_magg VALUES (2, 'X', 20)")
		afExec(t, db, ctx, "INSERT INTO v102_magg VALUES (3, 'Y', 30)")
		afExec(t, db, ctx, "INSERT INTO v102_magg VALUES (4, 'Y', 40)")
		afExec(t, db, ctx, "INSERT INTO v102_magg VALUES (5, 'Z', 50)")
		rows := afQuery(t, db, ctx, "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total FROM v102_magg GROUP BY grp ORDER BY 2 DESC")
		if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		rows = afQuery(t, db, ctx, "SELECT grp, SUM(val) AS total FROM v102_magg GROUP BY grp ORDER BY SUM(val)")
		if len(rows) != 3 { t.Fatalf("expected 3, got %d", len(rows)) }
		if fmt.Sprintf("%v", rows[0][0]) != "X" { t.Fatalf("expected X, got %v", rows[0][0]) }
	})

	t.Run("MoreFunctions", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		t.Run("IIF", func(t *testing.T) {
			afExpectVal(t, db, ctx, "SELECT IIF(1 > 0, 'yes', 'no')", "yes")
			afExpectVal(t, db, ctx, "SELECT IIF(1 < 0, 'yes', 'no')", "no")
		})
		t.Run("TYPEOF", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT TYPEOF(42)")
			if len(rows) == 0 { t.Fatal("no result") }
		})
		t.Run("REVERSE", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT REVERSE('hello')", "olleh") })
		t.Run("REPEAT", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT REPEAT('ab', 3)", "ababab") })
		t.Run("INSTR", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT INSTR('hello world', 'world')", 7) })
		t.Run("REPLACE_Func", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT REPLACE('hello world', 'world', 'there')", "hello there") })
		t.Run("COALESCE", func(t *testing.T) { afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, NULL, 'found')", "found") })
		t.Run("NULLIF", func(t *testing.T) {
			rows := afQuery(t, db, ctx, "SELECT NULLIF(1, 1)")
			if len(rows) == 0 { t.Fatal("no result") }
			if rows[0][0] != nil { t.Fatalf("expected NULL, got %v", rows[0][0]) }
		})
	})

	t.Run("RecursiveCTEHierarchy", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v102_tree (id INTEGER PRIMARY KEY, parent_id INTEGER, label TEXT)")
		afExec(t, db, ctx, "INSERT INTO v102_tree VALUES (1, NULL, 'root')")
		afExec(t, db, ctx, "INSERT INTO v102_tree VALUES (2, 1, 'child1')")
		afExec(t, db, ctx, "INSERT INTO v102_tree VALUES (3, 1, 'child2')")
		afExec(t, db, ctx, "INSERT INTO v102_tree VALUES (4, 2, 'grandchild1')")
		rows := afQuery(t, db, ctx, "WITH RECURSIVE tree_path(id, label, lvl) AS (SELECT id, label, 0 FROM v102_tree WHERE parent_id IS NULL UNION ALL SELECT t.id, t.label, tp.lvl + 1 FROM v102_tree t JOIN tree_path tp ON t.parent_id = tp.id) SELECT id, label, lvl FROM tree_path ORDER BY id")
		if len(rows) != 4 { t.Fatalf("expected 4, got %d", len(rows)) }
		if fmt.Sprintf("%v", rows[0][1]) != "root" { t.Fatalf("expected root, got %v", rows[0][1]) }
		if fmt.Sprintf("%v", rows[3][2]) != "2" { t.Fatalf("expected level 2, got %v", rows[3][2]) }
	})

	t.Run("JoinGroupByOrderBy", func(t *testing.T) {
		db, ctx := af(t)
		defer db.Close()
		afExec(t, db, ctx, "CREATE TABLE v102_jcat (id INTEGER PRIMARY KEY, cname TEXT)")
		afExec(t, db, ctx, "INSERT INTO v102_jcat VALUES (1, 'Electronics')")
		afExec(t, db, ctx, "INSERT INTO v102_jcat VALUES (2, 'Books')")
		afExec(t, db, ctx, "CREATE TABLE v102_jprod (id INTEGER PRIMARY KEY, cat_id INTEGER, pname TEXT, price REAL)")
		afExec(t, db, ctx, "INSERT INTO v102_jprod VALUES (1, 1, 'Laptop', 999.0)")
		afExec(t, db, ctx, "INSERT INTO v102_jprod VALUES (2, 1, 'Phone', 599.0)")
		afExec(t, db, ctx, "INSERT INTO v102_jprod VALUES (3, 2, 'Novel', 15.0)")
		afExec(t, db, ctx, "INSERT INTO v102_jprod VALUES (4, 2, 'Guide', 25.0)")
		rows := afQuery(t, db, ctx, "SELECT c.cname, SUM(p.price) AS total FROM v102_jcat c JOIN v102_jprod p ON c.id = p.cat_id GROUP BY c.cname ORDER BY SUM(p.price) DESC")
		if len(rows) != 2 { t.Fatalf("expected 2, got %d", len(rows)) }
		if fmt.Sprintf("%v", rows[0][0]) != "Electronics" { t.Fatalf("expected Electronics, got %v", rows[0][0]) }
		rows = afQuery(t, db, ctx, "SELECT c.cname, COUNT(*) AS cnt FROM v102_jcat c JOIN v102_jprod p ON c.id = p.cat_id GROUP BY c.cname ORDER BY c.cname DESC")
		if len(rows) != 2 { t.Fatalf("expected 2, got %d", len(rows)) }
		if fmt.Sprintf("%v", rows[0][0]) != "Electronics" { t.Fatalf("expected Electronics, got %v", rows[0][0]) }
	})
}
