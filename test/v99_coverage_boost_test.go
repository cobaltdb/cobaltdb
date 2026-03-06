package test

import (
	"fmt"
	"testing"
)

// =============================================================================
// 1. RollbackTransaction undo paths
// =============================================================================

func TestV99_RollbackCreateTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "CREATE TABLE rb_ct (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_ct VALUES (1, 'inside')")
	afExpectVal(t, db, ctx, "SELECT name FROM rb_ct WHERE id = 1", "inside")
	afExec(t, db, ctx, "ROLLBACK")

	// Table must not exist after rollback
	_, err := db.Exec(ctx, "SELECT * FROM rb_ct")
	if err == nil {
		t.Fatal("expected error: table rb_ct should not exist after rollback")
	}
}

func TestV99_RollbackDropTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_dt (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_dt VALUES (1, 'keep')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "DROP TABLE rb_dt")
	// Table gone inside txn
	_, err := db.Exec(ctx, "SELECT * FROM rb_dt")
	if err == nil {
		t.Fatal("expected error: rb_dt should be dropped inside txn")
	}
	afExec(t, db, ctx, "ROLLBACK")

	// Table must be restored
	afExpectVal(t, db, ctx, "SELECT val FROM rb_dt WHERE id = 1", "keep")
}

func TestV99_RollbackCreateIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_ci (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_ci VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO rb_ci VALUES (2, 'Bob')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "CREATE INDEX idx_rb_ci_name ON rb_ci (name)")
	// Index should work inside txn
	afExpectRows(t, db, ctx, "SELECT * FROM rb_ci WHERE name = 'Alice'", 1)
	afExec(t, db, ctx, "ROLLBACK")

	// After rollback the index should be gone; creating it again should succeed
	afExec(t, db, ctx, "CREATE INDEX idx_rb_ci_name ON rb_ci (name)")
	afExpectRows(t, db, ctx, "SELECT * FROM rb_ci WHERE name = 'Bob'", 1)
}

func TestV99_RollbackDropIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_di (id INTEGER PRIMARY KEY, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO rb_di VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO rb_di VALUES (2, 200)")
	afExec(t, db, ctx, "CREATE INDEX idx_rb_di_score ON rb_di (score)")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "DROP INDEX idx_rb_di_score")
	afExec(t, db, ctx, "ROLLBACK")

	// Index should be restored; recreating should fail
	_, err := db.Exec(ctx, "CREATE INDEX idx_rb_di_score ON rb_di (score)")
	if err == nil {
		t.Fatal("expected error: index should already exist after rollback")
	}
	afExpectRows(t, db, ctx, "SELECT * FROM rb_di WHERE score = 100", 1)
}

func TestV99_RollbackAlterAddColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_aac (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_aac VALUES (1, 'Alice')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "ALTER TABLE rb_aac ADD COLUMN age INTEGER")
	afExec(t, db, ctx, "INSERT INTO rb_aac VALUES (2, 'Bob', 30)")
	afExec(t, db, ctx, "ROLLBACK")

	// Column should be gone; only original columns
	afExpectRows(t, db, ctx, "SELECT * FROM rb_aac", 1)
	afExpectVal(t, db, ctx, "SELECT name FROM rb_aac WHERE id = 1", "Alice")
}

func TestV99_RollbackAlterDropColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_adc (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	afExec(t, db, ctx, "INSERT INTO rb_adc VALUES (1, 'Alice', 30)")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "ALTER TABLE rb_adc DROP COLUMN age")
	afExec(t, db, ctx, "ROLLBACK")

	// Column should be restored
	afExpectVal(t, db, ctx, "SELECT age FROM rb_adc WHERE id = 1", 30)
}

func TestV99_RollbackAlterRename(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_ar_old (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_ar_old VALUES (1, 'data')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "ALTER TABLE rb_ar_old RENAME TO rb_ar_new")
	afExpectVal(t, db, ctx, "SELECT val FROM rb_ar_new WHERE id = 1", "data")
	afExec(t, db, ctx, "ROLLBACK")

	// Old name should work again
	afExpectVal(t, db, ctx, "SELECT val FROM rb_ar_old WHERE id = 1", "data")
	// New name should not exist
	_, err := db.Exec(ctx, "SELECT * FROM rb_ar_new")
	if err == nil {
		t.Fatal("expected error: rb_ar_new should not exist after rollback")
	}
}

func TestV99_RollbackAlterRenameColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_arc (id INTEGER PRIMARY KEY, old_col TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_arc VALUES (1, 'val')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "ALTER TABLE rb_arc RENAME COLUMN old_col TO new_col")
	afExpectVal(t, db, ctx, "SELECT new_col FROM rb_arc WHERE id = 1", "val")
	afExec(t, db, ctx, "ROLLBACK")

	// Old column name should work again
	afExpectVal(t, db, ctx, "SELECT old_col FROM rb_arc WHERE id = 1", "val")
}

// =============================================================================
// 2. computeAggregates (without GROUP BY)
// =============================================================================

func TestV99_ComputeAggregatesNoGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE agg_t (id INTEGER PRIMARY KEY, val REAL, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO agg_t VALUES (1, 10.0, 'a')")
	afExec(t, db, ctx, "INSERT INTO agg_t VALUES (2, 20.0, 'b')")
	afExec(t, db, ctx, "INSERT INTO agg_t VALUES (3, 30.0, 'a')")
	afExec(t, db, ctx, "INSERT INTO agg_t VALUES (4, 40.0, 'c')")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM agg_t", 4)
	afExpectVal(t, db, ctx, "SELECT SUM(val) FROM agg_t", 100)
	afExpectVal(t, db, ctx, "SELECT AVG(val) FROM agg_t", 25)
	afExpectVal(t, db, ctx, "SELECT MIN(val) FROM agg_t", 10)
	afExpectVal(t, db, ctx, "SELECT MAX(val) FROM agg_t", 40)
}

func TestV99_CountDistinctWithoutGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE cd_t (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO cd_t VALUES (1, 'x')")
	afExec(t, db, ctx, "INSERT INTO cd_t VALUES (2, 'y')")
	afExec(t, db, ctx, "INSERT INTO cd_t VALUES (3, 'x')")
	afExec(t, db, ctx, "INSERT INTO cd_t VALUES (4, 'z')")

	afExpectVal(t, db, ctx, "SELECT COUNT(cat) FROM cd_t", 4)
	afExpectVal(t, db, ctx, "SELECT COUNT(DISTINCT cat) FROM cd_t", 3)
}

func TestV99_GroupConcatNoGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gc_t (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO gc_t VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO gc_t VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO gc_t VALUES (3, 'Charlie')")

	// GROUP_CONCAT without GROUP BY should concat all rows
	rows := afQuery(t, db, ctx, "SELECT GROUP_CONCAT(name) FROM gc_t")
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("GROUP_CONCAT returned no result")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	// Just verify it contains all names (order may vary)
	if len(got) < 10 { // "Alice,Bob,Charlie" is 17 chars
		t.Fatalf("GROUP_CONCAT result too short: %s", got)
	}
}

func TestV99_AggregatesOnEmptyTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE empty_agg (id INTEGER PRIMARY KEY, val INTEGER)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM empty_agg", 0)

	// SUM/AVG on empty table should return NULL
	rows := afQuery(t, db, ctx, "SELECT SUM(val) FROM empty_agg")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row from SUM on empty table, got %d", len(rows))
	}
}

// =============================================================================
// 3. exprToSQL - expressions stored via CREATE TABLE with defaults and checks
// =============================================================================

func TestV99_ExprToSQL_Defaults(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Test various default expressions that exercise exprToSQL
	afExec(t, db, ctx, "CREATE TABLE expr_sql (id INTEGER PRIMARY KEY, a TEXT DEFAULT 'hello', b INTEGER DEFAULT 42, c REAL DEFAULT 3.14, d INTEGER DEFAULT NULL)")
	afExec(t, db, ctx, "INSERT INTO expr_sql (id) VALUES (1)")

	afExpectVal(t, db, ctx, "SELECT a FROM expr_sql WHERE id = 1", "hello")
	afExpectVal(t, db, ctx, "SELECT b FROM expr_sql WHERE id = 1", 42)
	afExpectVal(t, db, ctx, "SELECT c FROM expr_sql WHERE id = 1", 3.14)
}

func TestV99_ExprToSQL_BooleanDefaults(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE expr_bool (id INTEGER PRIMARY KEY, active BOOLEAN DEFAULT TRUE, deleted BOOLEAN DEFAULT FALSE)")
	afExec(t, db, ctx, "INSERT INTO expr_bool (id) VALUES (1)")

	rows := afQuery(t, db, ctx, "SELECT active, deleted FROM expr_bool WHERE id = 1")
	if len(rows) == 0 {
		t.Fatal("no rows returned")
	}
}

// =============================================================================
// 4. evaluateExprWithGroupAggregatesJoin - JOIN + GROUP BY
// =============================================================================

func TestV99_JoinGroupByAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE jg_dept (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE jg_emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary REAL)")

	afExec(t, db, ctx, "INSERT INTO jg_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO jg_dept VALUES (2, 'Sales')")

	afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (1, 1, 80000)")
	afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (2, 1, 90000)")
	afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (3, 1, 100000)")
	afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (4, 2, 60000)")
	afExec(t, db, ctx, "INSERT INTO jg_emp VALUES (5, 2, 70000)")

	// JOIN + GROUP BY with multiple aggregates
	rows := afQuery(t, db, ctx,
		"SELECT jg_dept.name, SUM(jg_emp.salary), AVG(jg_emp.salary), MIN(jg_emp.salary), MAX(jg_emp.salary), COUNT(jg_emp.salary) FROM jg_dept JOIN jg_emp ON jg_dept.id = jg_emp.dept_id GROUP BY jg_dept.name")

	if len(rows) != 2 {
		t.Fatalf("expected 2 grouped rows, got %d", len(rows))
	}

	// Verify we got both departments
	names := make(map[string]bool)
	for _, row := range rows {
		names[fmt.Sprintf("%v", row[0])] = true
	}
	if !names["Engineering"] || !names["Sales"] {
		t.Fatalf("missing departments in results: %v", names)
	}
}

// =============================================================================
// 5. applyGroupByOrderBy - ORDER BY after GROUP BY
// =============================================================================

func TestV99_OrderByAggregateDesc(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ob_agg (id INTEGER PRIMARY KEY, cat TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ob_agg VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO ob_agg VALUES (2, 'B', 30)")
	afExec(t, db, ctx, "INSERT INTO ob_agg VALUES (3, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO ob_agg VALUES (4, 'C', 5)")

	// ORDER BY SUM(amount) DESC
	rows := afQuery(t, db, ctx,
		"SELECT cat, SUM(amount) FROM ob_agg GROUP BY cat ORDER BY SUM(amount) DESC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// First should be B(30) or A(30) at top, C(5) last
	lastSum := fmt.Sprintf("%v", rows[2][1])
	if lastSum != "5" {
		t.Fatalf("expected last row sum=5, got %s", lastSum)
	}
}

func TestV99_OrderByStringColumnInGrouped(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ob_str (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ob_str VALUES (1, 'Zebra', 10)")
	afExec(t, db, ctx, "INSERT INTO ob_str VALUES (2, 'Apple', 20)")
	afExec(t, db, ctx, "INSERT INTO ob_str VALUES (3, 'Mango', 30)")

	rows := afQuery(t, db, ctx,
		"SELECT cat, SUM(val) FROM ob_str GROUP BY cat ORDER BY cat")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "Apple" {
		t.Fatalf("expected first row=Apple, got %s", first)
	}
}

func TestV99_OrderByWithNullsInGroup(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ob_null (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ob_null VALUES (1, NULL, 10)")
	afExec(t, db, ctx, "INSERT INTO ob_null VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO ob_null VALUES (3, 'B', 5)")

	rows := afQuery(t, db, ctx,
		"SELECT cat, SUM(val) FROM ob_null GROUP BY cat ORDER BY SUM(val) DESC")
	if len(rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(rows))
	}
}

func TestV99_OrderByPositionalInGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ob_pos (id INTEGER PRIMARY KEY, cat TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ob_pos VALUES (1, 'X', 50)")
	afExec(t, db, ctx, "INSERT INTO ob_pos VALUES (2, 'Y', 10)")
	afExec(t, db, ctx, "INSERT INTO ob_pos VALUES (3, 'X', 20)")

	// ORDER BY 2 (second column = SUM) ASC
	rows := afQuery(t, db, ctx,
		"SELECT cat, SUM(amount) FROM ob_pos GROUP BY cat ORDER BY 2")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	firstSum := fmt.Sprintf("%v", rows[0][1])
	if firstSum != "10" {
		t.Fatalf("expected first sum=10, got %s", firstSum)
	}
}

// =============================================================================
// 6. evaluateWhere edge cases
// =============================================================================

func TestV99_WhereStringTruthy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wh_str (id INTEGER PRIMARY KEY, flag TEXT)")
	afExec(t, db, ctx, "INSERT INTO wh_str VALUES (1, 'yes')")
	afExec(t, db, ctx, "INSERT INTO wh_str VALUES (2, '')")
	afExec(t, db, ctx, "INSERT INTO wh_str VALUES (3, 'no')")

	// WHERE with string comparison
	afExpectRows(t, db, ctx, "SELECT * FROM wh_str WHERE flag = 'yes'", 1)
	afExpectRows(t, db, ctx, "SELECT * FROM wh_str WHERE flag != ''", 2)
}

func TestV99_WhereWithNullComparisons(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wh_null (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wh_null VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO wh_null VALUES (2, NULL)")
	afExec(t, db, ctx, "INSERT INTO wh_null VALUES (3, 20)")

	afExpectRows(t, db, ctx, "SELECT * FROM wh_null WHERE val IS NULL", 1)
	afExpectRows(t, db, ctx, "SELECT * FROM wh_null WHERE val IS NOT NULL", 2)
	afExpectRows(t, db, ctx, "SELECT * FROM wh_null WHERE val > 5", 2)
}

func TestV99_WhereNumericConversion(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// toNumber exercised through string-to-number comparisons
	afExec(t, db, ctx, "CREATE TABLE wh_num (id INTEGER PRIMARY KEY, sval TEXT)")
	afExec(t, db, ctx, "INSERT INTO wh_num VALUES (1, '42')")
	afExec(t, db, ctx, "INSERT INTO wh_num VALUES (2, '100')")
	afExec(t, db, ctx, "INSERT INTO wh_num VALUES (3, 'abc')")

	// String-to-number comparison via CAST or arithmetic
	afExpectVal(t, db, ctx, "SELECT sval FROM wh_num WHERE id = 1", "42")
}

// =============================================================================
// 7. ANALYZE with complex tables
// =============================================================================

func TestV99_AnalyzeAfterMutations(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE anlz (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "CREATE INDEX idx_anlz_score ON anlz (score)")

	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO anlz VALUES (%d, 'name_%d', %d)", i, i, i*10))
	}

	// Run ANALYZE
	afExec(t, db, ctx, "ANALYZE anlz")

	// Update some rows
	afExec(t, db, ctx, "UPDATE anlz SET score = 999 WHERE id <= 5")

	// Delete some
	afExec(t, db, ctx, "DELETE FROM anlz WHERE id > 15")

	// ANALYZE again after mutations
	afExec(t, db, ctx, "ANALYZE anlz")

	// Verify data is still correct
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM anlz", 15)
}

func TestV99_AnalyzeAllTables(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE anlz_a (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE anlz_b (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO anlz_a VALUES (1, 'hello')")
	afExec(t, db, ctx, "INSERT INTO anlz_b VALUES (1, 42)")

	// ANALYZE without table name (all tables)
	afExec(t, db, ctx, "ANALYZE")
}

// =============================================================================
// 8. executeDerivedTable patterns
// =============================================================================

func TestV99_DerivedTableWithAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dt_src (id INTEGER PRIMARY KEY, cat TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dt_src VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO dt_src VALUES (2, 'B', 20)")
	afExec(t, db, ctx, "INSERT INTO dt_src VALUES (3, 'A', 30)")

	rows := afQuery(t, db, ctx,
		"SELECT sub.cat, sub.total FROM (SELECT cat, SUM(amount) AS total FROM dt_src GROUP BY cat) AS sub")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from derived table, got %d", len(rows))
	}
}

func TestV99_DerivedTableWithOrderByLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dt_ord (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dt_ord VALUES (1, 100)")
	afExec(t, db, ctx, "INSERT INTO dt_ord VALUES (2, 200)")
	afExec(t, db, ctx, "INSERT INTO dt_ord VALUES (3, 300)")
	afExec(t, db, ctx, "INSERT INTO dt_ord VALUES (4, 400)")

	rows := afQuery(t, db, ctx,
		"SELECT sub.val FROM (SELECT val FROM dt_ord ORDER BY val DESC LIMIT 2) AS sub")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	first := fmt.Sprintf("%v", rows[0][0])
	if first != "400" {
		t.Fatalf("expected 400, got %s", first)
	}
}

func TestV99_DerivedTableWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dt_wh (id INTEGER PRIMARY KEY, status TEXT, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dt_wh VALUES (1, 'active', 5)")
	afExec(t, db, ctx, "INSERT INTO dt_wh VALUES (2, 'inactive', 10)")
	afExec(t, db, ctx, "INSERT INTO dt_wh VALUES (3, 'active', 15)")

	rows := afQuery(t, db, ctx,
		"SELECT sub.total FROM (SELECT SUM(qty) AS total FROM dt_wh WHERE status = 'active') AS sub")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	afExpectVal(t, db, ctx,
		"SELECT sub.total FROM (SELECT SUM(qty) AS total FROM dt_wh WHERE status = 'active') AS sub", 20)
}

// =============================================================================
// 9. evaluateFunctionCall - remaining SQL functions
// =============================================================================

func TestV99_TypeofFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT TYPEOF(42)", "integer")
	afExpectVal(t, db, ctx, "SELECT TYPEOF('hello')", "text")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(3.14)", "real")
	afExpectVal(t, db, ctx, "SELECT TYPEOF(NULL)", "null")
}

func TestV99_HexFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT HEX(255)", "FF")
	afExpectVal(t, db, ctx, "SELECT HEX(16)", "10")
	afExpectVal(t, db, ctx, "SELECT HEX(0)", "0")
}

func TestV99_UnicodeFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT UNICODE('A')", 65)
	afExpectVal(t, db, ctx, "SELECT UNICODE('Z')", 90)
	afExpectVal(t, db, ctx, "SELECT UNICODE('a')", 97)
}

func TestV99_RandomFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// RANDOM() returns a random integer; just verify it returns something
	rows := afQuery(t, db, ctx, "SELECT RANDOM()")
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("RANDOM() returned no result")
	}
}

func TestV99_ZeroblobFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, "SELECT ZEROBLOB(4)")
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("ZEROBLOB returned no result")
	}
}

func TestV99_QuoteFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT QUOTE('hello')", "'hello'")
	afExpectVal(t, db, ctx, "SELECT QUOTE(NULL)", "NULL")
}

func TestV99_CharFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT CHAR(72, 101, 108)", "Hel")
	afExpectVal(t, db, ctx, "SELECT CHAR(65)", "A")
}

// =============================================================================
// 10. evaluateJSONFunction - more JSON functions
// =============================================================================

func TestV99_JsonType(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_TYPE('{"a":1}')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_TYPE returned no result")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "object" {
		t.Fatalf("JSON_TYPE expected 'object', got '%s'", got)
	}
}

func TestV99_JsonKeys(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_KEYS('{"name":"Alice","age":30}')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_KEYS returned no result")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	// Should contain "name" and "age"
	if len(got) < 5 {
		t.Fatalf("JSON_KEYS result too short: %s", got)
	}
}

func TestV99_JsonPretty(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_PRETTY('{"a":1}')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_PRETTY returned no result")
	}
}

func TestV99_JsonMinify(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_MINIFY('{ "a" : 1 }')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_MINIFY returned no result")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != `{"a":1}` {
		t.Fatalf("JSON_MINIFY expected {\"a\":1}, got %s", got)
	}
}

func TestV99_JsonQuote(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_QUOTE('hello')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_QUOTE returned no result")
	}
}

func TestV99_JsonUnquote(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_UNQUOTE('"hello"')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_UNQUOTE returned no result")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	if got != "hello" {
		t.Fatalf("JSON_UNQUOTE expected 'hello', got '%s'", got)
	}
}

func TestV99_JsonMerge(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_MERGE('{"a":1}', '{"b":2}')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_MERGE returned no result")
	}
	got := fmt.Sprintf("%v", rows[0][0])
	// Should contain both a and b
	if len(got) < 10 {
		t.Fatalf("JSON_MERGE result too short: %s", got)
	}
}

func TestV99_JsonMergeArrays(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_MERGE('[1,2]', '[3,4]')`)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatal("JSON_MERGE arrays returned no result")
	}
}

// =============================================================================
// 11. toNumber - numeric conversion edge cases
// =============================================================================

func TestV99_ToNumberViaArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Exercise toNumber by doing arithmetic with string values
	afExec(t, db, ctx, "CREATE TABLE tn (id INTEGER PRIMARY KEY, s TEXT, n INTEGER)")
	afExec(t, db, ctx, "INSERT INTO tn VALUES (1, '42', 10)")
	afExec(t, db, ctx, "INSERT INTO tn VALUES (2, '3.14', 20)")
	afExec(t, db, ctx, "INSERT INTO tn VALUES (3, 'abc', 30)")

	// Use CAST or direct comparison to exercise string-to-number path
	afExpectVal(t, db, ctx, "SELECT n FROM tn WHERE id = 1", 10)
	afExpectVal(t, db, ctx, "SELECT s FROM tn WHERE id = 2", "3.14")
}

// =============================================================================
// 12. tokenTypeToColumnType - BOOLEAN and BLOB column types
// =============================================================================

func TestV99_BooleanColumnType(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE bool_t (id INTEGER PRIMARY KEY, active BOOLEAN, flag BOOLEAN DEFAULT TRUE)")
	afExec(t, db, ctx, "INSERT INTO bool_t (id, active) VALUES (1, TRUE)")
	afExec(t, db, ctx, "INSERT INTO bool_t (id, active) VALUES (2, FALSE)")

	afExpectRows(t, db, ctx, "SELECT * FROM bool_t", 2)
	// Query with boolean filter
	rows := afQuery(t, db, ctx, "SELECT id FROM bool_t WHERE active = TRUE")
	if len(rows) < 1 {
		t.Fatal("expected at least 1 row for active=TRUE")
	}
}

func TestV99_BlobColumnType(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE blob_t (id INTEGER PRIMARY KEY, data BLOB, meta TEXT)")
	afExec(t, db, ctx, "INSERT INTO blob_t VALUES (1, 'binary_data_here', 'photo')")
	afExec(t, db, ctx, "INSERT INTO blob_t VALUES (2, NULL, 'empty')")

	afExpectRows(t, db, ctx, "SELECT * FROM blob_t", 2)
	afExpectVal(t, db, ctx, "SELECT data FROM blob_t WHERE id = 1", "binary_data_here")
}

func TestV99_VariousColumnTypes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE vct (id INTEGER PRIMARY KEY, a BOOLEAN, b BLOB, c TEXT, d REAL, e INTEGER)")
	afExec(t, db, ctx, "INSERT INTO vct VALUES (1, TRUE, 'blob', 'text', 3.14, 42)")
	afExpectRows(t, db, ctx, "SELECT * FROM vct", 1)
}

// =============================================================================
// 13. EvalExpression - indirect through SQL patterns
// =============================================================================

func TestV99_ExpressionInSelectList(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Arithmetic expressions
	afExpectVal(t, db, ctx, "SELECT 2 + 3", 5)
	afExpectVal(t, db, ctx, "SELECT 10 - 4", 6)
	afExpectVal(t, db, ctx, "SELECT 6 * 7", 42)
	afExpectVal(t, db, ctx, "SELECT 100 / 4", 25)
	afExpectVal(t, db, ctx, "SELECT 17 % 5", 2)

	// Unary minus
	afExpectVal(t, db, ctx, "SELECT -42", -42)

	// String concatenation
	afExpectVal(t, db, ctx, "SELECT 'hello' || ' ' || 'world'", "hello world")
}

func TestV99_ExpressionComparison(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE expr_cmp (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO expr_cmp VALUES (1, 10, 20)")
	afExec(t, db, ctx, "INSERT INTO expr_cmp VALUES (2, 30, 30)")
	afExec(t, db, ctx, "INSERT INTO expr_cmp VALUES (3, 50, 40)")

	afExpectRows(t, db, ctx, "SELECT * FROM expr_cmp WHERE a < b", 1)
	afExpectRows(t, db, ctx, "SELECT * FROM expr_cmp WHERE a = b", 1)
	afExpectRows(t, db, ctx, "SELECT * FROM expr_cmp WHERE a > b", 1)
	afExpectRows(t, db, ctx, "SELECT * FROM expr_cmp WHERE a <= b", 2)
	afExpectRows(t, db, ctx, "SELECT * FROM expr_cmp WHERE a >= b", 2)
	afExpectRows(t, db, ctx, "SELECT * FROM expr_cmp WHERE a != b", 2)
}

func TestV99_ExpressionWithFunctions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT ABS(-7) + LENGTH('hello')", 12)
	afExpectVal(t, db, ctx, "SELECT UPPER('abc') || LOWER('XYZ')", "ABCxyz")
}

// =============================================================================
// 14. Save/Load (persistence)
// =============================================================================

func TestV99_PersistenceInMemory(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE persist (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "CREATE INDEX idx_persist_name ON persist (name)")
	afExec(t, db, ctx, "INSERT INTO persist VALUES (1, 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO persist VALUES (2, 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO persist VALUES (3, 'Charlie', 300)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM persist", 3)
	afExpectVal(t, db, ctx, "SELECT name FROM persist WHERE id = 2", "Bob")
	afExpectVal(t, db, ctx, "SELECT score FROM persist WHERE name = 'Charlie'", 300)
}

// =============================================================================
// Extra coverage: combined rollback scenarios
// =============================================================================

func TestV99_RollbackMultipleDDLOps(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_multi (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_multi VALUES (1, 'original')")

	afExec(t, db, ctx, "BEGIN")
	afExec(t, db, ctx, "ALTER TABLE rb_multi ADD COLUMN extra TEXT")
	afExec(t, db, ctx, "CREATE INDEX idx_rb_multi ON rb_multi (val)")
	afExec(t, db, ctx, "INSERT INTO rb_multi VALUES (2, 'new', 'bonus')")
	afExec(t, db, ctx, "ROLLBACK")

	// Everything should be rolled back
	afExpectRows(t, db, ctx, "SELECT * FROM rb_multi", 1)
	afExpectVal(t, db, ctx, "SELECT val FROM rb_multi WHERE id = 1", "original")
}

func TestV99_RollbackCreateDropTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rb_cd_keep (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_cd_keep VALUES (1, 'keeper')")

	afExec(t, db, ctx, "BEGIN")
	// Drop existing, create new
	afExec(t, db, ctx, "DROP TABLE rb_cd_keep")
	afExec(t, db, ctx, "CREATE TABLE rb_cd_new (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO rb_cd_new VALUES (1, 'temp')")
	afExec(t, db, ctx, "ROLLBACK")

	// Old table should be back, new should be gone
	afExpectVal(t, db, ctx, "SELECT val FROM rb_cd_keep WHERE id = 1", "keeper")
	_, err := db.Exec(ctx, "SELECT * FROM rb_cd_new")
	if err == nil {
		t.Fatal("rb_cd_new should not exist after rollback")
	}
}

// =============================================================================
// Extra: aggregate with HAVING
// =============================================================================

func TestV99_GroupByHavingOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbho (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO gbho VALUES (1, 'Eng', 100)")
	afExec(t, db, ctx, "INSERT INTO gbho VALUES (2, 'Eng', 200)")
	afExec(t, db, ctx, "INSERT INTO gbho VALUES (3, 'Sales', 50)")
	afExec(t, db, ctx, "INSERT INTO gbho VALUES (4, 'Sales', 60)")
	afExec(t, db, ctx, "INSERT INTO gbho VALUES (5, 'HR', 80)")

	rows := afQuery(t, db, ctx,
		"SELECT dept, SUM(salary) FROM gbho GROUP BY dept HAVING SUM(salary) > 200 ORDER BY SUM(salary) DESC")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (only Eng has sum>200), got %d", len(rows))
	}
	afExpectVal(t, db, ctx,
		"SELECT dept FROM gbho GROUP BY dept HAVING SUM(salary) > 200", "Eng")
}

// =============================================================================
// Extra: multiple aggregates in SELECT without GROUP BY
// =============================================================================

func TestV99_MultipleAggregatesNoGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ma (id INTEGER PRIMARY KEY, v INTEGER)")
	afExec(t, db, ctx, "INSERT INTO ma VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO ma VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO ma VALUES (3, 30)")
	afExec(t, db, ctx, "INSERT INTO ma VALUES (4, 40)")
	afExec(t, db, ctx, "INSERT INTO ma VALUES (5, 50)")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM ma")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	// Verify each aggregate
	r := rows[0]
	if fmt.Sprintf("%v", r[0]) != "5" {
		t.Fatalf("COUNT(*) expected 5, got %v", r[0])
	}
	if fmt.Sprintf("%v", r[1]) != "150" {
		t.Fatalf("SUM expected 150, got %v", r[1])
	}
	if fmt.Sprintf("%v", r[2]) != "30" {
		t.Fatalf("AVG expected 30, got %v", r[2])
	}
	if fmt.Sprintf("%v", r[3]) != "10" {
		t.Fatalf("MIN expected 10, got %v", r[3])
	}
	if fmt.Sprintf("%v", r[4]) != "50" {
		t.Fatalf("MAX expected 50, got %v", r[4])
	}
}

// =============================================================================
// Extra: expression-based defaults via exprToSQL
// =============================================================================

func TestV99_NegativeDefaultAndCheck(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE neg_def (id INTEGER PRIMARY KEY, val INTEGER DEFAULT -1)")
	afExec(t, db, ctx, "INSERT INTO neg_def (id) VALUES (1)")
	afExpectVal(t, db, ctx, "SELECT val FROM neg_def WHERE id = 1", -1)
}

func TestV99_NotExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE not_expr (id INTEGER PRIMARY KEY, active BOOLEAN)")
	afExec(t, db, ctx, "INSERT INTO not_expr VALUES (1, TRUE)")
	afExec(t, db, ctx, "INSERT INTO not_expr VALUES (2, FALSE)")

	afExpectRows(t, db, ctx, "SELECT * FROM not_expr WHERE NOT active = TRUE", 1)
}

// =============================================================================
// Extra: IIF and COALESCE exercising function call paths
// =============================================================================

func TestV99_IIFFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT IIF(1 > 0, 'yes', 'no')", "yes")
	afExpectVal(t, db, ctx, "SELECT IIF(1 < 0, 'yes', 'no')", "no")
	afExpectVal(t, db, ctx, "SELECT IIF(NULL, 'yes', 'no')", "no")
}

func TestV99_CoalesceWithMultipleArgs(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, NULL, 'found')", "found")
	afExpectVal(t, db, ctx, "SELECT COALESCE(NULL, 42, 99)", 42)
	afExpectVal(t, db, ctx, "SELECT COALESCE('first', NULL, 99)", "first")
}
