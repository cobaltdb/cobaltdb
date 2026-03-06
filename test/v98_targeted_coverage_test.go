package test

import (
	"fmt"
	"testing"
)

// =============================================================================
// v98: Targeted tests for specific uncovered code paths in catalog.go
//   - Derived tables (FROM subquery) → executeDerivedTable + applyOuterQuery
//   - Window functions on CTE results → large uncovered block
//   - Complex views with outer WHERE → applyOuterQuery
//   - INSERT...SELECT type conversion paths
//   - UPDATE with PK change → updateLocked PK change path
//   - selectLocked: INSERT SELECT, column mapping, VIEW inlining
// =============================================================================

// --- Derived Tables (FROM subquery) ---

func TestV98_DerivedTableBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_dt (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_dt VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t98_dt VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t98_dt VALUES (3, 30)")

	// Basic derived table
	rows := afQuery(t, db, ctx, "SELECT * FROM (SELECT id, val FROM t98_dt WHERE val > 15) AS sub")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from derived table, got %d", len(rows))
	}
}

func TestV98_DerivedTableWithOuterWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_dt2 (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_dt2 VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO t98_dt2 VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO t98_dt2 VALUES (3, 'c', 30)")

	// Derived table with outer WHERE
	rows := afQuery(t, db, ctx, "SELECT id, val FROM (SELECT id, val, score FROM t98_dt2) AS sub WHERE score >= 20")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV98_DerivedTableWithAggregates(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_dt3 (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_dt3 VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t98_dt3 VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t98_dt3 VALUES (3, 'B', 30)")

	// Derived table with aggregate in outer query
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM (SELECT * FROM t98_dt3 WHERE cat = 'A') AS sub", float64(2))
}

func TestV98_DerivedTableWithOrderByLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_dt4 (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t98_dt4 VALUES (%d, %d)", i, i*10))
	}

	// Derived table with ORDER BY and LIMIT in outer query
	rows := afQuery(t, db, ctx, "SELECT id FROM (SELECT id, val FROM t98_dt4) AS sub ORDER BY val DESC LIMIT 3")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// --- Window Functions on CTE Results ---

func TestV98_CTEWithWindowFunction(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_win (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_win VALUES (1, 'eng', 100)")
	afExec(t, db, ctx, "INSERT INTO t98_win VALUES (2, 'eng', 200)")
	afExec(t, db, ctx, "INSERT INTO t98_win VALUES (3, 'sales', 150)")

	// CTE + window function (ROW_NUMBER)
	rows := afQuery(t, db, ctx, `
		WITH emp AS (SELECT id, dept, salary FROM t98_win)
		SELECT id, dept, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn
		FROM emp
	`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows from CTE + window, got %d", len(rows))
	}
}

func TestV98_CTEWithWindowPartitionBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_win2 (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_win2 VALUES (1, 'A', 10)")
	afExec(t, db, ctx, "INSERT INTO t98_win2 VALUES (2, 'A', 20)")
	afExec(t, db, ctx, "INSERT INTO t98_win2 VALUES (3, 'B', 30)")
	afExec(t, db, ctx, "INSERT INTO t98_win2 VALUES (4, 'B', 40)")

	// CTE + window function with PARTITION BY
	rows := afQuery(t, db, ctx, `
		WITH data AS (SELECT id, grp, val FROM t98_win2)
		SELECT id, grp, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val) as rn
		FROM data
	`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestV98_CTEWithWindowAndLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_win3 (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 5; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t98_win3 VALUES (%d, %d)", i, i*10))
	}

	// CTE + window + LIMIT
	rows := afQuery(t, db, ctx, `
		WITH nums AS (SELECT id, val FROM t98_win3)
		SELECT id, ROW_NUMBER() OVER (ORDER BY val DESC) as rn
		FROM nums
		ORDER BY rn
		LIMIT 3
	`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows with limit, got %d", len(rows))
	}
}

// --- Complex Views with Outer Queries ---

func TestV98_ComplexViewWithGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_cv (id INTEGER PRIMARY KEY, cat TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_cv VALUES (1, 'X', 100)")
	afExec(t, db, ctx, "INSERT INTO t98_cv VALUES (2, 'X', 200)")
	afExec(t, db, ctx, "INSERT INTO t98_cv VALUES (3, 'Y', 300)")
	afExec(t, db, ctx, "INSERT INTO t98_cv VALUES (4, 'Y', 400)")

	// Complex view with GROUP BY
	afExec(t, db, ctx, "CREATE VIEW v98_totals AS SELECT cat, SUM(amount) as total FROM t98_cv GROUP BY cat")

	// Query view with outer WHERE
	afExpectVal(t, db, ctx, "SELECT total FROM v98_totals WHERE cat = 'X'", float64(300))
	afExpectVal(t, db, ctx, "SELECT total FROM v98_totals WHERE cat = 'Y'", float64(700))
}

func TestV98_ComplexViewWithDistinct(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_cvd (id INTEGER PRIMARY KEY, cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_cvd VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t98_cvd VALUES (2, 'A')")
	afExec(t, db, ctx, "INSERT INTO t98_cvd VALUES (3, 'B')")

	// Complex view with DISTINCT
	afExec(t, db, ctx, "CREATE VIEW v98_distinct AS SELECT DISTINCT cat FROM t98_cvd")

	rows := afQuery(t, db, ctx, "SELECT * FROM v98_distinct ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 distinct rows, got %d", len(rows))
	}
}

func TestV98_ComplexViewWithAlias(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_cva (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_cva VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO t98_cva VALUES (2, 'Bob')")

	// View with aliased columns (triggers complex view path)
	afExec(t, db, ctx, "CREATE VIEW v98_aliased AS SELECT id AS uid, name AS uname FROM t98_cva")

	rows := afQuery(t, db, ctx, "SELECT uid, uname FROM v98_aliased ORDER BY uid")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][1] != "Alice" {
		t.Errorf("expected Alice, got %v", rows[0][1])
	}
}

func TestV98_SimpleViewWithWhereInlining(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_sv (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_sv VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO t98_sv VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO t98_sv VALUES (3, 'c', 30)")

	// Simple view (no aggregates, no aliases, no DISTINCT)
	afExec(t, db, ctx, "CREATE VIEW v98_simple AS SELECT id, val, score FROM t98_sv WHERE score > 5")

	// Outer query with additional WHERE - should inline
	rows := afQuery(t, db, ctx, "SELECT val FROM v98_simple WHERE score > 15")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after inlined WHERE, got %d", len(rows))
	}

	// SELECT * from simple view
	rows2 := afQuery(t, db, ctx, "SELECT * FROM v98_simple")
	if len(rows2) != 3 {
		t.Fatalf("expected 3 rows from SELECT *, got %d", len(rows2))
	}
}

// --- INSERT...SELECT Type Conversion ---

func TestV98_InsertSelectBasic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_src (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_src VALUES (1, 'hello', 100)")
	afExec(t, db, ctx, "INSERT INTO t98_src VALUES (2, 'world', 200)")

	afExec(t, db, ctx, "CREATE TABLE t98_dst (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)")

	// INSERT...SELECT
	afExec(t, db, ctx, "INSERT INTO t98_dst SELECT * FROM t98_src")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_dst", float64(2))
	afExpectVal(t, db, ctx, "SELECT val FROM t98_dst WHERE id = 1", "hello")
}

func TestV98_InsertSelectWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_src2 (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_src2 VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t98_src2 VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t98_src2 VALUES (3, 30)")

	afExec(t, db, ctx, "CREATE TABLE t98_dst2 (id INTEGER PRIMARY KEY, val INTEGER)")

	// INSERT...SELECT with WHERE
	afExec(t, db, ctx, "INSERT INTO t98_dst2 SELECT * FROM t98_src2 WHERE val >= 20")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_dst2", float64(2))
}

func TestV98_InsertSelectWithNullAndBool(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_src3 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_src3 VALUES (1, 'test')")
	afExec(t, db, ctx, "INSERT INTO t98_src3 VALUES (2, NULL)")

	afExec(t, db, ctx, "CREATE TABLE t98_dst3 (id INTEGER PRIMARY KEY, val TEXT)")

	// INSERT...SELECT with NULL values
	afExec(t, db, ctx, "INSERT INTO t98_dst3 SELECT * FROM t98_src3")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_dst3", float64(2))
	afExpectVal(t, db, ctx, "SELECT val FROM t98_dst3 WHERE id = 2", nil)
}

func TestV98_InsertSelectWithColumns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_src4 (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_src4 VALUES (1, 'Alice', 90)")
	afExec(t, db, ctx, "INSERT INTO t98_src4 VALUES (2, 'Bob', 85)")

	afExec(t, db, ctx, "CREATE TABLE t98_dst4 (id INTEGER PRIMARY KEY, name TEXT)")

	// INSERT with column list and SELECT
	afExec(t, db, ctx, "INSERT INTO t98_dst4 (id, name) SELECT id, name FROM t98_src4")
	afExpectVal(t, db, ctx, "SELECT name FROM t98_dst4 WHERE id = 1", "Alice")
}

// --- UPDATE with PK Change ---

func TestV98_UpdatePrimaryKeyChange(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_pk (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_pk VALUES (1, 'first')")
	afExec(t, db, ctx, "INSERT INTO t98_pk VALUES (2, 'second')")

	// Change PK value
	afExec(t, db, ctx, "UPDATE t98_pk SET id = 10 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT val FROM t98_pk WHERE id = 10", "first")

	// Old key should not exist
	rows := afQuery(t, db, ctx, "SELECT * FROM t98_pk WHERE id = 1")
	if len(rows) != 0 {
		t.Error("old PK=1 should not exist after PK change")
	}
}

func TestV98_UpdatePKDuplicateError(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_pk2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_pk2 VALUES (1, 'first')")
	afExec(t, db, ctx, "INSERT INTO t98_pk2 VALUES (2, 'second')")

	// Changing PK to existing value should fail
	_, err := db.Exec(ctx, "UPDATE t98_pk2 SET id = 2 WHERE id = 1")
	if err == nil {
		t.Error("expected PK duplicate error")
	}
}

func TestV98_UpdateTextPKChange(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_tpk (key TEXT PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_tpk VALUES ('old_key', 'data')")

	// Change TEXT PK
	afExec(t, db, ctx, "UPDATE t98_tpk SET key = 'new_key' WHERE key = 'old_key'")
	afExpectVal(t, db, ctx, "SELECT val FROM t98_tpk WHERE key = 'new_key'", "data")

	rows := afQuery(t, db, ctx, "SELECT * FROM t98_tpk WHERE key = 'old_key'")
	if len(rows) != 0 {
		t.Error("old TEXT PK should not exist after change")
	}
}

// --- UPDATE with FK Constraints ---

func TestV98_UpdateFKConstraintCheck(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t98_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES t98_parent(id))")

	afExec(t, db, ctx, "INSERT INTO t98_parent VALUES (1, 'Parent1')")
	afExec(t, db, ctx, "INSERT INTO t98_parent VALUES (2, 'Parent2')")
	afExec(t, db, ctx, "INSERT INTO t98_child VALUES (1, 1)")

	// Update FK to valid ref should succeed
	afExec(t, db, ctx, "UPDATE t98_child SET parent_id = 2 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT parent_id FROM t98_child WHERE id = 1", float64(2))

	// Update FK to invalid ref should fail
	_, err := db.Exec(ctx, "UPDATE t98_child SET parent_id = 999 WHERE id = 1")
	if err == nil {
		t.Error("expected FK constraint error")
	}
}

// --- UPDATE with NOT NULL and CHECK ---

func TestV98_UpdateNotNullViolation(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_nn (id INTEGER PRIMARY KEY, val TEXT NOT NULL)")
	afExec(t, db, ctx, "INSERT INTO t98_nn VALUES (1, 'hello')")

	// Setting NOT NULL column to NULL should fail
	_, err := db.Exec(ctx, "UPDATE t98_nn SET val = NULL WHERE id = 1")
	if err == nil {
		t.Error("expected NOT NULL constraint error")
	}
}

func TestV98_UpdateCheckViolation(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_chk (id INTEGER PRIMARY KEY, score INTEGER CHECK (score >= 0))")
	afExec(t, db, ctx, "INSERT INTO t98_chk VALUES (1, 50)")

	// Setting value that violates CHECK should fail
	_, err := db.Exec(ctx, "UPDATE t98_chk SET score = -1 WHERE id = 1")
	if err == nil {
		t.Error("expected CHECK constraint error")
	}

	// Valid update should succeed
	afExec(t, db, ctx, "UPDATE t98_chk SET score = 100 WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT score FROM t98_chk WHERE id = 1", float64(100))
}

// --- UPDATE with Indexes ---

func TestV98_UpdateWithIndexMaintenance(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_upidx (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_upidx_val ON t98_upidx (val)")
	afExec(t, db, ctx, "INSERT INTO t98_upidx VALUES (1, 'original')")
	afExec(t, db, ctx, "INSERT INTO t98_upidx VALUES (2, 'other')")

	// Update indexed column
	afExec(t, db, ctx, "UPDATE t98_upidx SET val = 'modified' WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT val FROM t98_upidx WHERE id = 1", "modified")

	// Verify index is maintained - query should still work
	afExpectVal(t, db, ctx, "SELECT id FROM t98_upidx WHERE val = 'modified'", float64(1))
}

// --- Trigger Coverage ---

func TestV98_TriggerAfterUpdate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_trg (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t98_log (id INTEGER PRIMARY KEY, msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER trg_after_update AFTER UPDATE ON t98_trg
		FOR EACH ROW BEGIN INSERT INTO t98_log VALUES (NULL, 'after_update'); END`)

	afExec(t, db, ctx, "INSERT INTO t98_trg VALUES (1, 'initial')")
	afExec(t, db, ctx, "UPDATE t98_trg SET val = 'changed' WHERE id = 1")

	// Trigger should have fired
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_log", float64(1))
}

func TestV98_TriggerAfterDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_trg2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t98_log2 (id INTEGER PRIMARY KEY, msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER trg_after_delete AFTER DELETE ON t98_trg2
		FOR EACH ROW BEGIN INSERT INTO t98_log2 VALUES (NULL, OLD.val); END`)

	afExec(t, db, ctx, "INSERT INTO t98_trg2 VALUES (1, 'doomed')")
	afExec(t, db, ctx, "DELETE FROM t98_trg2 WHERE id = 1")

	afExpectVal(t, db, ctx, "SELECT msg FROM t98_log2 WHERE id = 1", "doomed")
}

// --- DELETE with Index Cleanup ---

func TestV98_DeleteWithIndexCleanup(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_delidx (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE UNIQUE INDEX idx_delidx ON t98_delidx (val)")
	afExec(t, db, ctx, "INSERT INTO t98_delidx VALUES (1, 'unique_val')")

	// Delete row
	afExec(t, db, ctx, "DELETE FROM t98_delidx WHERE id = 1")

	// Re-insert with same unique value should work
	afExec(t, db, ctx, "INSERT INTO t98_delidx VALUES (2, 'unique_val')")
	afExpectVal(t, db, ctx, "SELECT id FROM t98_delidx WHERE val = 'unique_val'", float64(2))
}

// --- Aggregate edge cases ---

func TestV98_AggregatesInJoinWithGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_dept (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t98_emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t98_dept VALUES (1, 'Eng')")
	afExec(t, db, ctx, "INSERT INTO t98_dept VALUES (2, 'Sales')")
	afExec(t, db, ctx, "INSERT INTO t98_emp VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t98_emp VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO t98_emp VALUES (3, 2, 150)")
	afExec(t, db, ctx, "INSERT INTO t98_emp VALUES (4, 2, 250)")

	// JOIN + GROUP BY + multiple aggregates
	rows := afQuery(t, db, ctx, `
		SELECT t98_dept.name, COUNT(*) as cnt, SUM(t98_emp.salary) as total, AVG(t98_emp.salary) as avg_sal
		FROM t98_dept JOIN t98_emp ON t98_dept.id = t98_emp.dept_id
		GROUP BY t98_dept.name
		ORDER BY t98_dept.name
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(rows))
	}
	if rows[0][0] != "Eng" {
		t.Errorf("expected Eng first, got %v", rows[0][0])
	}
}

// --- Qualified identifiers in SELECT ---

func TestV98_QualifiedIdentifierInSelect(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_qi1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t98_qi2 (id INTEGER PRIMARY KEY, ref_id INTEGER, info TEXT)")

	afExec(t, db, ctx, "INSERT INTO t98_qi1 VALUES (1, 'alpha')")
	afExec(t, db, ctx, "INSERT INTO t98_qi2 VALUES (1, 1, 'detail')")

	// SELECT with qualified identifiers
	rows := afQuery(t, db, ctx, "SELECT t98_qi1.val, t98_qi2.info FROM t98_qi1 JOIN t98_qi2 ON t98_qi1.id = t98_qi2.ref_id")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != "alpha" || rows[0][1] != "detail" {
		t.Errorf("unexpected row values: %v", rows[0])
	}
}

// --- CTE with JOIN ---

func TestV98_CTEWithJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_cj1 (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t98_cj2 (id INTEGER PRIMARY KEY, cj1_id INTEGER, score INTEGER)")

	afExec(t, db, ctx, "INSERT INTO t98_cj1 VALUES (1, 'A')")
	afExec(t, db, ctx, "INSERT INTO t98_cj1 VALUES (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO t98_cj2 VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO t98_cj2 VALUES (2, 2, 200)")

	// CTE joined with regular table
	rows := afQuery(t, db, ctx, `
		WITH high_scores AS (SELECT cj1_id, score FROM t98_cj2 WHERE score > 50)
		SELECT t98_cj1.name, high_scores.score
		FROM t98_cj1 JOIN high_scores ON t98_cj1.id = high_scores.cj1_id
		ORDER BY high_scores.score
	`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 CTE+JOIN rows, got %d", len(rows))
	}
}

// --- UNION in derived table ---

func TestV98_UnionInDerivedTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_u1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t98_u2 (id INTEGER PRIMARY KEY, val TEXT)")

	afExec(t, db, ctx, "INSERT INTO t98_u1 VALUES (1, 'from_t1')")
	afExec(t, db, ctx, "INSERT INTO t98_u2 VALUES (1, 'from_t2')")

	// UNION in derived table
	rows := afQuery(t, db, ctx, "SELECT val FROM (SELECT val FROM t98_u1 UNION ALL SELECT val FROM t98_u2) AS combined")
	if len(rows) != 2 {
		t.Fatalf("expected 2 union rows, got %d", len(rows))
	}
}

// --- VACUUM and ANALYZE deeper ---

func TestV98_VacuumAfterManyDeletes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_vac (id INTEGER PRIMARY KEY, data TEXT)")
	for i := 1; i <= 50; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t98_vac VALUES (%d, 'row_%d')", i, i))
	}

	// Delete most rows
	afExec(t, db, ctx, "DELETE FROM t98_vac WHERE id > 5")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_vac", float64(5))

	// Vacuum
	afExec(t, db, ctx, "VACUUM")

	// Data should still be intact
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_vac", float64(5))
	afExpectVal(t, db, ctx, "SELECT data FROM t98_vac WHERE id = 3", "row_3")
}

func TestV98_AnalyzeWithIndexes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_an (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "CREATE INDEX idx_an_cat ON t98_an (cat)")
	for i := 1; i <= 20; i++ {
		cat := "A"
		if i > 10 {
			cat = "B"
		}
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t98_an VALUES (%d, '%s', %d)", i, cat, i*10))
	}

	// Analyze should work with indexes
	afExec(t, db, ctx, "ANALYZE")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_an", float64(20))
}

// --- Transaction rollback with updates ---

func TestV98_TransactionRollbackUpdate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_txr (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_txr VALUES (1, 'original')")

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "UPDATE t98_txr SET val = 'changed' WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT val FROM t98_txr WHERE id = 1", "changed")
	afExec(t, db, ctx, "ROLLBACK")

	// Value should be restored
	afExpectVal(t, db, ctx, "SELECT val FROM t98_txr WHERE id = 1", "original")
}

func TestV98_TransactionRollbackDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_txd (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_txd VALUES (1, 'keep')")
	afExec(t, db, ctx, "INSERT INTO t98_txd VALUES (2, 'delete')")

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "DELETE FROM t98_txd WHERE id = 2")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_txd", float64(1))
	afExec(t, db, ctx, "ROLLBACK")

	// Row should be restored
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_txd", float64(2))
}

// --- DropTable with indexes ---

func TestV98_DropTableCleansIndexes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_drop (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_drop ON t98_drop (val)")
	afExec(t, db, ctx, "INSERT INTO t98_drop VALUES (1, 'test')")

	afExec(t, db, ctx, "DROP TABLE t98_drop")

	// Recreate with same index name
	afExec(t, db, ctx, "CREATE TABLE t98_drop (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE INDEX idx_drop ON t98_drop (val)")
	afExec(t, db, ctx, "INSERT INTO t98_drop VALUES (1, 'new')")
	afExpectVal(t, db, ctx, "SELECT val FROM t98_drop WHERE id = 1", "new")
}

// --- Edge case: INSERT with expressions ---

func TestV98_InsertWithExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_expr (id INTEGER PRIMARY KEY, val TEXT, num INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_expr VALUES (1, 'a' || 'b', 10 + 20)")
	afExpectVal(t, db, ctx, "SELECT val FROM t98_expr WHERE id = 1", "ab")
	afExpectVal(t, db, ctx, "SELECT num FROM t98_expr WHERE id = 1", float64(30))
}

// --- SELECT with OFFSET ---

func TestV98_SelectWithOffset(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_off (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO t98_off VALUES (%d, %d)", i, i*10))
	}

	// LIMIT with OFFSET
	rows := afQuery(t, db, ctx, "SELECT id FROM t98_off ORDER BY id LIMIT 3 OFFSET 5")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows with offset, got %d", len(rows))
	}
}

// --- Multiple aggregates without GROUP BY ---

func TestV98_MultipleAggregatesNoGroupBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_ma (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO t98_ma VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO t98_ma VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO t98_ma VALUES (3, 30)")

	rows := afQuery(t, db, ctx, "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM t98_ma")
	if len(rows) != 1 {
		t.Fatalf("expected 1 aggregate row, got %d", len(rows))
	}
	// COUNT should be 3
	if fmt.Sprintf("%v", rows[0][0]) != "3" {
		t.Errorf("COUNT expected 3, got %v", rows[0][0])
	}
}

// --- Savepoint paths (catalog-level) ---

func TestV98_SavepointAndRelease(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_sp (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_sp VALUES (1, 'base')")

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO t98_sp VALUES (2, 'in_sp')")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_sp", float64(2))
	afExec(t, db, ctx, "RELEASE SAVEPOINT sp1")
	afExec(t, db, ctx, "COMMIT")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_sp", float64(2))
}

func TestV98_SavepointRollback(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE t98_sp2 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO t98_sp2 VALUES (1, 'keep')")

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO t98_sp2 VALUES (2, 'temp')")
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp1")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_sp2", float64(1))
	afExec(t, db, ctx, "COMMIT")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM t98_sp2", float64(1))
}
