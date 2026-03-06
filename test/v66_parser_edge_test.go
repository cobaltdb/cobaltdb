package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestV66ParserEdge tests parser edge cases: multi-value INSERT, nested parens,
// expression edge cases, string escaping, numeric boundaries, and complex DDL.
func TestV66ParserEdge(t *testing.T) {
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
		if err != nil {
			pass++
		} else {
			t.Errorf("[FAIL] %s: expected error but got none", desc)
		}
	}
	_ = checkError

	// ============================================================
	// === MULTI-VALUE INSERT ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v66_multi (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)`)

	// MV1: Multiple rows in single INSERT
	checkNoError("MV1 multi-value INSERT",
		`INSERT INTO v66_multi VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)`)
	check("MV1 count", "SELECT COUNT(*) FROM v66_multi", 3)
	check("MV1 verify", "SELECT name FROM v66_multi WHERE id = 2", "b")

	// ============================================================
	// === NESTED PARENTHESES IN EXPRESSIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v66_expr (id INTEGER PRIMARY KEY, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v66_expr VALUES (1, 10)")

	// NP1: Deeply nested parentheses
	check("NP1 nested parens",
		"SELECT ((((val + 5) * 2) - 3) + 1) FROM v66_expr WHERE id = 1", 28)
	// ((10+5)*2 - 3) + 1 = (15*2 - 3) + 1 = (30-3) + 1 = 27 + 1 = 28

	// NP2: Nested in WHERE
	checkRowCount("NP2 nested WHERE",
		"SELECT * FROM v66_expr WHERE (val > 5 AND (val < 20 OR val = 10))", 1)

	// ============================================================
	// === STRING EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v66_str (id INTEGER PRIMARY KEY, val TEXT)`)

	// SE1: Empty string
	checkNoError("SE1 empty string", "INSERT INTO v66_str VALUES (1, '')")
	check("SE1 verify", "SELECT LENGTH(val) FROM v66_str WHERE id = 1", 0)

	// SE2: String with single quote (escaped as '')
	checkNoError("SE2 escaped quote", "INSERT INTO v66_str VALUES (2, 'it''s')")
	check("SE2 verify", "SELECT val FROM v66_str WHERE id = 2", "it's")

	// SE3: Long string
	longStr := strings.Repeat("abcdef", 50) // 300 chars
	checkNoError("SE3 long string",
		fmt.Sprintf("INSERT INTO v66_str VALUES (3, '%s')", longStr))
	check("SE3 verify length",
		"SELECT LENGTH(val) FROM v66_str WHERE id = 3", 300)

	// SE4: String with special characters
	checkNoError("SE4 special chars", "INSERT INTO v66_str VALUES (4, 'hello\nworld')")

	// ============================================================
	// === NUMERIC EDGE CASES ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v66_num (id INTEGER PRIMARY KEY, val INTEGER)`)

	// NE1: Zero
	checkNoError("NE1 zero", "INSERT INTO v66_num VALUES (1, 0)")
	check("NE1 verify", "SELECT val FROM v66_num WHERE id = 1", 0)

	// NE2: Negative number
	checkNoError("NE2 negative", "INSERT INTO v66_num VALUES (2, -999)")
	check("NE2 verify", "SELECT val FROM v66_num WHERE id = 2", -999)

	// NE3: Large number
	checkNoError("NE3 large", "INSERT INTO v66_num VALUES (3, 2147483647)")
	check("NE3 verify", "SELECT val FROM v66_num WHERE id = 3", 2147483647)

	// ============================================================
	// === COMPLEX DDL ===
	// ============================================================

	// CD1: Table with DEFAULT values
	checkNoError("CD1 table DEFAULT",
		`CREATE TABLE v66_defaults (
			id INTEGER PRIMARY KEY,
			name TEXT DEFAULT 'unknown',
			val INTEGER DEFAULT 0,
			active INTEGER DEFAULT 1
		)`)
	checkNoError("CD1 insert no defaults",
		"INSERT INTO v66_defaults (id) VALUES (1)")
	check("CD1 verify name", "SELECT name FROM v66_defaults WHERE id = 1", "unknown")
	check("CD1 verify val", "SELECT val FROM v66_defaults WHERE id = 1", 0)
	check("CD1 verify active", "SELECT active FROM v66_defaults WHERE id = 1", 1)

	// CD2: Table with NOT NULL
	checkNoError("CD2 table NOT NULL",
		`CREATE TABLE v66_notnull (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			val INTEGER
		)`)
	checkNoError("CD2 insert", "INSERT INTO v66_notnull VALUES (1, 'test', NULL)")
	checkError("CD2 null name", "INSERT INTO v66_notnull VALUES (2, NULL, 10)")

	// CD3: Table with CHECK constraint
	checkNoError("CD3 table CHECK",
		`CREATE TABLE v66_check (
			id INTEGER PRIMARY KEY,
			age INTEGER CHECK(age >= 0),
			score INTEGER CHECK(score >= 0 AND score <= 100)
		)`)
	checkNoError("CD3 valid insert", "INSERT INTO v66_check VALUES (1, 25, 85)")
	checkError("CD3 negative age", "INSERT INTO v66_check VALUES (2, -1, 50)")
	checkError("CD3 score > 100", "INSERT INTO v66_check VALUES (3, 20, 101)")

	// ============================================================
	// === ALTER TABLE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v66_alter (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v66_alter VALUES (1, 'test')")

	// AT1: ALTER TABLE ADD COLUMN
	checkNoError("AT1 ADD COLUMN",
		"ALTER TABLE v66_alter ADD COLUMN val INTEGER DEFAULT 0")
	check("AT1 verify", "SELECT val FROM v66_alter WHERE id = 1", 0)
	// ALTER ADD COLUMN with DEFAULT now backfills existing rows

	// AT2: Insert with new column
	checkNoError("AT2 insert new col",
		"INSERT INTO v66_alter VALUES (2, 'test2', 42)")
	check("AT2 verify", "SELECT val FROM v66_alter WHERE id = 2", 42)

	// ============================================================
	// === COMPLEX SELECT PATTERNS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v66_data (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v66_data VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO v66_data VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO v66_data VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO v66_data VALUES (4, 'b', 40)")
	afExec(t, db, ctx, "INSERT INTO v66_data VALUES (5, 'c', 50)")

	// CP1: Subquery in WHERE with aggregate
	checkRowCount("CP1 subquery agg WHERE",
		`SELECT * FROM v66_data
		 WHERE val > (SELECT AVG(val) FROM v66_data)`, 2)
	// AVG = 30; >30: 40, 50 = 2

	// CP2: Multiple subqueries in WHERE
	checkRowCount("CP2 multi subquery WHERE",
		`SELECT * FROM v66_data
		 WHERE val > (SELECT MIN(val) FROM v66_data)
		 AND val < (SELECT MAX(val) FROM v66_data)`, 3)
	// >10 AND <50: 20, 30, 40 = 3

	// CP3: Subquery with DISTINCT in WHERE
	checkRowCount("CP3 subquery DISTINCT WHERE",
		`SELECT * FROM v66_data
		 WHERE cat IN (SELECT DISTINCT cat FROM v66_data WHERE val > 25)`, 5)
	// All cats have at least one val>25 → all 5 rows match

	// CP4: CASE with subquery
	check("CP4 CASE subquery",
		`SELECT CASE
			WHEN val > (SELECT AVG(val) FROM v66_data) THEN 'above'
			ELSE 'below'
		 END FROM v66_data WHERE id = 4`, "above")
	// val=40 > AVG=30 → above

	// CP5: Nested CASE with aggregate
	check("CP5 nested CASE agg",
		`SELECT CASE
			WHEN SUM(val) > 100 THEN 'high'
			WHEN SUM(val) > 50 THEN 'medium'
			ELSE 'low'
		 END FROM v66_data`, "high")
	// SUM = 150 > 100 → high

	// ============================================================
	// === INDEX CREATION ===
	// ============================================================

	// IX1: CREATE INDEX
	checkNoError("IX1 CREATE INDEX",
		"CREATE INDEX idx_v66_cat ON v66_data (cat)")

	// IX2: Queries still work after index
	check("IX2 query with index",
		"SELECT SUM(val) FROM v66_data WHERE cat = 'a'", 40)

	// ============================================================
	// === DROP TABLE ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v66_temp (id INTEGER PRIMARY KEY)`)
	checkNoError("DT1 DROP TABLE", "DROP TABLE v66_temp")
	checkError("DT2 query dropped table", "SELECT * FROM v66_temp")

	// DT3: DROP TABLE IF EXISTS
	checkNoError("DT3 DROP IF EXISTS", "DROP TABLE IF EXISTS v66_temp")

	t.Logf("\n=== V66 PARSER EDGE CASES: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
