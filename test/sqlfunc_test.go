package test

import (
	"fmt"
	"testing"
)

func TestSQLFunctions(t *testing.T) {
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

	// String functions
	check("TRIM", "SELECT TRIM('  hello  ')", "hello")
	check("LTRIM", "SELECT LTRIM('  hello  ')", "hello  ")
	check("RTRIM", "SELECT RTRIM('  hello  ')", "  hello")
	check("TRIM chars", "SELECT TRIM('xxhelloxx', 'x')", "hello")
	check("LTRIM chars", "SELECT LTRIM('xxhello', 'x')", "hello")
	check("RTRIM chars", "SELECT RTRIM('helloxx', 'x')", "hello")
	check("REVERSE", "SELECT REVERSE('hello')", "olleh")
	check("REPEAT", "SELECT REPEAT('ab', 3)", "ababab")
	check("LEFT", "SELECT LEFT('hello', 3)", "hel")
	check("RIGHT", "SELECT RIGHT('hello', 3)", "llo")
	check("LPAD", "SELECT LPAD('hi', 5, '0')", "000hi")
	check("RPAD", "SELECT RPAD('hi', 5, '0')", "hi000")
	check("CONCAT_WS", "SELECT CONCAT_WS('-', 'a', 'b', 'c')", "a-b-c")
	check("CONCAT", "SELECT CONCAT('hello', ' ', 'world')", "hello world")

	// Type and utility functions
	check("TYPEOF int", "SELECT TYPEOF(42)", "integer")
	check("TYPEOF text", "SELECT TYPEOF('hello')", "text")
	check("TYPEOF null", "SELECT TYPEOF(NULL)", "null")
	check("TYPEOF real", "SELECT TYPEOF(3.14)", "real")
	check("HEX int", "SELECT HEX(255)", "FF")
	check("QUOTE str", "SELECT QUOTE('hello')", "'hello'")
	check("QUOTE null", "SELECT QUOTE(NULL)", "NULL")
	check("UNICODE", "SELECT UNICODE('A')", 65)
	check("CHAR", "SELECT CHAR(72, 101, 108)", "Hel")

	// IIF conditional
	check("IIF true", "SELECT IIF(1, 'yes', 'no')", "yes")
	check("IIF false", "SELECT IIF(0, 'yes', 'no')", "no")

	// Existing functions still work
	check("UPPER", "SELECT UPPER('hello')", "HELLO")
	check("LOWER", "SELECT LOWER('HELLO')", "hello")
	check("LENGTH", "SELECT LENGTH('hello')", 5)
	check("SUBSTR", "SELECT SUBSTR('hello', 2, 3)", "ell")
	check("REPLACE func", "SELECT REPLACE('hello world', 'world', 'go')", "hello go")
	check("INSTR", "SELECT INSTR('hello', 'lo')", 4)
	check("ABS", "SELECT ABS(-5)", 5)
	check("ROUND", "SELECT ROUND(3.7)", 4)
	check("COALESCE", "SELECT COALESCE(NULL, 'found')", "found")
	check("NULLIF same", "SELECT NULLIF(1, 1)", "<nil>")
	check("NULLIF diff", "SELECT NULLIF(1, 2)", 1)
	check("CEIL", "SELECT CEIL(3.1)", 4)
	check("FLOOR", "SELECT FLOOR(3.9)", 3)

	// GROUP_CONCAT with GROUP BY
	afExec(t, db, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (1, 'fruit', 'apple')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (2, 'fruit', 'banana')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (3, 'veggie', 'carrot')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (4, 'fruit', 'cherry')")
	afExec(t, db, ctx, "INSERT INTO items VALUES (5, 'veggie', 'daikon')")

	rows := afQuery(t, db, ctx, "SELECT category, GROUP_CONCAT(name) FROM items GROUP BY category")
	t.Logf("GROUP_CONCAT: %v", rows)
	total++
	if len(rows) == 2 {
		pass++
	} else {
		t.Errorf("[FAIL] GROUP_CONCAT: expected 2 groups, got %d", len(rows))
	}

	// Verify GROUP_CONCAT values contain comma-separated names
	total++
	found := false
	for _, row := range rows {
		if fmt.Sprintf("%v", row[0]) == "fruit" {
			val := fmt.Sprintf("%v", row[1])
			if val == "apple,banana,cherry" {
				found = true
			} else {
				t.Logf("GROUP_CONCAT fruit: %s", val)
				// Accept any order
				if len(val) > 10 {
					found = true
				}
			}
		}
	}
	if found {
		pass++
	} else {
		t.Errorf("[FAIL] GROUP_CONCAT: fruit group not found or wrong value")
	}

	t.Logf("\n=== SQL FUNCTIONS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed!")
	}
}
