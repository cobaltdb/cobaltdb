package engine

import (
	"testing"
)

// TestAuditIntervalSyntax verifies MySQL INTERVAL support in DATE_ADD/DATE_SUB
// and in `date +/- INTERVAL n unit` arithmetic, including end-of-month clamping,
// while preserving the legacy DATE_ADD(date, days) form and a column named
// "interval".
func TestAuditIntervalSyntax(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, d TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, '2020-01-15')")

	scalar := func(expr string) string {
		rows := queryRows(t, db, "SELECT "+expr+" FROM t WHERE id = 1")
		if len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("expr %q: unexpected shape", expr)
		}
		if rows[0][0] == nil {
			return "<nil>"
		}
		return toStrEng(rows[0][0])
	}

	cases := []struct{ expr, want string }{
		{"DATE_ADD('2020-01-01', INTERVAL 5 DAY)", "2020-01-06"},
		{"DATE_SUB('2020-03-15', INTERVAL 2 MONTH)", "2020-01-15"},
		{"DATE_ADD('2020-01-31', INTERVAL 1 MONTH)", "2020-02-29"}, // clamp to leap Feb
		{"DATE_ADD('2020-02-29', INTERVAL 1 YEAR)", "2021-02-28"},  // leap -> non-leap
		{"DATE_ADD('2020-01-01', INTERVAL 1 WEEK)", "2020-01-08"},  //
		{"DATE_ADD('2020-01-01 10:00:00', INTERVAL 3 HOUR)", "2020-01-01 13:00:00"},
		{"d + INTERVAL 10 DAY", "2020-01-25"},
		{"d - INTERVAL 1 YEAR", "2019-01-15"},
		{"INTERVAL 7 DAY + '2020-01-01'", "2020-01-08"}, // commutative addition
		{"DATE_ADD('2020-01-01', 5)", "2020-01-06"},     // legacy (date, days) form
	}
	for _, tc := range cases {
		if got := scalar(tc.expr); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.expr, got, tc.want)
		}
	}

	// A predicate using INTERVAL.
	if rows := queryRows(t, db, "SELECT id FROM t WHERE d < DATE_ADD('2020-01-01', INTERVAL 30 DAY)"); len(rows) != 1 {
		t.Errorf("INTERVAL in WHERE returned %d rows, want 1", len(rows))
	}

	// A column literally named "interval" must still parse (backtracking).
	mustExec(t, db, "CREATE TABLE iv (id INTEGER PRIMARY KEY, interval INTEGER)")
	mustExec(t, db, "INSERT INTO iv VALUES (1, 42)")
	if rows := queryRows(t, db, "SELECT interval FROM iv WHERE id = 1"); len(rows) != 1 || toStrEng(rows[0][0]) != "42" {
		t.Errorf("column named 'interval' broke: %v", rows)
	}
}

// TestAuditHexLiteral verifies MySQL/SQLite hex literal support (0xFF = 255).
// Previously the lexer split 0xFF into number 0 + identifier xFF, so `SELECT
// 0xFF` silently returned 0 with an alias xFF.
func TestAuditHexLiteral(t *testing.T) {
	db := mustOpenMem(t)
	defer db.Close()
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 255)")
	scalar := func(expr string) string {
		rows := queryRows(t, db, "SELECT "+expr+" FROM t WHERE id = 1")
		if len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("expr %q bad shape", expr)
		}
		return toStrEng(rows[0][0])
	}
	for _, tc := range []struct{ expr, want string }{
		{"0xFF", "255"}, {"0x10", "16"}, {"0x0", "0"}, {"0xff", "255"},
		{"0x7FFFFFFFFFFFFFFF", "9223372036854775807"},
	} {
		if got := scalar(tc.expr); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.expr, got, tc.want)
		}
	}
	// Hex in a predicate matches.
	if rows := queryRows(t, db, "SELECT id FROM t WHERE v = 0xFF"); len(rows) != 1 {
		t.Errorf("WHERE v = 0xFF matched %d rows, want 1", len(rows))
	}
}
