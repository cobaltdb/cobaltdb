package catalog

import "testing"

// TestAuditMathNonFiniteReturnsNull verifies that math functions producing a
// non-finite result (NaN/±Inf) return SQL NULL rather than erroring (SQRT of a
// negative) or leaking NaN/Inf to the client (POWER/EXP), matching MySQL/SQLite.
func TestAuditMathNonFiniteReturnsNull(t *testing.T) {
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE one (x INTEGER)")
	semExec(t, c, "INSERT INTO one VALUES (1)")

	nullExprs := []string{
		"SQRT(-1)", "SQRT(-0.5)",
		"POWER(-2, 0.5)", "POWER(0, -1)",
		"EXP(1000)",
	}
	for _, e := range nullExprs {
		rows := semQuery(t, c, "SELECT "+e+" FROM one")
		if len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("%s: bad shape", e)
		}
		if rows[0][0] != nil {
			t.Errorf("%s = %v (%T), want NULL", e, rows[0][0], rows[0][0])
		}
	}

	// Valid results are preserved.
	valid := map[string]float64{"SQRT(4)": 2, "POWER(2, 10)": 1024}
	for e, want := range valid {
		rows := semQuery(t, c, "SELECT "+e+" FROM one")
		if got := semNum(t, rows[0][0]); got != want {
			t.Errorf("%s = %v, want %v", e, got, want)
		}
	}
	// A negative SQRT in row context must not abort the whole query.
	semExec(t, c, "INSERT INTO one VALUES (200)")
	rows := semQuery(t, c, "SELECT SQRT(x - 100) FROM one ORDER BY x")
	if len(rows) != 2 {
		t.Fatalf("row-context SQRT aborted query: got %d rows, want 2", len(rows))
	}
	if rows[0][0] != nil {
		t.Errorf("SQRT(1-100) = %v, want NULL", rows[0][0])
	}
}
