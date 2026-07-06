package catalog

import (
	"fmt"
	"testing"
	"unicode/utf8"
)

// TestAuditStringAndCastFixes locks down the MySQL-compat fixes for HEX on
// numeric-looking strings, rune-based SUBSTR/LEFT/RIGHT/INSTR/LOCATE (no invalid
// UTF-8 on multibyte input), and CAST leading-numeric-prefix parsing.
func TestAuditStringAndCastFixes(t *testing.T) {
	c := newSemanticsTestCatalog(t)
	semExec(t, c, "CREATE TABLE one (x INTEGER)")
	semExec(t, c, "INSERT INTO one VALUES (1)")

	scalar := func(expr string) string {
		rows := semQuery(t, c, "SELECT "+expr+" FROM one")
		if len(rows) != 1 || len(rows[0]) != 1 {
			t.Fatalf("expr %q returned %d rows/%d cols", expr, len(rows), func() int {
				if len(rows) > 0 {
					return len(rows[0])
				}
				return 0
			}())
		}
		return fmt.Sprintf("%v", rows[0][0])
	}

	cases := []struct{ expr, want string }{
		// HEX: string args are byte-encoded; numeric args are unsigned hex.
		{"HEX('41')", "3431"},
		{"HEX('abc')", "616263"},
		{"HEX(255)", "FF"},
		{"HEX(0)", "0"},
		// SUBSTR/LEFT/RIGHT on multibyte input stay on rune boundaries.
		{"SUBSTR('héllo', 1, 2)", "hé"},
		{"SUBSTR('héllo', 2, 1)", "é"},
		{"LEFT('héllo', 2)", "hé"},
		{"RIGHT('héllo', 2)", "lo"},
		{"SUBSTR('héllo', -2)", "lo"},
		// ASCII SUBSTR unchanged.
		{"SUBSTR('hello', 2, 3)", "ell"},
		{"LEFT('hello', 3)", "hel"},
		{"RIGHT('hello', 3)", "llo"},
		// INSTR/LOCATE return character positions.
		{"INSTR('héllo', 'l')", "3"},
		{"INSTR('hello world', 'world')", "7"},
		{"LOCATE('l', 'héllo')", "3"},
		// CAST parses the leading numeric run; pure non-numeric stays 0.
		{"CAST('12abc' AS INTEGER)", "12"},
		{"CAST(' 10 ' AS INTEGER)", "10"},
		{"CAST('abc' AS INTEGER)", "0"},
		{"CAST('3.7' AS INTEGER)", "3"},
		{"CAST('1.5xyz' AS REAL)", "1.5"},
		{"CAST('42' AS INTEGER)", "42"},
	}
	for _, tc := range cases {
		if got := scalar(tc.expr); got != tc.want {
			t.Errorf("%s = %q, want %q", tc.expr, got, tc.want)
		}
	}

	// The multibyte results must be valid UTF-8 (the original bug produced "h\xc3").
	for _, expr := range []string{"SUBSTR('héllo', 1, 2)", "LEFT('héllo', 2)", "RIGHT('héllo', 2)"} {
		if got := scalar(expr); !utf8.ValidString(got) {
			t.Errorf("%s produced invalid UTF-8: %q", expr, got)
		}
	}
}
