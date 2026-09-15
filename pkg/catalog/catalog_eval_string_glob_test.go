package catalog

import (
	"strings"
	"testing"
)

// TestGLOBBracketClasses pins SQLite-style bracket character classes in GLOB
// (docs/MYSQL_COMPATIBILITY.md documents GLOB as "SQLite-style").
//
// Regression: evalStringGlob ran the pattern through regexp.QuoteMeta, which
// escapes [ and ], so character classes never matched — GLOB('h[ae]llo',
// 'hello') returned false. The conversion now supports classes ([abc]),
// ranges ([a-c]), ^ negation, a leading ] as a literal member ([]] matches
// "]"), and an unterminated [ as a literal; patterns are compiled once and
// cached (bounded) instead of recompiled on every evaluation.
func TestGLOBBracketClasses(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want bool
	}{
		// Classes, ranges, negation.
		{"SELECT GLOB('h[ae]llo', 'hello')", true},
		{"SELECT GLOB('h[ae]llo', 'hillo')", false},
		{"SELECT GLOB('[a-c]at', 'bat')", true},
		{"SELECT GLOB('[a-c]at', 'dat')", false},
		{"SELECT GLOB('[^a]pple', 'bpple')", true},
		{"SELECT GLOB('[^a]pple', 'apple')", false},
		// Leading ] is a literal member ([]] matches "]").
		{"SELECT GLOB('[]]', ']')", true},
		// Classes combined with wildcards.
		{"SELECT GLOB('*[0-9]', 'abc7')", true},
		{"SELECT GLOB('*[0-9]', 'abcx')", false},
		// Wildcard and case-sensitivity behavior must not regress.
		{"SELECT GLOB('hello', 'hello')", true},
		{"SELECT GLOB('hello', 'hellx')", false},
		{"SELECT GLOB('h?llo', 'hello')", true},
		{"SELECT GLOB('h?llo', 'hllo')", false},
		{"SELECT GLOB('h*llo', 'heeeello')", true},
		{"SELECT GLOB('a*b*c', 'aXbYc')", true},
		{"SELECT GLOB('HELLO', 'hello')", false},
		{"SELECT GLOB('hel*', 'hello')", true},
		// Unterminated [ is a literal.
		{"SELECT GLOB('a[b', 'a[b')", true},
		{"SELECT GLOB('a[b', 'ab')", false},
		// A reversed (empty) range never matches and must not error or panic.
		{"SELECT GLOB('x[z-a]y', 'xzy')", false},
		{"SELECT GLOB('x[z-a]y', 'x[y')", false},
	}

	for _, tc := range cases {
		res, err := c.ExecuteQuery(tc.sql)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.sql, err)
			continue
		}
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
			t.Errorf("%s: want 1x1 result, got %v", tc.sql, res.Rows)
			continue
		}
		got, ok := res.Rows[0][0].(bool)
		if !ok {
			t.Errorf("%s: want bool result, got %T (%v)", tc.sql, res.Rows[0][0], res.Rows[0][0])
			continue
		}
		if got != tc.want {
			t.Errorf("%s = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

// TestGLOBNullAndArity pins the NULL propagation and arity contracts around
// the matcher (any NULL argument yields NULL; fewer than 2 arguments is an
// error).
func TestGLOBNullAndArity(t *testing.T) {
	c := newTestCatalog(t)

	for _, sql := range []string{
		"SELECT GLOB(NULL, 'hello')",
		"SELECT GLOB('hello', NULL)",
	} {
		res, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", sql, err)
			continue
		}
		if len(res.Rows) != 1 || res.Rows[0][0] != nil {
			t.Errorf("%s = %v, want NULL", sql, res.Rows)
		}
	}

	if _, err := c.ExecuteQuery("SELECT GLOB('hello')"); err == nil {
		t.Error("SELECT GLOB('hello'): want arity error, got nil")
	} else if !strings.Contains(err.Error(), "requires 2 arguments") {
		t.Errorf("want \"requires 2 arguments\" error, got: %v", err)
	}
}
