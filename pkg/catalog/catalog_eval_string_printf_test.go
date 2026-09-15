package catalog

import (
	"strings"
	"testing"
)

// TestPRINTFPercentEscape pins the %% escape in PRINTF: it must emit ONE
// literal % and consume no argument, and the character after it stays a
// literal (C/SQLite printf semantics).
//
// Regression: evalStringPrintf had no %% case — the first % hit the default
// branch and was written literally, then the second % started a fresh format
// spec, so PRINTF('100%%') returned "100%%" and PRINTF('%%s', 'x') returned
// "%x" (swallowing the argument) instead of "%s".
func TestPRINTFPercentEscape(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want interface{}
	}{
		// Escape collapse.
		{"SELECT PRINTF('100%%')", "100%"},
		{"SELECT PRINTF('a%%b%%c')", "a%b%c"},
		{"SELECT PRINTF('%%')", "%"},
		{"SELECT PRINTF('a%%')", "a%"},
		// The character after %% stays literal: no argument is consumed.
		{"SELECT PRINTF('%%s', 'x')", "%s"},
		{"SELECT PRINTF('%%d', 5)", "%d"},
		// Mixed with real specs.
		{"SELECT PRINTF('%d%%', 50)", "50%"},
		// Existing behavior that must not regress.
		{"SELECT PRINTF('hello %s', 'world')", "hello world"},
		{"SELECT PRINTF('%d apples', 3)", "3 apples"},
		{"SELECT PRINTF('%f', 1.5)", "1.500000"},
		{"SELECT PRINTF('no specs')", "no specs"},
		// A lone trailing % is a literal (unchanged).
		{"SELECT PRINTF('trailing %')", "trailing %"},
		{"SELECT PRINTF('%s %s', 'a', 'b')", "a b"},
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
		got := res.Rows[0][0]
		gs, ok := got.(string)
		if !ok {
			t.Errorf("%s: want string result, got %T (%v)", tc.sql, got, got)
			continue
		}
		if gs != tc.want.(string) {
			t.Errorf("%s = %q, want %q", tc.sql, gs, tc.want.(string))
		}
	}
}

// TestPRINTFArity pins the argument-count error (already listed in the
// funcsErr coverage suite; asserted here with its exact message).
func TestPRINTFArity(t *testing.T) {
	c := newTestCatalog(t)

	if _, err := c.ExecuteQuery("SELECT PRINTF()"); err == nil {
		t.Error("SELECT PRINTF(): want arity error, got nil")
	} else if !strings.Contains(err.Error(), "requires at least 1 argument") {
		t.Errorf("want \"requires at least 1 argument\" error, got: %v", err)
	}
}

// TestPRINTFFormatSpecs pins the C/SQLite-style conversion specs added after
// the %% escape fix: the - + 0 space # flags, minimum width, precision, and
// the %x %o %e %g %c %q verbs. Width and precision count runes, not bytes.
//
// Regression: everything except %%/%s/%d/%i/%f fell through to the literal-%
// default branch — PRINTF('%5.2f', 3.14159) returned "%5.2f".
func TestPRINTFFormatSpecs(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want interface{}
	}{
		// Width / precision / flags.
		{"SELECT PRINTF('%5.2f', 3.14159)", " 3.14"},
		{"SELECT PRINTF('%-10s|', 'abc')", "abc       |"},
		{"SELECT PRINTF('%05d', 42)", "00042"},
		{"SELECT PRINTF('%05d', -42)", "-0042"},
		{"SELECT PRINTF('%x', -1)", "ffffffffffffffff"},
		{"SELECT PRINTF('%+d', 42)", "+42"},
		{"SELECT PRINTF('%08.3f', 3.14159)", "0003.142"},
		{"SELECT PRINTF('%-8.2f|', 3.14159)", "3.14    |"},
		{"SELECT PRINTF('%.2s', 'hello')", "he"},
		{"SELECT PRINTF('%10.3s|', 'hello')", "       hel|"},
		{"SELECT PRINTF('%.0d', 0)", ""},
		{"SELECT PRINTF('%5.3d', 7)", "  007"},
		{"SELECT PRINTF('%6s|', 'hél')", "   hél|"},
		// Spec characters.
		{"SELECT PRINTF('%x', 255)", "ff"},
		{"SELECT PRINTF('%#x', 255)", "0xff"},
		{"SELECT PRINTF('%x', 0)", "0"},
		{"SELECT PRINTF('%o', 8)", "10"},
		{"SELECT PRINTF('%e', 12345.6789)", "1.234568e+04"},
		{"SELECT PRINTF('%g', 1234567.0)", "1.23457e+06"},
		{"SELECT PRINTF('%c', 65)", "A"},
		{"SELECT PRINTF('[%q]', CHAR(39))", "['']"},
		// Existing behavior that must not regress.
		{"SELECT PRINTF('hello %s', 'world')", "hello world"},
		{"SELECT PRINTF('%d apples', 3)", "3 apples"},
		{"SELECT PRINTF('%f', 1.5)", "1.500000"},
		{"SELECT PRINTF('%s %s', 'a', 'b')", "a b"},
		{"SELECT PRINTF('100%%')", "100%"},
		{"SELECT PRINTF('%%s', 'x')", "%s"},
		{"SELECT PRINTF('trailing %')", "trailing %"},
		{"SELECT PRINTF('%5')", "%5"},
		{"SELECT PRINTF('%z', 'x')", "%z"},
		{"SELECT PRINTF('%d %d', 1)", "1 "},
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
		got := res.Rows[0][0]
		gs, ok := got.(string)
		if !ok {
			t.Errorf("%s: want string result, got %T (%v)", tc.sql, got, got)
			continue
		}
		if gs != tc.want.(string) {
			t.Errorf("%s = %q, want %q", tc.sql, gs, tc.want.(string))
		}
	}
}
