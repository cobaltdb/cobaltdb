package catalog

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestLPADRPADCharacterSemantics pins the character-based (rune) MySQL
// semantics of LPAD/RPAD.
//
// Regression: evalStringLPad/evalStringRPad padded and truncated on BYTE
// offsets while every other string function in catalog_eval_string.go
// (LEFT, RIGHT, SUBSTR, INSTR, LOCATE) is character-based. Byte truncation
// split multibyte runes and emitted invalid UTF-8 to the wire
// (RPAD('héé',2,'x') -> "h\xc3"), and byte padding produced wrong character
// counts (LPAD('é',3,'ab') -> "bé" instead of "abé").
func TestLPADRPADCharacterSemantics(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want interface{} // string result, or nil for SQL NULL
	}{
		// Multibyte contract cases (fail under byte-based padding).
		{"SELECT LPAD('hé', 1, 'x')", "h"},
		{"SELECT LPAD('é', 3, 'ab')", "abé"},
		{"SELECT RPAD('héé', 2, 'x')", "hé"},
		{"SELECT RPAD('é', 4, 'x')", "éxxx"},
		// ASCII behavior that must stay identical.
		{"SELECT LPAD('hi', 5, '??')", "???hi"},
		{"SELECT RPAD('hi', 5, '??')", "hi???"},
		{"SELECT LPAD('hi', 1, '??')", "h"},
		{"SELECT RPAD('hi', 1, '??')", "h"},
		// Pre-existing edge contracts: zero length truncates to empty,
		// empty pad truncates but never pads.
		{"SELECT LPAD('hello', 0, 'x')", ""},
		{"SELECT RPAD('hello', 0, 'x')", ""},
		{"SELECT LPAD('hello', 3, '')", "hel"},
		{"SELECT RPAD('hello', 3, '')", "hel"},
		{"SELECT LPAD('abc', 5, '')", "abc"},
		{"SELECT RPAD('abc', 5, '')", "abc"},
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
		if tc.want == nil {
			if got != nil {
				t.Errorf("%s = %v, want NULL", tc.sql, got)
			}
			continue
		}
		gs, ok := got.(string)
		if !ok {
			t.Errorf("%s: want string result, got %T (%v)", tc.sql, got, got)
			continue
		}
		if gs != tc.want.(string) {
			t.Errorf("%s = %q, want %q", tc.sql, gs, tc.want.(string))
			continue
		}
		if !utf8.ValidString(gs) {
			t.Errorf("%s produced invalid UTF-8: %q", tc.sql, gs)
		}
	}
}

// TestLPADRPADNullAndArgCount pins the NULL and arity contracts that surround
// the padding logic (MySQL: any NULL argument yields NULL; fewer than 3
// arguments is an error).
func TestLPADRPADNullAndArgCount(t *testing.T) {
	c := newTestCatalog(t)

	for _, sql := range []string{
		"SELECT LPAD(NULL, 5, 'x')",
		"SELECT RPAD(NULL, 5, 'x')",
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

	for _, sql := range []string{
		"SELECT LPAD('a', 5)",
		"SELECT RPAD('a', 5)",
	} {
		if _, err := c.ExecuteQuery(sql); err == nil {
			t.Errorf("%s: want arity error, got nil", sql)
		} else if !strings.Contains(err.Error(), "requires 3 arguments") {
			t.Errorf("%s: want \"requires 3 arguments\" error, got: %v", sql, err)
		}
	}
}

// TestLPADPadRemainderDirection pins the MySQL rule for the FINAL PARTIAL pad
// copy: the gap is filled with whole pad copies plus the FIRST
// (gap mod padlen) characters of padstr — never padstr's tail.
//
// Regression: evalStringLPad cycled whole pad copies and kept the last `len`
// runes of the over-padded result, so the partial copy contributed its TAIL
// characters (LPAD('a',2,'xyz') = "za"; MySQL: "xa"). RPAD was already
// correct: keeping the first runes of str+pad+pad... left-truncates the
// trailing partial copy.
func TestLPADPadRemainderDirection(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want interface{}
	}{
		{"SELECT LPAD('a', 2, 'xyz')", "xa"},     // gap=1 < padlen: first pad char
		{"SELECT LPAD('ab', 4, 'cde')", "cdab"},  // gap=2 < padlen: first two pad chars + str
		{"SELECT LPAD('a', 3, 'héllo')", "héa"},  // multibyte pad, partial = leading runes
		{"SELECT LPAD('hi', 5, '??')", "???hi"},  // documented MySQL example
		{"SELECT LPAD('hi', 6, '??')", "????hi"}, // exact multiple, no partial copy
		{"SELECT RPAD('a', 2, 'xyz')", "ax"},     // RPAD keeps the head (already MySQL-correct)
		{"SELECT RPAD('hi', 5, '??')", "hi???"},  // RPAD guard
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
			continue
		}
		if !utf8.ValidString(gs) {
			t.Errorf("%s produced invalid UTF-8: %q", tc.sql, gs)
		}
	}
}
