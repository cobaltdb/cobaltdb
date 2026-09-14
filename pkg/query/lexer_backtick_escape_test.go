package query

import (
	"strings"
	"testing"
)

// Regression tests for MySQL-style doubled-backtick escapes in backtick-quoted
// identifiers. readBacktickString previously terminated at the FIRST backtick,
// so `a``b` lexed as two identifiers ("a", "b") instead of one identifier
// containing a literal backtick ("a`b") — silently changing SELECT-list shapes
// (e.g. SELECT `a``b` FROM t returned two columns instead of one).

func selectListIdentifiers(t *testing.T, sql string) []string {
	t.Helper()
	tokens, err := Tokenize(sql)
	if err != nil {
		t.Fatalf("Tokenize(%q) error: %v", sql, err)
	}
	var out []string
	seenSelect := false
	for _, tok := range tokens {
		isKeyword := tok.Type != TokenIdentifier
		switch {
		case !seenSelect:
			if isKeyword && strings.EqualFold(tok.Literal, "SELECT") {
				seenSelect = true
			}
		case isKeyword && strings.EqualFold(tok.Literal, "FROM"):
			return out
		case tok.Type == TokenIdentifier:
			out = append(out, tok.Literal)
		}
	}
	return out
}

func TestTokenizeUnescapesDoubledBacktickIdentifier(t *testing.T) {
	cases := []struct {
		sql  string
		want []string
	}{
		{"SELECT `a``b` FROM t", []string{"a`b"}},
		{"SELECT `a``b``c` FROM t", []string{"a`b`c"}},
		{"SELECT `weird``col name` FROM t", []string{"weird`col name"}},
	}
	for _, tc := range cases {
		got := selectListIdentifiers(t, tc.sql)
		if len(got) != len(tc.want) {
			t.Fatalf("Tokenize(%q) select list = %q, want %q", tc.sql, got, tc.want)
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("Tokenize(%q) select list = %q, want %q", tc.sql, got, tc.want)
			}
		}
	}
}

func TestParseSingleColumnForDoubledBacktickIdentifier(t *testing.T) {
	stmt, err := Parse("SELECT `a``b` FROM t")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Parse returned %T, want *SelectStmt", stmt)
	}
	if len(sel.Columns) != 1 {
		t.Fatalf("Parse produced %d select columns, want 1", len(sel.Columns))
	}
}

func TestUnterminatedAfterDoubledBacktickStillErrors(t *testing.T) {
	// Escape pairs are consumed atomically; input that ends inside or right
	// after an unterminated quoted identifier must still be rejected (the
	// round-1 unterminated contract). Note `a```b is VALID (identifier "a`"
	// followed by identifier b) and therefore deliberately not listed here.
	for _, in := range []string{"`a``b", "`a``"} {
		if _, err := Tokenize(in); err == nil {
			t.Errorf("Tokenize(%q) unexpectedly accepted; want unterminated-identifier error", in)
		}
	}
}
