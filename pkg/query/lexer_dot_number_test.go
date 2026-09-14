package query

import (
	"testing"
)

// Regression tests for leading-dot numeric literals (MySQL grammar: ".5" == 0.5).
// The lexer previously emitted Dot + Number("5"), so MySQL-valid statements
// like "SELECT .5" or "SELECT * FROM t WHERE x = .5" failed to parse.

func TestTokenizeLeadingDotNumberIsSingleNumberToken(t *testing.T) {
	tokens, err := Tokenize("SELECT .5")
	if err != nil {
		t.Fatalf("Tokenize error: %v", err)
	}
	var numbers []string
	for _, tok := range tokens {
		if tok.Type == TokenNumber {
			numbers = append(numbers, tok.Literal)
		}
	}
	if len(numbers) != 1 || numbers[0] != ".5" {
		t.Fatalf("number tokens = %q, want [\".5\"]", numbers)
	}
}

func TestParseLeadingDotNumber(t *testing.T) {
	for _, sql := range []string{
		"SELECT .5",
		"SELECT .25, .5e2",
		"SELECT * FROM t WHERE x = .5",
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) error: %v", sql, err)
		}
	}
}

func TestQualifiedReferenceStillDot(t *testing.T) {
	// The dot before an identifier must remain a Dot token.
	cases := []struct {
		sql  string
		want int
	}{
		{"SELECT t.col FROM t", 1},
		{"SELECT a . b FROM t", 1},
	}
	for _, tc := range cases {
		tokens, err := Tokenize(tc.sql)
		if err != nil {
			t.Fatalf("Tokenize(%q) error: %v", tc.sql, err)
		}
		dots := 0
		for _, tok := range tokens {
			if tok.Type == TokenDot {
				dots++
			}
		}
		if dots != tc.want {
			t.Errorf("Tokenize(%q) Dot tokens = %d, want %d", tc.sql, dots, tc.want)
		}
	}
	// A lone "." is still a Dot token followed by EOF.
	tokens, err := Tokenize(".")
	if err != nil {
		t.Fatalf("Tokenize(\".\") error: %v", err)
	}
	if len(tokens) != 2 || tokens[0].Type != TokenDot || tokens[1].Type != TokenEOF {
		t.Fatalf("Tokenize(\".\") = %+v, want [Dot, EOF]", tokens)
	}
}

func TestNumericLiteralFormsUnchanged(t *testing.T) {
	// Neighbouring numeric forms keep working (hex goes through the parser's
	// base-0 ParseInt path; exponents through ParseFloat).
	for _, sql := range []string{"SELECT 0.5", "SELECT 0xFF", "SELECT 1.5e-3"} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) error: %v", sql, err)
		}
	}
}
