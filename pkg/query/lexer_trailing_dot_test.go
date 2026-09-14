package query

import (
	"testing"
)

// Regression tests for trailing-dot numeric literals (MySQL grammar: "1." == 1.0).
// readNumber previously never consumed a trailing dot, so "1." lexed as
// Number("1") + Dot and MySQL-valid statements like "SELECT 1." failed to parse.

func TestTokenizeTrailingDotNumberIsSingleNumberToken(t *testing.T) {
	tokens, err := Tokenize("SELECT 1.")
	if err != nil {
		t.Fatalf("Tokenize error: %v", err)
	}
	var numbers []string
	dots := 0
	for _, tok := range tokens {
		if tok.Type == TokenNumber {
			numbers = append(numbers, tok.Literal)
		}
		if tok.Type == TokenDot {
			dots++
		}
	}
	if len(numbers) != 1 || numbers[0] != "1." || dots != 0 {
		t.Fatalf("number tokens = %q, Dot tokens = %d; want [\"1.\"] and 0 dots", numbers, dots)
	}
}

func TestParseTrailingDotNumber(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1.",
		"SELECT * FROM t WHERE x = 1.",
		"SELECT 1.e5",
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) error: %v", sql, err)
		}
	}
}

func TestMalformedDotFormsStillError(t *testing.T) {
	// The fix consumes exactly one trailing dot; malformed sequences must stay
	// errors (they were errors before the fix too).
	for _, sql := range []string{
		"SELECT 1.5.6",
		"SELECT 1..2",
	} {
		if _, err := Parse(sql); err == nil {
			t.Errorf("Parse(%q) unexpectedly accepted; malformed decimal forms must stay rejected", sql)
		}
	}
}

func TestFractionalAndExponentFormsUnchanged(t *testing.T) {
	// The fractional branch consumes the dot before the trailing-dot check can
	// run; ordinary decimals and exponents must be unchanged.
	for _, sql := range []string{"SELECT 1.5", "SELECT 1.5e2", "SELECT .5", "SELECT 1"} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) error: %v", sql, err)
		}
	}
}
