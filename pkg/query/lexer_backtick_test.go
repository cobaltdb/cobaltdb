package query

import (
	"strings"
	"testing"
)

// Regression tests for the unterminated backtick-quoted identifier fix.
//
// readBacktickString previously returned the swallowed remainder of the input
// with no unterminated flag, so Tokenize("SELECT `abc FROM t WHERE x = 1")
// succeeded with a single identifier token "abc FROM t WHERE x = 1" instead of
// reporting an error. Unterminated ' and " literals already error; backtick
// identifiers must follow the same contract.

func TestTokenizeRejectsUnterminatedBacktickIdentifier(t *testing.T) {
	inputs := []string{
		"`",                              // lone backtick (previously: empty identifier token)
		"SELECT `abc",                    // remainder of input silently swallowed
		"SELECT `abc FROM t WHERE x = 1", // exact case from the round proof
		"SELECT * FROM t WHERE name = `abc",
	}
	for _, in := range inputs {
		tokens, err := Tokenize(in)
		if err == nil {
			t.Errorf("Tokenize(%q) = %v, want error for unterminated backtick identifier", in, tokens)
			continue
		}
		if !strings.Contains(err.Error(), "unterminated") && !strings.Contains(err.Error(), "illegal") {
			t.Errorf("Tokenize(%q) error = %v, want it to mention the unterminated/illegal token", in, err)
		}
	}
}

func TestTokenizeAcceptsTerminatedBacktickIdentifier(t *testing.T) {
	// Control: properly terminated backtick identifiers must keep working.
	if _, err := Tokenize("SELECT `weird col` FROM t"); err != nil {
		t.Fatalf("Tokenize rejected terminated backtick identifier: %v", err)
	}
	if _, err := Tokenize("SELECT `a b` FROM t WHERE `c d` = 1"); err != nil {
		t.Fatalf("Tokenize rejected terminated backtick identifiers: %v", err)
	}
}

func TestParseRejectsUnterminatedBacktickIdentifier(t *testing.T) {
	// The end-to-end parse path must reject it too, not just the lexer.
	if _, err := Parse("SELECT `abc FROM t"); err == nil {
		t.Fatal("Parse accepted SQL containing an unterminated backtick identifier")
	}
}
