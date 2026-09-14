package query

import (
	"strings"
	"testing"
)

// Regression tests for the NUL-byte rejection fix. The lexer previously used
// ch==0 as its end-of-input sentinel, so a literal NUL (0x00) byte inside the
// SQL input was indistinguishable from end-of-input: Tokenize reported success
// and the remainder of the statement was silently truncated — fail-open, e.g.
// "SELECT * FROM t WHERE id = 1\x00 AND tenant = 5" parsed WITHOUT the tenant
// filter. MySQL and SQLite reject NUL bytes in statement text.

func TestTokenizeRejectsLiteralNulByte(t *testing.T) {
	inputs := []string{
		"SELECT * FROM t WHERE id = 1\x00 AND tenant = 5", // trailing predicate dropped pre-fix
		"SELECT 1\x00",                     // NUL as the very last input byte
		"\x00SELECT 1",                     // leading NUL
		"SELECT * FROM -- comment\x00\n t", // NUL inside a line comment
	}
	for _, in := range inputs {
		tokens, err := Tokenize(in)
		if err == nil {
			t.Errorf("Tokenize(%q) = %d tokens, want error for literal NUL byte", in, len(tokens))
			continue
		}
		if !strings.Contains(err.Error(), "NUL") && !strings.Contains(err.Error(), "illegal") {
			t.Errorf("Tokenize(%q) error = %v, want it to mention the NUL/illegal token", in, err)
		}
	}
}

func TestParseRejectsLiteralNulByte(t *testing.T) {
	// End-to-end: a NUL-containing statement must never parse into a Statement.
	if _, err := Parse("SELECT * FROM t WHERE id = 1\x00 AND tenant = 5"); err == nil {
		t.Fatal("Parse accepted SQL containing a literal NUL byte; trailing predicates would be silently dropped")
	}
}

func TestNulInsideStringStillFailsClosed(t *testing.T) {
	// A raw NUL inside a quoted string still fails closed (unterminated string
	// literal) — unchanged by the fix, pinned here as a boundary.
	if _, err := Tokenize("SELECT 'a\x00b'"); err == nil {
		t.Fatal("Tokenize accepted a raw NUL inside a string literal")
	}
}

func TestEscapedNulInStringStillWorks(t *testing.T) {
	// The backslash-zero ESCAPE sequence inside a string literal keeps working:
	// the token VALUE contains the NUL byte, the input stream has no raw NUL.
	tokens, err := Tokenize(`SELECT 'a\0b'`)
	if err != nil {
		t.Fatalf("Tokenize rejected backslash-zero escape: %v", err)
	}
	if len(tokens) < 2 || tokens[1].Type != TokenString || tokens[1].Literal != "a\x00b" {
		t.Fatalf("Tokenize produced wrong tokens for backslash-zero escape: %+v", tokens)
	}
	if _, err := Parse(`SELECT 'a\0b'`); err != nil {
		t.Fatalf("Parse rejected backslash-zero escape: %v", err)
	}
}
