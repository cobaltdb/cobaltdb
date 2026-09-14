package query

import (
	"strings"
	"testing"
)

// Regression tests for scientific-notation exponent backtracking (MySQL rule:
// an exponent forms only when an optional sign and at least one digit follow
// the e/E; otherwise the e/E belongs to the following identifier).
// readNumber previously consumed e/E unconditionally, so "SELECT 1exp" lexed
// as Number("1e") + Identifier("xp") and failed with "invalid number: 1e",
// where MySQL yields 1 with the implicit alias exp.

func TestTokenizeExponentBacktracksWithoutDigits(t *testing.T) {
	cases := []struct {
		sql  string
		nums []string
		idnt []string
	}{
		{"SELECT 1exp", []string{"1"}, []string{"exp"}},
		{"SELECT 1entries", []string{"1"}, []string{"entries"}},
		{"SELECT 1e", []string{"1"}, []string{"e"}},
		{"SELECT 1E", []string{"1"}, []string{"E"}},
	}
	for _, tc := range cases {
		tokens, err := Tokenize(tc.sql)
		if err != nil {
			t.Errorf("Tokenize(%q) error: %v", tc.sql, err)
			continue
		}
		var nums, idents []string
		seenSelect := false
		for _, tok := range tokens {
			switch {
			case !seenSelect:
				if tok.Type != TokenIdentifier && tok.Type != TokenNumber && strings.EqualFold(tok.Literal, "SELECT") {
					seenSelect = true
				}
			case tok.Type == TokenNumber:
				nums = append(nums, tok.Literal)
			case tok.Type == TokenIdentifier:
				idents = append(idents, tok.Literal)
			}
		}
		if strings.Join(nums, ",") != strings.Join(tc.nums, ",") || strings.Join(idents, ",") != strings.Join(tc.idnt, ",") {
			t.Errorf("Tokenize(%q) numbers=%q identifiers=%q; want %q + %q", tc.sql, nums, idents, tc.nums, tc.idnt)
		}
	}
}

func TestParseExponentBacktrackForms(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1exp",
		"SELECT 1entries",
		"SELECT 1e",
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) error: %v — MySQL treats this as 1 with an implicit alias", sql, err)
		}
	}
}

func TestScientificNotationUnchanged(t *testing.T) {
	for _, sql := range []string{
		"SELECT 1e5",
		"SELECT 1E+5",
		"SELECT 1e-5",
		"SELECT 1.5e2",
		"SELECT 1.e5",
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("Parse(%q) error: %v", sql, err)
		}
	}
}

func TestIncompleteExponentPlusStaysError(t *testing.T) {
	// An exponent still requires digits after the optional sign; "1e+" is not
	// a number and not a complete expression.
	if _, err := Parse("SELECT 1e+"); err == nil {
		t.Fatal("Parse(\"SELECT 1e+\") unexpectedly accepted; malformed exponent forms must stay rejected")
	}
}
