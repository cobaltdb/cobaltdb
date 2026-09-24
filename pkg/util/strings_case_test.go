package util

import (
	"strings"
	"testing"
)

// TestToUpperToLowerFastMatchesStrings pins the contract that ToUpperFast and
// ToLowerFast are exact equivalents of strings.ToUpper and strings.ToLower.
//
// Regression: both helpers scanned for ASCII case only, so a string
// containing only non-ASCII cased characters (e.g. "ñ") was returned
// unchanged even though strings.ToUpper("ñ") = "Ñ" — and the SQL
// UPPER()/LOWER() functions route through them (catalog_eval_string.go).
func TestToUpperToLowerFastMatchesStrings(t *testing.T) {
	inputs := []string{
		"ñ", "Ñ", "ß", "ǅ", "Ǆ", "İ", "ı",
		"Ñoño", "STRASSE", "straße",
		"日本語", "café", "CAFÉ", "abc", "ABC", "aBc123",
		"", "already UPPER", "already lower",
	}
	for _, s := range inputs {
		if got, want := ToUpperFast(s), strings.ToUpper(s); got != want {
			t.Errorf("ToUpperFast(%q) = %q, want %q", s, got, want)
		}
		if got, want := ToLowerFast(s), strings.ToLower(s); got != want {
			t.Errorf("ToLowerFast(%q) = %q, want %q", s, got, want)
		}
	}
}
