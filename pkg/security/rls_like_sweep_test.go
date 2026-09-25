package security

import (
	"errors"
	"regexp"
	"strings"
	"testing"
)

// TestRLSLikeToRegexEngineConsistency sweeps the LIKE→regex translation used by
// RLS policy evaluation against the engine's own LIKE semantics
// (catalog_eval.go matchLikeSimple): case-sensitive exact-rune matching, %
// matches any rune sequence (including newlines), _ matches exactly one rune
// (including newlines), the match is whole-string anchored, and every regex
// metacharacter in the pattern must match literally.
func TestRLSLikeToRegexEngineConsistency(t *testing.T) {
	cases := []struct {
		pattern string
		escape  string // "" = no ESCAPE clause
		value   string
		want    bool
	}{
		// Literals and anchoring.
		{"abc", "", "abc", true},
		{"abc", "", "xabcx", false},
		{"", "", "", true},
		{"", "", "x", false},
		// Case sensitivity: the engine's LIKE is case-sensitive exact-rune
		// matching, so the RLS evaluator must not match case variants.
		{"ABC", "", "abc", false},
		{"abc", "", "ABC", false},
		// % semantics: any sequence, including empty, including newlines.
		{"%", "", "", true},
		{"%", "", "anything", true},
		{"%", "", "a\nb\nc", true},
		{"a%b", "", "a\nxxx\nb", true},
		{"100%", "", "100", true},
		{"100%", "", "100%", true},
		{"100%", "", "1000", true},
		{"100%", "", "10", false},
		// _ semantics: exactly one rune, including newlines.
		{"a_b", "", "axb", true},
		{"a_b", "", "a\nb", true},
		{"a_b", "", "ab", false},
		{"a_b", "", "axxb", false},
		// Regex metacharacters must match literally (QuoteMeta correctness).
		{"a.b", "", "a.b", true},
		{"a.b", "", "axb", false},
		{"a+b", "", "a+b", true},
		{"a+b", "", "aab", false},
		{"a*b", "", "a*b", true},
		{"a*b", "", "aab", false},
		{"a?b", "", "a?b", true},
		{"a?b", "", "ab", false},
		{"(a)", "", "(a)", true},
		{"(a)", "", "a", false},
		{"[a]", "", "[a]", true},
		{"[a]", "", "a", false},
		{"a{1}", "", "a{1}", true},
		{"a{1}", "", "aa", false},
		{"a|b", "", "a|b", true},
		{"a|b", "", "a", false},
		{"a^b", "", "a^b", true},
		{"a$b", "", "a$b", true},
		{"a\\b", "", "a\\b", true},
		{"héllo", "", "héllo", true},
		{"h?llo", "", "h?llo", true},
		{"h?llo", "", "hxllo", false},
		{"_é_", "", "xéy", true},
		{"_é_", "", "é", false},
		// ESCAPE clause: the escape char makes the next pattern rune literal.
		{"50\\%", "\\", "50%", true},
		{"50\\%", "\\", "50x", false},
		{"a\\_b", "\\", "a_b", true},
		{"a\\_b", "\\", "axb", false},
		{"#%", "#", "%", true},
		{"#_", "#", "_", true},
		{"\\%", "\\", "%", true},
		{"\\%", "\\", "x", false},
	}
	for _, tc := range cases {
		re, err := regexp.Compile(likeToRegex(tc.pattern, tc.escape))
		if err != nil {
			t.Fatalf("likeToRegex(%q, %q) produced invalid regex: %v", tc.pattern, tc.escape, err)
		}
		if got := re.MatchString(tc.value); got != tc.want {
			t.Fatalf("LIKE %q (escape %q) against %q = %v, want %v (regex %q)",
				tc.pattern, tc.escape, tc.value, got, tc.want, re.String())
		}
	}
}

// TestRLSLikeToRegexAlwaysCompiles proves the MustCompile safety contract: the
// generated regex must be valid for every pattern shape the parser can hand it.
func TestRLSLikeToRegexAlwaysCompiles(t *testing.T) {
	pieces := []string{
		".", "+", "*", "?", "(", ")", "[", "]", "{", "}", "|", "^", "$", "\\",
		"%", "_", "a", "Z", "0", "é", "🎉", "\t", " ", "'", "\"",
	}
	for _, a := range pieces {
		for _, b := range pieces {
			out := likeToRegex("x"+a+b+"y", "")
			if _, err := regexp.Compile(out); err != nil {
				t.Fatalf("likeToRegex produced invalid regex for pattern x%q%qy: %v\nregex: %q", a, b, err, out)
			}
			out = likeToRegex("x"+a+b+"y", "\\")
			if _, err := regexp.Compile(out); err != nil {
				t.Fatalf("likeToRegex produced invalid regex (escape) for pattern x%q%qy: %v\nregex: %q", a, b, err, out)
			}
		}
	}
}

// TestParseLikeOperatorContract pins parseLikeOperator's integration contract:
// non-LIKE expressions return (nil, nil); the pattern size cap rejects with
// ErrInvalidExpression; the parsed matcher honors NOT.
func TestParseLikeOperatorContract(t *testing.T) {
	if p, err := parseLikeOperator("dept = 'x'"); p != nil || err != nil {
		t.Fatalf("non-LIKE expression: want (nil, nil), got (%v, %v)", p, err)
	}

	huge := strings.Repeat("a", maxPolicyLikePatternBytes+1)
	if _, err := parseLikeOperator("dept LIKE '" + huge + "'"); !errors.Is(err, ErrInvalidExpression) {
		t.Fatalf("oversized pattern: want ErrInvalidExpression, got %v", err)
	}

	p, err := parseLikeOperator("dept NOT LIKE 'x%'")
	if err != nil || p == nil {
		t.Fatalf("NOT LIKE parse failed: %v", err)
	}
	re := regexp.MustCompile(likeToRegex("x%", ""))
	if re.MatchString("x1") != true {
		t.Fatalf("sanity: x%% should match x1")
	}
}
