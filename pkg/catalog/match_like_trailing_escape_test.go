package catalog

import "testing"

// TestMatchLikeSimpleTrailingEscape pins the engine-side reference semantics
// for a LIKE pattern that ENDS with the escape character: matchLikeSimple's
// escape case requires a following pattern rune, so a dangling escape falls
// through to the literal branch and matches itself. pkg/security's RLS
// likeToRegex must agree (pinned in rls_like_sweep_test.go) — a pattern like
// 'a\' (ESCAPE '\') matches exactly the value "a\" and nothing else.
func TestMatchLikeSimpleTrailingEscape(t *testing.T) {
	cases := []struct {
		s, pattern string
		escape     byte
		want       bool
	}{
		{"a\\", "a\\", '\\', true}, // dangling escape matches itself…
		{"a", "a\\", '\\', false},  // …and does not match the bare prefix
		{"a!", "a!", '!', true},    // same shape with a plain escape char
		{"a", "a!", '!', false},
		{"a\\", "a\\", 0, true}, // no-escape control: backslash is a literal
	}
	for _, tc := range cases {
		if got := matchLikeSimple(tc.s, tc.pattern, tc.escape); got != tc.want {
			t.Errorf("matchLikeSimple(%q, %q, %q) = %v, want %v",
				tc.s, tc.pattern, tc.escape, got, tc.want)
		}
	}
}
