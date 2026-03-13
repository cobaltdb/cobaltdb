package catalog

import (
	"testing"
)

// TestJSONQuoteEdgeCases tests JSONQuote with edge cases
func TestJSONQuoteEdgeCases(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", `""`},
		{"hello", `"hello"`},
		{"hello world", `"hello world"`},
		{"with\"quotes", `"with\"quotes"`},
		{"with\nnewline", `"with\nnewline"`},
		{"special: \t\r", `"special: \t\r"`},
	}

	for _, tc := range tests {
		result := JSONQuote(tc.input)
		if result != tc.expected {
			t.Errorf("JSONQuote(%q) = %q, expected %q", tc.input, result, tc.expected)
		}
	}
}

// TestJSONUnquoteEdgeCases tests JSONUnquote with edge cases
func TestJSONUnquoteEdgeCases(t *testing.T) {
	tests := []struct {
		input       string
		expected    string
		expectError bool
	}{
		{"", "", false},
		{`"hello"`, "hello", false},
		{`"hello world"`, "hello world", false},
		{`"with\"quotes"`, `with"quotes`, false},
		{`not a quoted string`, "", true},
		{`invalid json`, "", true},
		{`"`, "", true},
	}

	for _, tc := range tests {
		result, err := JSONUnquote(tc.input)
		if tc.expectError {
			if err == nil {
				t.Errorf("JSONUnquote(%q) expected error, got %q", tc.input, result)
			}
		} else {
			if err != nil {
				t.Errorf("JSONUnquote(%q) unexpected error: %v", tc.input, err)
			}
			if result != tc.expected {
				t.Errorf("JSONUnquote(%q) = %q, expected %q", tc.input, result, tc.expected)
			}
		}
	}
}

// TestIsValidJSONEdgeCases tests IsValidJSON with various inputs
func TestIsValidJSONEdgeCases(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"", false},
		{"null", true},
		{"true", true},
		{"false", true},
		{"123", true},
		{`"string"`, true},
		{"[]", true},
		{"{}", true},
		{"[1,2,3]", true},
		{`{"key":"value"}`, true},
		{"invalid", false},
		{"{invalid}", false},
		{"[1,2,", false},
	}

	for _, tc := range tests {
		result := IsValidJSON(tc.input)
		if result != tc.expected {
			t.Errorf("IsValidJSON(%q) = %v, expected %v", tc.input, result, tc.expected)
		}
	}
}

// TestRegexMatchEdgeCases tests RegexMatch with various patterns
func TestRegexMatchEdgeCases(t *testing.T) {
	tests := []struct {
		str         string
		pattern     string
		shouldMatch bool
		expectError bool
	}{
		{"hello", "^hello$", true, false},
		{"hello world", "^hello", true, false},
		{"world hello", "hello$", true, false},
		{"HELLO", "(?i)^hello$", true, false},
		{"123", "^[0-9]+$", true, false},
		{"abc", "^[0-9]+$", false, false},
		{"test", "[invalid", false, true},
	}

	for _, tc := range tests {
		match, err := RegexMatch(tc.str, tc.pattern)
		if tc.expectError {
			if err == nil {
				t.Errorf("RegexMatch(%q, %q) expected error", tc.str, tc.pattern)
			}
		} else {
			if err != nil {
				t.Errorf("RegexMatch(%q, %q) unexpected error: %v", tc.str, tc.pattern, err)
			}
			if match != tc.shouldMatch {
				t.Errorf("RegexMatch(%q, %q) = %v, expected %v", tc.str, tc.pattern, match, tc.shouldMatch)
			}
		}
	}
}

// TestRegexReplaceEdgeCases tests RegexReplace with various inputs
func TestRegexReplaceEdgeCases(t *testing.T) {
	tests := []struct {
		str         string
		pattern     string
		replacement string
		expected    string
		expectError bool
	}{
		{"hello world", "world", "universe", "hello universe", false},
		{"abc123def", "[0-9]+", "", "abcdef", false},
		{"foo bar baz", `\b\w+\b`, "X", "X X X", false},
		{"test", "[invalid", "", "", true},
	}

	for _, tc := range tests {
		result, err := RegexReplace(tc.str, tc.pattern, tc.replacement)
		if tc.expectError {
			if err == nil {
				t.Errorf("RegexReplace(%q, %q, %q) expected error", tc.str, tc.pattern, tc.replacement)
			}
		} else {
			if err != nil {
				t.Errorf("RegexReplace(%q, %q, %q) unexpected error: %v", tc.str, tc.pattern, tc.replacement, err)
			}
			if result != tc.expected {
				t.Errorf("RegexReplace(%q, %q, %q) = %q, expected %q", tc.str, tc.pattern, tc.replacement, result, tc.expected)
			}
		}
	}
}

// TestRegexExtractEdgeCases tests RegexExtract with various patterns
func TestRegexExtractEdgeCases(t *testing.T) {
	tests := []struct {
		str         string
		pattern     string
		expectMatch bool
		expectError bool
	}{
		{"hello world", `^\w+`, true, false},
		{"email@test.com", `\w+@\w+\.\w+`, true, false},
		{"no match here", "xyz", false, false},
		{"test", "[invalid", false, true},
	}

	for _, tc := range tests {
		result, err := RegexExtract(tc.str, tc.pattern)
		if tc.expectError {
			if err == nil {
				t.Errorf("RegexExtract(%q, %q) expected error", tc.str, tc.pattern)
			}
		} else {
			if err != nil {
				t.Errorf("RegexExtract(%q, %q) unexpected error: %v", tc.str, tc.pattern, err)
			}
			if tc.expectMatch && len(result) == 0 {
				t.Errorf("RegexExtract(%q, %q) expected matches, got none", tc.str, tc.pattern)
			}
			if !tc.expectMatch && len(result) > 0 {
				t.Errorf("RegexExtract(%q, %q) expected no matches, got %v", tc.str, tc.pattern, result)
			}
		}
	}
}
