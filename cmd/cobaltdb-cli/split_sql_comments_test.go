package main

import "testing"

// TestSplitSQLStatementsKeepsCommentsIntact pins that a ';' inside a SQL
// comment (-- line or /* block */) does not split statements, and that
// comment text no longer feeds the string/keyword state machines (an
// apostrophe in a line comment used to flip inString until end of input).
func TestSplitSQLStatementsKeepsCommentsIntact(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "semicolon inside line comment",
			input:    "SELECT 1 -- note; ignored\n",
			expected: []string{"SELECT 1 -- note; ignored\n"},
		},
		{
			name:     "semicolon inside block comment",
			input:    "SELECT 1 /* a;b */",
			expected: []string{"SELECT 1 /* a;b */"},
		},
		{
			name:     "two statements with comment between",
			input:    "SELECT 1;\n-- setup; stuff\nSELECT 2;",
			expected: []string{"SELECT 1;", "\n-- setup; stuff\nSELECT 2;"},
		},
		{
			name:     "multiline block comment with semicolons",
			input:    "SELECT 1 /* first;\nsecond; */ WHERE 1;",
			expected: []string{"SELECT 1 /* first;\nsecond; */ WHERE 1;"},
		},
		{
			name:     "apostrophe in line comment does not open string state",
			input:    "SELECT 1 -- don't; more\nSELECT 2;",
			expected: []string{"SELECT 1 -- don't; more\nSELECT 2;"},
		},
		{
			// Control: string-awareness is unchanged.
			name:     "semicolon inside string still no split",
			input:    "SELECT 'a;b';",
			expected: []string{"SELECT 'a;b';"},
		},
		{
			// Control: plain splitting unchanged.
			name:     "plain split",
			input:    "SELECT 1; SELECT 2;",
			expected: []string{"SELECT 1;", " SELECT 2;"},
		},
		{
			// Control: division is not a comment opener.
			name:     "division untouched",
			input:    "SELECT 1/2;",
			expected: []string{"SELECT 1/2;"},
		},
	}
	for _, test := range tests {
		result := splitSQLStatements(test.input)
		if len(result) != len(test.expected) {
			t.Errorf("%s: splitSQLStatements(%q) len = %d, expected %d", test.name, test.input, len(result), len(test.expected))
			continue
		}
		for i := range result {
			if result[i] != test.expected[i] {
				t.Errorf("%s: splitSQLStatements(%q)[%d] = %q, expected %q", test.name, test.input, i, result[i], test.expected[i])
			}
		}
	}
}
