package catalog

import (
	"strings"
	"testing"
)

// TestMySQLBase64 pins the MySQL TO_BASE64/FROM_BASE64 builtins.
//
// Regression: TO_BASE64 returned Go's plain StdEncoding output without
// MySQL's line wrapping (the manual: base-64 encoded strings contain a
// newline after every 76 characters), and FROM_BASE64 rejected any input
// containing whitespace — including the newlines TO_BASE64 itself embeds,
// so the documented round-trip failed. FROM_BASE64 now ignores space, tab,
// CR, LF, form feed, and vertical tab while decoding and still yields NULL
// for an invalid base-64 string.
func TestMySQLBase64(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want interface{} // string result, or nil for SQL NULL
	}{
		// ---- TO_BASE64: MySQL 76-character line wrapping ----
		{"SELECT TO_BASE64(REPEAT('a', 60))",
			strings.Repeat("YWFh", 19) + "\n" + "YWFh"}, // 80 chars: break after 76
		{"SELECT TO_BASE64(REPEAT('a', 58))",
			strings.Repeat("YWFh", 19) + "\n" + "YQ=="}, // break before the final padded group
		{"SELECT TO_BASE64(REPEAT('a', 57))",
			strings.Repeat("YWFh", 19)}, // exactly 76 chars: no trailing newline
		{"SELECT TO_BASE64(REPEAT('a', 120))",
			strings.Repeat("YWFh", 19) + "\n" + strings.Repeat("YWFh", 19) + "\n" + strings.Repeat("YWFh", 2)}, // two breaks: 76+76+8
		{"SELECT TO_BASE64('abc')", "YWJj"},
		{"SELECT TO_BASE64('????')", "Pz8/Pw=="}, // '/' and padding stay standard-alphabet
		{"SELECT TO_BASE64(NULL)", nil},

		// ---- FROM_BASE64: whitespace ignored, invalid -> NULL ----
		{"SELECT FROM_BASE64('YWJj ZGVm')", "abcdef"},
		{"SELECT FROM_BASE64('YWJj\tZGVm')", "abcdef"},
		{"SELECT FROM_BASE64('YWJj\r\nZGVm')", "abcdef"},
		{"SELECT FROM_BASE64('!!!')", nil},
		{"SELECT FROM_BASE64(NULL)", nil},
		{"SELECT FROM_BASE64('YWJj')", "abc"},

		// ---- round trip through the wrapped encoding ----
		{"SELECT FROM_BASE64(TO_BASE64(REPEAT('x', 100)))", strings.Repeat("x", 100)},
		{"SELECT FROM_BASE64(TO_BASE64(REPEAT('a', 60)))", strings.Repeat("a", 60)},
	}

	for _, tc := range cases {
		res, err := c.ExecuteQuery(tc.sql)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.sql, err)
			continue
		}
		if len(res.Rows) != 1 || len(res.Rows[0]) != 1 {
			t.Errorf("%s: want 1x1 result, got %v", tc.sql, res.Rows)
			continue
		}
		got := res.Rows[0][0]
		switch want := tc.want.(type) {
		case nil:
			if got != nil {
				t.Errorf("%s = %v, want NULL", tc.sql, got)
			}
		case string:
			g, ok := got.(string)
			if !ok {
				t.Errorf("%s: want string result, got %T (%v)", tc.sql, got, got)
				continue
			}
			if g != want {
				t.Errorf("%s = %q, want %q", tc.sql, g, want)
			}
		}
	}
}
