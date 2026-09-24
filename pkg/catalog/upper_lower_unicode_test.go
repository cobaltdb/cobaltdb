package catalog

import "testing"

// TestUpperLowerUnicode pins that the SQL UPPER()/LOWER() functions map
// non-ASCII cased characters. They route through util.ToUpperFast/ToLowerFast,
// which previously scanned for ASCII case only and left "ñ" unchanged.
func TestUpperLowerUnicode(t *testing.T) {
	c := newTestCatalog(t)
	cases := []struct{ sql, want string }{
		{"SELECT UPPER('ñ')", "Ñ"},
		{"SELECT LOWER('Ñ')", "ñ"},
		{"SELECT UPPER('ñoño')", "ÑOÑO"},
		{"SELECT LOWER('ÑOÑO')", "ñoño"},
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
		if got, ok := res.Rows[0][0].(string); !ok || got != tc.want {
			t.Errorf("%s = %v, want %q", tc.sql, res.Rows[0][0], tc.want)
		}
	}
}
