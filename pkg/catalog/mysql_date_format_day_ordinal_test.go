package catalog

import "testing"

// TestMySQLDateFormatDayOrdinal pins the DATE_FORMAT %D specifier: MySQL
// renders the day of month with its English ordinal suffix
// ("0th, 1st, 2nd, 3rd, ..., 31st").
//
// Regression: applyMySQLDateFormat had no case for 'D', so %D fell through
// to the default branch and echoed the literal letter —
// SELECT DATE_FORMAT('2024-03-05','%D') returned "D" instead of "5th",
// and '%D %M %Y' rendered as "D March 2024".
func TestMySQLDateFormatDayOrdinal(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want string
	}{
		// Every suffix class, including the 11th-13th teens that take
		// "th" despite ending in 1-3.
		{"SELECT DATE_FORMAT('2024-03-01','%D')", "1st"},
		{"SELECT DATE_FORMAT('2024-03-02','%D')", "2nd"},
		{"SELECT DATE_FORMAT('2024-03-03','%D')", "3rd"},
		{"SELECT DATE_FORMAT('2024-03-04','%D')", "4th"},
		{"SELECT DATE_FORMAT('2024-03-11','%D')", "11th"},
		{"SELECT DATE_FORMAT('2024-03-12','%D')", "12th"},
		{"SELECT DATE_FORMAT('2024-03-13','%D')", "13th"},
		{"SELECT DATE_FORMAT('2024-03-21','%D')", "21st"},
		{"SELECT DATE_FORMAT('2024-03-22','%D')", "22nd"},
		{"SELECT DATE_FORMAT('2024-03-23','%D')", "23rd"},
		{"SELECT DATE_FORMAT('2024-03-30','%D')", "30th"},
		{"SELECT DATE_FORMAT('2024-03-31','%D')", "31st"},

		// Composed with other specifiers.
		{"SELECT DATE_FORMAT('2024-03-05','%D %M %Y')", "5th March 2024"},

		// Control: neighboring day specifiers must stay untouched, and the
		// week-family fix must not regress.
		{"SELECT DATE_FORMAT('2024-03-05','%d')", "05"},
		{"SELECT DATE_FORMAT('2024-03-05','%e')", "5"},
		{"SELECT DATE_FORMAT('2008-02-20','%U')", "07"},
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
		got, ok := res.Rows[0][0].(string)
		if !ok {
			t.Errorf("%s: want string result, got %T (%v)", tc.sql, res.Rows[0][0], res.Rows[0][0])
			continue
		}
		if got != tc.want {
			t.Errorf("%s = %q, want %q", tc.sql, got, tc.want)
		}
	}
}
