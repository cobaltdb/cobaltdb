package catalog

import "testing"

// TestMySQLWeek pins the WEEK(date[, mode]) builtin: MySQL returns the week
// number under the 0..7 mode table, default mode 0.
//
// Regression: WEEK was entirely missing from the MySQL compat layer
// (SELECT WEEK('2008-02-20') failed with "unknown function: WEEK"; the only
// "WEEK" hits were the INTERVAL date-add dispatch). Unlike YEARWEEK, WEEK
// does not roll the year: modes 0/1/4/5 report week 00 for days before
// week 1, and modes 2/3/6/7 use week-year numbering (range 1-53), rolling
// into the neighbouring year where the mode's week-1 rule puts them there.
func TestMySQLWeek(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want interface{} // int64 result, string result, or nil for SQL NULL
	}{
		// ---- default mode 0 and explicit modes 0/2 (Sunday-first) ----
		{"SELECT WEEK('2008-02-20')", int64(7)},    // manual anchor
		{"SELECT WEEK('2008-02-20', 0)", int64(7)}, // manual anchor
		{"SELECT WEEK('2008-12-31')", int64(52)},   // manual anchor: default mode 0
		{"SELECT WEEK('2021-01-01')", int64(0)},    // Friday Jan 1: first Sunday is Jan 3
		{"SELECT WEEK('2008-02-20', 2)", int64(7)},
		{"SELECT WEEK('2021-01-01', 2)", int64(52)}, // week-00 day rolls to 2020's final week
		{"SELECT WEEK('2024-12-30', 2)", int64(52)}, // no forward roll at year end

		// ---- modes 1 and 3 (Monday-first) ----
		{"SELECT WEEK('2008-02-20', 1)", int64(8)},  // manual anchor
		{"SELECT WEEK('2008-12-31', 1)", int64(53)}, // manual anchor
		{"SELECT WEEK('2008-12-31', 3)", int64(1)},  // ISO 2009-W01
		{"SELECT WEEK('2021-01-01', 3)", int64(53)}, // ISO 2020-W53

		// ---- modes 4 and 6 (Sunday-first, 4-or-more-days week 1) ----
		{"SELECT WEEK('2008-02-20', 4)", int64(8)},
		{"SELECT WEEK('2021-01-01', 4)", int64(0)},  // Sun-week Dec 27-Jan 2: 2 days in 2021
		{"SELECT WEEK('2024-12-30', 4)", int64(53)}, // stays in 2024's numbering
		{"SELECT WEEK('2023-12-31', 6)", int64(1)},  // rolling variant: 2024-W01

		// ---- modes 5 and 7 (Monday-first, first-Monday week 1) ----
		{"SELECT WEEK('2008-02-20', 5)", int64(7)},
		{"SELECT WEEK('2023-01-01', 5)", int64(0)},  // Sun Jan 1: first Monday is Jan 2
		{"SELECT WEEK('2023-12-31', 5)", int64(52)}, // Monday-week Dec 25-31
		{"SELECT WEEK('2023-01-01', 7)", int64(52)}, // rolling variant: 2022's final week
		{"SELECT WEEK('2023-12-31', 7)", int64(52)},

		// ---- NULL / invalid handling ----
		{"SELECT WEEK(NULL)", nil},
		{"SELECT WEEK('not-a-date')", nil},
		{"SELECT WEEK('2024-01-01', 9)", nil}, // mode outside 0..7
		{"SELECT WEEK('2024-01-01', NULL)", nil},

		// ---- controls: prior-round fixes must not regress ----
		{"SELECT YEARWEEK('1987-01-01')", int64(198652)},
		{"SELECT DATE_FORMAT('2008-02-20','%U')", "07"},
		{"SELECT DATE_FORMAT('2024-03-05','%D')", "5th"},
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
		case int64:
			g, ok := got.(int64)
			if !ok {
				t.Errorf("%s: want int64 result, got %T (%v)", tc.sql, got, got)
				continue
			}
			if g != want {
				t.Errorf("%s = %d, want %d", tc.sql, g, want)
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
