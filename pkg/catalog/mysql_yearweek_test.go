package catalog

import "testing"

// TestMySQLYearWeek pins the YEARWEEK(date[, mode]) builtin: MySQL returns
// year*100 + week with week-year semantics — the result year may differ from
// the date's year for the first and last week of the year, so WEEK()'s
// week 00 never appears.
//
// Regression: YEARWEEK was entirely missing from the MySQL compat layer
// (SELECT YEARWEEK('1987-01-01') failed with "unknown function: YEARWEEK").
// The implementation maps the WEEK() mode table under forced week-year
// semantics, which collapses the eight modes pairwise: {0,2} Sunday-first
// week 1 at the first Sunday (shared with DATE_FORMAT %V/%X), {1,3}
// ISO-8601, {4,6} Sunday-first first-4-day week, {5,7} Monday-first
// first-Monday week. The 2023-01-01 mode-5/7 cases additionally guard the
// week-start anchor direction: Jan 1 2023 is a Sunday, the Monday-started
// week containing it opened in 2022, and its week number must come from the
// previous year's final week (202252), not from a forward-anchored grid.
func TestMySQLYearWeek(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want interface{} // int64 result, string result, or nil for SQL NULL
	}{
		// ---- default mode 0 (Sunday-first, first-Sunday week 1) ----
		{"SELECT YEARWEEK('1987-01-01')", int64(198652)}, // manual anchor: week-00 rolls to 1986
		{"SELECT YEARWEEK('2000-01-01')", int64(199952)}, // manual anchor: rolls to 1999
		{"SELECT YEARWEEK('2008-02-20')", int64(200807)}, // WEEK('2008-02-20',0)=7 manual anchor
		{"SELECT YEARWEEK('2008-02-20', 0)", int64(200807)},
		{"SELECT YEARWEEK('2008-02-20', 2)", int64(200807)},
		{"SELECT YEARWEEK('2024-12-30')", int64(202452)},
		{"SELECT YEARWEEK('2023-12-31')", int64(202353)}, // 2023 opens on a Sunday: 53 weeks

		// ---- modes 1 and 3 (ISO-8601 under YEARWEEK) ----
		{"SELECT YEARWEEK('1987-01-01', 1)", int64(198701)}, // ISO W1 1987 contains Thu Jan 1
		{"SELECT YEARWEEK('2021-01-01', 1)", int64(202053)}, // ISO 2020-W53
		{"SELECT YEARWEEK('2024-12-30', 1)", int64(202501)}, // ISO rolls forward
		{"SELECT YEARWEEK('2008-12-31', 1)", int64(200901)}, // ISO 2009-W01
		{"SELECT YEARWEEK('2008-02-20', 3)", int64(200808)}, // ISO W8

		// ---- modes 4 and 6 (Sunday-first, 4-or-more-days week 1) ----
		{"SELECT YEARWEEK('2008-02-20', 4)", int64(200808)},
		{"SELECT YEARWEEK('2008-02-20', 6)", int64(200808)},
		{"SELECT YEARWEEK('2023-12-31', 4)", int64(202401)}, // Sunday-week Dec 31-Jan 6 has 6 days in 2024
		{"SELECT YEARWEEK('2023-12-31', 6)", int64(202401)},

		// ---- modes 5 and 7 (Monday-first, first-Monday week 1) ----
		{"SELECT YEARWEEK('2008-02-20', 5)", int64(200807)},
		{"SELECT YEARWEEK('2008-02-20', 7)", int64(200807)},
		{"SELECT YEARWEEK('2023-12-31', 5)", int64(202352)}, // Monday-week Dec 25-31 = week 52
		{"SELECT YEARWEEK('2023-12-31', 7)", int64(202352)},
		{"SELECT YEARWEEK('2023-12-25', 5)", int64(202352)},
		{"SELECT YEARWEEK('2023-01-02', 5)", int64(202301)}, // first Monday of 2023 opens week 1
		// Sunday Jan 1 2023: the Monday-week containing it opened in 2022, so
		// the result rolls to 2022's final week instead of reporting week 1.
		{"SELECT YEARWEEK('2023-01-01', 5)", int64(202252)},
		{"SELECT YEARWEEK('2023-01-01', 7)", int64(202252)},

		// ---- NULL / invalid handling ----
		{"SELECT YEARWEEK(NULL)", nil},
		{"SELECT YEARWEEK('not-a-date')", nil},
		{"SELECT YEARWEEK('2024-01-01', 9)", nil}, // mode outside 0..7
		{"SELECT YEARWEEK('2024-01-01', NULL)", nil},

		// ---- controls: prior-round fixes must not regress ----
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
