package catalog

import "testing"

// TestMySQLDateFormatWeekSpecifiers pins the MySQL week-family DATE_FORMAT
// specifiers (%U %u %V %v %X %x).
//
// Regression: applyMySQLDateFormat handled %U/%u with t.ISOWeek() and had no
// case for %V/%v/%X/%x, so they fell through to the default branch and echoed
// the letter itself. ISO-8601 numbering is Monday-first with a 4-day rule and
// rolls across year boundaries, so it is wrong for BOTH documented week modes:
//
//	%U  WEEK() mode 0: Sunday-first, week 01 = week starting at the first
//	    Sunday of the year (WEEK('2008-02-20',0)=7 manual anchor; ISO says 8).
//	%u  WEEK() mode 1: Monday-first, week 01 = first week with >=4 days in
//	    the year (WEEK('2008-12-31',1)=53 manual anchor; ISO says 2009-W01).
//	%V/%X  mode 2: %U numbering with week-year semantics — week-00 days
//	    belong to the previous year's final week (YEARWEEK('1987-01-01')
//	    = 198652 manual anchor).
//	%v/%x  mode 3: ISO-8601 week / ISO week-year (Go time.ISOWeek).
func TestMySQLDateFormatWeekSpecifiers(t *testing.T) {
	c := newTestCatalog(t)

	cases := []struct {
		sql  string
		want string
	}{
		// %U: Sunday-first, documented WEEK(,0) anchors.
		{"SELECT DATE_FORMAT('2008-02-20','%U')", "07"},
		{"SELECT DATE_FORMAT('2008-12-31','%U')", "52"},
		{"SELECT DATE_FORMAT('2023-01-01','%U')", "01"}, // Sunday Jan 1 starts week 01
		{"SELECT DATE_FORMAT('2021-01-01','%U')", "00"}, // Friday Jan 1: first Sunday Jan 3
		{"SELECT DATE_FORMAT('2000-01-01','%U')", "00"}, // Saturday Jan 1 (YEARWEEK 199952)
		{"SELECT DATE_FORMAT('2024-06-03','%U')", "22"}, // Monday-start year, mid-year

		// %u: Monday-first, documented WEEK(,1) anchors and week-00 leads.
		{"SELECT DATE_FORMAT('2008-02-20','%u')", "08"},
		{"SELECT DATE_FORMAT('2008-12-31','%u')", "53"},
		{"SELECT DATE_FORMAT('2023-01-01','%u')", "00"}, // Sunday Jan 1: 1 day in opening week
		{"SELECT DATE_FORMAT('2021-01-01','%u')", "00"}, // Friday Jan 1: 3 days in opening week
		{"SELECT DATE_FORMAT('2024-12-30','%u')", "53"}, // Monday-start year end (ISO: 2025-W01)

		// %V/%X: Sunday-first week-year; week-00 days roll to the previous year.
		{"SELECT DATE_FORMAT('2009-01-01','%V')", "52"}, // Thursday Jan 1 -> 2008 week 52
		{"SELECT DATE_FORMAT('2009-01-01','%X')", "2008"},
		{"SELECT DATE_FORMAT('2024-12-30','%V')", "52"}, // year end does NOT roll forward
		{"SELECT DATE_FORMAT('2024-12-30','%X')", "2024"},

		// %v/%x: ISO-8601 (mode 3), including the year-boundary roll ISO does make.
		{"SELECT DATE_FORMAT('2008-02-20','%v')", "08"},
		{"SELECT DATE_FORMAT('2008-12-31','%v')", "01"}, // ISO 2009-W01
		{"SELECT DATE_FORMAT('2008-12-31','%x')", "2009"},

		// Control: non-week specifiers must stay untouched.
		{"SELECT DATE_FORMAT('2024-03-05 14:07:09','%Y-%m-%d')", "2024-03-05"},
		{"SELECT DATE_FORMAT('2024-03-05 14:07:09','%H:%i:%s')", "14:07:09"},
		{"SELECT DATE_FORMAT('2024-03-05','%j')", "065"},
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
