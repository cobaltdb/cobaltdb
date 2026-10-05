package catalog

import (
	"strings"
	"testing"
)

func TestGroupedHavingPropagatesEvaluationErrors(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE sample (id INTEGER PRIMARY KEY, grp INTEGER)", "INSERT INTO sample VALUES (1,1),(2,2)", "CREATE TABLE empty_sample (id INTEGER PRIMARY KEY, grp INTEGER)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		"SELECT grp, COUNT(*) FROM sample GROUP BY grp HAVING 1/0 = 1",
		"SELECT COUNT(*) FROM sample HAVING 1/0 = 1",
		"SELECT COUNT(*) FROM empty_sample HAVING 1/0 = 1",
	} {
		_, err := c.ExecuteQuery(sql)
		if err == nil || !strings.Contains(err.Error(), "division by zero") {
			t.Fatalf("HAVING error swallowed: %s; %v", sql, err)
		}
	}
	for _, tc := range []struct {
		sql   string
		count int
	}{
		{"SELECT grp, COUNT(*) FROM sample GROUP BY grp HAVING COUNT(*) > 0", 2},
		{"SELECT grp, COUNT(*) FROM sample GROUP BY grp HAVING COUNT(*) > 1", 0},
		{"SELECT grp, COUNT(*) FROM sample GROUP BY grp HAVING NULL", 0},
		{"SELECT grp, COUNT(*) FROM sample GROUP BY grp HAVING CASE WHEN COUNT(*) > 0 THEN TRUE ELSE 1/0 = 1 END", 2},
		{"SELECT grp, COUNT(*) FROM empty_sample GROUP BY grp HAVING 1/0 = 1", 0},
		{"SELECT COUNT(*) FROM empty_sample HAVING COUNT(*) = 0", 1},
		{"SELECT COUNT(*) FROM empty_sample HAVING COUNT(*) > 0", 0},
		{"SELECT COUNT(*) FROM empty_sample HAVING NULL", 0},
	} {
		r, err := c.ExecuteQuery(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if len(r.Rows) != tc.count {
			t.Fatalf("%s: got %v; want %d rows", tc.sql, r.Rows, tc.count)
		}
	}
}
