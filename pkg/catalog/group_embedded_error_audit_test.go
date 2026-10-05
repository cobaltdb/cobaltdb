package catalog

import (
	"strings"
	"testing"
)

func TestEmbeddedGroupAggregatesPropagateEvaluationErrors(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE sample (id INTEGER PRIMARY KEY, grp INTEGER, val INTEGER)", "INSERT INTO sample VALUES (1,1,2),(2,1,3),(3,2,NULL)", "CREATE TABLE empty_sample (id INTEGER PRIMARY KEY, grp INTEGER, val INTEGER)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{
		"SELECT SUM(val)/0 FROM sample GROUP BY grp",
		"SELECT SUM(val/0)+1 FROM sample GROUP BY grp",
		"SELECT COALESCE(SUM(val/0),0) FROM sample GROUP BY grp",
		"SELECT COUNT(*)/0 FROM empty_sample",
		"SELECT COUNT(*)/0 FROM sample WHERE id = 0",
	} {
		_, err := c.ExecuteQuery(sql)
		if err == nil || !strings.Contains(err.Error(), "division by zero") {
			t.Fatalf("aggregate error swallowed: %s; %v", sql, err)
		}
	}
	for _, tc := range []struct {
		sql  string
		want interface{}
	}{
		{"SELECT SUM(val)+1 FROM sample WHERE grp = 1 GROUP BY grp", float64(6)},
		{"SELECT SUM(val)/0 FROM sample WHERE grp = 2 GROUP BY grp", nil},
		{"SELECT CASE WHEN COUNT(*) > 0 THEN SUM(val) ELSE 1/0 END FROM sample WHERE grp = 1 GROUP BY grp", float64(5)},
		{"SELECT COUNT(*)+1 FROM empty_sample", float64(1)},
		{"SELECT COALESCE(SUM(val),0)+1 FROM empty_sample", float64(1)},
		{"SELECT SUM(val)/0 FROM empty_sample", nil},
	} {
		r, err := c.ExecuteQuery(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if len(r.Rows) != 1 || len(r.Rows[0]) != 1 {
			t.Fatalf("%s: expected one scalar row, got %#v", tc.sql, r.Rows)
		}
		matched := r.Rows[0][0] == nil && tc.want == nil
		if tc.want != nil {
			got, ok := toFloat64(r.Rows[0][0])
			matched = ok && got == tc.want.(float64)
		}
		if !matched {
			t.Fatalf("%s: got %#v want %#v", tc.sql, r.Rows, tc.want)
		}
	}
	r, err := c.ExecuteQuery("SELECT SUM(val)/0 FROM empty_sample GROUP BY grp")
	if err != nil || len(r.Rows) != 0 {
		t.Fatalf("empty grouped query: %v %v", r, err)
	}
}
