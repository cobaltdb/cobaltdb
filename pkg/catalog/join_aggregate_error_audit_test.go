package catalog

import (
	"strings"
	"testing"
)

func TestJoinAggregatesPropagateEvaluationErrors(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE a (id INTEGER PRIMARY KEY, grp INTEGER)", "CREATE TABLE b (id INTEGER PRIMARY KEY, ref INTEGER, val INTEGER)", "INSERT INTO a VALUES (1,1),(2,2),(3,3)", "INSERT INTO b VALUES (1,1,2),(2,1,3),(3,2,4),(4,3,NULL)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{"SELECT SUM(b.val/0) FROM a JOIN b ON a.id=b.ref GROUP BY a.grp", "SELECT SUM(b.val)/0 FROM a JOIN b ON a.id=b.ref GROUP BY a.grp", "SELECT SUM(b.val/0)+1 FROM a JOIN b ON a.id=b.ref GROUP BY a.grp", "SELECT COALESCE(SUM(b.val/0),0) FROM a JOIN b ON a.id=b.ref GROUP BY a.grp", "SELECT AVG(b.val/0) FROM a JOIN b ON a.id=b.ref GROUP BY a.grp", "SELECT SUM(b.val)/0 FROM a JOIN b ON a.id=b.ref "} {
		_, err := c.ExecuteQuery(sql)
		if err == nil || !strings.Contains(err.Error(), "division by zero") {
			t.Fatalf("error swallowed: %s: %v", sql, err)
		}
	}
	for _, tc := range []struct {
		sql  string
		want interface{}
	}{
		{"SELECT SUM(b.val)+1 FROM a JOIN b ON a.id=b.ref WHERE a.id=1 GROUP BY a.grp", float64(6)},
		{"SELECT SUM(b.val)/0 FROM a JOIN b ON a.id=b.ref WHERE a.id=3 GROUP BY a.grp", nil},
		{"SELECT CASE WHEN COUNT(*)>0 THEN SUM(b.val) ELSE 1/0 END FROM a JOIN b ON a.id=b.ref WHERE a.id=1 GROUP BY a.grp", float64(5)},
		{"SELECT SUM(b.val)/0 FROM a LEFT JOIN b ON a.id=b.ref AND b.id=99 WHERE a.id=1 GROUP BY a.grp", nil},
		{"SELECT COUNT(*) FROM a JOIN b ON a.id=b.ref AND a.id=99", float64(0)},
	} {
		r, err := c.ExecuteQuery(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if len(r.Rows) != 1 || len(r.Rows[0]) != 1 {
			t.Fatalf("scalar shape: %s: %v", tc.sql, r.Rows)
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
	r, err := c.ExecuteQuery("SELECT SUM(b.val)/0 FROM a JOIN b ON a.id=b.ref AND a.id=99 GROUP BY a.grp")
	if err != nil || len(r.Rows) != 0 {
		t.Fatalf("empty joined groups: %v %v", r, err)
	}
	for _, tc := range []struct {
		sql  string
		want []int64
	}{
		{"SELECT a.grp FROM a JOIN b ON a.id=b.ref GROUP BY a.grp HAVING COUNT(*)>1", []int64{1}},
		{"SELECT a.grp FROM a JOIN b ON a.id=b.ref GROUP BY a.grp ORDER BY COUNT(*) DESC, a.grp ASC", []int64{1, 2, 3}},
	} {
		r, err := c.ExecuteQuery(tc.sql)
		if err != nil {
			t.Fatalf("hidden COUNT(*): %s: %v", tc.sql, err)
		}
		if len(r.Rows) != len(tc.want) {
			t.Fatalf("hidden COUNT(*) shape: %s: %v", tc.sql, r.Rows)
		}
		for i, row := range r.Rows {
			if len(row) != 1 || row[0] != tc.want[i] {
				t.Fatalf("hidden COUNT(*): %s: %#v", tc.sql, r.Rows)
			}
		}
	}
}
