package catalog

import (
	"strings"
	"testing"
)

func TestGroupedJoinWherePropagatesEvaluationErrors(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE a (id INTEGER PRIMARY KEY, grp INTEGER)", "CREATE TABLE b (id INTEGER PRIMARY KEY, ref INTEGER, val INTEGER)", "INSERT INTO a VALUES (1,1),(2,2),(3,3)", "INSERT INTO b VALUES (1,1,2),(2,1,3),(3,2,4),(4,3,NULL)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{"SELECT a.grp, COUNT(*) FROM a JOIN b ON a.id=b.ref WHERE b.val/0=1 GROUP BY a.grp", "SELECT COUNT(*) FROM a JOIN b ON a.id=b.ref WHERE b.val/0=1"} {
		_, err := c.ExecuteQuery(sql)
		if err == nil || !strings.Contains(err.Error(), "division by zero") {
			t.Fatalf("error swallowed: %s: %v", sql, err)
		}
	}
	for _, tc := range []struct {
		sql   string
		count int
	}{{"SELECT a.grp, COUNT(*) FROM a JOIN b ON a.id=b.ref WHERE b.val>0 GROUP BY a.grp", 2}, {"SELECT a.grp, COUNT(*) FROM a JOIN b ON a.id=b.ref WHERE b.val<0 GROUP BY a.grp", 0}, {"SELECT a.grp, COUNT(*) FROM a JOIN b ON a.id=b.ref WHERE NULL GROUP BY a.grp", 0}, {"SELECT a.grp, COUNT(*) FROM a JOIN b ON a.id=b.ref WHERE CASE WHEN b.val IS NULL THEN FALSE WHEN b.val>0 THEN TRUE ELSE 1/0=1 END GROUP BY a.grp", 2}, {"SELECT a.grp, COUNT(*) FROM a JOIN b ON a.id=b.ref AND a.id=99 WHERE b.val/0=1 GROUP BY a.grp", 0}, {"SELECT COUNT(*) FROM a JOIN b ON a.id=b.ref AND a.id=99 WHERE b.val/0=1", 1}, {"SELECT a.grp, COUNT(*) FROM a JOIN b ON a.id=b.ref AND a.id=3 WHERE b.val/0=1 GROUP BY a.grp", 0}} {
		r, err := c.ExecuteQuery(tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if len(r.Rows) != tc.count {
			t.Fatalf("%s: got %v want %d rows", tc.sql, r.Rows, tc.count)
		}
	}
}
