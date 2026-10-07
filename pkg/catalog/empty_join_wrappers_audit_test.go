package catalog

import (
	"fmt"
	"strings"
	"testing"
)

func TestEmptyJoinAggregateWrappers(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE a(id INTEGER PRIMARY KEY)", "CREATE TABLE b(id INTEGER PRIMARY KEY)"} {
		if _, e := c.ExecuteQuery(sql); e != nil {
			t.Fatal(e)
		}
	}
	for _, tc := range []struct {
		expr string
		want interface{}
	}{{"COUNT(*)+1", float64(1)}, {"COALESCE(SUM(a.id),0)+1", float64(1)}, {"SUM(a.id)+1", nil}, {"COUNT(*)", float64(0)}} {
		r, e := c.ExecuteQuery("SELECT " + tc.expr + " FROM a JOIN b ON a.id=b.id")
		if e != nil || len(r.Rows) != 1 {
			t.Fatalf("%s: %v %v", tc.expr, r, e)
		}
		if tc.want == nil {
			if r.Rows[0][0] != nil {
				t.Fatal(r.Rows)
			}
		} else {
			v, ok := toFloat64(r.Rows[0][0])
			if !ok || v != tc.want.(float64) {
				t.Fatal(r.Rows)
			}
		}
	}
	if _, e := c.ExecuteQuery("SELECT COUNT(*)/0 FROM a JOIN b ON a.id=b.id"); e == nil || !strings.Contains(e.Error(), "division by zero") {
		t.Fatal(e)
	}
	r, e := c.ExecuteQuery("SELECT COUNT(*)+1 FROM a JOIN b ON a.id=b.id GROUP BY a.id")
	if e != nil || len(r.Rows) != 0 {
		t.Fatal(r, e)
	}
	for _, sql := range []string{"INSERT INTO a VALUES(1)", "INSERT INTO b VALUES(1)"} {
		if _, e := c.ExecuteQuery(sql); e != nil {
			t.Fatal(e)
		}
	}
	r, e = c.ExecuteQuery("SELECT COUNT(*)+1 FROM a JOIN b ON a.id=b.id")
	if e != nil || len(r.Rows) != 1 || fmt.Sprint(r.Rows[0][0]) != "2" {
		t.Fatal(r, e)
	}
}
