package catalog

import (
	"reflect"
	"testing"
)

func TestGroupedOrderByHonorsNullPlacement(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE sample (id INTEGER PRIMARY KEY, grp INTEGER)", "INSERT INTO sample VALUES (1,NULL),(2,2),(3,1)", "CREATE TABLE empty_sample (id INTEGER PRIMARY KEY, grp INTEGER)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		order string
		want  []interface{}
	}{
		{"ASC", []interface{}{int64(1), int64(2), nil}},
		{"DESC", []interface{}{nil, int64(2), int64(1)}},
		{"ASC NULLS FIRST", []interface{}{nil, int64(1), int64(2)}},
		{"ASC NULLS LAST", []interface{}{int64(1), int64(2), nil}},
		{"DESC NULLS FIRST", []interface{}{nil, int64(2), int64(1)}},
		{"DESC NULLS LAST", []interface{}{int64(2), int64(1), nil}},
	} {
		for _, sql := range []string{
			"SELECT grp FROM sample ORDER BY grp " + tc.order,
			"SELECT grp FROM sample GROUP BY grp ORDER BY grp " + tc.order,
			"SELECT grp AS group_value FROM sample GROUP BY grp ORDER BY group_value " + tc.order,
			"SELECT grp FROM sample GROUP BY grp ORDER BY 1 " + tc.order,
			"SELECT MIN(grp) AS group_value FROM sample GROUP BY grp ORDER BY group_value " + tc.order,
		} {
			r, err := c.ExecuteQuery(sql)
			if err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			got := make([]interface{}, len(r.Rows))
			for i, row := range r.Rows {
				got[i] = row[0]
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("%s: got %#v want %#v", sql, got, tc.want)
			}
		}
	}
	r, err := c.ExecuteQuery("SELECT grp FROM sample GROUP BY grp ORDER BY COUNT(*) ASC, grp ASC NULLS LAST")
	if err != nil || !reflect.DeepEqual(r.Rows, [][]interface{}{{int64(1)}, {int64(2)}, {nil}}) {
		t.Fatalf("tied primary keys: %v %v", r, err)
	}
	for _, tc := range []struct {
		sql   string
		count int
	}{
		{"SELECT grp FROM empty_sample GROUP BY grp ORDER BY grp DESC NULLS LAST", 0},
		{"SELECT grp FROM sample WHERE id = 1 GROUP BY grp ORDER BY grp ASC NULLS LAST", 1},
	} {
		r, err := c.ExecuteQuery(tc.sql)
		if err != nil || len(r.Rows) != tc.count {
			t.Fatalf("empty/single edge %s: %v %v", tc.sql, r, err)
		}
	}
}
