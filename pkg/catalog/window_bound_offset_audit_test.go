package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"math"
	"reflect"
	"testing"
)

func TestWindowOffsetsUseBoundArguments(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE sample (id INTEGER PRIMARY KEY)", "INSERT INTO sample VALUES (1),(2),(3)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		expr string
		args []interface{}
		want []interface{}
	}{
		{"LAG(id, 2)", nil, []interface{}{nil, nil, int64(1)}},
		{"LAG(id, ?)", []interface{}{int64(2)}, []interface{}{nil, nil, int64(1)}},
		{"LEAD(id, ?)", []interface{}{int64(2)}, []interface{}{int64(3), nil, nil}},
		{"LAG(id, ?, id + 10)", []interface{}{int64(2)}, []interface{}{int64(11), int64(12), int64(1)}},
		{"LEAD(id, ?, id + 10)", []interface{}{int64(2)}, []interface{}{int64(3), int64(12), int64(13)}},
		{"LAG(id, ?)", []interface{}{int64(0)}, []interface{}{int64(1), int64(2), int64(3)}},
		{"LEAD(id, ?)", []interface{}{int64(0)}, []interface{}{int64(1), int64(2), int64(3)}},
		{"LAG(id, ?, 9)", []interface{}{int64(math.MaxInt64)}, []interface{}{float64(9), float64(9), float64(9)}},
		{"LEAD(id, ?, 9)", []interface{}{int64(math.MaxInt64)}, []interface{}{float64(9), float64(9), float64(9)}},
		{"LAG(id)", nil, []interface{}{nil, int64(1), int64(2)}},
		{"LEAD(id)", nil, []interface{}{int64(2), int64(3), nil}},
		{"LAG(id, ? + 1)", []interface{}{int64(1)}, []interface{}{nil, nil, int64(1)}},
	}
	for _, tc := range cases {
		stmt, err := query.Parse("SELECT " + tc.expr + " OVER (ORDER BY id) FROM sample ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		_, rows, err := c.Select(stmt.(*query.SelectStmt), tc.args)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]interface{}, len(rows))
		for i, row := range rows {
			got[i] = row[0]
		}
		fmt.Printf("EXPECTED: %v; ACTUAL: %v (%s)\n", tc.want, got, tc.expr)
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("offset mismatch: %s; got %#v want %#v", tc.expr, got, tc.want)
		}
	}
	for _, where := range []string{"id = 0", "id = 1"} {
		stmt, err := query.Parse("SELECT LAG(id, ?) OVER (ORDER BY id) FROM sample WHERE " + where)
		if err != nil {
			t.Fatal(err)
		}
		_, rows, err := c.Select(stmt.(*query.SelectStmt), []interface{}{int64(2)})
		if err != nil {
			t.Fatal(err)
		}
		if where == "id = 0" && len(rows) != 0 || where == "id = 1" && (len(rows) != 1 || rows[0][0] != nil) {
			t.Fatal("empty/single-row edge")
		}
	}
	fmt.Println("FIX VERIFIED")
}
