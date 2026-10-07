package catalog

import (
	"fmt"
	"reflect"
	"testing"
)

func TestWindowValueFunctionsRespectRowsFrame(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	for _, sql := range []string{"CREATE TABLE sample (id INTEGER PRIMARY KEY)", "INSERT INTO sample VALUES (1),(2),(3)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		expr, frame string
		want        []interface{}
	}{
		{"FIRST_VALUE(id)", "ROWS BETWEEN CURRENT ROW AND CURRENT ROW", []interface{}{int64(1), int64(2), int64(3)}},
		{"LAST_VALUE(id)", "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", []interface{}{int64(1), int64(2), int64(3)}},
		{"NTH_VALUE(id, 2)", "ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING", []interface{}{int64(2), int64(3), nil}},
		{"FIRST_VALUE(id)", "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW", []interface{}{int64(1), int64(1), int64(2)}},
		{"LAST_VALUE(id)", "ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING", []interface{}{int64(3), int64(3), nil}},
		{"NTH_VALUE(id, 2)", "ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING", []interface{}{nil, nil, int64(2)}},
		{"FIRST_VALUE(id)", "", []interface{}{int64(1), int64(1), int64(1)}},
		{"LAST_VALUE(id)", "", []interface{}{int64(1), int64(2), int64(3)}},
	}
	for _, tc := range cases {
		sql := "SELECT " + tc.expr + " OVER (ORDER BY id " + tc.frame + ") FROM sample ORDER BY id"
		r, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]interface{}, len(r.Rows))
		for i, row := range r.Rows {
			got[i] = row[0]
		}
		fmt.Printf("EXPECTED: %v; ACTUAL: %v (%s %s)\n", tc.want, got, tc.expr, tc.frame)
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("frame mismatch: %s", sql)
		}
	}
	for _, condition := range []string{"id = 1", "id = 0"} {
		r, err := c.ExecuteQuery("SELECT FIRST_VALUE(id) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM sample WHERE " + condition)
		if err != nil {
			t.Fatal(err)
		}
		if condition == "id = 1" && (len(r.Rows) != 1 || r.Rows[0][0] != int64(1)) {
			t.Fatal("single row mismatch")
		}
		if condition == "id = 0" && len(r.Rows) != 0 {
			t.Fatal("empty input mismatch")
		}
	}
	fmt.Println("FIX VERIFIED")
}
