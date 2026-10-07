package catalog

import (
	"fmt"
	"testing"
)

func TestNestedJSONExtractAcceptsNativeDocuments(t *testing.T) {
	for _, tc := range []struct {
		doc        interface{}
		path, want string
	}{{[]interface{}{float64(1), float64(2)}, "$[0]", "1"}, {map[string]interface{}{"a": "hello", "b": true}, "$.a", "hello"}, {map[string]interface{}{"a": "hello", "b": true}, "$.b", "true"}, {float64(3), "$", "3"}, {`{"a":1}`, "$.a", "1"}, {[]interface{}{}, "$[0]", "<nil>"}} {
		v, e := evaluateJSONFunction("JSON_EXTRACT", []interface{}{tc.doc, tc.path})
		if e != nil || fmt.Sprint(v) != tc.want {
			t.Fatalf("%+v: %v %v", tc, v, e)
		}
	}
	c := newCTEResourceTestCatalog(t)
	r, e := c.ExecuteQuery(`SELECT JSON_EXTRACT(JSON_EXTRACT('{"a":[1,2]}','$.a'),'$[0]')`)
	if e != nil || len(r.Rows) != 1 || fmt.Sprint(r.Rows[0][0]) != "1" {
		t.Fatal(r, e)
	}
}
