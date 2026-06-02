package catalog

import (
	"fmt"
	"testing"
)

func jcExec(t *testing.T, c *Catalog, sql string) {
	t.Helper()
	if _, err := c.ExecuteQuery(sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func jcScalar(t *testing.T, c *Catalog, sql string) string {
	t.Helper()
	r, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	if len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
		t.Fatalf("query %q returned no scalar", sql)
	}
	return fmt.Sprintf("%v", r.Rows[0][0])
}

// JSON_SET must preserve non-string scalar values (numbers, bools) rather than
// dropping them to an empty string.
func TestJSONSetPreservesScalarTypes(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	jcExec(t, c, "CREATE TABLE js (id INTEGER PRIMARY KEY, doc TEXT)")
	jcExec(t, c, `INSERT INTO js VALUES (1, '{"n":1}')`)

	if got := jcScalar(t, c, `SELECT JSON_EXTRACT(JSON_SET(doc, '$.n', 31), '$.n') FROM js`); got != "31" {
		t.Fatalf("JSON_SET numeric: $.n = %s, want 31", got)
	}
	if got := jcScalar(t, c, `SELECT JSON_TYPE(JSON_EXTRACT(JSON_SET(doc, '$.n', 31), '$.n')) FROM js`); got != "number" {
		t.Fatalf("JSON_SET numeric type = %s, want number", got)
	}
	if got := jcScalar(t, c, `SELECT JSON_EXTRACT(JSON_SET(doc, '$.b', true), '$.b') FROM js`); got != "true" {
		t.Fatalf("JSON_SET bool: $.b = %s, want true", got)
	}
	// String values are still stored as strings.
	if got := jcScalar(t, c, `SELECT JSON_EXTRACT(JSON_SET(doc, '$.s', 'hi'), '$.s') FROM js`); got != "hi" {
		t.Fatalf("JSON_SET string: $.s = %s, want hi", got)
	}
}

// A native value returned by JSON_EXTRACT (array, number) must be re-usable by
// JSON_ARRAY_LENGTH / JSON_TYPE — i.e. composed JSON calls work.
func TestJSONComposedExtract(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	jcExec(t, c, "CREATE TABLE jc (id INTEGER PRIMARY KEY, doc TEXT)")
	jcExec(t, c, `INSERT INTO jc VALUES (1, '{"age":30,"tags":["a","b","c"]}')`)

	if got := jcScalar(t, c, `SELECT JSON_ARRAY_LENGTH(JSON_EXTRACT(doc, '$.tags')) FROM jc`); got != "3" {
		t.Fatalf("JSON_ARRAY_LENGTH(JSON_EXTRACT tags) = %s, want 3", got)
	}
	if got := jcScalar(t, c, `SELECT JSON_TYPE(JSON_EXTRACT(doc, '$.age')) FROM jc`); got != "number" {
		t.Fatalf("JSON_TYPE(JSON_EXTRACT age) = %s, want number", got)
	}
	if got := jcScalar(t, c, `SELECT JSON_TYPE(JSON_EXTRACT(doc, '$.tags')) FROM jc`); got != "array" {
		t.Fatalf("JSON_TYPE(JSON_EXTRACT tags) = %s, want array", got)
	}
	// Direct document type still works (not double-encoded).
	if got := jcScalar(t, c, `SELECT JSON_TYPE(doc) FROM jc`); got != "object" {
		t.Fatalf("JSON_TYPE(doc) = %s, want object", got)
	}
}
