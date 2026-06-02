package catalog

import (
	"math"
	"testing"
)

// Vector functions must accept JSON-array string literals (and string-typed
// values), not only already-parsed arrays.
func TestVectorFunctionsAcceptStringLiterals(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)

	scalar := func(sql string) float64 {
		t.Helper()
		r, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
		if len(r.Rows) == 0 || len(r.Rows[0]) == 0 {
			t.Fatalf("no scalar from %q", sql)
		}
		f, ok := toFloat64(r.Rows[0][0])
		if !ok {
			t.Fatalf("result of %q is not numeric: %v", sql, r.Rows[0][0])
		}
		return f
	}
	approx := func(got, want float64) bool { return math.Abs(got-want) < 1e-9 }

	if v := scalar(`SELECT COSINE_SIMILARITY('[1,0]', '[1,0]')`); !approx(v, 1) {
		t.Fatalf("cosine identical = %v, want 1", v)
	}
	if v := scalar(`SELECT COSINE_SIMILARITY('[1,0]', '[0,1]')`); !approx(v, 0) {
		t.Fatalf("cosine orthogonal = %v, want 0", v)
	}
	if v := scalar(`SELECT COSINE_SIMILARITY('[1,2]', '[2,4]')`); !approx(v, 1) {
		t.Fatalf("cosine parallel = %v, want ~1", v)
	}
	if v := scalar(`SELECT L2_DISTANCE('[0,0]', '[3,4]')`); !approx(v, 5) {
		t.Fatalf("L2 distance = %v, want 5", v)
	}
	if v := scalar(`SELECT INNER_PRODUCT('[1,2,3]', '[4,5,6]')`); !approx(v, 32) {
		t.Fatalf("inner product = %v, want 32", v)
	}

	// A string-typed column value also parses.
	if _, err := c.ExecuteQuery("CREATE TABLE vecs (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery(`INSERT INTO vecs VALUES (1, '[3,4]')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if v := scalar(`SELECT L2_DISTANCE(v, '[0,0]') FROM vecs WHERE id = 1`); !approx(v, 5) {
		t.Fatalf("L2 from column = %v, want 5", v)
	}
}
