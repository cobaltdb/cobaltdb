package catalog

import (
	"math"
	"testing"
)

func TestVarianceConstantFiniteInputs(t *testing.T) {
	for _, fn := range []string{"VAR_POP", "VAR_SAMP", "VARIANCE", "STDDEV_POP", "STDDEV_SAMP", "STDDEV", "STD"} {
		for _, values := range [][]interface{}{{2.0, 2.0}, {1e308, 1e308}, {-1e308, -1e308}, {1e308, 1e308, 1e308}, {nil, 1e308, 1e308}} {
			actual := computeStdevVar(values, fn)
			if actual != float64(0) {
				t.Fatalf("%s values=%v: %v", fn, values, actual)
			}
		}
	}
	for _, values := range [][]interface{}{nil, {nil, nil}} {
		if got := computeStdevVar(values, "VAR_POP"); got != nil {
			t.Fatal(got)
		}
	}
	if got := computeStdevVar([]interface{}{1e308}, "VAR_SAMP"); got != nil {
		t.Fatal(got)
	}
	if got := computeStdevVar([]interface{}{1e308}, "VAR_POP"); got != float64(0) {
		t.Fatal(got)
	}
	if got := computeStdevVar([]interface{}{1e308, -1e308, 1e308}, "VAR_POP").(float64); !math.IsInf(got, 1) {
		t.Fatalf("true overflow: %v", got)
	}
	if got := computeStdevVar([]interface{}{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}, "VAR_POP").(float64); math.Abs(got-4) > 1e-12 {
		t.Fatalf("ordinary variance: %v", got)
	}
	c, cleanup := setupEvalTestCatalog(t)
	defer cleanup()
	for _, sql := range []string{"CREATE TABLE evidence_variance (id INTEGER PRIMARY KEY, grp INTEGER, v REAL)", "INSERT INTO evidence_variance VALUES (1,1,1e308),(2,1,1e308)"} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{"SELECT VAR_POP(v), STDDEV_POP(v) FROM evidence_variance GROUP BY grp", "SELECT VAR_SAMP(v), STDDEV_SAMP(v) FROM evidence_variance"} {
		r, err := c.ExecuteQuery(sql)
		if err != nil || len(r.Rows) != 1 || r.Rows[0][0] != float64(0) || r.Rows[0][1] != float64(0) {
			t.Fatalf("%s: result=%v error=%v", sql, r, err)
		}
	}
}
