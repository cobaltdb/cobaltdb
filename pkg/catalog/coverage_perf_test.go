package catalog

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func setupCovCatalog(t *testing.T) (*Catalog, func()) {
	t.Helper()
	b := storage.NewMemory()
	p := storage.NewBufferPool(4096, b)
	tr, _ := btree.NewBTree(p)
	c := New(tr, p, nil)
	return c, func() { p.Close() }
}

func covRun(t *testing.T, c *Catalog, sql string) {
	t.Helper()
	res, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	_ = res
}

func covQuery(t *testing.T, c *Catalog, sql string) *QueryResult {
	t.Helper()
	res, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	return res
}

func TestToFloat64Safe(t *testing.T) {
	tests := []struct {
		name   string
		val    interface{}
		want   float64
		wantOK bool
	}{
		{"int64", int64(42), 42, true},
		{"float64", float64(3.14), 3.14, true},
		{"int", int(7), 7, true},
		{"int32", int32(99), 99, true},
		{"string", "nope", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toFloat64Safe(tt.val)
			if ok != tt.wantOK {
				t.Errorf("ok: got %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Errorf("val: got %g, want %g", got, tt.want)
			}
		})
	}
}

func TestCountStarWithWhereCoverage(t *testing.T) {
	c, cleanup := setupCovCatalog(t)
	defer cleanup()

	covRun(t, c, "CREATE TABLE cov_count (id INTEGER PRIMARY KEY, status TEXT, val INTEGER)")
	covRun(t, c, "INSERT INTO cov_count VALUES (1, 'active', 10)")
	covRun(t, c, "INSERT INTO cov_count VALUES (2, 'inactive', 20)")
	covRun(t, c, "INSERT INTO cov_count VALUES (3, 'active', 30)")

	// COUNT(*) without WHERE — fast path
	res := covQuery(t, c, "SELECT COUNT(*) FROM cov_count")
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(3) {
		t.Errorf("COUNT(*): got %v, want 3", res.Rows)
	}

	// COUNT(*) with WHERE
	res = covQuery(t, c, "SELECT COUNT(*) FROM cov_count WHERE status = 'active'")
	if len(res.Rows) != 1 || res.Rows[0][0] != int64(2) {
		t.Errorf("COUNT(*) WHERE: got %v, want 2", res.Rows)
	}

	// COUNT(*) with alias
	res = covQuery(t, c, "SELECT COUNT(*) AS total FROM cov_count")
	if res.Columns[0] != "total" {
		t.Errorf("alias: got %s, want total", res.Columns[0])
	}
}

func TestHavingCoveragePaths(t *testing.T) {
	c, cleanup := setupCovCatalog(t)
	defer cleanup()

	covRun(t, c, "CREATE TABLE cov_hav (id INTEGER PRIMARY KEY, dept TEXT, salary INTEGER)")
	for i := 1; i <= 20; i++ {
		dept := "A"
		if i > 10 {
			dept = "B"
		}
		covRun(t, c, fmt.Sprintf("INSERT INTO cov_hav VALUES (%d, '%s', %d)", i, dept, i*100))
	}

	// HAVING with COUNT
	res := covQuery(t, c, "SELECT dept, COUNT(*) as c FROM cov_hav GROUP BY dept HAVING COUNT(*) >= 10")
	if len(res.Rows) != 2 {
		t.Errorf("HAVING COUNT: got %d rows, want 2", len(res.Rows))
	}

	// HAVING with SUM
	res = covQuery(t, c, "SELECT dept, SUM(salary) as total FROM cov_hav GROUP BY dept HAVING SUM(salary) > 10000")
	if len(res.Rows) != 1 {
		t.Errorf("HAVING SUM: got %d rows, want 1", len(res.Rows))
	}
}

func TestGroupByOrderByCoveragePaths(t *testing.T) {
	c, cleanup := setupCovCatalog(t)
	defer cleanup()

	covRun(t, c, "CREATE TABLE cov_gbo (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	covRun(t, c, "INSERT INTO cov_gbo VALUES (1, 'B', 30)")
	covRun(t, c, "INSERT INTO cov_gbo VALUES (2, 'A', 10)")
	covRun(t, c, "INSERT INTO cov_gbo VALUES (3, 'B', 20)")
	covRun(t, c, "INSERT INTO cov_gbo VALUES (4, 'A', 40)")

	// GROUP BY with ORDER BY
	res := covQuery(t, c, "SELECT cat, SUM(val) as total FROM cov_gbo GROUP BY cat ORDER BY total ASC")
	if len(res.Rows) != 2 {
		t.Errorf("got %d rows, want 2", len(res.Rows))
	}

	// GROUP BY with LIMIT
	res = covQuery(t, c, "SELECT cat, COUNT(*) FROM cov_gbo GROUP BY cat LIMIT 1")
	if len(res.Rows) != 1 {
		t.Errorf("got %d rows, want 1", len(res.Rows))
	}
}

func TestAggFastPathMinMaxCoverage(t *testing.T) {
	c, cleanup := setupCovCatalog(t)
	defer cleanup()

	covRun(t, c, "CREATE TABLE cov_mm (id INTEGER PRIMARY KEY, val INTEGER)")
	covRun(t, c, "INSERT INTO cov_mm VALUES (1, 100)")
	covRun(t, c, "INSERT INTO cov_mm VALUES (2, 200)")
	covRun(t, c, "INSERT INTO cov_mm VALUES (3, 300)")

	// MIN
	res := covQuery(t, c, "SELECT MIN(val) FROM cov_mm")
	if res.Rows[0][0] != int64(100) {
		t.Errorf("MIN: got %v, want 100", res.Rows[0][0])
	}

	// MAX
	res = covQuery(t, c, "SELECT MAX(val) FROM cov_mm")
	if res.Rows[0][0] != int64(300) {
		t.Errorf("MAX: got %v, want 300", res.Rows[0][0])
	}

	// COUNT(column)
	res = covQuery(t, c, "SELECT COUNT(val) FROM cov_mm")
	if res.Rows[0][0] != int64(3) {
		t.Errorf("COUNT(col): got %v, want 3", res.Rows[0][0])
	}

	// Multi-aggregate
	res = covQuery(t, c, "SELECT COUNT(*), SUM(val), AVG(val), MIN(val), MAX(val) FROM cov_mm")
	if len(res.Rows) != 1 || len(res.Rows[0]) != 5 {
		t.Errorf("multi agg: got %v", res.Rows)
	}
}

func TestDeletedRowSkippingCoverage(t *testing.T) {
	c, cleanup := setupCovCatalog(t)
	defer cleanup()

	covRun(t, c, "CREATE TABLE cov_del (id INTEGER PRIMARY KEY, val INTEGER)")
	covRun(t, c, "INSERT INTO cov_del VALUES (1, 10)")
	covRun(t, c, "INSERT INTO cov_del VALUES (2, 20)")
	covRun(t, c, "INSERT INTO cov_del VALUES (3, 30)")
	covRun(t, c, "DELETE FROM cov_del WHERE id = 2")

	res := covQuery(t, c, "SELECT COUNT(*) FROM cov_del")
	if res.Rows[0][0] != int64(2) {
		t.Errorf("COUNT after delete: got %v, want 2", res.Rows[0][0])
	}

	res = covQuery(t, c, "SELECT SUM(val) FROM cov_del")
	if res.Rows[0][0] != float64(40) {
		t.Errorf("SUM after delete: got %v, want 40", res.Rows[0][0])
	}
}
