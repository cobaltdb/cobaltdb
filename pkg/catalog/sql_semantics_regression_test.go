package catalog

import (
	"fmt"
	"testing"
)

// These tests lock in tricky SQL semantics that are easy to regress: NULL-aware
// three-valued logic, NULL handling in aggregates and ordering, window
// functions, and correlated subqueries. They were verified correct against the
// current engine; this file guards them.

func ssExec(t *testing.T, c *Catalog, sql string) [][]interface{} {
	t.Helper()
	r, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return r.Rows
}

func ssScalar(t *testing.T, c *Catalog, sql string) string {
	t.Helper()
	rows := ssExec(t, c, sql)
	if len(rows) == 0 || len(rows[0]) == 0 {
		t.Fatalf("query %q returned no scalar", sql)
	}
	return fmt.Sprintf("%v", rows[0][0])
}

// NOT IN with a NULL in the list/subquery yields UNKNOWN (no rows), per SQL
// three-valued logic — a trap many engines get wrong.
func TestSQLSemantics_NotInNull(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	ssExec(t, c, "CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
	ssExec(t, c, "INSERT INTO t VALUES (1,1),(2,2),(3,3)")
	if rows := ssExec(t, c, "SELECT id FROM t WHERE v NOT IN (1, NULL)"); len(rows) != 0 {
		t.Fatalf("NOT IN (1,NULL) must match no rows, got %v", rows)
	}
	ssExec(t, c, "CREATE TABLE s (x INTEGER)")
	ssExec(t, c, "INSERT INTO s VALUES (1),(NULL)")
	if rows := ssExec(t, c, "SELECT id FROM t WHERE v NOT IN (SELECT x FROM s)"); len(rows) != 0 {
		t.Fatalf("NOT IN (subquery with NULL) must match no rows, got %v", rows)
	}
	if rows := ssExec(t, c, "SELECT id FROM t WHERE v NOT IN (1, 2)"); len(rows) != 1 || fmt.Sprintf("%v", rows[0][0]) != "3" {
		t.Fatalf("NOT IN (1,2) want [3], got %v", rows)
	}
	// IN with NULL: a match still returns true.
	if rows := ssExec(t, c, "SELECT id FROM t WHERE v IN (2, NULL)"); len(rows) != 1 || fmt.Sprintf("%v", rows[0][0]) != "2" {
		t.Fatalf("IN (2,NULL) want [2], got %v", rows)
	}
}

// Aggregates ignore NULL inputs; an all-NULL aggregate (except COUNT) is NULL.
func TestSQLSemantics_AggregateNull(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	ssExec(t, c, "CREATE TABLE a (id INTEGER PRIMARY KEY, g TEXT, v INTEGER)")
	ssExec(t, c, "INSERT INTO a VALUES (1,'x',10),(2,'x',NULL),(3,'y',5),(4,'y',5)")
	if got := ssScalar(t, c, "SELECT COUNT(*) FROM a"); got != "4" {
		t.Fatalf("COUNT(*) want 4, got %s", got)
	}
	if got := ssScalar(t, c, "SELECT COUNT(v) FROM a"); got != "3" {
		t.Fatalf("COUNT(v) want 3 (NULL ignored), got %s", got)
	}
	if got := ssScalar(t, c, "SELECT SUM(v) FROM a"); got != "20" {
		t.Fatalf("SUM(v) want 20, got %s", got)
	}
	if got := ssScalar(t, c, "SELECT MIN(v) FROM a"); got != "5" {
		t.Fatalf("MIN(v) want 5, got %s", got)
	}
	if got := ssScalar(t, c, "SELECT MAX(v) FROM a"); got != "10" {
		t.Fatalf("MAX(v) want 10, got %s", got)
	}
	if got := ssScalar(t, c, "SELECT COUNT(DISTINCT g) FROM a"); got != "2" {
		t.Fatalf("COUNT(DISTINCT g) want 2, got %s", got)
	}
	if got := ssScalar(t, c, "SELECT AVG(v) FROM a WHERE id = 2"); got != "<nil>" {
		t.Fatalf("AVG over all-NULL want <nil>, got %s", got)
	}
}

// NULLs sort last ASC, and DISTINCT collapses duplicate NULLs to one.
func TestSQLSemantics_NullOrderingAndDistinct(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	ssExec(t, c, "CREATE TABLE n (id INTEGER PRIMARY KEY, v INTEGER)")
	ssExec(t, c, "INSERT INTO n VALUES (1,3),(2,NULL),(3,1),(4,NULL),(5,3)")
	asc := ssExec(t, c, "SELECT v FROM n ORDER BY v ASC")
	want := []string{"1", "3", "3", "<nil>", "<nil>"}
	if len(asc) != len(want) {
		t.Fatalf("ORDER BY ASC row count want %d, got %d (%v)", len(want), len(asc), asc)
	}
	for i, w := range want {
		if got := fmt.Sprintf("%v", asc[i][0]); got != w {
			t.Fatalf("ORDER BY ASC pos %d want %s, got %s (full %v)", i, w, got, asc)
		}
	}
	if d := ssExec(t, c, "SELECT DISTINCT v FROM n"); len(d) != 3 {
		t.Fatalf("DISTINCT want 3 values (1,3,NULL), got %d: %v", len(d), d)
	}
}

// Window functions partition and order correctly.
func TestSQLSemantics_WindowFunctions(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	ssExec(t, c, "CREATE TABLE w (id INTEGER PRIMARY KEY, grp TEXT, v INTEGER)")
	ssExec(t, c, "INSERT INTO w VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15)")

	rn := ssExec(t, c, "SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY v) FROM w ORDER BY id")
	wantRN := []string{"1", "2", "1", "2"}
	for i, w := range wantRN {
		if got := fmt.Sprintf("%v", rn[i][1]); got != w {
			t.Fatalf("ROW_NUMBER row %d want %s, got %s (%v)", i, w, got, rn)
		}
	}
	so := ssExec(t, c, "SELECT id, SUM(v) OVER (PARTITION BY grp) FROM w ORDER BY id")
	wantSO := []string{"30", "30", "20", "20"}
	for i, w := range wantSO {
		if got := fmt.Sprintf("%v", so[i][1]); got != w {
			t.Fatalf("SUM OVER row %d want %s, got %s (%v)", i, w, got, so)
		}
	}
	lag := ssExec(t, c, "SELECT id, LAG(v) OVER (ORDER BY id) FROM w ORDER BY id")
	wantLag := []string{"<nil>", "10", "20", "5"}
	for i, w := range wantLag {
		if got := fmt.Sprintf("%v", lag[i][1]); got != w {
			t.Fatalf("LAG row %d want %s, got %s (%v)", i, w, got, lag)
		}
	}
}

// Correlated subqueries (scalar and EXISTS) resolve the outer reference per row.
func TestSQLSemantics_CorrelatedSubquery(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)
	ssExec(t, c, "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT)")
	ssExec(t, c, "CREATE TABLE emp (id INTEGER PRIMARY KEY, did INTEGER, sal INTEGER)")
	ssExec(t, c, "INSERT INTO dept VALUES (1,'eng'),(2,'sales')")
	ssExec(t, c, "INSERT INTO emp VALUES (1,1,100),(2,1,200),(3,2,150)")

	above := ssExec(t, c, "SELECT id FROM emp e WHERE sal > (SELECT AVG(sal) FROM emp WHERE did = e.did) ORDER BY id")
	if len(above) != 1 || fmt.Sprintf("%v", above[0][0]) != "2" {
		t.Fatalf("correlated >avg want [2], got %v", above)
	}
	ex := ssExec(t, c, "SELECT name FROM dept d WHERE EXISTS (SELECT 1 FROM emp WHERE did = d.id AND sal > 180) ORDER BY name")
	if len(ex) != 1 || fmt.Sprintf("%v", ex[0][0]) != "eng" {
		t.Fatalf("EXISTS want [eng], got %v", ex)
	}
}
