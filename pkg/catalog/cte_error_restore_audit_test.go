package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"testing"
)

func TestCTEErrorRestoresViews(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	stmt, err := query.Parse("SELECT 7 AS value")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CreateView("shared", stmt.(*query.SelectStmt)); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		"WITH shared AS (SELECT 99 AS value) SELECT * FROM shared",
		"WITH shared AS (SELECT 99 AS value), failing AS (SELECT * FROM missing) SELECT * FROM shared",
		"WITH shared AS (SELECT * FROM missing) SELECT * FROM shared",
		"WITH shared AS (SELECT 99 AS value), failing AS (SELECT 1 UNION ALL SELECT value FROM missing) SELECT * FROM shared",
		"WITH RECURSIVE shared AS (SELECT 99 AS value), failing(n) AS (SELECT 1 UNION ALL SELECT n FROM missing) SELECT * FROM shared",
		"WITH transient AS (SELECT 99 AS value), failing AS (SELECT * FROM missing) SELECT * FROM transient",
	} {
		_, err := c.ExecuteQuery(sql)
		if sql == "WITH shared AS (SELECT 99 AS value) SELECT * FROM shared" {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil {
			t.Fatalf("expected failure for %s", sql)
		}
		r, err := c.ExecuteQuery("SELECT value FROM shared")
		if err != nil || len(r.Rows) != 1 || r.Rows[0][0] != int64(7) {
			t.Fatalf("view not restored: %v %v", r, err)
		}
		for _, name := range []string{"transient", "failing"} {
			if _, exists := c.views[name]; exists {
				t.Fatalf("temporary view leaked: %s", name)
			}
		}
		if len(c.cteResults) != 0 {
			t.Fatalf("temporary rows leaked: %v", c.cteResults)
		}
		fmt.Println("EXPECTED: original view 7 and no temporary CTE state; ACTUAL: restored")
	}
	fmt.Println("FIX VERIFIED")
}
