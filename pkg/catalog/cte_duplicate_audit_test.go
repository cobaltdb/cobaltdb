package catalog

import (
	"fmt"
	"testing"
)

func TestCTEDuplicateNamesDoNotLeakViews(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	if _, err := c.ExecuteQuery("WITH alpha AS (SELECT 1 AS n), beta AS (SELECT 2 AS n) SELECT * FROM beta"); err != nil {
		t.Fatal("control", err)
	}
	fmt.Println("CONTROL EXPECTED: distinct CTE names accepted; ACTUAL: accepted")
	_, err := c.ExecuteQuery("WITH transient AS (SELECT 1 AS n), transient AS (SELECT 2 AS n) SELECT * FROM transient")
	_, leaked := c.views["transient"]
	fmt.Printf("EXPECTED: duplicate-name error and no leaked view; ACTUAL: error=%v leaked=%v\n", err, leaked)
	if err == nil || leaked {
		fmt.Println("PROBLEM CONFIRMED")
		t.FailNow()
	} // Case variants share the CTE namespace and must reject before mutation.
	for _, sql := range []string{
		"WITH Alpha AS (SELECT 1 AS n), alpha AS (SELECT 2 AS n) SELECT * FROM alpha",
		"WITH alpha AS (SELECT 1 AS n), ALPHA AS (SELECT 2 AS n) SELECT * FROM alpha",
	} {
		if _, err := c.ExecuteQuery(sql); err == nil {
			t.Fatal("case-variant duplicate accepted")
		}
		if len(c.views) != 0 || len(c.cteResults) != 0 {
			t.Fatal("temporary state leaked")
		}
	}
	if _, err := c.ExecuteQuery("WITH transient AS (SELECT 3 AS n) SELECT * FROM transient"); err != nil {
		t.Fatal("repeated independent statement", err)
	}
	if len(c.views) != 0 || len(c.cteResults) != 0 {
		t.Fatal("successful statement leaked temporary state")
	}
	fmt.Println("FIX VERIFIED")
}
