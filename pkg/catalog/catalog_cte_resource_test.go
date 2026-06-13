package catalog

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newCTEResourceTestCatalog(t *testing.T) *Catalog {
	t.Helper()

	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	return New(tree, pool, nil)
}

func TestRecursiveCTERejectsDepthExhaustion(t *testing.T) {
	c := newCTEResourceTestCatalog(t)

	_, err := c.ExecuteQuery(`WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums) SELECT * FROM nums`)
	if err == nil {
		t.Fatal("expected recursive CTE depth limit error")
	}
	if !strings.Contains(err.Error(), "recursive CTE depth limit exceeded") {
		t.Fatalf("expected depth limit error, got %v", err)
	}
	if _, ok := c.cteResults["nums"]; ok {
		t.Fatal("recursive CTE working set leaked after depth limit error")
	}
}

func TestRecursiveCTERejectsRowLimitExhaustion(t *testing.T) {
	c := newCTEResourceTestCatalog(t)
	if _, err := c.ExecuteQuery("CREATE TABLE cte_fanout (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create fanout table: %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := c.ExecuteQuery(fmt.Sprintf("INSERT INTO cte_fanout VALUES (%d)", i)); err != nil {
			t.Fatalf("insert fanout row %d: %v", i, err)
		}
	}

	_, err := c.ExecuteQuery(`WITH RECURSIVE boom(n) AS (
		SELECT 1
		UNION ALL
		SELECT boom.n FROM boom JOIN cte_fanout ON 1 = 1
	) SELECT * FROM boom`)
	if err == nil {
		t.Fatal("expected recursive CTE row limit error")
	}
	if !strings.Contains(err.Error(), "recursive CTE row limit exceeded") {
		t.Fatalf("expected row limit error, got %v", err)
	}
	if _, ok := c.cteResults["boom"]; ok {
		t.Fatal("recursive CTE working set leaked after row limit error")
	}
}
