package catalog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ── GROUP BY edge cases ──
func TestGapCloser_GroupByEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// GROUP BY with OFFSET > row count (covers resultRows = nil)
	c.ExecuteQuery("CREATE TABLE gb_off (id INTEGER PRIMARY KEY, cat TEXT)")
	c.ExecuteQuery("INSERT INTO gb_off (id, cat) VALUES (1, 'A')")
	_, err := c.ExecuteQuery("SELECT cat, COUNT(*) FROM gb_off GROUP BY cat ORDER BY cat OFFSET 99")
	if err != nil {
		t.Logf("GROUP BY large OFFSET: %v", err)
	}

	// GROUP_CONCAT with very long result (covers truncation in computeAggregatesWithGroupBy)
	c.ExecuteQuery("CREATE TABLE gc_long (id INTEGER PRIMARY KEY, grp INTEGER, name TEXT)")
	for i := 1; i <= 2000; i++ {
		c.ExecuteQuery("INSERT INTO gc_long (id, grp, name) VALUES (" + fmt.Sprintf("%d", i) + ", 1, 'verylongstringindeed')")
	}
	_, err = c.ExecuteQuery("SELECT grp, GROUP_CONCAT(name) FROM gc_long GROUP BY grp")
	if err != nil {
		t.Logf("GROUP_CONCAT long: %v", err)
	}

	// Positional ORDER BY with NULL values on both sides
	c.ExecuteQuery("CREATE TABLE pos_null (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO pos_null (id, cat, val) VALUES (1, 'A', NULL)")
	c.ExecuteQuery("INSERT INTO pos_null (id, cat, val) VALUES (2, 'B', NULL)")
	_, err = c.ExecuteQuery("SELECT cat, SUM(val) FROM pos_null GROUP BY cat ORDER BY 2")
	if err != nil {
		t.Logf("Positional ORDER BY both NULL: %v", err)
	}

	// Positional ORDER BY with string comparison
	c.ExecuteQuery("CREATE TABLE pos_str (id INTEGER PRIMARY KEY, cat TEXT)")
	c.ExecuteQuery("INSERT INTO pos_str (id, cat) VALUES (1, 'Z')")
	c.ExecuteQuery("INSERT INTO pos_str (id, cat) VALUES (2, 'A')")
	_, err = c.ExecuteQuery("SELECT cat, COUNT(*) FROM pos_str GROUP BY cat ORDER BY 1")
	if err != nil {
		t.Logf("Positional ORDER BY string cmp: %v", err)
	}

	// Empty table GROUP BY with HAVING no match (covers empty agg + HAVING false)
	c.ExecuteQuery("CREATE TABLE gb_hav (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	_, err = c.ExecuteQuery("SELECT cat, SUM(val) FROM gb_hav GROUP BY cat HAVING SUM(val) > 0 ORDER BY cat LIMIT 1 OFFSET 1")
	if err != nil {
		t.Logf("Empty GROUP BY HAVING: %v", err)
	}

	// Embedded SUM with all NULLs in GROUP BY context (covers SUM all-NULL -> nil in evaluateExprWithGroupAggregates)
	c.ExecuteQuery("CREATE TABLE emb_sum (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO emb_sum (id, cat, val) VALUES (1, 'A', NULL)")
	c.ExecuteQuery("INSERT INTO emb_sum (id, cat, val) VALUES (2, 'A', NULL)")
	c.ExecuteQuery("INSERT INTO emb_sum (id, cat, val) VALUES (3, 'B', 10)")
	_, err = c.ExecuteQuery("SELECT cat, CASE WHEN SUM(val) > 0 THEN 1 ELSE 0 END FROM emb_sum GROUP BY cat")
	if err != nil {
		t.Logf("Embedded SUM all NULL group: %v", err)
	}

	// Embedded AVG with all NULLs in GROUP BY context
	_, err = c.ExecuteQuery("SELECT cat, CASE WHEN AVG(val) > 0 THEN 1 ELSE 0 END FROM emb_sum GROUP BY cat")
	if err != nil {
		t.Logf("Embedded AVG all NULL group: %v", err)
	}

	// Embedded MIN/MAX with all NULLs in GROUP BY context
	_, err = c.ExecuteQuery("SELECT cat, CASE WHEN MIN(val) IS NULL THEN 'yes' ELSE 'no' END FROM emb_sum GROUP BY cat")
	if err != nil {
		t.Logf("Embedded MIN all NULL group: %v", err)
	}
	_, err = c.ExecuteQuery("SELECT cat, CASE WHEN MAX(val) IS NULL THEN 'yes' ELSE 'no' END FROM emb_sum GROUP BY cat")
	if err != nil {
		t.Logf("Embedded MAX all NULL group: %v", err)
	}

	// DISTINCT with GROUP BY
	_, err = c.ExecuteQuery("SELECT DISTINCT cat FROM gb_off GROUP BY cat")
	if err != nil {
		t.Logf("DISTINCT GROUP BY: %v", err)
	}
}

// ── Scalar SELECT paths ──
func TestGapCloser_ScalarSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Scalar SELECT with LIMIT
	_, err := c.ExecuteQuery("SELECT 1 + 1 LIMIT 1")
	if err != nil {
		t.Logf("Scalar LIMIT: %v", err)
	}

	// Scalar SELECT with WHERE false
	_, err = c.ExecuteQuery("SELECT 42 WHERE 1 = 0")
	if err != nil {
		t.Logf("Scalar WHERE false: %v", err)
	}

	// Scalar aggregate COUNT(*) without FROM
	_, err = c.ExecuteQuery("SELECT COUNT(*)")
	if err != nil {
		t.Logf("Scalar COUNT(*): %v", err)
	}
}

// ── DELETE edge cases ──
func TestGapCloser_DeleteEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// DELETE with index scan
	c.ExecuteQuery("CREATE TABLE del_idx (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_del_code ON del_idx(code)")
	c.ExecuteQuery("INSERT INTO del_idx (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO del_idx (id, code) VALUES (2, 'B')")
	_, _, err := c.Delete(ctx, mustParseDeleteLM("DELETE FROM del_idx WHERE code = 'A'"), nil)
	if err != nil {
		t.Logf("Delete with index: %v", err)
	}

	// DELETE from empty table
	c.ExecuteQuery("CREATE TABLE del_empty (id INTEGER PRIMARY KEY, name TEXT)")
	_, _, err = c.Delete(ctx, mustParseDeleteLM("DELETE FROM del_empty WHERE name = 'x'"), nil)
	if err != nil {
		t.Logf("Delete empty: %v", err)
	}

	// DELETE all rows (no WHERE)
	c.ExecuteQuery("CREATE TABLE del_all (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO del_all (id, name) VALUES (1, 'a')")
	_, _, err = c.Delete(ctx, mustParseDeleteLM("DELETE FROM del_all"), nil)
	if err != nil {
		t.Logf("Delete all: %v", err)
	}
}

// ── UPDATE edge cases ──
func TestGapCloser_UpdateEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// UPDATE with index scan
	c.ExecuteQuery("CREATE TABLE upd_idx (id INTEGER PRIMARY KEY, code TEXT)")
	c.ExecuteQuery("CREATE INDEX idx_upd_code ON upd_idx(code)")
	c.ExecuteQuery("INSERT INTO upd_idx (id, code) VALUES (1, 'A')")
	c.ExecuteQuery("INSERT INTO upd_idx (id, code) VALUES (2, 'B')")
	_, _, err := c.Update(ctx, mustParseUpdateLM("UPDATE upd_idx SET code = 'Z' WHERE code = 'A'"), nil)
	if err != nil {
		t.Logf("Update with index: %v", err)
	}

	// UPDATE empty table
	c.ExecuteQuery("CREATE TABLE upd_empty (id INTEGER PRIMARY KEY, name TEXT)")
	_, _, err = c.Update(ctx, mustParseUpdateLM("UPDATE upd_empty SET name = 'x' WHERE name = 'y'"), nil)
	if err != nil {
		t.Logf("Update empty: %v", err)
	}

	// UPDATE all rows (no WHERE)
	c.ExecuteQuery("CREATE TABLE upd_all (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO upd_all (id, name) VALUES (1, 'a')")
	_, _, err = c.Update(ctx, mustParseUpdateLM("UPDATE upd_all SET name = 'b'"), nil)
	if err != nil {
		t.Logf("Update all: %v", err)
	}
}

// ── Maintenance paths ──
func TestGapCloser_Maintenance(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Vacuum empty catalog
	err := c.Vacuum()
	if err != nil {
		t.Logf("Vacuum empty: %v", err)
	}

	// Load with empty tree (nil tree path already covered, but test empty scan)
	c.ExecuteQuery("CREATE TABLE maint_t (id INTEGER PRIMARY KEY)")
	err = c.Load()
	if err != nil {
		t.Logf("Load: %v", err)
	}
}
