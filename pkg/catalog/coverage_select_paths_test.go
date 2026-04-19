package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestSelectLockedCTEWithWindow(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create a base table
	c.ExecuteQuery("CREATE TABLE base (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO base (id, name) VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO base (id, name) VALUES (2, 'bob')")

	// CTE with window functions - needs 2 CTEs so first one is materialized
	result, err := c.ExecuteQuery("WITH cte1 AS (SELECT id, name FROM base), cte2 AS (SELECT id FROM base) SELECT ROW_NUMBER() OVER (ORDER BY id) AS rn, name FROM cte1")
	if err != nil {
		t.Logf("CTE with window: %v", err)
	} else {
		t.Logf("CTE with window returned %d rows", len(result.Rows))
	}

	// CTE with ORDER BY
	result, err = c.ExecuteQuery("WITH cte1 AS (SELECT id, name FROM base), cte2 AS (SELECT id FROM base) SELECT * FROM cte1 ORDER BY id DESC")
	if err != nil {
		t.Logf("CTE with ORDER BY: %v", err)
	} else {
		t.Logf("CTE with ORDER BY returned %d rows", len(result.Rows))
	}

	// CTE with LIMIT/OFFSET
	result, err = c.ExecuteQuery("WITH cte1 AS (SELECT id, name FROM base), cte2 AS (SELECT id FROM base) SELECT * FROM cte1 LIMIT 1 OFFSET 1")
	if err != nil {
		t.Logf("CTE with LIMIT/OFFSET: %v", err)
	} else {
		t.Logf("CTE with LIMIT/OFFSET returned %d rows", len(result.Rows))
	}

	// CTE with OFFSET > row count (covers projectedRows = nil path)
	result, err = c.ExecuteQuery("WITH cte1 AS (SELECT id, name FROM base), cte2 AS (SELECT id FROM base) SELECT * FROM cte1 LIMIT 1 OFFSET 99")
	if err != nil {
		t.Logf("CTE with large OFFSET: %v", err)
	} else {
		t.Logf("CTE with large OFFSET returned %d rows", len(result.Rows))
	}
}

func TestSelectLockedViewPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE vt (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO vt (id, name) VALUES (1, 'alice')")
	c.ExecuteQuery("INSERT INTO vt (id, name) VALUES (2, 'bob')")

	// Simple view - should inline
	c.CreateView("simple_view", mustParseSelect("SELECT * FROM vt"))
	result, err := c.ExecuteQuery("SELECT * FROM simple_view")
	if err != nil {
		t.Logf("Simple view: %v", err)
	} else {
		t.Logf("Simple view returned %d rows", len(result.Rows))
	}

	// Complex view with aggregate - should execute view then apply outer query
	c.CreateView("complex_view", mustParseSelect("SELECT name, COUNT(*) AS cnt FROM vt GROUP BY name"))
	result, err = c.ExecuteQuery("SELECT * FROM complex_view")
	if err != nil {
		t.Logf("Complex view: %v", err)
	} else {
		t.Logf("Complex view returned %d rows", len(result.Rows))
	}

	// Simple view with JOIN
	c.ExecuteQuery("CREATE TABLE vt2 (id INTEGER PRIMARY KEY, ref INTEGER)")
	c.ExecuteQuery("INSERT INTO vt2 (id, ref) VALUES (1, 1)")
	c.CreateView("view_with_join", mustParseSelect("SELECT vt.id, vt2.ref FROM vt JOIN vt2 ON vt.id = vt2.ref"))
	result, err = c.ExecuteQuery("SELECT * FROM view_with_join")
	if err != nil {
		t.Logf("View with JOIN: %v", err)
	} else {
		t.Logf("View with JOIN returned %d rows", len(result.Rows))
	}

	// Complex view with JOIN
	c.CreateView("complex_view_join", mustParseSelect("SELECT vt.name, COUNT(*) AS cnt FROM vt JOIN vt2 ON vt.id = vt2.ref GROUP BY vt.name"))
	result, err = c.ExecuteQuery("SELECT * FROM complex_view_join")
	if err != nil {
		t.Logf("Complex view with JOIN: %v", err)
	} else {
		t.Logf("Complex view with JOIN returned %d rows", len(result.Rows))
	}
}

func TestSelectLockedDerivedTableExtra(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE dt (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO dt (id, name) VALUES (1, 'alice')")

	// Derived table (subquery in FROM)
	result, err := c.ExecuteQuery("SELECT * FROM (SELECT id, name FROM dt) AS sub WHERE id = 1")
	if err != nil {
		t.Logf("Derived table: %v", err)
	} else {
		t.Logf("Derived table returned %d rows", len(result.Rows))
	}

	// Derived table with JOIN
	c.ExecuteQuery("CREATE TABLE dt2 (id INTEGER PRIMARY KEY, ref INTEGER)")
	c.ExecuteQuery("INSERT INTO dt2 (id, ref) VALUES (1, 1)")
	result, err = c.ExecuteQuery("SELECT * FROM (SELECT id, name FROM dt) AS sub JOIN dt2 ON sub.id = dt2.ref")
	if err != nil {
		t.Logf("Derived table with JOIN: %v", err)
	} else {
		t.Logf("Derived table with JOIN returned %d rows", len(result.Rows))
	}
}

func TestExecuteSelectWithJoinAndGroupByPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE jg1 (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("CREATE TABLE jg2 (id INTEGER PRIMARY KEY, jg1_id INTEGER, name TEXT)")
	c.ExecuteQuery("INSERT INTO jg1 (id, cat, val) VALUES (1, 'A', 10)")
	c.ExecuteQuery("INSERT INTO jg1 (id, cat, val) VALUES (2, 'A', 20)")
	c.ExecuteQuery("INSERT INTO jg2 (id, jg1_id, name) VALUES (1, 1, 'x')")
	c.ExecuteQuery("INSERT INTO jg2 (id, jg1_id, name) VALUES (2, 2, 'y')")

	// JOIN with GROUP BY
	result, err := c.ExecuteQuery("SELECT jg1.cat, SUM(jg1.val), jg2.name FROM jg1 JOIN jg2 ON jg1.id = jg2.jg1_id GROUP BY jg1.cat, jg2.name")
	if err != nil {
		t.Logf("JOIN with GROUP BY: %v", err)
	} else {
		t.Logf("JOIN with GROUP BY returned %d rows", len(result.Rows))
	}

	// LEFT JOIN with GROUP BY
	result, err = c.ExecuteQuery("SELECT jg1.cat, COUNT(jg2.name) FROM jg1 LEFT JOIN jg2 ON jg1.id = jg2.jg1_id GROUP BY jg1.cat")
	if err != nil {
		t.Logf("LEFT JOIN with GROUP BY: %v", err)
	} else {
		t.Logf("LEFT JOIN with GROUP BY returned %d rows", len(result.Rows))
	}

	// RIGHT JOIN with GROUP BY
	result, err = c.ExecuteQuery("SELECT jg2.name, COUNT(jg1.cat) FROM jg1 RIGHT JOIN jg2 ON jg1.id = jg2.jg1_id GROUP BY jg2.name")
	if err != nil {
		t.Logf("RIGHT JOIN with GROUP BY: %v", err)
	} else {
		t.Logf("RIGHT JOIN with GROUP BY returned %d rows", len(result.Rows))
	}

	// JOIN with GROUP BY and HAVING
	result, err = c.ExecuteQuery("SELECT jg1.cat, SUM(jg1.val) AS s FROM jg1 JOIN jg2 ON jg1.id = jg2.jg1_id GROUP BY jg1.cat HAVING s > 10")
	if err != nil {
		t.Logf("JOIN GROUP BY HAVING: %v", err)
	} else {
		t.Logf("JOIN GROUP BY HAVING returned %d rows", len(result.Rows))
	}

	// JOIN with DISTINCT
	result, err = c.ExecuteQuery("SELECT DISTINCT jg1.cat FROM jg1 JOIN jg2 ON jg1.id = jg2.jg1_id")
	if err != nil {
		t.Logf("JOIN DISTINCT: %v", err)
	} else {
		t.Logf("JOIN DISTINCT returned %d rows", len(result.Rows))
	}
}

func TestUpdateDeleteEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE upd_t (id INTEGER PRIMARY KEY, name TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO upd_t (id, name, val) VALUES (1, 'alice', 10)")
	c.ExecuteQuery("INSERT INTO upd_t (id, name, val) VALUES (2, 'bob', 20)")
	c.ExecuteQuery("INSERT INTO upd_t (id, name, val) VALUES (3, 'alice', 30)")

	// UPDATE with JOIN
	c.ExecuteQuery("CREATE TABLE upd_ref (id INTEGER PRIMARY KEY, new_name TEXT)")
	c.ExecuteQuery("INSERT INTO upd_ref (id, new_name) VALUES (1, 'ALICE')")
	_, _, err := c.Update(ctx, mustParseUpdate("UPDATE upd_t SET name = upd_ref.new_name FROM upd_ref WHERE upd_t.id = upd_ref.id"), nil)
	if err != nil {
		t.Logf("UPDATE with JOIN: %v", err)
	}

	// DELETE with USING
	c.ExecuteQuery("CREATE TABLE del_ref (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO del_ref (id) VALUES (1)")
	_, _, err = c.Delete(ctx, mustParseDelete("DELETE FROM upd_t USING del_ref WHERE upd_t.id = del_ref.id"), nil)
	if err != nil {
		t.Logf("DELETE with USING: %v", err)
	}

	// UPDATE RETURNING
	_, _, err = c.Update(ctx, mustParseUpdate("UPDATE upd_t SET val = val + 1 WHERE id = 2 RETURNING id, val"), nil)
	if err != nil {
		t.Logf("UPDATE RETURNING: %v", err)
	}

	// DELETE RETURNING
	_, _, err = c.Delete(ctx, mustParseDelete("DELETE FROM upd_t WHERE id = 3 RETURNING id, name"), nil)
	if err != nil {
		t.Logf("DELETE RETURNING: %v", err)
	}

	_ = ctx
}

// Helper to parse UPDATE statements
func mustParseUpdate(sql string) *query.UpdateStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	if upd, ok := parsed.(*query.UpdateStmt); ok {
		return upd
	}
	panic("parsed statement is not an UPDATE")
}

// Helper to parse DELETE statements
func mustParseDelete(sql string) *query.DeleteStmt {
	parsed, err := query.Parse(sql)
	if err != nil {
		panic(err)
	}
	if del, ok := parsed.(*query.DeleteStmt); ok {
		return del
	}
	panic("parsed statement is not a DELETE")
}
