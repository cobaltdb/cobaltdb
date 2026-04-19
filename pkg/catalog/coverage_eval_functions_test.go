package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestEvalFunctionCallEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// LENGTH with non-string arg
	if _, err := c.ExecuteQuery("SELECT LENGTH(123)"); err != nil {
		t.Logf("LENGTH(123): %v", err)
	}

	// UPPER with non-string arg
	if _, err := c.ExecuteQuery("SELECT UPPER(123)"); err != nil {
		t.Logf("UPPER(123): %v", err)
	}

	// LOWER with non-string arg
	if _, err := c.ExecuteQuery("SELECT LOWER(123)"); err != nil {
		t.Logf("LOWER(123): %v", err)
	}

	// TRIM with non-string arg and custom trim chars
	if _, err := c.ExecuteQuery("SELECT TRIM(123, 1)"); err != nil {
		t.Logf("TRIM(123, 1): %v", err)
	}

	// SUBSTR with non-string arg
	if _, err := c.ExecuteQuery("SELECT SUBSTR(123, 1)"); err != nil {
		t.Logf("SUBSTR(123, 1): %v", err)
	}

	// REPLACE with non-string args, empty old string
	if _, err := c.ExecuteQuery("SELECT REPLACE(123, 2, 3)"); err != nil {
		t.Logf("REPLACE(123, 2, 3): %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT REPLACE('abc', '', 'x')"); err != nil {
		t.Logf("REPLACE empty old: %v", err)
	}

	// INSTR with non-string args
	if _, err := c.ExecuteQuery("SELECT INSTR(123, 2)"); err != nil {
		t.Logf("INSTR(123, 2): %v", err)
	}

	// PRINTF with non-string format and default format char
	if _, err := c.ExecuteQuery("SELECT PRINTF(123, 'x')"); err != nil {
		t.Logf("PRINTF(123, 'x'): %v", err)
	}

	// CAST with non-string target type, INTEGER from string, INTEGER from bool, REAL from string, BOOLEAN
	if _, err := c.ExecuteQuery("SELECT CAST('42' AS INTEGER)"); err != nil {
		t.Logf("CAST string to int: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT CAST(1 AS BOOLEAN)"); err != nil {
		t.Logf("CAST int to bool: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT CAST('3.14' AS REAL)"); err != nil {
		t.Logf("CAST string to real: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT CAST('true' AS BOOLEAN)"); err != nil {
		t.Logf("CAST string to bool: %v", err)
	}

	// CONCAT_WS
	if _, err := c.ExecuteQuery("SELECT CONCAT_WS(',', 'a', 'b')"); err != nil {
		t.Logf("CONCAT_WS: %v", err)
	}

	// GROUP_CONCAT scalar fallback
	if _, err := c.ExecuteQuery("SELECT GROUP_CONCAT('hello')"); err != nil {
		t.Logf("GROUP_CONCAT scalar: %v", err)
	}

	// REPEAT with nil arg (via column) and max size
	if _, err := c.ExecuteQuery("SELECT REPEAT('a', 99999999)"); err != nil {
		t.Logf("REPEAT max size: %v", err)
	}

	// LEFT with nil arg, RIGHT with nil arg
	if _, err := c.ExecuteQuery("SELECT LEFT(NULL, 5)"); err != nil {
		t.Logf("LEFT NULL: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT RIGHT(NULL, 5)"); err != nil {
		t.Logf("RIGHT NULL: %v", err)
	}

	// LPAD with nil arg, max size, empty pad
	if _, err := c.ExecuteQuery("SELECT LPAD(NULL, 5, 'x')"); err != nil {
		t.Logf("LPAD NULL: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT LPAD('a', 99999999, 'x')"); err != nil {
		t.Logf("LPAD max size: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT LPAD('a', 5, '')"); err != nil {
		t.Logf("LPAD empty pad: %v", err)
	}

	// RPAD with nil arg, max size, empty pad
	if _, err := c.ExecuteQuery("SELECT RPAD(NULL, 5, 'x')"); err != nil {
		t.Logf("RPAD NULL: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT RPAD('a', 99999999, 'x')"); err != nil {
		t.Logf("RPAD max size: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT RPAD('a', 5, '')"); err != nil {
		t.Logf("RPAD empty pad: %v", err)
	}

	// TYPEOF with int, float, default text
	if _, err := c.ExecuteQuery("SELECT TYPEOF(1)"); err != nil {
		t.Logf("TYPEOF(1): %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT TYPEOF(1.5)"); err != nil {
		t.Logf("TYPEOF(1.5): %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT TYPEOF('hello')"); err != nil {
		t.Logf("TYPEOF('hello'): %v", err)
	}

	// ZEROBLOB max size
	if _, err := c.ExecuteQuery("SELECT ZEROBLOB(99999999)"); err != nil {
		t.Logf("ZEROBLOB max size: %v", err)
	}

	// QUOTE with non-string arg
	if _, err := c.ExecuteQuery("SELECT QUOTE(123)"); err != nil {
		t.Logf("QUOTE(123): %v", err)
	}

	// COSINE_SIMILARITY, L2_DISTANCE, INNER_PRODUCT error cases
	if _, err := c.ExecuteQuery("SELECT COSINE_SIMILARITY(1, 2)"); err != nil {
		t.Logf("COSINE_SIMILARITY error: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT L2_DISTANCE(1, 2)"); err != nil {
		t.Logf("L2_DISTANCE error: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT INNER_PRODUCT(1, 2)"); err != nil {
		t.Logf("INNER_PRODUCT error: %v", err)
	}

	// COALESCE with all NULLs
	if _, err := c.ExecuteQuery("SELECT COALESCE(NULL, NULL)"); err != nil {
		t.Logf("COALESCE all NULL: %v", err)
	}

	// NULLIF
	if _, err := c.ExecuteQuery("SELECT NULLIF(1, 1)"); err != nil {
		t.Logf("NULLIF equal: %v", err)
	}
	if _, err := c.ExecuteQuery("SELECT NULLIF(1, 2)"); err != nil {
		t.Logf("NULLIF not equal: %v", err)
	}

	// GLOB
	if _, err := c.ExecuteQuery("SELECT GLOB('hello*', 'hello world')"); err != nil {
		t.Logf("GLOB: %v", err)
	}

	// ABS with nil
	if _, err := c.ExecuteQuery("SELECT ABS(NULL)"); err != nil {
		t.Logf("ABS NULL: %v", err)
	}

	// ROUND with non-numeric
	if _, err := c.ExecuteQuery("SELECT ROUND('abc')"); err != nil {
		t.Logf("ROUND non-numeric: %v", err)
	}

	// FLOOR with non-numeric
	if _, err := c.ExecuteQuery("SELECT FLOOR('abc')"); err != nil {
		t.Logf("FLOOR non-numeric: %v", err)
	}

	// CEIL with non-numeric
	if _, err := c.ExecuteQuery("SELECT CEIL('abc')"); err != nil {
		t.Logf("CEIL non-numeric: %v", err)
	}

	// DATE/TIME/DATETIME
	if _, err := c.ExecuteQuery("SELECT DATE('2024-01-01')"); err != nil {
		t.Logf("DATE: %v", err)
	}

	// STRFTIME
	if _, err := c.ExecuteQuery("SELECT STRFTIME('%Y', '2024')"); err != nil {
		t.Logf("STRFTIME: %v", err)
	}
}

func TestInsertEdgeCases(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create parent table for FK
	c.ExecuteQuery("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO parent (id) VALUES (1)")

	// Create child table with FK
	c.ExecuteQuery("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id))")

	// FK violation
	_, err := c.ExecuteQuery("INSERT INTO child (id, parent_id) VALUES (1, 999)")
	if err == nil {
		t.Error("Expected FK violation error")
	}

	// Create table with CHECK constraint
	c.ExecuteQuery("CREATE TABLE check_t (id INTEGER PRIMARY KEY, val INTEGER CHECK (val > 0))")

	// CHECK violation
	_, err = c.ExecuteQuery("INSERT INTO check_t (id, val) VALUES (1, -1)")
	if err == nil {
		t.Error("Expected CHECK violation error")
	}

	// Create table with UNIQUE and test ConflictReplace
	c.ExecuteQuery("CREATE TABLE uniq_t (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
	c.ExecuteQuery("INSERT INTO uniq_t (id, name) VALUES (1, 'alice')")

	// ConflictIgnore
	_, err = c.ExecuteQuery("INSERT OR IGNORE INTO uniq_t (id, name) VALUES (2, 'alice')")
	if err != nil {
		t.Logf("ConflictIgnore error: %v", err)
	}

	// ConflictReplace
	_, err = c.ExecuteQuery("INSERT OR REPLACE INTO uniq_t (id, name) VALUES (3, 'alice')")
	if err != nil {
		t.Logf("ConflictReplace error: %v", err)
	}

	// INSERT...SELECT with bad subquery
	_, err = c.ExecuteQuery("INSERT INTO check_t (id, val) SELECT * FROM nonexistent")
	if err == nil {
		t.Error("Expected INSERT...SELECT error")
	}

	// INSERT with bad column name
	_, err = c.ExecuteQuery("INSERT INTO check_t (id, badcol) VALUES (1, 1)")
	if err == nil {
		t.Error("Expected bad column error")
	}

	// Insert with string PK
	c.ExecuteQuery("CREATE TABLE str_pk_t (id TEXT PRIMARY KEY, name TEXT)")
	_, err = c.ExecuteQuery("INSERT INTO str_pk_t (id, name) VALUES ('abc', 'hello')")
	if err != nil {
		t.Logf("String PK insert: %v", err)
	}

	// Insert with value count mismatch (too few values, no auto-inc)
	_, err = c.ExecuteQuery("INSERT INTO check_t (id, val) VALUES (1)")
	if err == nil {
		t.Error("Expected value count mismatch error")
	}

	// Insert returning
	c.ExecuteQuery("CREATE TABLE ret_t (id INTEGER PRIMARY KEY, name TEXT)")
	_, err = c.ExecuteQuery("INSERT INTO ret_t (id, name) VALUES (1, 'alice') RETURNING id, name")
	if err != nil {
		t.Logf("INSERT RETURNING: %v", err)
	}

	_ = ctx
}

func TestSelectJoinEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE a (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("CREATE TABLE b (id INTEGER PRIMARY KEY, name TEXT)")
	c.ExecuteQuery("INSERT INTO a (id, name) VALUES (1, 'a1')")
	c.ExecuteQuery("INSERT INTO b (id, name) VALUES (1, 'b1')")

	// NATURAL JOIN
	_, err := c.ExecuteQuery("SELECT * FROM a NATURAL JOIN b")
	if err != nil {
		t.Logf("NATURAL JOIN: %v", err)
	}

	// SELECT from non-existent table
	_, err = c.ExecuteQuery("SELECT * FROM nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent table")
	}

	// JOIN with non-existent table
	_, err = c.ExecuteQuery("SELECT * FROM a JOIN nonexistent ON a.id = nonexistent.id")
	if err != nil {
		t.Logf("JOIN nonexistent: %v", err)
	}

	// CROSS JOIN
	_, err = c.ExecuteQuery("SELECT * FROM a CROSS JOIN b")
	if err != nil {
		t.Logf("CROSS JOIN: %v", err)
	}
}

func TestAggregateEdgeCases(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE agg_t (id INTEGER PRIMARY KEY, cat TEXT, val INTEGER)")
	c.ExecuteQuery("INSERT INTO agg_t (id, cat, val) VALUES (1, 'A', 10)")
	c.ExecuteQuery("INSERT INTO agg_t (id, cat, val) VALUES (2, 'A', 20)")
	c.ExecuteQuery("INSERT INTO agg_t (id, cat, val) VALUES (3, 'B', 30)")

	// GROUP BY with HAVING
	_, err := c.ExecuteQuery("SELECT cat, SUM(val) FROM agg_t GROUP BY cat HAVING SUM(val) > 15")
	if err != nil {
		t.Logf("GROUP BY HAVING: %v", err)
	}

	// GROUP BY with ORDER BY aggregate
	_, err = c.ExecuteQuery("SELECT cat, SUM(val) as s FROM agg_t GROUP BY cat ORDER BY s DESC")
	if err != nil {
		t.Logf("GROUP BY ORDER BY aggregate: %v", err)
	}

	// DISTINCT with aggregate
	_, err = c.ExecuteQuery("SELECT COUNT(DISTINCT cat) FROM agg_t")
	if err != nil {
		t.Logf("COUNT DISTINCT: %v", err)
	}
}
