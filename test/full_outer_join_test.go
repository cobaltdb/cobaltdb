package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestFullOuterJoin_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create test tables with some non-matching rows in both
	afExec(t, db, ctx, "CREATE TABLE t1 (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE t2 (id INTEGER, value TEXT)")

	// Insert test data - some ids match, some don't
	afExec(t, db, ctx, "INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
	afExec(t, db, ctx, "INSERT INTO t2 VALUES (2, 'Two'), (3, 'Three'), (4, 'Four')")

	t.Run("FULL OUTER JOIN basic", func(t *testing.T) {
		// FULL OUTER JOIN should return all rows from both tables
		rows := afQuery(t, db, ctx, "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id")

		// Should have 4 rows:
		// - id=1: from t1 only (NULL t2)
		// - id=2: from both
		// - id=3: from both
		// - id=4: from t2 only (NULL t1)
		if len(rows) != 4 {
			t.Errorf("Expected 4 rows from FULL OUTER JOIN, got %d", len(rows))
		}
	})

	t.Run("FULL JOIN explicit", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM t1 FULL JOIN t2 ON t1.id = t2.id")

		if len(rows) != 4 {
			t.Errorf("Expected 4 rows from FULL JOIN, got %d", len(rows))
		}
	})

	t.Run("FULL OUTER JOIN with column selection", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT t1.name, t2.value FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id")

		if len(rows) != 4 {
			t.Errorf("Expected 4 rows, got %d", len(rows))
		}
	})
}

func TestFullOuterJoin_NoMatches(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE a (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE b (id INTEGER, value TEXT)")

	// No matching ids
	afExec(t, db, ctx, "INSERT INTO a VALUES (1, 'A'), (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO b VALUES (3, 'C'), (4, 'D')")

	t.Run("FULL OUTER JOIN with no matches", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")

		// Should have 4 rows (all from a with NULL b, all from b with NULL a)
		if len(rows) != 4 {
			t.Errorf("Expected 4 rows, got %d", len(rows))
		}
	})
}

func TestFullOuterJoin_AllMatches(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE a (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE b (id INTEGER, value TEXT)")

	// All ids match
	afExec(t, db, ctx, "INSERT INTO a VALUES (1, 'A'), (2, 'B')")
	afExec(t, db, ctx, "INSERT INTO b VALUES (1, 'X'), (2, 'Y')")

	t.Run("FULL OUTER JOIN with all matches", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")

		// Should have 2 rows (all match, no NULLs)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}

func TestFullOuterJoin_EmptyTables(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE a (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE b (id INTEGER, value TEXT)")

	// Only insert into a
	afExec(t, db, ctx, "INSERT INTO a VALUES (1, 'A'), (2, 'B')")

	t.Run("FULL OUTER JOIN with empty right table", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id")

		// Should have 2 rows (all from a, NULLs for b)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}
