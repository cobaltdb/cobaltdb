package test

import (
	"fmt"
	"testing"
)

func TestGroupByAlias(t *testing.T) {
	db, ctx := af(t)

	afExec(t, db, ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (1, 'A', 'Electronics', 100)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (2, 'B', 'Electronics', 200)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (3, 'C', 'Books', 50)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (4, 'D', 'Books', 30)")
	afExec(t, db, ctx, "INSERT INTO items VALUES (5, 'E', 'Food', 10)")

	// Test 1: GROUP BY column name (should work)
	rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM items GROUP BY category")
	t.Logf("GROUP BY column: %v", rows)
	if len(rows) != 3 {
		t.Errorf("GROUP BY column: expected 3 groups, got %d", len(rows))
	}

	// Test 2: GROUP BY alias (the bug)
	rows = afQuery(t, db, ctx, "SELECT category AS cat, COUNT(*) FROM items GROUP BY cat")
	t.Logf("GROUP BY alias: %v", rows)
	if len(rows) != 3 {
		t.Errorf("GROUP BY alias: expected 3 groups, got %d", len(rows))
	}

	// Test 3: GROUP BY with expression alias
	rows = afQuery(t, db, ctx, "SELECT CASE WHEN price > 50 THEN 'expensive' ELSE 'cheap' END AS tier, COUNT(*) FROM items GROUP BY tier")
	t.Logf("GROUP BY expr alias: %v", rows)
	if len(rows) != 2 {
		t.Errorf("GROUP BY expr alias: expected 2 groups, got %d", len(rows))
	}

	// Test 4: ORDER BY alias (should already work)
	rows = afQuery(t, db, ctx, "SELECT name AS n FROM items ORDER BY n")
	t.Logf("ORDER BY alias: %v", rows)
	if len(rows) != 5 {
		t.Errorf("ORDER BY alias: expected 5 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "A" {
		t.Errorf("ORDER BY alias: first should be A, got %v", rows[0][0])
	}
}
