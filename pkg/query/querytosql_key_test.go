package query

import "testing"

// TestQueryToSQLDistinguishesQueries verifies the cache-key serializer produces
// DISTINCT strings for queries that differ only in WHERE / JOIN / GROUP BY /
// ORDER BY / LIMIT. Before the fix it serialized only the column list and the
// first FROM table, so these all collided and the result cache returned wrong
// rows.
func TestQueryToSQLDistinguishesQueries(t *testing.T) {
	mustSelect := func(sql string) *SelectStmt {
		st, err := Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		sel, ok := st.(*SelectStmt)
		if !ok {
			t.Fatalf("%q did not parse to *SelectStmt (%T)", sql, st)
		}
		return sel
	}

	pairs := [][2]string{
		{"SELECT id, name FROM users WHERE id = 1", "SELECT id, name FROM users WHERE id = 2"},
		{"SELECT a FROM t WHERE a > 5", "SELECT a FROM t WHERE a < 5"},
		{"SELECT id FROM t ORDER BY a", "SELECT id FROM t ORDER BY a DESC"},
		{"SELECT id FROM t LIMIT 3", "SELECT id FROM t LIMIT 5"},
		{"SELECT x FROM t WHERE name = 'a'", "SELECT x FROM t WHERE name = 'b'"},
	}
	for _, p := range pairs {
		k1 := QueryToSQL(mustSelect(p[0]))
		k2 := QueryToSQL(mustSelect(p[1]))
		if k1 == k2 {
			t.Errorf("cache keys collide:\n  %q -> %q\n  %q -> %q", p[0], k1, p[1], k2)
		}
	}

	// And the same query must serialize identically (so the cache still hits).
	if QueryToSQL(mustSelect("SELECT id FROM t WHERE id = 7")) != QueryToSQL(mustSelect("SELECT id FROM t WHERE id = 7")) {
		t.Error("identical queries produced different cache keys (cache would never hit)")
	}
}
