package query

import "testing"

// TestQueryToSQLDistinguishesUnionDerivedTables verifies the cache-key
// serializer includes the body of derived tables that hold set operations
// (UNION/INTERSECT/EXCEPT). tableRefToString serialized the plain-subquery
// variant (TableRef.Subquery) but silently dropped the set-op variant
// (TableRef.SubqueryStmt), so any two queries whose FROM was a union-derived
// table with the same alias collided on one cache key and the result cache
// served the first query's rows to the second.
func TestQueryToSQLDistinguishesUnionDerivedTables(t *testing.T) {
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

	k1 := QueryToSQL(mustSelect("SELECT * FROM (SELECT 1 AS a UNION SELECT 2 AS a) AS t"))
	k2 := QueryToSQL(mustSelect("SELECT * FROM (SELECT 99 AS a UNION SELECT 98 AS a) AS t"))
	if k1 == k2 {
		t.Fatalf("cache keys collide for distinct union-derived tables:\n  k1=%q\n  k2=%q", k1, k2)
	}
	if ContainsUncacheableExpr(k1) || ContainsUncacheableExpr(k2) {
		t.Fatalf("union-derived tables must serialize deterministically, got marker:\n  k1=%q\n  k2=%q", k1, k2)
	}

	// Plain derived tables must keep their existing faithful serialization.
	plain := QueryToSQL(mustSelect("SELECT * FROM (SELECT 1 AS a) AS t"))
	if ContainsUncacheableExpr(plain) {
		t.Fatalf("plain derived table unexpectedly uncacheable: %q", plain)
	}
	if want := "SELECT * FROM (SELECT 1 AS a) t"; plain != want {
		t.Errorf("plain derived table key = %q, want %q", plain, want)
	}

	// INTERSECT/EXCEPT bodies must be distinguished too.
	k3 := QueryToSQL(mustSelect("SELECT * FROM (SELECT 1 AS a INTERSECT SELECT 2 AS a) AS t"))
	k4 := QueryToSQL(mustSelect("SELECT * FROM (SELECT 1 AS a EXCEPT SELECT 2 AS a) AS t"))
	if k3 == k4 {
		t.Fatalf("cache keys collide for INTERSECT vs EXCEPT derived tables:\n  k3=%q", k3)
	}

	// And the same query must still serialize identically (cache hits).
	if again := QueryToSQL(mustSelect("SELECT * FROM (SELECT 1 AS a UNION SELECT 2 AS a) AS t")); again != k1 {
		t.Errorf("identical query produced different key:\n  first=%q\n  again=%q", k1, again)
	}
}
