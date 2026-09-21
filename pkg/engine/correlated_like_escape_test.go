package engine

import (
	"context"
	"testing"
)

// TestCorrelatedLikeEscapePreserved pins the field-completeness of the
// correlated-subquery outer-reference resolver: when resolveOuterRefsInExpr
// rebuilds a LikeExpr whose expr/pattern contained an outer reference, the
// rebuilt node must retain the ESCAPE clause. The rebuild at
// catalog_core.go resolveOuterRefsInExpr dropped Escape, silently changing
// the LIKE's escape semantics for any correlated LIKE ... ESCAPE.
//
// Dataset semantics: pattern '50#%' with ESCAPE '#' means literal
// "50" + escaped "%" → matches only "50%". Without the ESCAPE clause the
// "#" is a literal character and "%" is a wildcard → matches only "50#zz".
// The correct answer is exactly one row: "50%".
func TestCorrelatedLikeEscapePreserved(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mustExec := func(sql string) {
		t.Helper()
		if _, e := db.Exec(ctx, sql); e != nil {
			t.Fatalf("exec %q: %v", sql, e)
		}
	}
	names := func(sql string) []string {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v interface{}
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, v.(string))
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("rows close: %v", err)
		}
		return out
	}
	expectSingle := func(sql string, want string) {
		t.Helper()
		got := names(sql)
		if len(got) != 1 || got[0] != want {
			t.Fatalf("query %q returned %v, want exactly [%q]", sql, got, want)
		}
	}

	mustExec("CREATE TABLE n (name TEXT)")
	mustExec("CREATE TABLE k (id TEXT, patt TEXT)")
	mustExec("INSERT INTO n VALUES ('50%'), ('50#zz')")
	mustExec("INSERT INTO k VALUES ('k', '50#%')")

	// CONTROL: non-correlated LIKE ... ESCAPE routes outside the outer-ref
	// resolver and must keep working.
	expectSingle("SELECT name FROM n WHERE name LIKE '50#%' ESCAPE '#'", "50%")

	// PROBE: the same LIKE inside a correlated EXISTS subquery. The LIKE's
	// left side (n.name) is an outer reference, so the resolver rebuilds the
	// LikeExpr — and the rebuild must carry the ESCAPE clause through.
	expectSingle(
		"SELECT name FROM n WHERE EXISTS (SELECT 1 FROM k WHERE k.id = 'k' AND n.name LIKE k.patt ESCAPE '#')",
		"50%")
}
