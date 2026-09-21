package engine

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// Regression for the expressionToString/LikeExpr.Escape gap: CREATE POLICY's
// USING clause parses a full expression, so LIKE ... ESCAPE reaches
// executeCreatePolicy. expressionToString's LikeExpr case rendered only
// "expr [NOT] LIKE pattern" — dropping the ESCAPE clause — and the rendered
// string is what the RLS evaluator re-parses (security/rls.go parses
// policy.Expression at evaluation time). The stored policy therefore enforced
// default-escape semantics instead of the authored ones, silently filtering
// the wrong rows.
//
// Semantics: pattern 'A#%' with ESCAPE '#' is the literal "A%" (the '#' is
// consumed as the escape marker) → matches only the row "A%". Rendered
// WITHOUT the ESCAPE clause, "#" is a literal and "%" a wildcard → matches
// only "A#zz".
func TestCreatePolicyLikeEscapePreservedInStoredExpression(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mustExec := func(sql string) {
		t.Helper()
		if _, e := db.Exec(ctx, sql); e != nil {
			t.Fatalf("exec %q: %v", sql, e)
		}
	}
	names := func(ctx context.Context, sql string) []string {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var v string
			if err := rows.Scan(&v); err != nil {
				t.Fatalf("scan: %v", err)
			}
			out = append(out, v)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("rows close: %v", err)
		}
		return out
	}

	mustExec(`CREATE TABLE docs (name VARCHAR(50))`)
	mustExec(`INSERT INTO docs VALUES ('A%'), ('A#zz')`)

	// CONTROL: the same LIKE evaluated directly (no RLS) honors the authored
	// ESCAPE clause — the LIKE engine is correct; only the policy render is
	// under test.
	bob := context.WithValue(ctx, security.RLSUserKey, "bob")
	direct := names(bob, "SELECT name FROM docs WHERE name LIKE 'A#%' ESCAPE '#'")
	if len(direct) != 1 || direct[0] != "A%" {
		t.Fatalf("direct LIKE ... ESCAPE control returned %v, want [A%%]", direct)
	}

	mustExec(`ALTER TABLE docs ENABLE ROW LEVEL SECURITY`)
	mustExec(`CREATE POLICY docs_like ON docs FOR SELECT USING (name LIKE 'A#%' ESCAPE '#')`)

	// PROBE: rows visible through the policy. The policy must enforce the
	// authored semantics (literal "A%") → exactly the row "A%".
	got := names(bob, "SELECT name FROM docs")
	if len(got) != 1 || got[0] != "A%" {
		t.Fatalf("RLS policy with LIKE ... ESCAPE filtered to %v, want exactly [A%%] (stored policy lost the ESCAPE clause and used default-escape semantics)", got)
	}
}
