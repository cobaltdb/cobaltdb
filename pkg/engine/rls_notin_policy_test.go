package engine

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// TestRLSPolicyNotInExpressionIsHonored pins that CREATE POLICY preserves the
// NOT of a NOT IN expression. expressionToString (pkg/engine/database_ddl.go)
// rendered *query.InExpr without its Not flag — `dept NOT IN ('secret')` was
// stored as `dept IN ('secret')` — and expressionRenderable never inspected
// Not, so the inverted policy passed validation. The RLS evaluator compiles
// the STORED string (security/rls.go compilePolicyInto parses
// policy.Expression and supports NOT IN), so enforcement was inverted with no
// error at DDL or query time: the rows the policy was written to HIDE became
// the only visible ones, and the rows it allowed were hidden — a silent
// authorization inversion.
//
// LikeExpr, IsNullExpr and BetweenExpr in the same renderer already honored
// their Not flag; the InExpr case now does too.
func TestRLSPolicyNotInExpressionIsHonored(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	seed := func(table, policyExpr string) {
		t.Helper()
		mustExec(t, db, "CREATE TABLE "+table+" (id INTEGER PRIMARY KEY, dept TEXT)")
		mustExec(t, db, "INSERT INTO "+table+" VALUES (1,'public'),(2,'public'),(3,'secret'),(4,'secret')")
		mustExec(t, db, "CREATE POLICY p_"+table+" ON "+table+" FOR SELECT USING ("+policyExpr+")")
	}
	idsAsAlice := func(table string) []int {
		t.Helper()
		alice := context.WithValue(context.Background(), security.RLSUserKey, "alice")
		rows, err := db.Query(alice, "SELECT id FROM "+table+" ORDER BY id")
		if err != nil {
			t.Fatalf("query %s as alice: %v", table, err)
		}
		defer rows.Close()
		var ids []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan %s: %v", table, err)
			}
			ids = append(ids, id)
		}
		return ids
	}
	equal := func(got []int, want ...int) bool {
		if len(got) != len(want) {
			return false
		}
		for i := range got {
			if got[i] != want[i] {
				return false
			}
		}
		return true
	}

	// Discriminator: NOT IN must hide the excluded rows.
	seed("t", "dept NOT IN ('secret')")
	// Control: the positive IN form must show only the listed rows.
	seed("t_in", "dept IN ('secret')")
	// Control: NOT LIKE — a Not shape the renderer already honored.
	seed("t_like", "dept NOT LIKE 'sec%'")

	if got := idsAsAlice("t"); !equal(got, 1, 2) {
		t.Fatalf("FAIL: NOT IN policy enforced inverted: alice sees %v, want [1 2]", got)
	}
	if got := idsAsAlice("t_in"); !equal(got, 3, 4) {
		t.Fatalf("control: IN policy: alice sees %v, want [3 4]", got)
	}
	if got := idsAsAlice("t_like"); !equal(got, 1, 2) {
		t.Fatalf("control: NOT LIKE policy: alice sees %v, want [1 2]", got)
	}
}
