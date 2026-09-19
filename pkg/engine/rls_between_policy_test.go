package engine

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// TestRLSPolicyBetweenExpressionIsHonored pins fail-closed policy rendering:
// CREATE POLICY ... USING (dept BETWEEN 2 AND 4) must restrict visible rows
// to the BETWEEN range. expressionToString (pkg/engine/database_ddl.go) had
// no *query.BetweenExpr case and rendered "", which executeCreatePolicy
// replaced with "TRUE" — silently turning the restrictive policy into
// allow-all (authorization bypass).
func TestRLSPolicyBetweenExpressionIsHonored(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{EnableRLS: true},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, `CREATE TABLE t (id INTEGER PRIMARY KEY, dept INTEGER)`)
	mustExec(t, db, `INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5)`)

	mustExec(t, db, `CREATE POLICY p ON t FOR SELECT USING (dept BETWEEN 2 AND 4)`)

	alice := context.WithValue(context.Background(), security.RLSUserKey, "alice")
	rows, err := db.Query(alice, `SELECT id FROM t ORDER BY id`)
	if err != nil {
		t.Fatalf("query as alice: %v", err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 3 || ids[0] != 2 || ids[1] != 3 || ids[2] != 4 {
		t.Fatalf("FAIL: BETWEEN policy not honored: alice sees %v, want [2 3 4]", ids)
	}
}
