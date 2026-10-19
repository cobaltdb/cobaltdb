package engine

import (
	"context"
	"sort"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// TestRLSIdentityLessQueryIsDenied pins that a query whose context carries NO
// user identity is DENIED on a row-level-security table, end to end through
// the real engine path (db.Query -> executeSelect -> catalog).
//
// Regression for the RLS fail-open defect: five sites in pkg/catalog gated
// enforcement on a non-empty identity (`if user == "" { return true }`), so a
// caller that omitted the user skipped row-level security outright. With
// `USING (owner = current_user())`, an identity-less SELECT returned EVERY row:
//
//	db.Query(context.Background(), "SELECT id, owner FROM docs")
//	  -> [1 alice] [2 bob] [3 alice]      // bob's row leaked across tenants
//
// Basis for the expected behaviour: security.Manager's documented contract -
// "RLS enabled but no policies = deny all" - so an absent identity cannot
// evaluate to allow. The one sanctioned exception is an EXPLICIT privileged
// bypass (security.RLSBypassKey) that `cobaltdb-cli dump`/`restore` set; that
// is asserted separately below so the fix cannot be trivially "deny
// everything".
func TestRLSIdentityLessQueryIsDenied(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, `CREATE TABLE docs (id INTEGER PRIMARY KEY, owner TEXT, body TEXT)`)
	mustExec(t, db, `INSERT INTO docs VALUES (1, 'alice', 'a1'), (2, 'bob', 'b1'), (3, 'alice', 'a2')`)
	mustExec(t, db, `ALTER TABLE docs ENABLE ROW LEVEL SECURITY`)
	mustExec(t, db, `CREATE POLICY docs_owner ON docs FOR SELECT USING (owner = current_user())`)

	// A table WITHOUT row-level security: an identity-less query must still
	// read it. This scopes the fix - denial must follow the policy, not turn
	// every anonymous query into a denial.
	mustExec(t, db, `CREATE TABLE public_notes (id INTEGER PRIMARY KEY, note TEXT)`)
	mustExec(t, db, `INSERT INTO public_notes VALUES (1, 'hello')`)

	// --- the defect: no identity at all ------------------------------------
	if got := queryIDs(t, db, context.Background(), "SELECT id, owner FROM docs ORDER BY id"); len(got) != 0 {
		t.Errorf("identity-less SELECT on an RLS table returned ids %v, want none: "+
			"an absent identity must fail CLOSED, not bypass row-level security "+
			"(cross-tenant row leak)", got)
	}

	// --- controls: the same query WITH an identity still works -------------
	// current_user() is identity-dependent, so each tenant sees only its rows.
	aliceCtx := context.WithValue(context.Background(), security.RLSUserKey, "alice")
	if got := queryIDs(t, db, aliceCtx, "SELECT id, owner FROM docs ORDER BY id"); !equalIDs(got, []int64{1, 3}) {
		t.Errorf("alice sees ids %v, want [1 3] (rows she owns)", got)
	}

	bobCtx := context.WithValue(context.Background(), security.RLSUserKey, "bob")
	if got := queryIDs(t, db, bobCtx, "SELECT id, owner FROM docs ORDER BY id"); !equalIDs(got, []int64{2}) {
		t.Errorf("bob sees ids %v, want [2] (his own row only, never alice's)", got)
	}

	// --- privileged bypass: the one sanctioned anonymous read --------------
	// `cobaltdb-cli dump`/`restore` operate on the PHYSICAL database, not on
	// any single tenant's view of it, and must see every row.
	bypassCtx := context.WithValue(context.Background(), security.RLSBypassKey, true)
	if got := queryIDs(t, db, bypassCtx, "SELECT id, owner FROM docs ORDER BY id"); !equalIDs(got, []int64{1, 2, 3}) {
		t.Errorf("privileged bypass sees ids %v, want [1 2 3] (bypass must read every row)", got)
	}

	// --- not a blanket deny: a table RLS is not enabled on ----------------
	if got := queryIDs(t, db, context.Background(), "SELECT id FROM public_notes"); !equalIDs(got, []int64{1}) {
		t.Errorf("identity-less SELECT on a NON-RLS table returned ids %v, want [1]: "+
			"RLS denial must follow the policy, not reject every anonymous query", got)
	}
}

// TestRLSIdentityLessWriteIsDenied is the write-side counterpart: WITH CHECK
// enforcement must also fail closed, or an identity-less caller could write
// rows the policy forbids.
func TestRLSIdentityLessWriteIsDenied(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec(t, db, `CREATE TABLE wdocs (id INTEGER PRIMARY KEY, owner TEXT)`)
	mustExec(t, db, `ALTER TABLE wdocs ENABLE ROW LEVEL SECURITY`)
	mustExec(t, db, `CREATE POLICY wdocs_owner ON wdocs FOR INSERT WITH CHECK (owner = current_user())`)

	bobCtx := context.WithValue(context.Background(), security.RLSUserKey, "bob")
	aliceCtx := context.WithValue(context.Background(), security.RLSUserKey, "alice")

	// bob may not write a row owned by alice.
	if _, err := db.Exec(bobCtx, `INSERT INTO wdocs VALUES (1, 'alice')`); err == nil {
		t.Errorf("bob's INSERT of an alice-owned row was ALLOWED; WITH CHECK must deny it")
	}
	// bob may write his own row.
	if _, err := db.Exec(bobCtx, `INSERT INTO wdocs VALUES (2, 'bob')`); err != nil {
		t.Errorf("bob's own row must be accepted, got: %v", err)
	}
	// alice may write her own row.
	if _, err := db.Exec(aliceCtx, `INSERT INTO wdocs VALUES (3, 'alice')`); err != nil {
		t.Errorf("alice's own row must be accepted, got: %v", err)
	}
	// The defect on the write side: no identity at all must be DENIED, not
	// waved through by checkRowCheckLocked's old `if user == "" { return true }`.
	if _, err := db.Exec(context.Background(), `INSERT INTO wdocs VALUES (4, 'alice')`); err == nil {
		t.Errorf("identity-less INSERT was ALLOWED; an absent identity must fail CLOSED on writes too")
	}
}

// queryIDs runs a query and returns the values of the FIRST projected column
// (the id), sorted, so cases compare by value rather than by position.
func queryIDs(t *testing.T, db *DB, ctx context.Context, sql string) []int64 {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer rows.Close()

	var ids []int64
	cols := rows.Columns()
	for rows.Next() {
		// Scan every projected column and read the first. Scanning only the id
		// would be a column-count mismatch on `SELECT id, owner`.
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		id, ok := vals[0].(int64)
		if !ok {
			t.Fatalf("first column is %T, want int64", vals[0])
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func equalIDs(got, want []int64) bool {
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
