package engine

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// CREATE POLICY's USING/WITH CHECK expressions are rendered to a string by
// expressionToString and re-parsed by the RLS evaluator (security/rls.go
// parses policy.Expression). String literals must round-trip with SQL-standard
// quote doubling: the parser unescapes 'O''Brien' to O'Brien, the renderer
// must emit '' for the embedded quote, and the evaluator must unescape it
// back. Regression: the renderer emitted the bare quote ('O'Brien'), whose
// third quote the evaluator's quote-aware scanner treated as the literal's
// terminator — everything after it (the OR clause) was swallowed and the
// policy silently degraded to a bogus comparison that denied every row it
// grants.

func policyQuoteExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func policyQuoteNames(t *testing.T, db *DB, ctx context.Context, sql string) []string {
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
	return out
}

// TestPolicyQuoteFreeLiteralRoundTrips pins the unaffected shapes: the
// direct engine semantics of the authored OR expression, and a policy
// without an embedded quote end-to-end.
func TestPolicyQuoteFreeLiteralRoundTrips(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	bob := context.WithValue(ctx, security.RLSUserKey, "bob")

	policyQuoteExec(t, db, `CREATE TABLE docs_c (name VARCHAR(50), active INTEGER)`)
	policyQuoteExec(t, db, `INSERT INTO docs_c VALUES ('O''Brien', 1)`)
	policyQuoteExec(t, db, `INSERT INTO docs_c VALUES ('plain', 1)`)
	policyQuoteExec(t, db, `INSERT INTO docs_c VALUES ('plain', 0)`)

	direct := policyQuoteNames(t, db, bob, "SELECT name FROM docs_c WHERE name = 'O''Brien' OR active = 1")
	if len(direct) != 2 {
		t.Fatalf("direct control: got %v, want the 2 granted rows (O'Brien + plain/active=1)", direct)
	}

	policyQuoteExec(t, db, `ALTER TABLE docs_c ENABLE ROW LEVEL SECURITY`)
	policyQuoteExec(t, db, `CREATE POLICY docs_c_p ON docs_c FOR SELECT USING (name = 'OBrien' OR active = 1)`)
	got := policyQuoteNames(t, db, bob, "SELECT name FROM docs_c")
	if len(got) != 2 {
		t.Fatalf("control policy: got %v, want the 2 rows granted by (name = 'OBrien' OR active = 1)", got)
	}
}

// TestPolicyQuoteLiteralRoundTrips pins the fix: a policy literal containing
// a single quote must keep the authored semantics (rows granted by the OR
// clause stay visible).
func TestPolicyQuoteLiteralRoundTrips(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	bob := context.WithValue(ctx, security.RLSUserKey, "bob")

	policyQuoteExec(t, db, `CREATE TABLE docs (name VARCHAR(50), active INTEGER)`)
	policyQuoteExec(t, db, `INSERT INTO docs VALUES ('O''Brien', 1)`)
	policyQuoteExec(t, db, `INSERT INTO docs VALUES ('plain', 1)`)
	policyQuoteExec(t, db, `INSERT INTO docs VALUES ('plain', 0)`)
	policyQuoteExec(t, db, `ALTER TABLE docs ENABLE ROW LEVEL SECURITY`)
	policyQuoteExec(t, db, `CREATE POLICY docs_q ON docs FOR SELECT USING (name = 'O''Brien' OR active = 1)`)

	got := policyQuoteNames(t, db, bob, "SELECT name FROM docs")
	if len(got) != 2 {
		t.Fatalf("FAIL: policy with quote-containing literal returned %v, want the 2 granted rows (O'Brien + plain/active=1) — the rendered expression must quote-double the embedded quote so the re-parsed policy keeps the OR clause", got)
	}

	// The first clause matches its exact row: the quote-containing name is
	// visible through the policy, not just the active=1 rows.
	sawOBrien := false
	for _, n := range got {
		if n == "O'Brien" {
			sawOBrien = true
		}
	}
	if !sawOBrien {
		t.Fatalf("FAIL: the quote-containing row O'Brien is not visible through the policy (got %v) — the literal comparison must round-trip exactly", got)
	}
}
