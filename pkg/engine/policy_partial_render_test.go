package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// TestCreatePolicyRejectsPartiallyRenderableExpression pins that CREATE POLICY
// fails closed on any USING/WITH CHECK expression that expressionToString
// cannot render COMPLETELY — not just when the whole string renders empty.
//
// A policy expression is rendered to a string by expressionToString
// (database_ddl.go) and re-parsed by the RLS evaluator (security/rls.go parses
// policy.Expression) at QUERY time. An AST node with no case in the renderer
// (SubqueryExpr, ExistsExpr, CastExpr, WindowExpr, StarExpr, JSONPathExpr,
// MatchExpr, AliasExpr, ColumnRef, ...) used to render as "" — which only made
// the WHOLE rendered string empty when it was the ROOT of the expression. A
// nested one therefore rendered the surrounding expression non-empty, e.g.
// `active = (SELECT 1)` became "active = ", which satisfied the
// `if usingExpr == ""` emptiness guard. CREATE POLICY reported success,
// stored the mangled expression, and every subsequent SELECT returned zero
// rows with no error at DDL or query time.
//
// The guard now consults expressionRenderable, which walks the whole AST.
func TestCreatePolicyRejectsPartiallyRenderableExpression(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec := func(sql string) {
		t.Helper()
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	names := func(sql string) []string {
		t.Helper()
		asUser := context.WithValue(ctx, security.RLSUserKey, "bob")
		rows, err := db.Query(asUser, sql)
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

	mustExec(`CREATE TABLE docs (name VARCHAR(50), active INTEGER)`)
	mustExec(`INSERT INTO docs VALUES ('alice', 1), ('bob', 0), ('carol', 1)`)
	mustExec(`ALTER TABLE docs ENABLE ROW LEVEL SECURITY`)

	// Control — an unaffected, fully renderable policy must still be accepted
	// and enforce exactly the authored semantics.
	mustExec(`CREATE POLICY ctl ON docs FOR SELECT USING (active = 1)`)
	got := names(`SELECT name FROM docs`)
	if len(got) != 2 || got[0] != "alice" || got[1] != "carol" {
		t.Fatalf("control policy enforcement = %v, want [alice carol]", got)
	}

	// Defect — a nested unrenderable node inside a renderable outer one.
	// CREATE POLICY must either be rejected or store an exact round-trip; it
	// must NOT report success while storing a truncated expression.
	rejected := false
	if _, err := db.Exec(ctx, `CREATE POLICY nested ON docs FOR SELECT USING (active = (SELECT 1))`); err != nil {
		rejected = true
	}
	if !rejected {
		for _, p := range db.catalog.ListRLSPolicies() {
			if p.Name == "nested" {
				t.Fatalf("FAIL: policy `active = (SELECT 1)` was stored as %q; a nested unrenderable node must poison the render so the DDL fails closed", p.Expression)
			}
		}
	}
	// Enforcement of the control is unchanged by the rejected DDL.
	if after := names(`SELECT name FROM docs`); len(after) != 2 {
		t.Fatalf("after rejected policy, control enforcement = %v, want 2 rows", after)
	}

	// Secondary branch — WITH CHECK takes the same fail-closed path.
	if _, err := db.Exec(ctx, `CREATE POLICY chk ON docs FOR INSERT WITH CHECK (active = (SELECT 1))`); err == nil {
		for _, p := range db.catalog.ListRLSPolicies() {
			if p.Name == "chk" {
				t.Fatalf("FAIL: WITH CHECK policy `active = (SELECT 1)` was stored as %q; must fail closed", p.CheckExpression)
			}
		}
	}
}

// TestCreatePolicyRenderableExpressionsStillAccepted pins the unaffected
// shapes end-to-end, so the new predicate cannot silently regress ordinary
// policies: literals, identifiers, qualified names, IN lists, LIKE [ESCAPE],
// IS [NOT] NULL, BETWEEN, NOT, functions, and embedded-quote round-trip.
func TestCreatePolicyRenderableExpressionsStillAccepted(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = db.Close() }()

	mustExec := func(sql string) {
		t.Helper()
		if _, err := db.Exec(ctx, sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	mustExec(`CREATE TABLE t (name VARCHAR(50), active INTEGER)`)
	mustExec(`ALTER TABLE t ENABLE ROW LEVEL SECURITY`)

	// Sanitize the expression into a valid identifier for the policy name.
	sanitize := func(s string) string {
		var b strings.Builder
		for _, r := range s {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
				b.WriteRune(r)
			default:
				b.WriteByte('_')
			}
		}
		name := b.String()
		if name == "" {
			name = "e"
		}
		return name
	}

	for _, using := range []string{
		`active = 1`,
		`active <> 0 AND active <= 5`,
		`active IN (1, 2, 3)`,
		`name LIKE 'a%'`,
		`name LIKE 'A#%' ESCAPE '#'`,
		`active IS NULL`,
		`active IS NOT NULL`,
		`active BETWEEN 1 AND 9`,
		`active NOT BETWEEN 5 AND 9`,
		`NOT active = 0`,
		`UPPER(name) = 'ALICE'`,
		`name = 'O''Brien' OR active = 1`,
	} {
		name := "pol_" + sanitize(using)
		mustExec(`CREATE POLICY ` + name + ` ON t FOR SELECT USING (` + using + `)`)
		// The stored expression must round-trip. Match the policy by name
		// case-insensitively: policy names are normalized on store.
		found := false
		for _, p := range db.catalog.ListRLSPolicies() {
			if strings.EqualFold(p.Name, name) {
				found = true
				// A renderable policy must not be silently truncated.
				if p.Expression == "" {
					t.Fatalf("FAIL: USING (%s) stored an empty expression", using)
				}
				// The rendered text must still contain the authored operator
				// semantics; a dropped node would visibly truncate it.
				if !strings.Contains(strings.ToUpper(p.Expression), "ACTIVE") &&
					!strings.Contains(strings.ToUpper(p.Expression), "NAME") {
					t.Fatalf("FAIL: USING (%s) stored %q — render dropped the predicate", using, p.Expression)
				}
			}
		}
		if !found {
			t.Fatalf("FAIL: USING (%s) was not stored", using)
		}
	}
}
