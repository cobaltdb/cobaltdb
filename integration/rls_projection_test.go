package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// setupRLSDocs creates a documents table owned by alice/bob with RLS enabled
// and a SELECT policy restricting rows to the current user.
func setupRLSDocs(t *testing.T) (*engine.DB, context.Context) {
	t.Helper()
	db, err := engine.Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()

	if _, err := db.Exec(ctx, `CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, owner TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO docs VALUES (1,'a-one','alice'),(2,'b-one','bob'),(3,'a-two','alice'),(4,'b-two','bob')`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(ctx, `ALTER TABLE docs ENABLE ROW LEVEL SECURITY`); err != nil {
		t.Fatalf("enable rls: %v", err)
	}
	if _, err := db.Exec(ctx, `CREATE POLICY docs_sel ON docs FOR SELECT USING (owner = CURRENT_USER)`); err != nil {
		t.Fatalf("create policy: %v", err)
	}

	aliceCtx := context.WithValue(ctx, security.RLSUserKey, "alice")
	return db, aliceCtx
}

func queryTitles(t *testing.T, db *engine.DB, ctx context.Context, sql string) []string {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, s)
	}
	return out
}

// queryTitleOwner runs a two-column (title, owner) query and returns
// "title/owner" pairs.
func queryTitleOwner(t *testing.T, db *engine.DB, ctx context.Context, sql string) []string {
	t.Helper()
	rows, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var title, owner string
		if err := rows.Scan(&title, &owner); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if owner != "alice" {
			t.Errorf("leaked row owned by %q: %q", owner, title)
		}
		out = append(out, title+"/"+owner)
	}
	return out
}

// TestRLSPolicyColumnNotInProjection verifies that an RLS policy referencing a
// column that is NOT in the SELECT list still filters correctly. Evaluating the
// policy against projected rows makes the policy column invisible, which
// silently drops every row.
func TestRLSPolicyColumnNotInProjection(t *testing.T) {
	db, aliceCtx := setupRLSDocs(t)
	defer func() { _ = db.Close() }()

	// `owner` is NOT selected, but the policy needs it.
	got := queryTitles(t, db, aliceCtx, `SELECT title FROM docs`)
	if len(got) != 2 {
		t.Errorf("SELECT title FROM docs: got %d rows %v, want 2 (alice's rows)", len(got), got)
	}
	for _, title := range got {
		if title != "a-one" && title != "a-two" {
			t.Errorf("leaked row not owned by alice: %q", title)
		}
	}
}

// TestRLSPolicyColumnInProjection is the control case: the policy column IS in
// the SELECT list, so post-projection evaluation happens to work.
func TestRLSPolicyColumnInProjection(t *testing.T) {
	db, aliceCtx := setupRLSDocs(t)
	defer func() { _ = db.Close() }()

	got := queryTitleOwner(t, db, aliceCtx, `SELECT title, owner FROM docs`)
	if len(got) != 2 {
		t.Errorf("SELECT title, owner FROM docs: got %d rows %v, want 2", len(got), got)
	}
}

// TestRLSAppliedBeforeLimit verifies LIMIT counts only rows the user may see.
// If RLS filters after LIMIT, the user gets fewer rows than requested even
// though more visible rows exist.
func TestRLSAppliedBeforeLimit(t *testing.T) {
	db, aliceCtx := setupRLSDocs(t)
	defer func() { _ = db.Close() }()

	got := queryTitleOwner(t, db, aliceCtx, `SELECT title, owner FROM docs LIMIT 2`)
	if len(got) != 2 {
		t.Errorf("LIMIT 2 with RLS: got %d rows %v, want 2 visible rows", len(got), got)
	}
}

// TestRLSWithoutPolicyColumnAcrossQueryShapes checks that a policy on a column
// absent from the SELECT list filters correctly across several query shapes,
// not just the simple scan.
func TestRLSWithoutPolicyColumnAcrossQueryShapes(t *testing.T) {
	db, aliceCtx := setupRLSDocs(t)
	defer func() { _ = db.Close() }()

	shapes := []struct {
		name string
		sql  string
		want int
	}{
		{"plain", `SELECT title FROM docs`, 2},
		{"order_by", `SELECT title FROM docs ORDER BY title`, 2},
		{"distinct", `SELECT DISTINCT title FROM docs`, 2},
		{"where", `SELECT title FROM docs WHERE id > 0`, 2},
		{"limit", `SELECT title FROM docs LIMIT 10`, 2},
		{"offset", `SELECT title FROM docs ORDER BY id LIMIT 10 OFFSET 1`, 1},
	}

	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			got := queryTitles(t, db, aliceCtx, s.sql)
			if len(got) != s.want {
				t.Errorf("%s: got %d rows %v, want %d", s.sql, len(got), got, s.want)
			}
			for _, title := range got {
				if title != "a-one" && title != "a-two" {
					t.Errorf("%s leaked non-alice row: %q", s.sql, title)
				}
			}
		})
	}
}

// TestRLSAggregateWithoutPolicyColumn verifies aggregates over a table whose
// policy column is not projected still see only permitted rows.
func TestRLSAggregateWithoutPolicyColumn(t *testing.T) {
	db, aliceCtx := setupRLSDocs(t)
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRow(aliceCtx, `SELECT COUNT(title) FROM docs`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("COUNT(title) = %d, want 2", n)
	}
}

// TestRLSCountStarRespectsPolicy verifies aggregates do not bypass RLS.
func TestRLSCountStarRespectsPolicy(t *testing.T) {
	db, aliceCtx := setupRLSDocs(t)
	defer func() { _ = db.Close() }()

	var n int
	if err := db.QueryRow(aliceCtx, `SELECT COUNT(*) FROM docs`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 2 {
		t.Errorf("COUNT(*) with RLS: got %d, want 2 (alice's rows only)", n)
	}
}
