package catalog

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestCTEColumnListJoinUsesDeclaredNames pins that a materialized CTE with an
// explicit column list is resolved through its DECLARED names when it is the
// main FROM table of a JOIN.
//
// ExecuteCTE registers a plain-SELECT CTE twice: as a view over the inner
// SELECT (inner column names) and, when a column list is present, as a
// materialized cteResults entry (declared names). In selectLockedInternal's
// dispatch, handleCTEResult declines JOIN statements, and the fall-through
// used to reach tryViewSelect — which resolved the FROM through the
// registered VIEW and its inner column names. A join whose ON clause
// referenced a declared name then matched nothing on the main side and the
// query silently returned ZERO ROWS with no error.
//
// The view lookup is now skipped whenever a CTE result exists for the FROM
// name, so executeSelectWithJoin resolves it through the materialized result
// (loadMainTableRows checks cteResults before views — the same order the
// join-side loader already used).
func TestCTEColumnListJoinUsesDeclaredNames(t *testing.T) {
	c := newTestCatalog(t)
	for _, sql := range []string{
		`CREATE TABLE t (n INTEGER)`,
		`INSERT INTO t VALUES (1),(2),(3)`,
		`CREATE TABLE t2 (n INTEGER, tag TEXT)`,
		`INSERT INTO t2 VALUES (1,'a'),(2,'b')`,
	} {
		if _, err := c.ExecuteQuery(sql); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}

	tags := func(sql string) []string {
		t.Helper()
		res, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Fatalf("query %q: %v", sql, err)
		}
		var out []string
		for _, row := range res.Rows {
			if len(row) != 1 {
				t.Fatalf("query %q: row %v, want one column", sql, row)
			}
			out = append(out, fmt.Sprint(row[0]))
		}
		return out
	}
	equal := func(got []string, want ...string) bool {
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

	// Discriminator: column-list CTE as the main FROM table of a JOIN.
	if got := tags(`WITH c(x) AS (SELECT n FROM t) SELECT t2.tag FROM c JOIN t2 ON c.x = t2.n ORDER BY t2.tag`); !equal(got, "a", "b") {
		t.Fatalf("FAIL: column-list CTE as FROM in JOIN returned %v, want [a b] — the join resolved the CTE through its registered view (inner names)", got)
	}

	// Control: no column list — resolves via the view, inner names.
	if got := tags(`WITH c AS (SELECT n FROM t) SELECT t2.tag FROM c JOIN t2 ON c.n = t2.n ORDER BY t2.tag`); !equal(got, "a", "b") {
		t.Fatalf("control: no-column-list CTE join returned %v, want [a b]", got)
	}

	// Control: the same column-list CTE on the RIGHT side of the join.
	if got := tags(`WITH c(x) AS (SELECT n FROM t) SELECT t2.tag FROM t2 JOIN c ON c.x = t2.n ORDER BY t2.tag`); !equal(got, "a", "b") {
		t.Fatalf("control: right-side column-list CTE join returned %v, want [a b]", got)
	}

	// Control: the same column-list CTE without a join.
	if got := tags(`WITH c(x) AS (SELECT n FROM t) SELECT x FROM c WHERE x > 1 ORDER BY x`); !equal(got, "2", "3") {
		t.Fatalf("control: no-join column-list CTE returned %v, want [2 3]", got)
	}

	// Secondary: a CTE shadowing a real VIEW must win in the join shape too.
	// (Catalog.ExecuteQuery rejects DDL, so the view is registered through
	// the catalog API on the parsed statement.)
	if _, err := c.ExecuteQuery(`CREATE TABLE base (v INTEGER)`); err != nil {
		t.Fatalf("exec base: %v", err)
	}
	if _, err := c.ExecuteQuery(`INSERT INTO base VALUES (7)`); err != nil {
		t.Fatalf("insert base: %v", err)
	}
	viewSQL := `CREATE VIEW sv AS SELECT v FROM base`
	parsed, err := query.Parse(viewSQL)
	if err != nil {
		t.Fatalf("parse view: %v", err)
	}
	cv, ok := parsed.(*query.CreateViewStmt)
	if !ok || cv.Query == nil {
		t.Fatalf("expected *query.CreateViewStmt, got %T", parsed)
	}
	if err := c.CreateViewSQL("sv", cv.Query, viewSQL); err != nil {
		t.Fatalf("create view: %v", err)
	}
	if got := tags(`WITH sv(x) AS (SELECT n FROM t) SELECT t2.tag FROM sv JOIN t2 ON sv.x = t2.n ORDER BY t2.tag`); !equal(got, "a", "b") {
		t.Fatalf("shadowing CTE join returned %v, want [a b]", got)
	}
	// The shadowed view must be intact afterwards.
	if got := tags(`SELECT v FROM sv`); !equal(got, "7") {
		t.Fatalf("view intact after shadow: returned %v, want [7]", got)
	}
}
