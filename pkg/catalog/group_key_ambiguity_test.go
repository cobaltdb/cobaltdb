package catalog

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// mustCreateJoinGroupTables creates t1(k INTEGER, a TEXT) and
// t2(k INTEGER, b TEXT) for the join GROUP BY collision proof.
func mustCreateJoinGroupTables(t *testing.T, c *Catalog) {
	t.Helper()
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "t1",
		Columns: []*query.ColumnDef{
			{Name: "k", Type: query.TokenInteger},
			{Name: "a", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("create t1: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "t2",
		Columns: []*query.ColumnDef{
			{Name: "k", Type: query.TokenInteger},
			{Name: "b", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("create t2: %v", err)
	}
}

// assertPipeGroups verifies that the GROUP BY result holds exactly the two
// distinct tuples ('x|y','z') and ('x','y|z'), each counted once. Observed
// pairs are keyed with %q quoting so pipe-vs-space distinctions are visible.
func assertPipeGroups(t *testing.T, rows [][]interface{}) {
	t.Helper()
	if len(rows) != 2 {
		t.Fatalf("GROUP BY produced %d rows (%v), want 2 distinct groups", len(rows), rows)
	}
	seen := map[string]int{}
	for _, row := range rows {
		if len(row) != 3 {
			t.Fatalf("GROUP BY row has %d columns (%v), want [a, b, count]", len(row), rows)
		}
		key := fmt.Sprintf("%q|%q", row[0], row[1])
		count := fmt.Sprint(row[2])
		var n int
		if _, err := fmt.Sscan(count, &n); err != nil {
			t.Fatalf("count %v is not numeric", row[2])
		}
		seen[key] = n
	}
	for _, key := range []string{`"x|y"|"z"`, `"x"|"y|z"`} {
		if seen[key] != 1 {
			t.Errorf("group %s count = %d, want 1", key, seen[key])
		}
	}
}

// TestJoinGroupByPipeCollision proves the JOIN+GROUP BY key builder is
// injective. executeSelectWithJoinAndGroupBy concatenated raw
// ValueToStringKey components with a literal "|", so the joined tuples
// ('x|y','z') and ('x','y|z') both keyed as "x|y|z" and merged into one
// group with COUNT = 2. (Single-table and view GROUP BY route through the
// typeTaggedKey+\x00 builder and were never affected.)
func TestJoinGroupByPipeCollision(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateJoinGroupTables(t, c)
	for _, q := range []string{
		"INSERT INTO t1 VALUES (1, 'x|y')",
		"INSERT INTO t1 VALUES (2, 'x')",
		"INSERT INTO t2 VALUES (1, 'z')",
		"INSERT INTO t2 VALUES (2, 'y|z')",
	} {
		if _, err := c.ExecuteQuery(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	rows := execJoinSelect(t, c, "SELECT t1.a AS a, t2.b AS b, COUNT(*) AS c FROM t1 JOIN t2 ON t1.k = t2.k GROUP BY t1.a, t2.b")
	assertPipeGroups(t, rows)
}
