package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// execJoinSelect parses and runs sql through the production Select path on c.
func execJoinSelect(t *testing.T, c *Catalog, sql string) [][]interface{} {
	t.Helper()
	stmt := parseScalarSelect(t, sql)
	_, rows, err := c.Select(stmt, nil)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return rows
}

// mustCreateJoinTable creates a single-column table structurally.
func mustCreateJoinTable(t *testing.T, c *Catalog, name, col string, integer bool) {
	t.Helper()
	colType := query.TokenText
	if integer {
		colType = query.TokenInteger
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: name,
		Columns: []*query.ColumnDef{
			{Name: col, Type: colType},
		},
	}); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}

// TestHashJoinMixedTypeEqualityMatches proves that the hash-join fast path
// matches the nested-loop oracle for mixed TEXT/NUMBER join columns.
// compareValues coerces mixed string/number pairs numerically ('01' = 1,
// '1.50' = 1.5) while comparing two strings exactly, but hashJoinKey keyed
// raw text against canonical numeric formatting ('01' vs '1', '1.50' vs
// '1.5') and the hash path never re-verified candidates — so the join
// silently dropped every row whose textual form differed from the other
// column's numeric form.
func TestHashJoinMixedTypeEqualityMatches(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateJoinTable(t, c, "a", "v", false)
	mustCreateJoinTable(t, c, "b", "v", true)
	for _, q := range []string{
		"INSERT INTO a VALUES ('01')",
		"INSERT INTO a VALUES ('1.50')",
		"INSERT INTO a VALUES ('x')",
		"INSERT INTO b VALUES (1)",
		"INSERT INTO b VALUES (2)",
	} {
		if _, err := c.ExecuteQuery(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	// Oracle semantics (nested loop / evaluateWhere → compareValues):
	// '01' = 1 matches numerically. The hash path must not drop it.
	rows := execJoinSelect(t, c, "SELECT a.v AS av, b.v AS bv FROM a JOIN b ON a.v = b.v")
	if len(rows) != 1 {
		t.Fatalf("mixed TEXT/INTEGER equality join returned %d rows (%v), want exactly the '01' = 1 match", len(rows), rows)
	}
	if got, _ := rows[0][0].(string); got != "01" {
		t.Errorf("joined a.v = %v, want '01'", rows[0][0])
	}
}

// TestHashJoinTextEqualityStaysExact pins the opposite invariant: two TEXT
// columns compare exactly ('0123' != '123' per compareValues), so the
// canonical-numeric keys used to fix the mixed-type join must not fabricate
// a match between distinct text values.
func TestHashJoinTextEqualityStaysExact(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateJoinTable(t, c, "t1", "v", false)
	mustCreateJoinTable(t, c, "t2", "v", false)
	for _, q := range []string{
		"INSERT INTO t1 VALUES ('0123')",
		"INSERT INTO t2 VALUES ('123')",
	} {
		if _, err := c.ExecuteQuery(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	rows := execJoinSelect(t, c, "SELECT t1.v AS x, t2.v AS y FROM t1 JOIN t2 ON t1.v = t2.v")
	if len(rows) != 0 {
		t.Errorf("TEXT/TEXT join returned %v, want no rows ('0123' != '123' as strings)", rows)
	}
}
