package optimizer

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func mustParseSelect(t *testing.T, sql string) *query.SelectStmt {
	t.Helper()
	stmt, err := query.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	sel, ok := stmt.(*query.SelectStmt)
	if !ok {
		t.Fatalf("expected *query.SelectStmt, got %T", stmt)
	}
	return sel
}

func joinTables(stmt *query.SelectStmt) []string {
	names := make([]string, 0, len(stmt.Joins))
	for _, j := range stmt.Joins {
		if j.Table != nil {
			names = append(names, j.Table.Name)
		} else {
			names = append(names, "?")
		}
	}
	return names
}

// TestOptimizeDoesNotMutateCallerStatement verifies Optimize deep-copies the
// statement before reordering; the caller's AST (possibly shared via the plan
// cache) must remain untouched.
func TestOptimizeDoesNotMutateCallerStatement(t *testing.T) {
	stmt := mustParseSelect(t,
		"SELECT * FROM a INNER JOIN big ON a.id = big.a_id INNER JOIN small ON a.id = small.a_id")

	// Give the tables statistics that force a reorder (big table first in
	// SQL, but 'small'... selectivity is type-based; both INNER. Force a
	// difference via row counts.)
	o := New(nil, nil)
	o.UpdateStatistics("big", &TableStatistics{TableName: "big", RowCount: 1000000})
	o.UpdateStatistics("small", &TableStatistics{TableName: "small", RowCount: 1})

	original := joinTables(stmt)
	originalPtrs := make([]*query.JoinClause, len(stmt.Joins))
	copy(originalPtrs, stmt.Joins)

	optimized, err := o.Optimize(stmt)
	if err != nil {
		t.Fatalf("optimize: %v", err)
	}

	// The caller's statement must be byte-for-byte untouched.
	after := joinTables(stmt)
	for i := range original {
		if original[i] != after[i] {
			t.Fatalf("caller AST mutated: joins %v -> %v", original, after)
		}
		if stmt.Joins[i] != originalPtrs[i] {
			t.Fatalf("caller join slice mutated at %d", i)
		}
	}

	// The result, when a reorder applies, must be a distinct object.
	if optimized == stmt {
		t.Fatal("Optimize returned the caller's statement instead of a copy")
	}
	if len(optimized.Joins) != len(stmt.Joins) {
		t.Fatalf("optimized join count changed: %d vs %d", len(optimized.Joins), len(stmt.Joins))
	}
}

// TestReorderJoinsPreservesOuterJoinPositions verifies only consecutive INNER
// joins are reordered; LEFT/RIGHT joins keep their positions (moving them
// changes query semantics).
func TestReorderJoinsPreservesOuterJoinPositions(t *testing.T) {
	stmt := mustParseSelect(t, `SELECT * FROM a
		INNER JOIN i1 ON a.x = i1.x
		INNER JOIN i2 ON a.x = i2.x
		LEFT JOIN l1 ON a.x = l1.x
		INNER JOIN i3 ON a.x = i3.x
		RIGHT JOIN r1 ON a.x = r1.x`)

	o := New(nil, nil)
	// Statistics that make i2 more selective than i1 (RowCount > 10000
	// multiplies selectivity by 0.8, so give i2 stats and i1 none).
	o.UpdateStatistics("i2", &TableStatistics{TableName: "i2", RowCount: 50000})
	o.UpdateStatistics("i1", &TableStatistics{TableName: "i1", RowCount: 10})
	o.UpdateStatistics("i3", &TableStatistics{TableName: "i3", RowCount: 10})
	o.UpdateStatistics("l1", &TableStatistics{TableName: "l1", RowCount: 50000})
	o.UpdateStatistics("r1", &TableStatistics{TableName: "r1", RowCount: 50000})

	optimized, err := o.Optimize(stmt)
	if err != nil {
		t.Fatalf("optimize: %v", err)
	}

	tables := joinTables(optimized)
	if len(tables) != 5 {
		t.Fatalf("expected 5 joins, got %v", tables)
	}
	// Outer joins must not move.
	if tables[2] != "l1" {
		t.Fatalf("LEFT JOIN moved: %v", tables)
	}
	if tables[4] != "r1" {
		t.Fatalf("RIGHT JOIN moved: %v", tables)
	}
	// The inner run before the LEFT join may only contain i1/i2, and the one
	// after may only contain i3.
	if !(tables[0] == "i1" || tables[0] == "i2") || !(tables[1] == "i1" || tables[1] == "i2") || tables[0] == tables[1] {
		t.Fatalf("first inner run corrupted: %v", tables)
	}
	if tables[3] != "i3" {
		t.Fatalf("inner join crossed an outer join boundary: %v", tables)
	}
	// i2 (more selective: 0.1*0.8=0.08) must sort before i1 (0.1) within its run.
	if tables[0] != "i2" {
		t.Fatalf("expected most selective inner join first in run, got %v", tables)
	}

	// Original statement untouched.
	orig := joinTables(stmt)
	want := []string{"i1", "i2", "l1", "i3", "r1"}
	for i := range want {
		if orig[i] != want[i] {
			t.Fatalf("caller AST mutated: %v", orig)
		}
	}
}
