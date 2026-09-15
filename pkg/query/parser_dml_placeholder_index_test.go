package query

// Regression tests for placeholder indexing in multi-row INSERT VALUES.
//
// The parser used to compute each row's placeholder offset as
// rowCount * len(columns) (slot-based). That is only correct when EVERY value
// slot is a placeholder; a row mixing literals with placeholders then produced
// non-contiguous indices in appearance order. Concretely,
//
//	INSERT INTO t (a,b) VALUES (1,?),(2,?)
//
// yielded indices {0, 2} for the two placeholders. The executor binds
// args[Index] (EvalExpression: "placeholder index out of range"; encodeRow:
// Index-first with a positional fallback), so with two args the second row
// silently bound args[0] and appended args[1] as an extra column value —
// silent data corruption. Placeholders bind in appearance order, so indices
// must be globally sequential: {0, 1}.

import (
	"slices"
	"testing"
)

func insertPlaceholderIndices(t *testing.T, sql string) []int {
	t.Helper()
	stmt, err := ParseStrict(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("parsed %T, want *InsertStmt", stmt)
	}
	var indices []int
	for _, row := range ins.Values {
		for _, expr := range row {
			for _, ph := range collectPlaceholders(expr) {
				indices = append(indices, ph.Index)
			}
		}
	}
	return indices
}

func TestInsertValuesPlaceholderIndicesAreAppearanceOrdered(t *testing.T) {
	indices := insertPlaceholderIndices(t, "INSERT INTO t (a,b) VALUES (1,?),(2,?)")
	want := []int{0, 1}
	if !slices.Equal(indices, want) {
		t.Fatalf("placeholder indices = %v, want %v (appearance order)", indices, want)
	}
}

func TestInsertValuesAllPlaceholderIndicesStayContiguous(t *testing.T) {
	// The fully-placeholderized form must keep working: (?,?),(?,?) -> 0,1,2,3.
	indices := insertPlaceholderIndices(t, "INSERT INTO t (a,b) VALUES (?,?),(?,?)")
	want := []int{0, 1, 2, 3}
	if !slices.Equal(indices, want) {
		t.Fatalf("placeholder indices = %v, want %v", indices, want)
	}
}

func TestInsertValuesNoColumnsPlaceholderIndicesAreAppearanceOrdered(t *testing.T) {
	// Without a column list the old code detected the slot count from the
	// first row's width, producing the same corruption for
	// VALUES (?,1),(?,2) -> indices {0, 2}.
	indices := insertPlaceholderIndices(t, "INSERT INTO t VALUES (?,1),(?,2)")
	want := []int{0, 1}
	if !slices.Equal(indices, want) {
		t.Fatalf("placeholder indices = %v, want %v (appearance order)", indices, want)
	}
}
