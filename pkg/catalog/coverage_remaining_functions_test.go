package catalog

import (
	"math"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// ==================== rowToMap tests ====================

func TestRowToMap(t *testing.T) {
	columns := []ColumnDef{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	}
	row := []interface{}{int64(1), "alice", "alice@test.com"}

	m := rowToMap(columns, row)
	if len(m) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(m))
	}
	if m["id"] != int64(1) {
		t.Errorf("expected id=1, got %v", m["id"])
	}
	if m["name"] != "alice" {
		t.Errorf("expected name='alice', got %v", m["name"])
	}
	if m["email"] != "alice@test.com" {
		t.Errorf("expected email='alice@test.com', got %v", m["email"])
	}
}

func TestRowToMapEmptyColumns(t *testing.T) {
	m := rowToMap(nil, []interface{}{})
	if len(m) != 0 {
		t.Errorf("expected empty map, got %d entries", len(m))
	}
}

func TestRowToMapShortRow(t *testing.T) {
	columns := []ColumnDef{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	}
	row := []interface{}{int64(1)} // shorter than columns

	m := rowToMap(columns, row)
	if len(m) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m))
	}
	if m["id"] != int64(1) {
		t.Errorf("expected id=1, got %v", m["id"])
	}
}

func TestRowToMapExtraValues(t *testing.T) {
	columns := []ColumnDef{
		{Name: "id"},
	}
	row := []interface{}{int64(1), "extra"} // longer than columns

	m := rowToMap(columns, row)
	if len(m) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(m))
	}
}

// ==================== getReturningColumns tests ====================

func TestGetReturningColumns(t *testing.T) {
	c := &Catalog{}

	tests := []struct {
		name  string
		exprs []query.Expression
		want  []string
	}{
		{
			name:  "column ref",
			exprs: []query.Expression{&query.ColumnRef{Column: "name"}},
			want:  []string{"name"},
		},
		{
			name:  "wildcard column ref",
			exprs: []query.Expression{&query.ColumnRef{Column: "*"}},
			want:  []string{"*"},
		},
		{
			name:  "qualified identifier",
			exprs: []query.Expression{&query.QualifiedIdentifier{Column: "t.name"}},
			want:  []string{"t.name"},
		},
		{
			name:  "identifier",
			exprs: []query.Expression{&query.Identifier{Name: "col1"}},
			want:  []string{"col1"},
		},
		{
			name:  "default expression",
			exprs: []query.Expression{&query.NumberLiteral{Value: 42}},
			want:  []string{"expr_0"},
		},
		{
			name: "mixed",
			exprs: []query.Expression{
				&query.ColumnRef{Column: "id"},
				&query.Identifier{Name: "name"},
				&query.NumberLiteral{Value: 1},
			},
			want: []string{"id", "name", "expr_2"},
		},
		{
			name:  "empty",
			exprs: []query.Expression{},
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.getReturningColumns(tt.exprs)
			if len(got) != len(tt.want) {
				t.Fatalf("expected %d columns, got %d: %v", len(tt.want), len(got), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("column[%d]: expected %q, got %q", i, tt.want[i], got[i])
				}
			}
		})
	}
}

// ==================== ClearReturning tests ====================

func TestClearReturning(t *testing.T) {
	c := &Catalog{}
	c.setLastReturning([][]interface{}{{int64(1)}}, []string{"id"})

	c.ClearReturning()

	if rows := c.GetLastReturningRows(); rows != nil {
		t.Error("expected nil rows after ClearReturning")
	}
	if cols := c.GetLastReturningColumns(); cols != nil {
		t.Error("expected nil columns after ClearReturning")
	}
}

func TestClearReturningEmpty(t *testing.T) {
	c := &Catalog{}
	// Clear when already empty should not panic
	c.ClearReturning()
}

// ==================== emptyJoinAggregateRow tests ====================

func TestEmptyJoinAggregateRow(t *testing.T) {
	cols := []selectColInfo{
		{name: "id", isAggregate: false},
		{name: "cnt", isAggregate: true, aggregateType: "COUNT"},
		{name: "val", isAggregate: false},
	}

	row, hasAgg := emptyJoinAggregateRow(cols)
	if !hasAgg {
		t.Error("expected hasAgg=true")
	}
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if len(row) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(row))
	}
	if row[0] != nil {
		t.Error("expected row[0] to be nil")
	}
	if row[1] != int64(0) {
		t.Errorf("expected row[1]=0 for COUNT, got %v", row[1])
	}
	if row[2] != nil {
		t.Error("expected row[2] to be nil")
	}
}

func TestEmptyJoinAggregateRowNoAggregates(t *testing.T) {
	cols := []selectColInfo{
		{name: "id", isAggregate: false},
		{name: "name", isAggregate: false},
	}

	row, hasAgg := emptyJoinAggregateRow(cols)
	if hasAgg {
		t.Error("expected hasAgg=false")
	}
	if row == nil {
		t.Fatal("expected non-nil row")
	}
	if len(row) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(row))
	}
}

func TestEmptyJoinAggregateRowEmpty(t *testing.T) {
	row, hasAgg := emptyJoinAggregateRow(nil)
	if hasAgg {
		t.Error("expected hasAgg=false")
	}
	if row != nil {
		t.Error("expected nil row for empty input")
	}
}

// ==================== distinctAggregateValues tests ====================

func TestDistinctAggregateValues(t *testing.T) {
	tests := []struct {
		name  string
		input []interface{}
		want  int // expected length
	}{
		{
			name:  "no duplicates",
			input: []interface{}{int64(1), int64(2), int64(3)},
			want:  3,
		},
		{
			name:  "with duplicates",
			input: []interface{}{int64(1), int64(2), int64(1), int64(3), int64(2)},
			want:  3,
		},
		{
			name:  "nil values dropped",
			input: []interface{}{int64(1), nil, int64(2), nil},
			want:  2,
		},
		{
			name:  "all nil",
			input: []interface{}{nil, nil, nil},
			want:  0,
		},
		{
			name:  "empty slice",
			input: []interface{}{},
			want:  0,
		},
		{
			name:  "strings",
			input: []interface{}{"a", "b", "a", "c"},
			want:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := distinctAggregateValues(tt.input)
			if len(got) != tt.want {
				t.Errorf("expected %d values, got %d: %v", tt.want, len(got), got)
			}
		})
	}
}

// ==================== derivedSelectColumnNames tests ====================

func TestDerivedSelectColumnNames(t *testing.T) {
	tests := []struct {
		name   string
		sel    *query.SelectStmt
		want   []string
		wantOK bool
	}{
		{
			name: "alias expressions",
			sel: &query.SelectStmt{
				Columns: []query.Expression{
					&query.AliasExpr{Alias: "user_id"},
					&query.AliasExpr{Alias: "full_name"},
				},
			},
			want:   []string{"user_id", "full_name"},
			wantOK: true,
		},
		{
			name: "identifiers",
			sel: &query.SelectStmt{
				Columns: []query.Expression{
					&query.Identifier{Name: "id"},
					&query.Identifier{Name: "name"},
				},
			},
			want:   []string{"id", "name"},
			wantOK: true,
		},
		{
			name: "qualified identifiers",
			sel: &query.SelectStmt{
				Columns: []query.Expression{
					&query.QualifiedIdentifier{Column: "t.id"},
					&query.QualifiedIdentifier{Column: "t.name"},
				},
			},
			want:   []string{"t.id", "t.name"},
			wantOK: true,
		},
		{
			name: "function calls",
			sel: &query.SelectStmt{
				Columns: []query.Expression{
					&query.FunctionCall{Name: "COUNT"},
				},
			},
			want:   []string{"COUNT()"},
			wantOK: true,
		},
		{
			name: "window expressions",
			sel: &query.SelectStmt{
				Columns: []query.Expression{
					&query.WindowExpr{Function: "ROW_NUMBER"},
				},
			},
			want:   []string{"ROW_NUMBER()"},
			wantOK: true,
		},
		{
			name: "star expression",
			sel: &query.SelectStmt{
				Columns: []query.Expression{
					&query.StarExpr{},
				},
			},
			want:   nil,
			wantOK: false,
		},
		{
			name: "empty columns",
			sel: &query.SelectStmt{
				Columns: []query.Expression{},
			},
			want:   []string{},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := derivedSelectColumnNames(tt.sel)
			if ok != tt.wantOK {
				t.Fatalf("expected ok=%v, got %v", tt.wantOK, ok)
			}
			if !tt.wantOK {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("expected %d names, got %d: %v", len(tt.want), len(got), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("name[%d]: expected %q, got %q", i, tt.want[i], got[i])
				}
			}
		})
	}
}

// ==================== hasTableLocked tests ====================

func TestHasTableLocked(t *testing.T) {
	c := &Catalog{
		tables: make(map[string]*TableDef),
	}
	c.tables["users"] = &TableDef{}

	if !c.hasTableLocked("users") {
		t.Error("expected hasTableLocked=true for 'users'")
	}
	if c.hasTableLocked("nonexistent") {
		t.Error("expected hasTableLocked=false for 'nonexistent'")
	}
}

func TestHasTableLockedCaseInsensitive(t *testing.T) {
	c := &Catalog{
		tables: make(map[string]*TableDef),
	}
	c.tables["Users"] = &TableDef{}

	if !c.hasTableLocked("users") {
		t.Error("expected hasTableLocked=true for 'users' (case-insensitive)")
	}
	if !c.hasTableLocked("USERS") {
		t.Error("expected hasTableLocked=true for 'USERS' (case-insensitive)")
	}
	if !c.hasTableLocked("Users") {
		t.Error("expected hasTableLocked=true for 'Users' (exact match)")
	}
}

func TestHasTableLockedEmptyCatalog(t *testing.T) {
	c := &Catalog{}
	// Has nil tables map, should not panic
	if c.hasTableLocked("anything") {
		t.Error("expected false for nil tables map")
	}
}

// ==================== GetQueryCache tests ====================

func TestGetQueryCache(t *testing.T) {
	c := &Catalog{}
	// Without initialization, query cache should be nil
	if cache := c.GetQueryCache(); cache != nil {
		t.Error("expected nil query cache by default")
	}
}

// ==================== ListVectorIndexDefs / cloneVectorIndexDef tests ====================

func TestListVectorIndexDefsEmpty(t *testing.T) {
	c := &Catalog{
		vectorIndexes: make(map[string]*VectorIndexDef),
	}
	defs := c.ListVectorIndexDefs()
	if len(defs) != 0 {
		t.Errorf("expected 0 definitions, got %d", len(defs))
	}
}

func TestListVectorIndexDefsNil(t *testing.T) {
	c := &Catalog{}
	// Nil vectorIndexes map should not panic
	defs := c.ListVectorIndexDefs()
	if len(defs) != 0 {
		t.Errorf("expected 0 definitions, got %d", len(defs))
	}
}

func TestCloneVectorIndexDefNil(t *testing.T) {
	cloned := cloneVectorIndexDef(nil)
	if cloned != nil {
		t.Error("expected nil for nil input")
	}
}

func TestCloneVectorIndexDefBasic(t *testing.T) {
	idx := &VectorIndexDef{
		Name:       "my_idx",
		TableName:  "my_table",
		ColumnName: "embedding",
		Dimensions: 128,
		IndexType:  "hnsw",
	}
	cloned := cloneVectorIndexDef(idx)
	if cloned == nil {
		t.Fatal("expected non-nil result")
	}
	if cloned.Name != "my_idx" {
		t.Errorf("expected Name=my_idx, got %s", cloned.Name)
	}
	if cloned.TableName != "my_table" {
		t.Errorf("expected TableName=my_table, got %s", cloned.TableName)
	}
	if cloned.ColumnName != "embedding" {
		t.Errorf("expected ColumnName=embedding, got %s", cloned.ColumnName)
	}
	if cloned.Dimensions != 128 {
		t.Errorf("expected Dimensions=128, got %d", cloned.Dimensions)
	}
	if cloned.IndexType != "hnsw" {
		t.Errorf("expected IndexType=hnsw, got %s", cloned.IndexType)
	}

	// Verify it's a deep copy
	cloned.Name = "modified"
	if idx.Name != "my_idx" {
		t.Error("modifying clone should not affect original")
	}
}

// ==================== ListMaterializedViewSQL tests ====================

func TestListMaterializedViewSQL(t *testing.T) {
	c := &Catalog{
		materializedViewSQL: make(map[string]string),
	}
	c.materializedViewSQL["mv1"] = "SELECT * FROM t1"

	views := c.ListMaterializedViewSQL()
	if len(views) != 1 {
		t.Fatalf("expected 1 view, got %d", len(views))
	}
	if views["mv1"] != "SELECT * FROM t1" {
		t.Errorf("unexpected SQL: %s", views["mv1"])
	}
}

func TestListMaterializedViewSQLEmpty(t *testing.T) {
	c := &Catalog{
		materializedViewSQL: make(map[string]string),
	}
	views := c.ListMaterializedViewSQL()
	if len(views) != 0 {
		t.Errorf("expected 0 views, got %d", len(views))
	}
}

func TestListMaterializedViewSQLNil(t *testing.T) {
	c := &Catalog{}
	// Nil map should not panic
	views := c.ListMaterializedViewSQL()
	if len(views) != 0 {
		t.Errorf("expected 0 views, got %d", len(views))
	}
}

// ==================== decodeVisibleRow tests ====================

func TestDecodeVisibleRowInvalidData(t *testing.T) {
	// Invalid data should return an error
	_, visible, err := decodeVisibleRow([]byte{0xff, 0xff}, 2, time.Now())
	if err == nil {
		t.Error("expected error for invalid binary data")
	}
	if visible {
		t.Error("expected visible=false on error")
	}
}

// ==================== GetQueryCacheStats tests ====================

func TestGetQueryCacheStatsNil(t *testing.T) {
	c := &Catalog{}
	hits, misses, size := c.GetQueryCacheStats()
	if hits != 0 || misses != 0 || size != 0 {
		t.Errorf("expected all zeros, got hits=%d, misses=%d, size=%d", hits, misses, size)
	}
}

// ==================== clampUint64ToInt64 tests ====================

func TestClampUint64ToInt64(t *testing.T) {
	tests := []struct {
		input uint64
		want  int64
	}{
		{0, 0},
		{1, 1},
		{math.MaxInt64, math.MaxInt64},
		{math.MaxUint64, math.MaxInt64},
		{math.MaxInt64 + 1, math.MaxInt64},
	}

	for _, tt := range tests {
		got := clampUint64ToInt64(tt.input)
		if got != tt.want {
			t.Errorf("clampUint64ToInt64(%d) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
