package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newDeepTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	return New(nil, pool, nil)
}

func deepCreateTable(t *testing.T, cat *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	err := cat.CreateTable(&query.CreateTableStmt{
		Table:   name,
		Columns: cols,
	})
	if err != nil {
		t.Fatalf("CreateTable %s: %v", name, err)
	}
}

func deepInsertRow(t *testing.T, cat *Catalog, table string, columns []string, values []query.Expression) {
	t.Helper()
	_, _, err := cat.Insert(&query.InsertStmt{
		Table:   table,
		Columns: columns,
		Values:  [][]query.Expression{values},
	}, nil)
	if err != nil {
		t.Fatalf("Insert into %s: %v", table, err)
	}
}

// ---------------------------------------------------------------------------
// 1. extractLiteralValue
// ---------------------------------------------------------------------------

func TestDeepCoverage_ExtractLiteralValue(t *testing.T) {
	cat := newDeepTestCatalog(t)

	t.Run("NumberLiteral", func(t *testing.T) {
		v := cat.extractLiteralValue(&query.NumberLiteral{Value: 42}, nil)
		if v != 42.0 {
			t.Fatalf("expected 42, got %v", v)
		}
	})

	t.Run("StringLiteral", func(t *testing.T) {
		v := cat.extractLiteralValue(&query.StringLiteral{Value: "hello"}, nil)
		if v != "hello" {
			t.Fatalf("expected hello, got %v", v)
		}
	})

	t.Run("BooleanLiteral_true", func(t *testing.T) {
		v := cat.extractLiteralValue(&query.BooleanLiteral{Value: true}, nil)
		if v != true {
			t.Fatalf("expected true, got %v", v)
		}
	})

	t.Run("BooleanLiteral_false", func(t *testing.T) {
		v := cat.extractLiteralValue(&query.BooleanLiteral{Value: false}, nil)
		if v != false {
			t.Fatalf("expected false, got %v", v)
		}
	})

	t.Run("PlaceholderExpr_with_args", func(t *testing.T) {
		args := []interface{}{"val0", "val1"}
		v := cat.extractLiteralValue(&query.PlaceholderExpr{Index: 1}, args)
		if v != "val1" {
			t.Fatalf("expected val1, got %v", v)
		}
	})

	t.Run("PlaceholderExpr_out_of_range", func(t *testing.T) {
		args := []interface{}{"only_one"}
		v := cat.extractLiteralValue(&query.PlaceholderExpr{Index: 5}, args)
		if v != nil {
			t.Fatalf("expected nil, got %v", v)
		}
	})

	t.Run("PlaceholderExpr_nil_args", func(t *testing.T) {
		v := cat.extractLiteralValue(&query.PlaceholderExpr{Index: 0}, nil)
		if v != nil {
			t.Fatalf("expected nil, got %v", v)
		}
	})

	t.Run("Default_Identifier", func(t *testing.T) {
		v := cat.extractLiteralValue(&query.Identifier{Name: "col"}, nil)
		if v != nil {
			t.Fatalf("expected nil for Identifier, got %v", v)
		}
	})
}

// ---------------------------------------------------------------------------
// 2. CheckForeignKeyConstraints
// ---------------------------------------------------------------------------

func TestDeepCoverage_CheckForeignKeyConstraints(t *testing.T) {
	ctx := context.Background()

	t.Run("nonexistent_table_returns_nil", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		fke := NewForeignKeyEnforcer(cat)
		err := fke.CheckForeignKeyConstraints(ctx, "nonexistent")
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
	})

	t.Run("table_with_no_foreign_keys", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "simple_fk", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		})
		deepInsertRow(t, cat, "simple_fk", []string{"id", "name"}, []query.Expression{
			&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "alice"},
		})
		fke := NewForeignKeyEnforcer(cat)
		err := fke.CheckForeignKeyConstraints(ctx, "simple_fk")
		if err != nil {
			t.Fatalf("expected nil for no FK, got %v", err)
		}
	})

	t.Run("table_with_satisfied_fk", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "parents_fk", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		deepInsertRow(t, cat, "parents_fk", []string{"id"}, []query.Expression{
			&query.NumberLiteral{Value: 1},
		})
		err := cat.CreateTable(&query.CreateTableStmt{
			Table: "children_fk",
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "parent_id", Type: query.TokenInteger},
			},
			ForeignKeys: []*query.ForeignKeyDef{
				{
					Columns:           []string{"parent_id"},
					ReferencedTable:   "parents_fk",
					ReferencedColumns: []string{"id"},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		deepInsertRow(t, cat, "children_fk", []string{"id", "parent_id"}, []query.Expression{
			&query.NumberLiteral{Value: 10}, &query.NumberLiteral{Value: 1},
		})
		fke := NewForeignKeyEnforcer(cat)
		err = fke.CheckForeignKeyConstraints(ctx, "children_fk")
		if err != nil {
			t.Fatalf("expected nil for satisfied FK, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 3. countRows
// ---------------------------------------------------------------------------

func TestDeepCoverage_CountRows(t *testing.T) {
	cat := newDeepTestCatalog(t)
	deepCreateTable(t, cat, "tbl_cr", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	sc := NewStatsCollector(cat)

	t.Run("empty_result_returns_zero", func(t *testing.T) {
		count, err := sc.countRows("tbl_cr")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected 0, got %d", count)
		}
	})
}

// ---------------------------------------------------------------------------
// 4. collectColumnStats
// ---------------------------------------------------------------------------

func TestDeepCoverage_CollectColumnStats(t *testing.T) {
	cat := newDeepTestCatalog(t)
	deepCreateTable(t, cat, "tbl_ccs", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	sc := NewStatsCollector(cat)

	t.Run("returns_stats_with_zero_distinct", func(t *testing.T) {
		cs, err := sc.collectColumnStats("tbl_ccs", "name")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cs == nil {
			t.Fatal("expected non-nil ColumnStats")
		}
		if cs.ColumnName != "name" {
			t.Fatalf("expected column name name, got %q", cs.ColumnName)
		}
		if cs.DistinctCount != 0 {
			t.Fatalf("expected 0 distinct, got %d", cs.DistinctCount)
		}
	})
}

// ---------------------------------------------------------------------------
// 5. valuesEqual
// ---------------------------------------------------------------------------

func TestDeepCoverage_ValuesEqual(t *testing.T) {
	cat := newDeepTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"nil_nil", nil, nil, true},
		{"nil_nonnil", nil, 1, false},
		{"nonnil_nil", 1, nil, false},
		{"int_int_eq", 10, 10, true},
		{"int_int_neq", 10, 20, false},
		{"int_int64_eq", 10, int64(10), true},
		{"int64_int_eq", int64(5), 5, true},
		{"float64_int", float64(7), 7, true},
		{"int_float64", 7, float64(7), true},
		{"int8_int16", int8(3), int16(3), true},
		{"int32_int64", int32(42), int64(42), true},
		{"uint_int", uint(9), 9, true},
		{"uint8_uint16", uint8(5), uint16(5), true},
		{"uint32_uint64", uint32(100), uint64(100), true},
		{"float32_float64", float32(3.5), float64(3.5), true},
		{"float32_int", float32(4), 4, true},
		{"string_string_eq", "abc", "abc", true},
		{"string_string_neq", "abc", "xyz", false},
		{"string_int_neq", "42", 42, false},
		{"int8_eq", int8(1), int8(1), true},
		{"int16_eq", int16(2), int16(2), true},
		{"int32_eq", int32(3), int32(3), true},
		{"uint_eq", uint(4), uint(4), true},
		{"uint8_eq", uint8(5), uint8(5), true},
		{"uint16_eq", uint16(6), uint16(6), true},
		{"uint32_eq", uint32(7), uint32(7), true},
		{"uint64_eq", uint64(8), uint64(8), true},
		{"float32_eq", float32(1.5), float32(1.5), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fke.valuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Fatalf("valuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 6. updateRowSlice
// ---------------------------------------------------------------------------

func TestDeepCoverage_UpdateRowSlice(t *testing.T) {
	t.Run("nonexistent_table", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		fke := NewForeignKeyEnforcer(cat)
		err := fke.updateRowSlice("nope", 1, []interface{}{1, "x"})
		if err == nil {
			t.Fatal("expected error for nonexistent table")
		}
	})

	t.Run("update_row_without_indexes", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "t1_urs", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		})
		deepInsertRow(t, cat, "t1_urs", []string{"id", "val"}, []query.Expression{
			&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "old"},
		})
		fke := NewForeignKeyEnforcer(cat)
		err := fke.updateRowSlice("t1_urs", 1, []interface{}{float64(1), "new"})
		if err != nil {
			t.Fatalf("updateRowSlice: %v", err)
		}
		row, err := fke.getRowSlice("t1_urs", 1)
		if err != nil {
			t.Fatalf("getRowSlice: %v", err)
		}
		if len(row) < 2 {
			t.Fatalf("expected 2 columns, got %d", len(row))
		}
		if row[1] != "new" {
			t.Fatalf("expected val=new, got %v", row[1])
		}
	})

	t.Run("update_row_with_indexes", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "t2_urs", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		})
		deepInsertRow(t, cat, "t2_urs", []string{"id", "name"}, []query.Expression{
			&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "alice"},
		})
		err := cat.CreateIndex(&query.CreateIndexStmt{
			Index:   "idx_t2_urs_name",
			Table:   "t2_urs",
			Columns: []string{"name"},
			Unique:  true,
		})
		if err != nil {
			t.Fatalf("CreateIndex: %v", err)
		}
		fke := NewForeignKeyEnforcer(cat)
		err = fke.updateRowSlice("t2_urs", 1, []interface{}{float64(1), "bob"})
		if err != nil {
			t.Fatalf("updateRowSlice with index: %v", err)
		}
		row, err := fke.getRowSlice("t2_urs", 1)
		if err != nil {
			t.Fatalf("getRowSlice: %v", err)
		}
		if row[1] != "bob" {
			t.Fatalf("expected name=bob, got %v", row[1])
		}
	})

	t.Run("update_within_transaction", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "t3_urs", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		})
		deepInsertRow(t, cat, "t3_urs", []string{"id", "val"}, []query.Expression{
			&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "before"},
		})
		cat.BeginTransaction(100)
		fke := NewForeignKeyEnforcer(cat)
		err := fke.updateRowSlice("t3_urs", 1, []interface{}{float64(1), "after"})
		if err != nil {
			t.Fatalf("updateRowSlice in txn: %v", err)
		}
		if len(cat.undoLog) == 0 {
			t.Fatal("expected undo log entries")
		}
		_ = cat.CommitTransaction()
	})
}

// ---------------------------------------------------------------------------
// 7. getRowSlice
// ---------------------------------------------------------------------------

func TestDeepCoverage_GetRowSlice(t *testing.T) {
	t.Run("existing_row", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "g1_grs", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		})
		deepInsertRow(t, cat, "g1_grs", []string{"id", "data"}, []query.Expression{
			&query.NumberLiteral{Value: 5}, &query.StringLiteral{Value: "hello"},
		})
		fke := NewForeignKeyEnforcer(cat)
		row, err := fke.getRowSlice("g1_grs", 5)
		if err != nil {
			t.Fatalf("getRowSlice: %v", err)
		}
		if len(row) < 2 {
			t.Fatalf("expected 2 cols, got %d", len(row))
		}
	})

	t.Run("nonexistent_table", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		fke := NewForeignKeyEnforcer(cat)
		_, err := fke.getRowSlice("nope", 1)
		if err == nil {
			t.Fatal("expected error for nonexistent table")
		}
	})

	t.Run("nonexistent_key", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "g2_grs", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		deepInsertRow(t, cat, "g2_grs", []string{"id"}, []query.Expression{
			&query.NumberLiteral{Value: 1},
		})
		fke := NewForeignKeyEnforcer(cat)
		_, err := fke.getRowSlice("g2_grs", 999)
		if err == nil {
			t.Fatal("expected error for nonexistent key")
		}
	})
}

// ---------------------------------------------------------------------------
// 8. Save / Load
// ---------------------------------------------------------------------------

func TestDeepCoverage_SaveLoad(t *testing.T) {
	t.Run("save_with_tables_and_no_tree", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "sv1", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		})
		deepInsertRow(t, cat, "sv1", []string{"id", "name"}, []query.Expression{
			&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "test"},
		})
		err := cat.Save()
		if err != nil {
			t.Fatalf("Save: %v", err)
		}
	})

	t.Run("load_with_nil_tree", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		err := cat.Load()
		if err != nil {
			t.Fatalf("Load with nil tree should return nil, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// 9. Analyze
// ---------------------------------------------------------------------------

func TestDeepCoverage_Analyze(t *testing.T) {
	t.Run("nonexistent_table", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		err := cat.Analyze("nonexistent")
		if err == nil {
			t.Fatal("expected error for nonexistent table")
		}
	})

	t.Run("table_with_data", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "an1", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "score", Type: query.TokenReal},
		})
		for i := 1; i <= 5; i++ {
			deepInsertRow(t, cat, "an1",
				[]string{"id", "name", "score"},
				[]query.Expression{
					&query.NumberLiteral{Value: float64(i)},
					&query.StringLiteral{Value: "user"},
					&query.NumberLiteral{Value: float64(i * 10)},
				},
			)
		}
		err := cat.Analyze("an1")
		if err != nil {
			t.Fatalf("Analyze: %v", err)
		}
		st, err := cat.GetTableStats("an1")
		if err != nil {
			t.Fatalf("GetTableStats: %v", err)
		}
		if st.RowCount != 5 {
			t.Fatalf("expected RowCount=5, got %d", st.RowCount)
		}
	})

	t.Run("table_with_null_values", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "an2", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "optional", Type: query.TokenText},
		})
		deepInsertRow(t, cat, "an2", []string{"id", "optional"}, []query.Expression{
			&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "present"},
		})
		_, _, err := cat.Insert(&query.InsertStmt{
			Table:   "an2",
			Columns: []string{"id"},
			Values: [][]query.Expression{
				{&query.NumberLiteral{Value: 2}},
			},
		}, nil)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		err = cat.Analyze("an2")
		if err != nil {
			t.Fatalf("Analyze: %v", err)
		}
		st, err := cat.GetTableStats("an2")
		if err != nil {
			t.Fatalf("GetTableStats: %v", err)
		}
		if st.RowCount != 2 {
			t.Fatalf("expected RowCount=2, got %d", st.RowCount)
		}
	})
}

// ---------------------------------------------------------------------------
// StatsCollector.CollectStats
// ---------------------------------------------------------------------------

func TestDeepCoverage_CollectStats(t *testing.T) {
	t.Run("nonexistent_table", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		sc := NewStatsCollector(cat)
		_, err := sc.CollectStats("nonexistent")
		if err == nil {
			t.Fatal("expected error for nonexistent table")
		}
	})

	t.Run("existing_table", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "cs1", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		sc := NewStatsCollector(cat)
		stats, err := sc.CollectStats("cs1")
		if err != nil {
			t.Fatalf("CollectStats: %v", err)
		}
		if stats == nil {
			t.Fatal("expected non-nil stats")
		}
		if stats.TableName != "cs1" {
			t.Fatalf("expected table name cs1, got %s", stats.TableName)
		}
	})
}

// ---------------------------------------------------------------------------
// ForeignKeyEnforcer - serialize / deserialize
// ---------------------------------------------------------------------------

func TestDeepCoverage_FKE_SerializeDeserialize(t *testing.T) {
	cat := newDeepTestCatalog(t)
	fke := NewForeignKeyEnforcer(cat)

	t.Run("serialize_string", func(t *testing.T) {
		b := fke.serializeValue("test")
		if string(b) != "S:test" {
			t.Fatalf("expected S:test, got %s", string(b))
		}
	})

	t.Run("serialize_int", func(t *testing.T) {
		b := fke.serializeValue(42)
		expected := "00000000000000000042"
		if string(b) != expected {
			t.Fatalf("expected %s, got %s", expected, string(b))
		}
	})

	t.Run("serialize_nil", func(t *testing.T) {
		b := fke.serializeValue(nil)
		if string(b) != "NULL" {
			t.Fatalf("expected NULL, got %s", string(b))
		}
	})

	t.Run("deserialize_string_prefix", func(t *testing.T) {
		v := fke.deserializeValue([]byte("S:hello"))
		if v != "hello" {
			t.Fatalf("expected hello, got %v", v)
		}
	})

	t.Run("deserialize_number", func(t *testing.T) {
		v := fke.deserializeValue([]byte("00000000000000000042"))
		if v != 42 {
			t.Fatalf("expected 42, got %v", v)
		}
	})

	t.Run("composite_key", func(t *testing.T) {
		b := fke.serializeCompositeKey([]interface{}{1, "x"})
		if len(b) == 0 {
			t.Fatal("expected non-empty composite key")
		}
	})
}

// ---------------------------------------------------------------------------
// ForeignKeyEnforcer - OnDelete / OnUpdate
// ---------------------------------------------------------------------------

func TestDeepCoverage_FKE_OnDeleteOnUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("OnDelete_no_referencing_tables", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "parent_od", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		fke := NewForeignKeyEnforcer(cat)
		err := fke.OnDelete(ctx, "parent_od", 1)
		if err != nil {
			t.Fatalf("OnDelete: %v", err)
		}
	})

	t.Run("OnUpdate_no_referencing_tables", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "parent_ou", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		fke := NewForeignKeyEnforcer(cat)
		err := fke.OnUpdate(ctx, "parent_ou", 1, 2)
		if err != nil {
			t.Fatalf("OnUpdate: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// ForeignKeyEnforcer - ValidateInsert / ValidateUpdate
// ---------------------------------------------------------------------------

func TestDeepCoverage_FKE_ValidateInsertUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("ValidateInsert_no_fk", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "nofk_vi", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		fke := NewForeignKeyEnforcer(cat)
		err := fke.ValidateInsert(ctx, "nofk_vi", map[string]interface{}{"id": 1})
		if err != nil {
			t.Fatalf("ValidateInsert: %v", err)
		}
	})

	t.Run("ValidateInsert_null_fk_value", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "ref_vi", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		err := cat.CreateTable(&query.CreateTableStmt{
			Table: "child_vi",
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "ref_id", Type: query.TokenInteger},
			},
			ForeignKeys: []*query.ForeignKeyDef{
				{
					Columns:           []string{"ref_id"},
					ReferencedTable:   "ref_vi",
					ReferencedColumns: []string{"id"},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		fke := NewForeignKeyEnforcer(cat)
		err = fke.ValidateInsert(ctx, "child_vi", map[string]interface{}{"id": 1, "ref_id": nil})
		if err != nil {
			t.Fatalf("ValidateInsert with NULL FK should succeed: %v", err)
		}
	})

	t.Run("ValidateUpdate_unchanged_fk", func(t *testing.T) {
		cat := newDeepTestCatalog(t)
		deepCreateTable(t, cat, "ref2_vu", []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		})
		err := cat.CreateTable(&query.CreateTableStmt{
			Table: "child2_vu",
			Columns: []*query.ColumnDef{
				{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
				{Name: "ref_id", Type: query.TokenInteger},
			},
			ForeignKeys: []*query.ForeignKeyDef{
				{
					Columns:           []string{"ref_id"},
					ReferencedTable:   "ref2_vu",
					ReferencedColumns: []string{"id"},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		fke := NewForeignKeyEnforcer(cat)
		err = fke.ValidateUpdate(ctx, "child2_vu",
			map[string]interface{}{"id": 1, "ref_id": 5},
			map[string]interface{}{"id": 1, "ref_id": 5},
		)
		if err != nil {
			t.Fatalf("ValidateUpdate unchanged FK should succeed: %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// ExecuteQuery stub
// ---------------------------------------------------------------------------

func TestDeepCoverage_ExecuteQuery(t *testing.T) {
	cat := newDeepTestCatalog(t)
	result, err := cat.ExecuteQuery("SELECT 1")
	if err != nil {
		t.Fatalf("ExecuteQuery: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if len(result.Rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(result.Rows))
	}
}

// ---------------------------------------------------------------------------
// StatsCollector utility methods
// ---------------------------------------------------------------------------

func TestDeepCoverage_StatsCollectorUtils(t *testing.T) {
	cat := newDeepTestCatalog(t)
	sc := NewStatsCollector(cat)

	t.Run("EstimateRowCount_no_stats", func(t *testing.T) {
		count := sc.EstimateRowCount("nonexistent")
		if count != 1000 {
			t.Fatalf("expected default 1000, got %d", count)
		}
	})

	t.Run("GetStatsSummary_empty", func(t *testing.T) {
		summary := sc.GetStatsSummary()
		if len(summary) != 0 {
			t.Fatalf("expected empty summary, got %d entries", len(summary))
		}
	})

	t.Run("InvalidateStats", func(t *testing.T) {
		sc.InvalidateStats("nonexistent")
	})

	t.Run("IsStale_no_stats", func(t *testing.T) {
		stale := sc.IsStale("nonexistent", 0)
		if !stale {
			t.Fatal("expected stale=true when no stats exist")
		}
	})

	t.Run("EstimateSelectivity_no_stats", func(t *testing.T) {
		sel := sc.EstimateSelectivity("nonexistent", "col", "=", 1)
		if sel != 0.1 {
			t.Fatalf("expected 0.1, got %f", sel)
		}
	})

	t.Run("EstimateSeqScanCost_no_stats", func(t *testing.T) {
		cost := sc.EstimateSeqScanCost("nonexistent", 0.5)
		if cost == 0 {
			t.Fatal("expected non-zero cost")
		}
	})

	t.Run("EstimateIndexScanCost_no_stats", func(t *testing.T) {
		cost := sc.EstimateIndexScanCost("nonexistent", "idx", 0.5)
		if cost == 0 {
			t.Fatal("expected non-zero cost")
		}
	})

	t.Run("EstimateJoinCosts", func(t *testing.T) {
		nl := sc.EstimateNestedLoopCost(100, 50)
		if nl != 5000 {
			t.Fatalf("expected 5000, got %f", nl)
		}
		hj := sc.EstimateHashJoinCost(100, 200)
		if hj == 0 {
			t.Fatal("expected non-zero hash join cost")
		}
		mj := sc.EstimateMergeJoinCost(100, 200)
		if mj == 0 {
			t.Fatal("expected non-zero merge join cost")
		}
	})
}

// ---------------------------------------------------------------------------
// updateRowSlice: transaction with indexes
// ---------------------------------------------------------------------------

func TestDeepCoverage_UpdateRowSlice_TxnWithIndexes(t *testing.T) {
	cat := newDeepTestCatalog(t)
	deepCreateTable(t, cat, "txidx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "tag", Type: query.TokenText},
	})
	deepInsertRow(t, cat, "txidx", []string{"id", "tag"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "old_tag"},
	})
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_txidx_tag",
		Table:   "txidx",
		Columns: []string{"tag"},
		Unique:  true,
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	cat.BeginTransaction(200)
	fke := NewForeignKeyEnforcer(cat)
	err = fke.updateRowSlice("txidx", 1, []interface{}{float64(1), "new_tag"})
	if err != nil {
		t.Fatalf("updateRowSlice txn+idx: %v", err)
	}

	hasIndexUndo := false
	for _, entry := range cat.undoLog {
		if len(entry.indexChanges) > 0 {
			hasIndexUndo = true
			break
		}
	}
	if !hasIndexUndo {
		t.Fatal("expected undo log to contain index changes")
	}
	_ = cat.CommitTransaction()
}
