package catalog

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ---------------------------------------------------------------------------
// Tier 1: Persistence (Save / Load)
// ---------------------------------------------------------------------------

// TestSaveAndLoadRoundTrip exercises Save() and Load() through the engine:
// create tables with various objects, save, reopen, verify they survive.
func TestSaveAndLoadRoundTrip(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	// Create a table
	_, err = cat.ExecuteQuery("CREATE TABLE persist_test (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Insert some data so storeTableDef is exercised
	_, err = cat.ExecuteQuery("INSERT INTO persist_test VALUES (1, 'alice', 30), (2, 'bob', 25)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Create an index
	_, err = cat.ExecuteQuery("CREATE INDEX idx_persist_name ON persist_test(name)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// The index will exist in c.indexes; persist so Save writes it.
	// Create a view, trigger, procedure, materialized view using direct methods.
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "persist_test"},
	}
	if err := cat.CreateView("v_persist", selectStmt); err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	// Set viewSQL so Save can persist it
	cat.viewSQL["v_persist"] = "CREATE VIEW v_persist AS SELECT * FROM persist_test"

	trigStmt := &query.CreateTriggerStmt{
		Name:  "trg_persist",
		Table: "persist_test",
		Event: "INSERT",
		Time:  "AFTER",
		Body:  []query.Statement{&query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}}},
	}
	if err := cat.CreateTrigger(trigStmt); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	cat.triggerSQL["trg_persist"] = "CREATE TRIGGER trg_persist AFTER INSERT ON persist_test BEGIN SELECT 1; END"

	procStmt := &query.CreateProcedureStmt{
		Name: "proc_persist",
		Body: []query.Statement{&query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}}},
	}
	if err := cat.CreateProcedure(procStmt); err != nil {
		t.Fatalf("CreateProcedure: %v", err)
	}
	cat.procedureSQL["proc_persist"] = "CREATE PROCEDURE proc_persist() BEGIN SELECT 1; END"

	// Create materialized view via direct API
	mvSelect := &query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "persist_test"},
	}
	if err := cat.CreateMaterializedView("mv_persist", mvSelect, false); err != nil {
		t.Fatalf("CreateMaterializedView: %v", err)
	}
	cat.materializedViewSQL["mv_persist"] = "CREATE MATERIALIZED VIEW mv_persist AS SELECT * FROM persist_test"

	// Save
	if err := cat.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Load into a fresh catalog
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Verify table survived
	if _, err := cat2.GetTable("persist_test"); err != nil {
		t.Errorf("persist_test table not found after Load: %v", err)
	}

	// Verify index survived
	if _, exists := cat2.indexes["idx_persist_name"]; !exists {
		t.Error("idx_persist_name not found after Load")
	}

	// Verify view survived
	if _, exists := cat2.views["v_persist"]; !exists {
		t.Error("view v_persist not found after Load")
	}

	// Verify trigger survived
	if _, exists := cat2.triggers["trg_persist"]; !exists {
		t.Error("trigger trg_persist not found after Load")
	}

	// Verify procedure survived
	if _, exists := cat2.procedures["proc_persist"]; !exists {
		t.Error("procedure proc_persist not found after Load")
	}

	// Verify materialized view survived
	if _, exists := cat2.materializedViews["mv_persist"]; !exists {
		t.Error("materialized view mv_persist not found after Load")
	}
}

// TestSaveNoTree exercises Save when c.tree is nil (edge case).
func TestSaveNoTree(t *testing.T) {
	cat := newEmptyCatalog()
	// c.tree is nil; Save should still not panic and flush should be skipped.
	// It has no tree, no pool, no WAL.
	if err := cat.Save(); err != nil {
		t.Fatalf("Save with nil tree: %v", err)
	}
}

// TestLoadNilTree exercises Load when c.tree is nil.
func TestLoadNilTree(t *testing.T) {
	cat := newEmptyCatalog()
	if err := cat.Load(); err != nil {
		t.Fatalf("Load with nil tree: %v", err)
	}
}

// TestSaveWithTemporaryTable ensures temporary tables are skipped during save.
func TestSaveWithTemporaryTable(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	// Create a temporary table
	_, err = cat.ExecuteQuery("CREATE TEMP TABLE tmp_save_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TEMP TABLE: %v", err)
	}

	// Create a temporary index
	_, err = cat.ExecuteQuery("CREATE INDEX idx_tmp_save ON tmp_save_test(id)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	// Save should skip the temporary table and temporary index
	if err := cat.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

// TestSaveLoadWithFTSAndJSONIndexes tests saving/loading FTS and JSON indexes.
func TestSaveLoadWithFTSAndJSONIndexes(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	_, err = cat.ExecuteQuery("CREATE TABLE fts_test (id INTEGER PRIMARY KEY, content TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	// Use the direct FTS index creation
	ftsDef := &FTSIndexDef{
		Name:      "ft_idx",
		TableName: "fts_test",
		Columns:   []string{"content"},
		Index:     make(map[string][]int64),
	}
	cat.ftsIndexes["ft_idx"] = ftsDef
	_, err = cat.ExecuteQuery("INSERT INTO fts_test VALUES (1, 'hello world')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := cat.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, exists := cat2.ftsIndexes["ft_idx"]; !exists {
		t.Error("FTS index ft_idx not found after Load")
	}
}

// TestSaveEmptyCatalog saves a catalog with no tables/views/triggers.
func TestSaveEmptyCatalog(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)
	if err := cat.Save(); err != nil {
		t.Fatalf("Save on empty catalog: %v", err)
	}
}

// TestLoadEmptyCatalog loads a catalog with no persisted data.
func TestLoadEmptyCatalog(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)
	if err := cat.Load(); err != nil {
		t.Fatalf("Load on empty catalog: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tier 2: Clone functions
// ---------------------------------------------------------------------------

func TestCloneSelectStmt(t *testing.T) {
	tests := []struct {
		name string
		stmt *query.SelectStmt
	}{
		{"nil", nil},
		{"simple select", &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "users"},
		}},
		{"with joins", &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "a"}},
			From:    &query.TableRef{Name: "t1"},
			Joins:   []*query.JoinClause{{Table: &query.TableRef{Name: "t2"}}},
		}},
		{"with group by", &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "c"}},
			From:    &query.TableRef{Name: "t"},
			GroupBy: []query.Expression{&query.Identifier{Name: "c"}},
		}},
		{"with order by", &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "c"}},
			From:    &query.TableRef{Name: "t"},
			OrderBy: []*query.OrderByExpr{{Expr: &query.Identifier{Name: "c"}, Desc: true}},
		}},
		{"with as of", &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "c"}},
			From:    &query.TableRef{Name: "t"},
			AsOf:    &query.TemporalExpr{Timestamp: &query.StringLiteral{Value: "2024-01-01"}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloneSelectStmt(tt.stmt)
			if tt.stmt == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil")
			}
			if len(got.Columns) != len(tt.stmt.Columns) {
				t.Errorf("columns length mismatch: %d vs %d", len(got.Columns), len(tt.stmt.Columns))
			}
			// Verify it's a deep copy by modifying original
			originalFrom := tt.stmt.From
			got.From = &query.TableRef{Name: "modified"}
			if tt.stmt.From.Name != originalFrom.Name {
				t.Error("clone was not a deep copy: modifying clone affected original")
			}
			// Restore
			tt.stmt.From = originalFrom
		})
	}
}

func TestClonePartitionInfo(t *testing.T) {
	tests := []struct {
		name      string
		partition *PartitionInfo
	}{
		{"nil", nil},
		{"empty partitions", &PartitionInfo{
			Type:   query.PartitionTypeRange,
			Column: "id",
		}},
		{"with partitions", &PartitionInfo{
			Type:       query.PartitionTypeRange,
			Column:     "created_at",
			NumParts:   3,
			Partitions: []PartitionDef{{Name: "p1", MinValue: 0, MaxValue: 100}, {Name: "p2", MinValue: 100, MaxValue: 200}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := clonePartitionInfo(tt.partition)
			if tt.partition == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil")
			}
			if got.Type != tt.partition.Type {
				t.Errorf("Type mismatch: %v vs %v", got.Type, tt.partition.Type)
			}
			if got.Column != tt.partition.Column {
				t.Errorf("Column mismatch: %s vs %s", got.Column, tt.partition.Column)
			}
			if len(got.Partitions) != len(tt.partition.Partitions) {
				t.Errorf("Partitions length mismatch: %d vs %d", len(got.Partitions), len(tt.partition.Partitions))
			}
		})
	}
}

func TestCloneColumnStats(t *testing.T) {
	tests := []struct {
		name  string
		stats *ColumnStats
	}{
		{"nil", nil},
		{"empty stats", &ColumnStats{ColumnName: "id"}},
		{"with histogram", &ColumnStats{
			ColumnName:    "age",
			NullCount:     5,
			DistinctCount: 100,
			MinValue:      int64(0),
			MaxValue:      int64(99),
			Histogram: []Bucket{
				{LowerBound: int64(0), UpperBound: int64(50), Count: 60},
				{LowerBound: int64(50), UpperBound: int64(100), Count: 40},
			},
		}},
		{"with byte slice values", &ColumnStats{
			ColumnName: "data",
			MinValue:   []byte{1, 2, 3},
			MaxValue:   []byte{4, 5, 6},
		}},
		{"with slice values", &ColumnStats{
			ColumnName: "tags",
			MinValue:   []interface{}{"a", "b"},
			MaxValue:   []interface{}{"c", "d"},
		}},
		{"with map values", &ColumnStats{
			ColumnName: "meta",
			MinValue:   map[string]interface{}{"key": "val"},
			MaxValue:   map[string]interface{}{"key": "val2"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloneColumnStats(tt.stats)
			if tt.stats == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			if got == nil {
				t.Fatal("expected non-nil")
			}
			if got.ColumnName != tt.stats.ColumnName {
				t.Errorf("ColumnName mismatch: %s vs %s", got.ColumnName, tt.stats.ColumnName)
			}
			if got.NullCount != tt.stats.NullCount {
				t.Errorf("NullCount mismatch: %d vs %d", got.NullCount, tt.stats.NullCount)
			}
		})
	}
}

func TestCloneInterfaceValue(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{"nil", nil},
		{"int", 42},
		{"int64", int64(100)},
		{"float64", 3.14},
		{"string", "hello"},
		{"bool", true},
		{"[]byte nil", []byte(nil)},
		{"[]byte non-nil", []byte{1, 2, 3}},
		{"[]interface{}", []interface{}{"a", 1, true}},
		{"map[string]interface{}", map[string]interface{}{"key1": "val1", "key2": int64(42)}},
		{"nested map in slice", []interface{}{
			map[string]interface{}{"nested": "value"},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloneInterfaceValue(tt.value)
			if tt.value == nil {
				if got != nil {
					t.Errorf("expected nil, got %v", got)
				}
				return
			}
			// Verify deep copy for slices and maps
			switch v := tt.value.(type) {
			case []byte:
				if v != nil {
					gotBytes := got.([]byte)
					gotBytes[0] = 255 // modify clone
					if v[0] == 255 {
						t.Error("clone was not a deep copy")
					}
				}
			case []interface{}:
				gotSlice := got.([]interface{})
				if len(gotSlice) != len(v) {
					t.Errorf("length mismatch: %d vs %d", len(gotSlice), len(v))
				}
			case map[string]interface{}:
				gotMap := got.(map[string]interface{})
				if len(gotMap) != len(v) {
					t.Errorf("length mismatch: %d vs %d", len(gotMap), len(v))
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tier 3: Scalar / Math / Date functions
// ---------------------------------------------------------------------------

func TestDateFieldFunc(t *testing.T) {
	yearFunc := dateFieldFunc(func(t time.Time) int64 { return int64(t.Year()) })
	monthFunc := dateFieldFunc(func(t time.Time) int64 { return int64(t.Month()) })
	dayFunc := dateFieldFunc(func(t time.Time) int64 { return int64(t.Day()) })
	hourFunc := dateFieldFunc(func(t time.Time) int64 { return int64(t.Hour()) })

	tests := []struct {
		name   string
		fn     functionHandler
		args   []interface{}
		want   interface{}
		wantOK bool
	}{
		{"year from date", yearFunc, []interface{}{"2024-01-15"}, int64(2024), true},
		{"year from datetime", yearFunc, []interface{}{"2024-06-15 14:30:00"}, int64(2024), true},
		{"month from date", monthFunc, []interface{}{"2024-01-15"}, int64(1), true},
		{"day from date", dayFunc, []interface{}{"2024-01-15"}, int64(15), true},
		{"hour from datetime", hourFunc, []interface{}{"2024-01-15 14:30:00"}, int64(14), true},
		{"nil arg", yearFunc, []interface{}{nil}, nil, true},
		{"empty args", yearFunc, []interface{}{}, nil, true},
		{"invalid date", yearFunc, []interface{}{"not-a-date"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fn(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplyStrftime(t *testing.T) {
	// Use a fixed time for deterministic results
	// Tuesday, 2024-01-02 03:04:05
	fixedTime := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name   string
		format string
		t      time.Time
		want   string
	}{
		{"full date", "%Y-%m-%d", fixedTime, "2024-01-02"},
		{"full datetime", "%Y-%m-%d %H:%M:%S", fixedTime, "2024-01-02 03:04:05"},
		{"year", "%Y", fixedTime, "2024"},
		{"month", "%m", fixedTime, "01"},
		{"day", "%d", fixedTime, "02"},
		{"hour", "%H", fixedTime, "03"},
		{"minute", "%M", fixedTime, "04"},
		{"second", "%S", fixedTime, "05"},
		{"weekday", "%w", fixedTime, "2"},
		{"year day", "%j", fixedTime, "002"},
		{"iso week", "%W", fixedTime, "01"},
		{"unix timestamp", "%s", fixedTime, "1704164645"},
		{"literal percent", "%%", fixedTime, "%"},
		{"unknown specifier", "%z", fixedTime, "%z"},
		{"no percent", "hello", fixedTime, "hello"},
		{"percent at end", "test%", fixedTime, "test%"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyStrftime(tt.format, tt.t)
			if got != tt.want {
				t.Errorf("applyStrftime(%q, %v) = %q, want %q", tt.format, tt.t, got, tt.want)
			}
		})
	}
}

func TestScalarMinMax(t *testing.T) {
	tests := []struct {
		name string
		args []interface{}
		max  bool
		want interface{}
	}{
		{"empty args", []interface{}{}, false, nil},
		{"single arg min", []interface{}{int64(42)}, false, int64(42)},
		{"single arg max", []interface{}{int64(42)}, true, int64(42)},
		{"min ints", []interface{}{int64(3), int64(1), int64(2)}, false, int64(1)},
		{"max ints", []interface{}{int64(3), int64(1), int64(2)}, true, int64(3)},
		{"min floats", []interface{}{3.5, 1.2, 2.8}, false, 1.2},
		{"max floats", []interface{}{3.5, 1.2, 2.8}, true, 3.5},
		{"min strings", []interface{}{"c", "a", "b"}, false, "a"},
		{"max strings", []interface{}{"c", "a", "b"}, true, "c"},
		{"with nil args min", []interface{}{int64(10), nil, int64(5)}, false, int64(5)},
		{"with nil args max", []interface{}{int64(10), nil, int64(5)}, true, int64(10)},
		{"all nil", []interface{}{nil, nil, nil}, false, nil},
		{"nil first then value", []interface{}{nil, int64(7)}, false, int64(7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := scalarMinMax(tt.args, tt.max)
			if got != tt.want {
				t.Errorf("scalarMinMax(%v, %v) = %v, want %v", tt.args, tt.max, got, tt.want)
			}
		})
	}
}

func TestCompareAsInt64(t *testing.T) {
	tests := []struct {
		name string
		v    interface{}
		want int64
		ok   bool
	}{
		{"int", int(42), 42, true},
		{"int8", int8(8), 8, true},
		{"int16", int16(16), 16, true},
		{"int32", int32(32), 32, true},
		{"int64", int64(64), 64, true},
		{"uint8", uint8(8), 8, true},
		{"uint16", uint16(16), 16, true},
		{"uint32", uint32(32), 32, true},
		{"uint small", uint(100), 100, true},
		{"uint64 small", uint64(100), 100, true},
		{"uint large (overflows)", uint64(math.MaxInt64) + 1, 0, false},
		{"float64", float64(3.14), 0, false},
		{"string", "hello", 0, false},
		{"nil", nil, 0, false},
		{"bool", true, 0, false},
		{"int64 max", int64(math.MaxInt64), math.MaxInt64, true},
		{"int64 min", int64(math.MinInt64), math.MinInt64, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := compareAsInt64(tt.v)
			if ok != tt.ok {
				t.Errorf("compareAsInt64(%v) ok = %v, want %v", tt.v, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("compareAsInt64(%v) = %d, want %d", tt.v, got, tt.want)
			}
		})
	}
}

func TestEvaluateMathFunctionRaw(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		args     []interface{}
		wantVal  interface{}
		wantHand bool
		wantErr  bool
	}{
		// ABS
		{"ABS nil", "ABS", []interface{}{nil}, nil, true, false},
		{"ABS positive", "ABS", []interface{}{5.0}, 5.0, true, false},
		{"ABS negative int", "ABS", []interface{}{int64(-5)}, 5.0, true, false},
		{"ABS negative float", "ABS", []interface{}{-3.5}, 3.5, true, false},
		{"ABS no args", "ABS", []interface{}{}, nil, true, true},

		// ROUND
		{"ROUND nil", "ROUND", []interface{}{nil}, nil, true, false},
		{"ROUND integer", "ROUND", []interface{}{3.0}, float64(3), true, false},
		{"ROUND decimal", "ROUND", []interface{}{3.7}, float64(4), true, false},
		{"ROUND with precision", "ROUND", []interface{}{3.14159, 2.0}, 3.14, true, false},
		{"ROUND no args", "ROUND", []interface{}{}, nil, true, true},

		// FLOOR
		{"FLOOR nil", "FLOOR", []interface{}{nil}, nil, true, false},
		{"FLOOR positive", "FLOOR", []interface{}{3.7}, 3.0, true, false},
		{"FLOOR negative", "FLOOR", []interface{}{-3.7}, -4.0, true, false},
		{"FLOOR no args", "FLOOR", []interface{}{}, nil, true, true},

		// CEIL
		{"CEIL nil", "CEIL", []interface{}{nil}, nil, true, false},
		{"CEIL positive", "CEIL", []interface{}{3.2}, 4.0, true, false},
		{"CEIL negative", "CEIL", []interface{}{-3.2}, -3.0, true, false},
		{"CEILING alias", "CEILING", []interface{}{1.5}, 2.0, true, false},
		{"CEIL no args", "CEIL", []interface{}{}, nil, true, true},

		// MOD
		{"MOD nil arg0", "MOD", []interface{}{nil, 3.0}, nil, true, false},
		{"MOD nil arg1", "MOD", []interface{}{10.0, nil}, nil, true, false},
		{"MOD basic", "MOD", []interface{}{10.0, 3.0}, int64(1), true, false},
		{"MOD divisible", "MOD", []interface{}{12.0, 4.0}, int64(0), true, false},
		{"MOD by zero", "MOD", []interface{}{5.0, 0.0}, nil, true, false},
		{"MOD no args", "MOD", []interface{}{}, nil, true, true},

		// POWER
		{"POWER nil base", "POWER", []interface{}{nil, 2.0}, nil, true, false},
		{"POWER basic", "POWER", []interface{}{2.0, 3.0}, 8.0, true, false},
		{"POWER with POW alias", "POW", []interface{}{3.0, 2.0}, 9.0, true, false},
		{"POWER no args", "POWER", []interface{}{}, nil, true, true},

		// Unknown function
		{"unknown func", "UNKNOWN", []interface{}{1.0}, nil, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotHand, gotErr := evaluateMathFunctionRaw(tt.funcName, tt.args)
			if tt.wantErr && gotErr == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && gotErr != nil {
				t.Errorf("unexpected error: %v", gotErr)
			}
			if gotHand != tt.wantHand {
				t.Errorf("handled = %v, want %v", gotHand, tt.wantHand)
			}
			if tt.wantHand && !tt.wantErr && gotVal != tt.wantVal {
				// For float comparisons, compare as strings
				if f, ok := gotVal.(float64); ok {
					if wf, ok2 := tt.wantVal.(float64); ok2 {
						if math.Abs(f-wf) > 0.0001 {
							t.Errorf("got %v, want %v", gotVal, tt.wantVal)
						}
						return
					}
				}
				t.Errorf("got %v (type %T), want %v (type %T)", gotVal, gotVal, tt.wantVal, tt.wantVal)
			}
		})
	}
}

func TestEvalUnaryExpr(t *testing.T) {
	tests := []struct {
		name    string
		val     interface{}
		op      query.TokenType
		want    interface{}
		wantErr bool
	}{
		// TokenMinus
		{"negate int64", int64(5), query.TokenMinus, int64(-5), false},
		{"negate int", int(10), query.TokenMinus, int(-10), false},
		{"negate float64", 3.14, query.TokenMinus, -3.14, false},
		{"negate string not numeric", "hello", query.TokenMinus, nil, true},
		{"negate nil", nil, query.TokenMinus, nil, true},

		// TokenNot
		{"not true", true, query.TokenNot, false, false},
		{"not false", false, query.TokenNot, true, false},
		{"not nil", nil, query.TokenNot, nil, false},
		{"not int zero", int(0), query.TokenNot, true, false},
		{"not int non-zero", int(1), query.TokenNot, false, false},

		// TokenBitNot
		{"bitnot int64", int64(5), query.TokenBitNot, ^int64(5), false},
		{"bitnot nil", nil, query.TokenBitNot, nil, false},
		{"bitnot non-int", "hello", query.TokenBitNot, nil, true},
	}
	ctx := &EvalContext{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ctx.EvalUnaryExpr(tt.val, tt.op)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("EvalUnaryExpr(%v, %v) = %v, want %v", tt.val, tt.op, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Tier 4: Aggregate functions
// ---------------------------------------------------------------------------

func TestExprHasAggregate(t *testing.T) {
	tests := []struct {
		name string
		expr query.Expression
		want bool
	}{
		{"nil", nil, false},
		{"simple identifier", &query.Identifier{Name: "id"}, false},
		{"number literal", &query.NumberLiteral{Value: 42}, false},
		{"count without args", &query.FunctionCall{Name: "COUNT"}, true},
		{"count star", &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}, true},
		{"sum", &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "x"}}}, true},
		{"min", &query.FunctionCall{Name: "MIN", Args: []query.Expression{&query.Identifier{Name: "x"}}}, true},
		{"max", &query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.Identifier{Name: "x"}}}, true},
		{"avg", &query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.Identifier{Name: "x"}}}, true},
		{"group_concat", &query.FunctionCall{Name: "GROUP_CONCAT", Args: []query.Expression{&query.Identifier{Name: "x"}}}, true},
		{"count lower", &query.FunctionCall{Name: "count", Args: []query.Expression{&query.StarExpr{}}}, true},
		{"count mixed", &query.FunctionCall{Name: "Count", Args: []query.Expression{&query.StarExpr{}}}, true},
		{"non-aggregate func", &query.FunctionCall{Name: "LENGTH", Args: []query.Expression{&query.Identifier{Name: "x"}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exprHasAggregate(tt.expr)
			if got != tt.want {
				t.Errorf("exprHasAggregate(%v) = %v, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

func TestCollectAggregateInput(t *testing.T) {
	cat := newEmptyCatalog()
	cat.tables["test"] = &TableDef{Columns: []ColumnDef{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "INTEGER"}}}
	cols := []ColumnDef{{Name: "a", Type: "INTEGER"}, {Name: "b", Type: "INTEGER"}}

	tests := []struct {
		name string
		ci   selectColInfo
		row  []interface{}
		want interface{}
		ok   bool
	}{
		{
			name: "COUNT multi-arg non-nil",
			ci:   selectColInfo{aggregateType: "COUNT", aggregateArgs: []query.Expression{&query.Identifier{Name: "a"}, &query.Identifier{Name: "b"}}},
			row:  []interface{}{int64(1), int64(2)},
			want: "I:1\x00I:2",
			ok:   true,
		},
		{
			name: "COUNT multi-arg with nil",
			ci:   selectColInfo{aggregateType: "COUNT", aggregateArgs: []query.Expression{&query.Identifier{Name: "a"}, &query.Identifier{Name: "b"}}},
			row:  []interface{}{nil, int64(2)},
			want: nil,
			ok:   false,
		},
		{
			name: "JSON_OBJECTAGG key val",
			ci:   selectColInfo{aggregateType: "JSON_OBJECTAGG", aggregateArgs: []query.Expression{&query.Identifier{Name: "a"}, &query.Identifier{Name: "b"}}},
			row:  []interface{}{"k1", "v1"},
			want: jsonObjectAggPair{key: "k1", value: "v1"},
			ok:   true,
		},
		{
			name: "JSON_OBJECTAGG nil key",
			ci:   selectColInfo{aggregateType: "JSON_OBJECTAGG", aggregateArgs: []query.Expression{&query.Identifier{Name: "a"}, &query.Identifier{Name: "b"}}},
			row:  []interface{}{nil, "v1"},
			want: nil,
			ok:   false,
		},
		{
			name: "JSON_ARRAYAGG",
			ci:   selectColInfo{aggregateType: "JSON_ARRAYAGG", aggregateArgs: []query.Expression{&query.Identifier{Name: "a"}}},
			row:  []interface{}{"val1"},
			want: "val1",
			ok:   true,
		},
		{
			name: "JSON_ARRAYAGG no args",
			ci:   selectColInfo{aggregateType: "JSON_ARRAYAGG", aggregateArgs: []query.Expression{}},
			row:  []interface{}{},
			want: nil,
			ok:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := cat.collectAggregateInput(tt.ci, tt.row, cols, nil, func() (interface{}, bool) { return nil, false })
			if ok != tt.ok {
				t.Errorf("collectAggregateInput ok = %v, want %v", ok, tt.ok)
			}
			if tt.ok && got != tt.want {
				t.Errorf("collectAggregateInput = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupConcatSeparatorForRows(t *testing.T) {
	cat := newEmptyCatalog()
	fc := &query.FunctionCall{
		Name: "GROUP_CONCAT",
		Args: []query.Expression{&query.Identifier{Name: "x"}, &query.StringLiteral{Value: "|"}},
	}
	sep := cat.groupConcatSeparatorForRows(fc, [][]interface{}{{int64(1)}}, nil, nil)
	if sep != "|" {
		t.Errorf("expected '|', got %q", sep)
	}
}

func TestGroupConcatOrderedRows(t *testing.T) {
	cat := newEmptyCatalog()
	rows := [][]interface{}{
		{int64(3)},
		{int64(1)},
		{int64(2)},
	}
	orderBy := []*query.OrderByExpr{
		{Expr: &query.Identifier{Name: "val"}},
	}
	columns := []ColumnDef{{Name: "val", Type: "INTEGER"}}

	ordered := cat.groupConcatOrderedRows(orderBy, rows, columns, nil)
	if len(ordered) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(ordered))
	}
	if ordered[0][0] != int64(1) {
		t.Errorf("expected first row val 1, got %v", ordered[0][0])
	}
	if ordered[1][0] != int64(2) {
		t.Errorf("expected second row val 2, got %v", ordered[1][0])
	}
	if ordered[2][0] != int64(3) {
		t.Errorf("expected third row val 3, got %v", ordered[2][0])
	}
}

func TestFilterAggregateRows(t *testing.T) {
	cat := newEmptyCatalog()
	rows := [][]interface{}{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
	}
	columns := []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "TEXT"}}

	filtered := cat.filterAggregateRows(nil, rows, columns, nil)
	if len(filtered) != 3 {
		t.Errorf("expected 3 rows, got %d", len(filtered))
	}
}

// ---------------------------------------------------------------------------
// Tier 5-12: Window, RLS, Triggers, RETURNING, FOR UPDATE, DDL, FK, Buffered insert
// ---------------------------------------------------------------------------

func TestResolveHiddenWindowCols(t *testing.T) {
	cat := newEmptyCatalog()
	table := &TableDef{
		Name:    "test",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "INTEGER"}},
	}
	table.buildColumnIndexCache()
	cat.tables["test"] = table

	selectCols := &[]selectColInfo{{isWindow: true, windowExpr: &query.WindowExpr{
		Function:    "ROW_NUMBER",
		PartitionBy: []query.Expression{&query.Identifier{Name: "val"}},
		OrderBy:     []*query.OrderByExpr{{Expr: &query.Identifier{Name: "id"}, Desc: false}},
		Args:        []query.Expression{},
	}}}

	hidden := cat.resolveHiddenWindowCols(nil, selectCols, table.Columns, "", table.Columns, nil)
	t.Logf("hidden cols: %d", hidden)
}

func TestSelectNeedsFullRowsForRLS(t *testing.T) {
	cat := newEmptyCatalog()
	cat.tables["test"] = &TableDef{Name: "test"}

	if cat.selectNeedsFullRowsForRLS("test") {
		t.Errorf("expected false when RLS not enabled")
	}

	cat.enableRLS = true
	cat.rlsManager = security.NewManager()
	cat.rlsManager.EnableTable("test")
	cat.rlsCtx = context.WithValue(context.Background(), security.RLSUserKey, "testuser")

	if !cat.selectNeedsFullRowsForRLS("test") {
		t.Errorf("expected true when RLS is enabled for table")
	}
}

func TestCheckRowAccessLocked(t *testing.T) {
	cat := newEmptyCatalog()
	cat.enableRLS = true
	cat.rlsManager = security.NewManager()
	cat.rlsManager.EnableTable("test")

	cat2 := newEmptyCatalog()
	allowed, err := cat2.checkRowAccessLocked(nil, "test", nil, nil, security.PolicySelect)
	if err != nil {
		t.Fatalf("checkRowAccessLocked: %v", err)
	}
	if !allowed {
		t.Errorf("expected allowed when RLS not enabled")
	}

	allowed, err = cat.checkRowAccessLocked(nil, "test", nil, nil, security.PolicySelect)
	if err != nil {
		t.Fatalf("checkRowAccessLocked: %v", err)
	}
	if !allowed {
		t.Errorf("expected allowed when no user in context")
	}
}

func TestCheckRowCheckLocked(t *testing.T) {
	cat := newEmptyCatalog()
	cat.enableRLS = true
	cat.rlsManager = security.NewManager()
	cat.rlsManager.EnableTable("test")

	cat2 := newEmptyCatalog()
	allowed, err := cat2.checkRowCheckLocked(nil, "test", nil, nil, security.PolicyInsert)
	if err != nil {
		t.Fatalf("checkRowCheckLocked: %v", err)
	}
	if !allowed {
		t.Errorf("expected allowed when RLS not enabled")
	}
}

func TestFilterRowsForSelectRLSLocked(t *testing.T) {
	cat := newEmptyCatalog()
	cat.tables["test"] = &TableDef{Name: "test", Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}}}

	rows := [][]interface{}{{int64(1)}, {int64(2)}}
	filtered, err := cat.filterRowsForSelectRLSLocked(nil, "test", nil, rows)
	if err != nil {
		t.Fatalf("filterRowsForSelectRLSLocked: %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("expected 2 rows, got %d", len(filtered))
	}
}

func TestExecuteTriggersList(t *testing.T) {
	cat := newEmptyCatalog()
	cat.tables["test"] = &TableDef{
		Name:    "test",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}},
	}
	columns := []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "val", Type: "TEXT"}}

	err := cat.executeTriggersList(nil, nil, "INSERT", "AFTER", nil, nil, columns)
	if err != nil {
		t.Fatalf("executeTriggersList nil triggers: %v", err)
	}

	triggers := []*query.CreateTriggerStmt{
		{Name: "trg1", Table: "test", Event: "INSERT", Time: "BEFORE", Body: []query.Statement{&query.SelectStmt{}}},
	}
	err = cat.executeTriggersList(nil, triggers, "INSERT", "AFTER", nil, nil, columns)
	if err != nil {
		t.Fatalf("executeTriggersList wrong timing: %v", err)
	}

	triggers2 := []*query.CreateTriggerStmt{
		{Name: "trg2", Table: "test", Event: "INSERT", Time: "AFTER"},
	}
	err = cat.executeTriggersList(nil, triggers2, "INSERT", "AFTER", nil, nil, columns)
	if err != nil {
		t.Fatalf("executeTriggersList empty body: %v", err)
	}

	triggers3 := []*query.CreateTriggerStmt{
		{
			Name:      "trg3",
			Table:     "test",
			Event:     "INSERT",
			Time:      "AFTER",
			Condition: &query.BooleanLiteral{Value: false},
			Body:      []query.Statement{&query.SelectStmt{Columns: []query.Expression{&query.NumberLiteral{Value: 1}}}},
		},
	}
	err = cat.executeTriggersList(nil, triggers3, "INSERT", "AFTER", nil, nil, columns)
	if err != nil {
		t.Fatalf("executeTriggersList false condition: %v", err)
	}
}

func TestEvaluateReturningExpr(t *testing.T) {
	cat := newEmptyCatalog()
	table := &TableDef{
		Name:    "test",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "name", Type: "TEXT"}},
	}
	row := []interface{}{int64(1), "alice"}

	tests := []struct {
		name    string
		expr    query.Expression
		wantVal interface{}
		wantCol string
		wantErr bool
	}{
		{
			name:    "star column ref",
			expr:    &query.ColumnRef{Column: "*"},
			wantVal: int64(1),
			wantCol: "id",
		},
		{
			name:    "identifier column",
			expr:    &query.Identifier{Name: "id"},
			wantVal: int64(1),
			wantCol: "id",
		},
		{
			name:    "missing column",
			expr:    &query.Identifier{Name: "nonexistent"},
			wantErr: true,
		},
		{
			name:    "qualified identifier",
			expr:    &query.QualifiedIdentifier{Table: "test", Column: "name"},
			wantVal: "alice",
			wantCol: "name",
		},
		{
			name:    "complex expression",
			expr:    &query.NumberLiteral{Value: 42},
			wantVal: float64(42),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vals, cols, err := cat.evaluateReturningExpr(tt.expr, row, table, nil)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.wantErr {
				if len(vals) == 0 {
					t.Fatal("expected at least 1 value")
				}
				if vals[0] != tt.wantVal {
					t.Errorf("value = %v, want %v", vals[0], tt.wantVal)
				}
				if tt.wantCol != "" && len(cols) > 0 && cols[0] != tt.wantCol {
					t.Errorf("column = %s, want %s", cols[0], tt.wantCol)
				}
			}
		})
	}
}

func TestRecordForUpdateReadsNoTxn(t *testing.T) {
	cat := newEmptyCatalog()
	table := &TableDef{Name: "test"}
	err := cat.recordForUpdateReads(table, nil, nil)
	if err != nil {
		t.Fatalf("recordForUpdateReads: %v", err)
	}
}

func TestCreateCollectionAndDropTable(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	_, err = cat.ExecuteQuery("CREATE TABLE regular_tbl (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Use CreateCollection directly since ExecuteQuery doesn't support it
	err = cat.CreateCollection(&query.CreateCollectionStmt{Name: "coll_test"})
	if err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}

	err = cat.DropTable(&query.DropTableStmt{Table: "regular_tbl", IfExists: false})
	if err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	err = cat.DropTable(&query.DropTableStmt{Table: "coll_test", IfExists: false})
	if err != nil {
		t.Fatalf("DropTable collection: %v", err)
	}
}

func TestAlterTableAddForeignKeyConstraint(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	_, err = cat.ExecuteQuery("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("CREATE TABLE parent: %v", err)
	}
	_, err = cat.ExecuteQuery("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE child: %v", err)
	}

	alterStmt := &query.AlterTableStmt{
		Table:          "child",
		ConstraintName: "fk_child_parent",
		ForeignKey: &query.ForeignKeyDef{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"id"},
		},
	}
	err = cat.AlterTableAddForeignKeyConstraint(nil, alterStmt)
	if err != nil {
		t.Fatalf("AlterTableAddForeignKeyConstraint: %v", err)
	}

	childTable, _ := cat.GetTable("child")
	if len(childTable.ForeignKeys) != 1 {
		t.Errorf("expected 1 FK, got %d", len(childTable.ForeignKeys))
	}
}

func TestValuesEqual(t *testing.T) {
	fke := &ForeignKeyEnforcer{}
	tests := []struct {
		name string
		a, b interface{}
		want bool
	}{
		{"both nil", nil, nil, true},
		{"a nil b non-nil", nil, int64(1), false},
		{"a non-nil b nil", int64(1), nil, false},
		{"both int64 equal", int64(5), int64(5), true},
		{"both int64 different", int64(5), int64(10), false},
		{"int64 vs float64 equal", int64(5), float64(5), true},
		{"float64 vs float64 equal", 3.14, 3.14, true},
		{"float64 vs float64 different", 3.14, 2.71, false},
		{"string equal", "hello", "hello", true},
		{"string different", "hello", "world", false},
		{"int vs int64 equal", int(5), int64(5), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fke.valuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("valuesEqual(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestPendingIndexDeletesForRow(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	_, err = cat.ExecuteQuery("CREATE TABLE idx_test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = cat.ExecuteQuery("CREATE INDEX idx_test_val ON idx_test(val)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	table, _ := cat.GetTable("idx_test")
	fke := &ForeignKeyEnforcer{catalog: cat}
	updates := fke.pendingIndexDeletesForRow(table, "idx_test", "pk:1", []interface{}{int64(1), int64(42)})
	t.Logf("pending index deletes: %d", len(updates))
}

func TestPendingIndexUpdatesForRowChange(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	_, err = cat.ExecuteQuery("CREATE TABLE upd_test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	_, err = cat.ExecuteQuery("CREATE INDEX upd_test_val ON upd_test(val)")
	if err != nil {
		t.Fatalf("CREATE INDEX: %v", err)
	}

	table, _ := cat.GetTable("upd_test")
	fke := &ForeignKeyEnforcer{catalog: cat}
	updates := fke.pendingIndexUpdatesForRowChange(table, "upd_test", "pk:1",
		[]interface{}{int64(1), int64(42)},
		[]interface{}{int64(1), int64(99)})
	t.Logf("pending index updates: %d", len(updates))
}

func TestGetInsertTargetTree(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	_, err = cat.ExecuteQuery("CREATE TABLE ins_test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	table, _ := cat.GetTable("ins_test")

	insertStmt := &query.InsertStmt{
		Table: "ins_test",
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 10}},
		},
	}

	targetTree, _, err := cat.getInsertTargetTree(table, insertStmt, nil)
	if err != nil {
		t.Fatalf("getInsertTargetTree: %v", err)
	}
	if targetTree == nil {
		t.Error("expected non-nil tree")
	}
}

func TestEvaluateMathFunctionMore(t *testing.T) {
	val, handled, err := evaluateMathFunctionRaw("ROUND", []interface{}{123.456, -1.0})
	if err != nil {
		t.Fatalf("ROUND(-1): %v", err)
	}
	if !handled {
		t.Fatal("ROUND should be handled")
	}
	if val.(float64) < 119 || val.(float64) > 121 {
		t.Errorf("ROUND(123.456, -1) = %v, want ~120", val)
	}

	val, handled, _ = evaluateMathFunctionRaw("ROUND", []interface{}{"hello"})
	if !handled || val != "hello" {
		t.Errorf("ROUND non-numeric = %v, want hello", val)
	}

	val, handled, _ = evaluateMathFunctionRaw("ABS", []interface{}{-3.5})
	if !handled || val != 3.5 {
		t.Errorf("ABS(-3.5) = %v, want 3.5", val)
	}

	val, handled, _ = evaluateMathFunctionRaw("ABS", []interface{}{"non-numeric"})
	if !handled || val != "non-numeric" {
		t.Errorf("ABS(non-numeric) = %v, want non-numeric", val)
	}
}

func TestEvalUnaryExprMore(t *testing.T) {
	ctx := &EvalContext{}

	val, err := ctx.EvalUnaryExpr(int64(7), query.TokenBitNot)
	if err != nil {
		t.Fatalf("BitNot int64: %v", err)
	}
	if val != ^int64(7) {
		t.Errorf("BitNot 7 = %v, want %v", val, ^int64(7))
	}

	val, err = ctx.EvalUnaryExpr(nil, query.TokenBitNot)
	if err != nil {
		t.Fatalf("BitNot nil: %v", err)
	}
	if val != nil {
		t.Errorf("BitNot nil = %v, want nil", val)
	}
}

func TestPersistEmptyCatalog(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)
	if err := cat.Save(); err != nil {
		t.Fatalf("Save empty: %v", err)
	}
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatalf("Load empty: %v", err)
	}
}

func TestPersistTableWithCheckAndDefault(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	_, err = cat.ExecuteQuery("CREATE TABLE chk_tbl (id INTEGER PRIMARY KEY, age INTEGER DEFAULT 0 CHECK (age >= 0))")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := cat.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	cat2 := New(tree, pool, nil)
	if err := cat2.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, err := cat2.GetTable("chk_tbl"); err != nil {
		t.Errorf("table not found after load: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Additional coverage for remaining low-coverage functions
// ---------------------------------------------------------------------------

// TestSelectColInfoWithGroupConcatSeparator tests the GROUP_CONCAT separator resolution.
func TestSelectColInfoWithGroupConcatSeparator(t *testing.T) {
	cat := newEmptyCatalog()
	ci := selectColInfo{
		aggregateType:    "GROUP_CONCAT",
		aggregateSepExpr: &query.StringLiteral{Value: ";"},
	}
	groupRows := [][]interface{}{{int64(1)}}
	columns := []ColumnDef{{Name: "val", Type: "INTEGER"}}

	result := cat.selectColInfoWithGroupConcatSeparator(ci, groupRows, columns, nil)
	if !result.aggregateSepOK {
		t.Errorf("expected aggregateSepOK to be true")
	}
	if result.aggregateSep != ";" {
		t.Errorf("expected separator ';', got %q", result.aggregateSep)
	}
}

// TestDropTableIfExists tests DropTable with IfExists for non-existent table.
func TestDropTableIfExists(t *testing.T) {
	pool := storage.NewBufferPool(4096, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree: %v", err)
	}
	cat := New(tree, pool, nil)

	// Drop non-existent table with IfExists
	err = cat.DropTable(&query.DropTableStmt{Table: "nonexistent", IfExists: true})
	if err != nil {
		t.Fatalf("DropTable IfExists: %v", err)
	}

	// Drop non-existent table without IfExists
	err = cat.DropTable(&query.DropTableStmt{Table: "nonexistent", IfExists: false})
	if err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

// TestWindowAggFuncBasic tests evalWindowAggFunc with COUNT window function.
func TestWindowAggFuncBasic(t *testing.T) {
	cat := newEmptyCatalog()
	cat.tables["test"] = &TableDef{Name: "test", Columns: []ColumnDef{{Name: "x", Type: "INTEGER"}}}
	rows := [][]interface{}{{nil}, {nil}, {nil}}
	entries := []windowPartEntry{
		{originalIdx: 0, row: []interface{}{int64(1)}},
		{originalIdx: 1, row: []interface{}{int64(2)}},
		{originalIdx: 2, row: []interface{}{int64(3)}},
	}

	// COUNT(*) - uses StarExpr so no argument evaluation needed
	we := &query.WindowExpr{
		Function: "COUNT",
		Args:     []query.Expression{&query.StarExpr{}},
	}

	result := cat.evalWindowAggFunc(rows, 0, entries, we, nil, cat.tables["test"], nil)
	if !result {
		t.Errorf("expected true (handled)")
	}
	if rows[0][0] != int64(3) {
		t.Errorf("expected COUNT=3, got %v", rows[0][0])
	}
}

// TestCheckRowCheckLockedWithUser tests checkRowCheckLocked with a user context.
func TestCheckRowCheckLockedWithUser(t *testing.T) {
	cat := newEmptyCatalog()
	cat.enableRLS = true
	cat.rlsManager = security.NewManager()
	cat.rlsManager.EnableTable("test")

	ctx := context.WithValue(context.Background(), security.RLSUserKey, "testuser")
	allowed, err := cat.checkRowCheckLocked(ctx, "test", nil, nil, security.PolicyInsert)
	if err != nil {
		t.Fatalf("checkRowCheckLocked: %v", err)
	}
	// Just verify no panic - the actual result depends on RLS policy configuration
	t.Logf("checkRowCheckLocked with user: allowed=%v", allowed)
}

// TestFilterRowsForSelectRLSLockedWithRLS tests RLS filtering with RLS enabled.
func TestFilterRowsForSelectRLSLockedWithRLS(t *testing.T) {
	cat := newEmptyCatalog()
	cat.tables["test"] = &TableDef{Name: "test", Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}}}
	cat.enableRLS = true
	cat.rlsManager = security.NewManager()
	cat.rlsManager.EnableTable("test")

	rows := [][]interface{}{{int64(1)}, {int64(2)}}
	filtered, err := cat.filterRowsForSelectRLSLocked(nil, "test", nil, rows)
	if err != nil {
		t.Fatalf("filterRowsForSelectRLSLocked: %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("expected 2 rows, got %d", len(filtered))
	}
}

// TestDateAddDays tests the dateAddDays function.
func TestDateAddDays(t *testing.T) {
	addDays := dateAddDays(1)
	subDays := dateAddDays(-1)
	tests := []struct {
		name string
		fn   functionHandler
		args []interface{}
		want interface{}
	}{
		{"add 1 day", addDays, []interface{}{"2024-01-01", int64(1)}, "2024-01-02"},
		{"subtract 1 day", subDays, []interface{}{"2024-01-01", int64(1)}, "2023-12-31"},
		{"add 0 days", addDays, []interface{}{"2024-01-15", int64(0)}, "2024-01-15"},
		{"cross month", addDays, []interface{}{"2024-01-31", int64(1)}, "2024-02-01"},
		{"datetime", addDays, []interface{}{"2024-01-01 12:30:00", int64(1)}, "2024-01-02 12:30:00"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fn(tt.args)
			if err != nil {
				t.Fatalf("dateAddDays: %v", err)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}

	got, err := addDays([]interface{}{nil, int64(1)})
	if err != nil {
		t.Fatalf("nil date: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

// TestSelectColInfoWithGroupConcatSeparator tests the GROUP_CONCAT separator resolution.
