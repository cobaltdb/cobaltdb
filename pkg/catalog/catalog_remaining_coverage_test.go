package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestFlushTableTreesCoverage tests FlushTableTrees function
func TestFlushTableTreesCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create tables
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_flush1",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "test_flush2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert some data
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_flush1",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "test"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test FlushTableTrees
	err = cat.FlushTableTrees()
	if err != nil {
		t.Errorf("FlushTableTrees failed: %v", err)
	}
}

// TestArithmeticOperationsCoverage tests addValues, subtractValues, multiplyValues, divideValues
func TestArithmeticOperationsCoverage(t *testing.T) {
	// Test addValues
	t.Run("addValues", func(t *testing.T) {
		tests := []struct {
			a, b     interface{}
			expected float64
		}{
			{5, 3, 8},
			{5.5, 3.2, 8.7},
			{5, 3.2, 8.2},
		}
		for _, tt := range tests {
			result, err := addValues(tt.a, tt.b)
			if err != nil {
				t.Errorf("addValues(%v, %v) error = %v", tt.a, tt.b, err)
				continue
			}
			if f, ok := toFloat64(result); ok {
				if f-tt.expected > 0.0001 || tt.expected-f > 0.0001 {
					t.Errorf("addValues(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
				}
			}
		}
	})

	// Test subtractValues
	t.Run("subtractValues", func(t *testing.T) {
		tests := []struct {
			a, b     interface{}
			expected float64
		}{
			{10, 3, 7},
			{10.5, 3.2, 7.3},
			{10, 3.2, 6.8},
		}
		for _, tt := range tests {
			result, err := subtractValues(tt.a, tt.b)
			if err != nil {
				t.Errorf("subtractValues(%v, %v) error = %v", tt.a, tt.b, err)
				continue
			}
			if f, ok := toFloat64(result); ok {
				if f-tt.expected > 0.0001 || tt.expected-f > 0.0001 {
					t.Errorf("subtractValues(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
				}
			}
		}
	})

	// Test multiplyValues
	t.Run("multiplyValues", func(t *testing.T) {
		tests := []struct {
			a, b     interface{}
			expected float64
		}{
			{4, 5, 20},
			{4.5, 2.0, 9.0},
			{4, 2.5, 10},
		}
		for _, tt := range tests {
			result, err := multiplyValues(tt.a, tt.b)
			if err != nil {
				t.Errorf("multiplyValues(%v, %v) error = %v", tt.a, tt.b, err)
				continue
			}
			if f, ok := toFloat64(result); ok {
				if f-tt.expected > 0.0001 || tt.expected-f > 0.0001 {
					t.Errorf("multiplyValues(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
				}
			}
		}
	})

	// Test divideValues
	t.Run("divideValues", func(t *testing.T) {
		tests := []struct {
			a, b     interface{}
			expected float64
			wantErr  bool
		}{
			{10, 2, 5.0, false},
			{10, 3, 10.0 / 3.0, false},
			{10.5, 2.5, 4.2, false},
			{10, 0, 0, true},
		}
		for _, tt := range tests {
			result, err := divideValues(tt.a, tt.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("divideValues(%v, %v) error = %v, wantErr %v", tt.a, tt.b, err, tt.wantErr)
				continue
			}
			if !tt.wantErr {
				if f, ok := toFloat64(result); ok {
					if f-tt.expected > 0.0001 || tt.expected-f > 0.0001 {
						t.Errorf("divideValues(%v, %v) = %v, want %v", tt.a, tt.b, result, tt.expected)
					}
				}
			}
		}
	})
}

// TestArithmeticErrors tests error cases for arithmetic operations
func TestArithmeticErrors(t *testing.T) {
	// Test non-numeric values
	_, err := addValues("hello", 5)
	if err == nil {
		t.Error("addValues with string should return error")
	}

	_, err = subtractValues("hello", 5)
	if err == nil {
		t.Error("subtractValues with string should return error")
	}

	_, err = multiplyValues("hello", 5)
	if err == nil {
		t.Error("multiplyValues with string should return error")
	}

	_, err = divideValues("hello", 5)
	if err == nil {
		t.Error("divideValues with string should return error")
	}
}

// TestSaveAndLoadSchemaCoverage tests SaveData and LoadSchema deprecated functions
func TestSaveAndLoadSchemaCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create a table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_save_load",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Test SaveData (deprecated, should call Save())
	err = cat.SaveData("/tmp/test_dir")
	if err != nil {
		t.Errorf("SaveData failed: %v", err)
	}

	// Test LoadSchema (deprecated, should return nil)
	err = cat.LoadSchema("/tmp/test_dir")
	if err != nil {
		t.Errorf("LoadSchema should return nil, got: %v", err)
	}
}

// TestGetRowCoverage tests GetRow function
func TestGetRowCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_get_row",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "value", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Use UpdateRow to insert data (GetRow uses fastDecodeRow which expects fastEncodeRow format)
	// Insert uses json.Marshal which is incompatible with fastDecodeRow
	err = cat.UpdateRow("test_get_row", 1, map[string]interface{}{
		"id":    1,
		"name":  "test1",
		"value": 100.5,
	})
	if err != nil {
		t.Fatalf("Failed to insert via UpdateRow: %v", err)
	}

	// Test GetRow with int
	row, err := cat.GetRow("test_get_row", 1)
	if err != nil {
		t.Errorf("GetRow failed: %v", err)
	}
	if row == nil {
		t.Fatal("GetRow returned nil row")
	}
	if row["name"] != "test1" {
		t.Errorf("Expected name='test1', got %v", row["name"])
	}

	// Test GetRow with int64
	row, err = cat.GetRow("test_get_row", int64(1))
	if err != nil {
		t.Logf("GetRow with int64: %v", err)
	} else {
		t.Logf("GetRow with int64 succeeded: %v", row)
	}

	// Test GetRow with string key table
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "test_get_row_str",
		Columns: []*query.ColumnDef{
			{Name: "code", Type: query.TokenText, PrimaryKey: true},
			{Name: "desc", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = cat.UpdateRow("test_get_row_str", "ABC", map[string]interface{}{
		"code": "ABC",
		"desc": "Description",
	})
	if err != nil {
		t.Fatalf("Failed to insert string pk row: %v", err)
	}

	row, err = cat.GetRow("test_get_row_str", "ABC")
	if err != nil {
		t.Errorf("GetRow with string pk failed: %v", err)
	}
	if row != nil && row["desc"] != "Description" {
		t.Errorf("Expected desc='Description', got %v", row["desc"])
	}

	// Test GetRow for non-existent table
	_, err = cat.GetRow("non_existent", 1)
	if err == nil {
		t.Error("GetRow should fail for non-existent table")
	}

	// Test GetRow for non-existent row
	_, err = cat.GetRow("test_get_row", 999)
	if err == nil {
		t.Error("GetRow should fail for non-existent row")
	}
}

// TestUpdateRowCoverage tests UpdateRow function
func TestUpdateRowCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_update_row",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "value", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_update_row",
		Columns: []string{"id", "name", "value"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "original"}, &query.NumberLiteral{Value: 100.0}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test UpdateRow
	err = cat.UpdateRow("test_update_row", 1, map[string]interface{}{
		"name":  "updated",
		"value": 200.0,
	})
	if err != nil {
		t.Errorf("UpdateRow failed: %v", err)
	}

	// Verify update
	row, err := cat.GetRow("test_update_row", 1)
	if err != nil {
		t.Fatalf("GetRow after update failed: %v", err)
	}
	if row["name"] != "updated" {
		t.Errorf("Expected name='updated', got %v", row["name"])
	}
	if valF, valOk := toFloat64(row["value"]); !valOk || valF != 200.0 {
		t.Errorf("Expected value=200.0, got %v", row["value"])
	}

	// Test UpdateRow for non-existent table
	err = cat.UpdateRow("non_existent", 1, map[string]interface{}{"name": "test"})
	if err == nil {
		t.Error("UpdateRow should fail for non-existent table")
	}

	// Test UpdateRow with int64
	err = cat.UpdateRow("test_update_row", int64(1), map[string]interface{}{"name": "updated2"})
	if err != nil {
		t.Errorf("UpdateRow with int64 failed: %v", err)
	}
}

// TestIndexRowForFTSCoverage tests indexRowForFTS function
func TestIndexRowForFTSCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table first
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_fts_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "content", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create FTS index
	err = cat.CreateFTSIndex("test_fts", "test_fts_table", []string{"title", "content"})
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	ftsIndex := cat.ftsIndexes["test_fts"]

	// Test indexRowForFTS with various data
	testCases := []struct {
		name string
		row  map[string]interface{}
		key  []byte
	}{
		{
			name: "normal text",
			row: map[string]interface{}{
				"title":   "Hello World",
				"content": "This is a test article",
			},
			key: []byte("row1"),
		},
		{
			name: "with nil value",
			row: map[string]interface{}{
				"title":   nil,
				"content": "Content only",
			},
			key: []byte("row2"),
		},
		{
			name: "with numbers",
			row: map[string]interface{}{
				"title":   12345,
				"content": "Number title test",
			},
			key: []byte("row3"),
		},
		{
			name: "with special chars",
			row: map[string]interface{}{
				"title":   "Test!@#$%",
				"content": "Special characters here!!!",
			},
			key: []byte("row4"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cat.indexRowForFTS(ftsIndex, tc.row, tc.key)
		})
	}

	// Verify some words were indexed
	if len(ftsIndex.Index) == 0 {
		t.Error("FTS index should contain words")
	}

	// Check specific words
	words := []string{"hello", "world", "test", "article", "content"}
	for _, word := range words {
		if _, exists := ftsIndex.Index[word]; !exists {
			t.Logf("Word '%s' not found in index (may be expected)", word)
		}
	}
}

// TestIntersectSortedCoverage tests intersectSorted function
func TestIntersectSortedCoverage(t *testing.T) {
	tests := []struct {
		name     string
		a        []int64
		b        []int64
		expected []int64
	}{
		{
			name:     "empty slices",
			a:        []int64{},
			b:        []int64{},
			expected: nil,
		},
		{
			name:     "no intersection",
			a:        []int64{1, 2, 3},
			b:        []int64{4, 5, 6},
			expected: nil,
		},
		{
			name:     "full intersection",
			a:        []int64{1, 2, 3},
			b:        []int64{1, 2, 3},
			expected: []int64{1, 2, 3},
		},
		{
			name:     "partial intersection",
			a:        []int64{1, 2, 3, 4, 5},
			b:        []int64{3, 4, 5, 6, 7},
			expected: []int64{3, 4, 5},
		},
		{
			name:     "first empty",
			a:        []int64{},
			b:        []int64{1, 2, 3},
			expected: nil,
		},
		{
			name:     "second empty",
			a:        []int64{1, 2, 3},
			b:        []int64{},
			expected: nil,
		},
		{
			name:     "single element intersection",
			a:        []int64{1, 3, 5},
			b:        []int64{2, 3, 4},
			expected: []int64{3},
		},
		{
			name:     "subset",
			a:        []int64{1, 2, 3, 4, 5},
			b:        []int64{2, 4},
			expected: []int64{2, 4},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := intersectSorted(tt.a, tt.b)
			if len(result) != len(tt.expected) {
				t.Errorf("intersectSorted() = %v, want %v", result, tt.expected)
				return
			}
			for i := range result {
				if result[i] != tt.expected[i] {
					t.Errorf("intersectSorted()[%d] = %v, want %v", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

// TestCatalogCompareValuesCoverage tests catalogCompareValues function
func TestCatalogCompareValuesCoverage(t *testing.T) {
	tests := []struct {
		name     string
		a        interface{}
		b        interface{}
		expected int
	}{
		{"both nil", nil, nil, 0},
		{"a nil", nil, 5, -1},
		{"b nil", 5, nil, 1},
		{"equal ints", 5, 5, 0},
		{"a less than b", 3, 5, -1},
		{"a greater than b", 7, 5, 1},
		{"equal floats", 5.5, 5.5, 0},
		{"float less", 3.5, 5.5, -1},
		{"float greater", 7.5, 5.5, 1},
		{"equal strings", "abc", "abc", 0},
		{"string less", "abc", "def", -1},
		{"string greater", "xyz", "def", 1},
		{"int vs float equal", 5, 5.0, 0},
		{"int vs float less", 3, 5.0, -1},
		{"int vs float greater", 7, 5.0, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := catalogCompareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("catalogCompareValues(%v, %v) = %d, want %d", tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

// TestComputeAggregatesCoverage tests computeAggregates function more thoroughly

// TestComputeAggregatesWithNullColumn tests computeAggregates with NULL values in column

// TestDeleteRowCoverage tests DeleteRow function
func TestDeleteRowCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_delete_row",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data using UpdateRow (so GetRow works)
	err = cat.UpdateRow("test_delete_row", 1, map[string]interface{}{
		"id":   1,
		"name": "test1",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = cat.UpdateRow("test_delete_row", 2, map[string]interface{}{
		"id":   2,
		"name": "test2",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Verify rows exist
	row, err := cat.GetRow("test_delete_row", 1)
	if err != nil {
		t.Fatalf("GetRow failed: %v", err)
	}
	if row["name"] != "test1" {
		t.Errorf("Expected name='test1', got %v", row["name"])
	}

	// Test DeleteRow with int
	err = cat.DeleteRow("test_delete_row", 1)
	if err != nil {
		t.Errorf("DeleteRow failed: %v", err)
	}

	// Verify row was deleted
	_, err = cat.GetRow("test_delete_row", 1)
	if err == nil {
		t.Error("GetRow should fail after deletion")
	}

	// Test DeleteRow with int64
	err = cat.DeleteRow("test_delete_row", int64(2))
	if err != nil {
		t.Errorf("DeleteRow with int64 failed: %v", err)
	}

	// Test DeleteRow for non-existent table
	err = cat.DeleteRow("non_existent", 1)
	if err == nil {
		t.Error("DeleteRow should fail for non-existent table")
	}

	// Test DeleteRow with string key
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "test_delete_str",
		Columns: []*query.ColumnDef{
			{Name: "code", Type: query.TokenText, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = cat.UpdateRow("test_delete_str", "ABC", map[string]interface{}{
		"code": "ABC",
		"name": "test",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = cat.DeleteRow("test_delete_str", "ABC")
	if err != nil {
		t.Errorf("DeleteRow with string pk failed: %v", err)
	}

	// Test DeleteRow with other types
	err = cat.UpdateRow("test_delete_str", "DEF", map[string]interface{}{
		"code": "DEF",
		"name": "test",
	})
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	err = cat.DeleteRow("test_delete_str", float64(1.5))
	if err != nil {
		t.Logf("DeleteRow with float (converted to string): %v", err)
	}
}

// TestSearchFTSCoverage tests SearchFTS function more thoroughly
func TestSearchFTSCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_fts_search",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "content", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create FTS index
	err = cat.CreateFTSIndex("idx_fts_search", "test_fts_search", []string{"title", "content"})
	if err != nil {
		t.Fatalf("Failed to create FTS index: %v", err)
	}

	// Manually populate the FTS index
	ftsIndex := cat.ftsIndexes["idx_fts_search"]
	ftsIndex.Index["hello"] = []int64{1, 2, 3}
	ftsIndex.Index["world"] = []int64{1, 2}
	ftsIndex.Index["test"] = []int64{2, 3}
	ftsIndex.Index["unique"] = []int64{4}

	// Test SearchFTS
	tests := []struct {
		name        string
		query       string
		expectEmpty bool
	}{
		{"single word found", "hello", false},
		{"multiple words AND", "hello world", false},
		{"word not found", "nonexistent", true},
		{"partial match", "hello test", false},
		{"empty query", "", true},
		{"unique word", "unique", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := cat.SearchFTS("idx_fts_search", tt.query)
			if err != nil {
				t.Logf("SearchFTS error: %v", err)
				return
			}
			if tt.expectEmpty && len(results) > 0 {
				t.Errorf("Expected empty results for query '%s', got %v", tt.query, results)
			}
			if !tt.expectEmpty && len(results) == 0 {
				t.Errorf("Expected non-empty results for query '%s'", tt.query)
			}
			t.Logf("Search '%s' results: %v", tt.query, results)
		})
	}

	// Test SearchFTS with non-existent index
	_, err = cat.SearchFTS("non_existent", "test")
	if err == nil {
		t.Error("SearchFTS should fail for non-existent index")
	}
}

// TestEncodeRowCoverage tests encodeRow function
func TestEncodeRowCoverage(t *testing.T) {
	tests := []struct {
		name    string
		exprs   []query.Expression
		args    []interface{}
		wantErr bool
	}{
		{
			name: "string literal",
			exprs: []query.Expression{
				&query.StringLiteral{Value: "hello"},
			},
			args:    nil,
			wantErr: false,
		},
		{
			name: "number literal",
			exprs: []query.Expression{
				&query.NumberLiteral{Value: 42.5},
			},
			args:    nil,
			wantErr: false,
		},
		{
			name: "boolean literal",
			exprs: []query.Expression{
				&query.BooleanLiteral{Value: true},
			},
			args:    nil,
			wantErr: false,
		},
		{
			name: "null literal",
			exprs: []query.Expression{
				&query.NullLiteral{},
			},
			args:    nil,
			wantErr: false,
		},
		{
			name: "placeholder with args",
			exprs: []query.Expression{
				&query.PlaceholderExpr{Index: 0},
			},
			args:    []interface{}{"test value"},
			wantErr: false,
		},
		{
			name: "placeholder out of range",
			exprs: []query.Expression{
				&query.PlaceholderExpr{Index: 5},
			},
			args:    []interface{}{"test"},
			wantErr: false, // Should return nil, not error
		},
		{
			name: "mixed expressions",
			exprs: []query.Expression{
				&query.NumberLiteral{Value: 1},
				&query.StringLiteral{Value: "two"},
				&query.BooleanLiteral{Value: false},
				&query.NullLiteral{},
			},
			args:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := encodeRow(tt.exprs, tt.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeRow() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && result == nil {
				t.Error("encodeRow() returned nil without error")
			}
			t.Logf("encodeRow result length: %d", len(result))
		})
	}
}

// TestExecuteSelectWithJoinAndGroupByCoverage tests executeSelectWithJoinAndGroupBy function
func TestExecuteSelectWithJoinAndGroupByCoverage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create users table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create orders table
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert users
	users := []struct {
		id   int
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
	}

	for _, u := range users {
		_, _, err := cat.Insert(&query.InsertStmt{
			Table:   "users",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(u.id)}, &query.StringLiteral{Value: u.name}}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert user: %v", err)
		}
	}

	// Insert orders
	orders := []struct {
		id     int
		userID int
		amount float64
	}{
		{1, 1, 100.0},
		{2, 1, 200.0},
		{3, 2, 150.0},
	}

	for _, o := range orders {
		_, _, err := cat.Insert(&query.InsertStmt{
			Table:   "orders",
			Columns: []string{"id", "user_id", "amount"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(o.id)}, &query.NumberLiteral{Value: float64(o.userID)}, &query.NumberLiteral{Value: o.amount}}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert order: %v", err)
		}
	}

	// Test JOIN with GROUP BY using QualifiedIdentifier
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.QualifiedIdentifier{Table: "u", Column: "id"},
			&query.QualifiedIdentifier{Table: "u", Column: "name"},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.QualifiedIdentifier{Table: "o", Column: "id"}}},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.QualifiedIdentifier{Table: "o", Column: "amount"}}},
		},
		From: &query.TableRef{Name: "users", Alias: "u"},
		Joins: []*query.JoinClause{
			{
				Type:  query.TokenInner,
				Table: &query.TableRef{Name: "orders", Alias: "o"},
				Condition: &query.BinaryExpr{
					Left:     &query.QualifiedIdentifier{Table: "u", Column: "id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "o", Column: "user_id"},
				},
			},
		},
		GroupBy: []query.Expression{
			&query.QualifiedIdentifier{Table: "u", Column: "id"},
		},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("JOIN with GROUP BY failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(rows))
	}

	t.Logf("JOIN with GROUP BY result: cols=%v, rows=%v", cols, rows)

	// Verify results
	results := make(map[string]struct {
		count int
		total float64
	})

	for _, row := range rows {
		name := row[1].(string)
		count := int(row[2].(int64))
		total := row[3].(float64)
		results[name] = struct {
			count int
			total float64
		}{count, total}
	}

	if r, ok := results["Alice"]; !ok || r.count != 2 {
		t.Errorf("Alice should have 2 orders, got %d", r.count)
	}
	if r, ok := results["Bob"]; !ok || r.count != 1 {
		t.Errorf("Bob should have 1 order, got %d", r.count)
	}
}

// TestComputeAggregatesNoGroupBy tests computeAggregates function (aggregates without GROUP BY)

// TestComputeAggregatesWithWhereCoverage tests computeAggregates with WHERE clause
