package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestJSONPathGet_WildcardMiddle(t *testing.T) {
	jp, err := ParseJSONPath("$[*].name")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := []interface{}{
		map[string]interface{}{"name": "alice"},
		map[string]interface{}{"name": "bob"},
	}
	result, err := jp.Get(data)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result != "alice" {
		t.Errorf("expected alice, got %v", result)
	}
}

func TestJSONPathGet_WildcardFinalOnArray(t *testing.T) {
	jp, err := ParseJSONPath("$.items[*]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := map[string]interface{}{
		"items": []interface{}{1, 2, 3},
	}
	result, err := jp.Get(data)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	arr, ok := result.([]interface{})
	if !ok {
		t.Fatalf("expected []interface{}, got %T", result)
	}
	if len(arr) != 3 {
		t.Errorf("expected 3 elements, got %d", len(arr))
	}
}

func TestJSONPathGet_WildcardOnNonArray(t *testing.T) {
	jp, err := ParseJSONPath("$[*].x")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	result, err := jp.Get("not an array")
	if err != nil {
		t.Fatalf("Get should not error, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestJSONPathGet_WildcardOnEmptyArray(t *testing.T) {
	jp, err := ParseJSONPath("$[*].x")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := []interface{}{}
	result, err := jp.Get(data)
	if err != nil {
		t.Fatalf("Get should not error, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestJSONPathGet_ArrayIndex(t *testing.T) {
	jp, err := ParseJSONPath("$[0]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := []interface{}{"first", "second", "third"}
	result, err := jp.Get(data)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if result != "first" {
		t.Errorf("expected first, got %v", result)
	}
}

func TestJSONPathGet_ArrayIndexOnNonArray(t *testing.T) {
	jp, err := ParseJSONPath("$[0]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := map[string]interface{}{"key": "val"}
	result, err := jp.Get(data)
	if err != nil {
		t.Fatalf("Get should not error, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestJSONPathGet_ArrayIndexOutOfBounds(t *testing.T) {
	jp, err := ParseJSONPath("$[99]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := []interface{}{1, 2}
	result, err := jp.Get(data)
	if err != nil {
		t.Fatalf("Get should not error, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestJSONPathGet_NonObjectForKey(t *testing.T) {
	jp, err := ParseJSONPath("$.name")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	_, err = jp.Get("just a string")
	if err == nil {
		t.Fatal("expected error for non-object access")
	}
}

func TestJSONPathGet_NonExistentKey(t *testing.T) {
	jp, err := ParseJSONPath("$.missing")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := map[string]interface{}{"name": "alice"}
	_, err = jp.Get(data)
	if err == nil {
		t.Fatal("expected error for non-existent key")
	}
}

func TestJSONPathGet_NilInMiddle(t *testing.T) {
	jp, err := ParseJSONPath("$.a.b")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	data := map[string]interface{}{"a": nil}
	result, err := jp.Get(data)
	if err != nil {
		t.Fatalf("Get should not error, got: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestJSONPathSet_ArrayIndexPath(t *testing.T) {
	jp, err := ParseJSONPath("$[0].name")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = []interface{}{
		map[string]interface{}{"name": "alice"},
	}
	err = jp.Set(&data, "bob")
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	arr := data.([]interface{})
	obj := arr[0].(map[string]interface{})
	if obj["name"] != "bob" {
		t.Errorf("expected bob, got %v", obj["name"])
	}
}

func TestJSONPathSet_ArrayAtFinalPosition(t *testing.T) {
	jp, err := ParseJSONPath("$.arr[1]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = map[string]interface{}{
		"arr": []interface{}{10, 20, 30},
	}
	err = jp.Set(&data, 99)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	obj := data.(map[string]interface{})
	arr := obj["arr"].([]interface{})
	if arr[1] != 99 {
		t.Errorf("expected 99, got %v", arr[1])
	}
}

func TestJSONPathSet_OutOfBoundsNavigation(t *testing.T) {
	jp, err := ParseJSONPath("$[99]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = []interface{}{1, 2}
	err = jp.Set(&data, 42)
	if err == nil {
		t.Fatal("expected error for out-of-bounds set")
	}
}

func TestJSONPathSet_NonArrayForArrayIndex(t *testing.T) {
	jp, err := ParseJSONPath("$[0].name")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = "not an array"
	err = jp.Set(&data, "value")
	if err == nil {
		t.Fatal("expected error for non-array in navigation")
	}
}

func TestJSONPathSet_NonObjectForKey(t *testing.T) {
	jp, err := ParseJSONPath("$.a.b")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = map[string]interface{}{
		"a": "not an object",
	}
	err = jp.Set(&data, "value")
	if err == nil {
		t.Fatal("expected error for non-object in navigation")
	}
}

func TestJSONPathSet_OutOfBoundsAtFinal(t *testing.T) {
	jp, err := ParseJSONPath("$.arr[99]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = map[string]interface{}{
		"arr": []interface{}{1, 2},
	}
	err = jp.Set(&data, 42)
	if err == nil {
		t.Fatal("expected error for out-of-bounds at final segment")
	}
}

func TestJSONPathSet_NonArrayAtFinalSegment(t *testing.T) {
	jp, err := ParseJSONPath("$.val[0]")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = map[string]interface{}{
		"val": "not an array",
	}
	err = jp.Set(&data, 42)
	if err == nil {
		t.Fatal("expected error for non-array at final segment")
	}
}

func TestJSONPathSet_NilPathNavigation(t *testing.T) {
	jp, err := ParseJSONPath("$.a.b")
	if err != nil {
		t.Fatalf("ParseJSONPath failed: %v", err)
	}
	var data interface{} = map[string]interface{}{
		"a": nil,
	}
	err = jp.Set(&data, "value")
	if err == nil {
		t.Fatal("expected error for nil in path navigation")
	}
}

func TestJSONPretty_EmptyString(t *testing.T) {
	result, err := JSONPretty("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestJSONPretty_InvalidJSON(t *testing.T) {
	_, err := JSONPretty("{invalid json")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestJSONMinify_EmptyString(t *testing.T) {
	result, err := JSONMinify("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != "" {
		t.Errorf("expected empty string, got %q", result)
	}
}

func TestJSONMinify_InvalidJSON(t *testing.T) {
	_, err := JSONMinify("{invalid json")
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestJSONQuote_SimpleString(t *testing.T) {
	result := JSONQuote("hello")
	expected := `"hello"`
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestJSONQuote_EmptyString(t *testing.T) {
	result := JSONQuote("")
	expected := `""`
	if result != expected {
		t.Errorf("expected %s, got %s", expected, result)
	}
}

func TestStatsCountRows_EmptyResult(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	sc := NewStatsCollector(cat)
	count, err := sc.countRows("nonexistent")
	if err != nil {
		t.Fatalf("countRows should not error: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestStatsCollectColumnStats_EmptyResult(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	sc := NewStatsCollector(cat)
	stats, err := sc.collectColumnStats("anytable", "anycolumn")
	if err != nil {
		t.Fatalf("collectColumnStats should not error: %v", err)
	}
	if stats.ColumnName != "anycolumn" {
		t.Errorf("expected column name anycolumn, got %q", stats.ColumnName)
	}
	if stats.DistinctCount != 0 {
		t.Errorf("expected DistinctCount 0, got %d", stats.DistinctCount)
	}
	if stats.NullCount != 0 {
		t.Errorf("expected NullCount 0, got %d", stats.NullCount)
	}
}

func TestSaveLoad_WithRealTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	cat := New(tree, pool, nil)
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	err = cat.Save()
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	cat2 := New(tree, pool, nil)
	err = cat2.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	tbl, err := cat2.GetTable("users")
	if err != nil {
		t.Fatalf("GetTable after Load failed: %v", err)
	}
	if tbl.Name != "users" {
		t.Errorf("expected table name users, got %q", tbl.Name)
	}
	if len(tbl.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(tbl.Columns))
	}
}

func TestSaveLoad_WithDefaultAndCheck(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	cat := New(tree, pool, nil)
	defaultExpr, _ := query.ParseExpression("0")
	checkExpr, _ := query.ParseExpression("age > 0")
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "people",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "age", Type: query.TokenInteger, Default: defaultExpr, Check: checkExpr},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	err = cat.Save()
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	cat2 := New(tree, pool, nil)
	err = cat2.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	tbl, err := cat2.GetTable("people")
	if err != nil {
		t.Fatalf("GetTable after Load failed: %v", err)
	}
	if len(tbl.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(tbl.Columns))
	}
}

func TestSaveLoad_NilTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	err := cat.Load()
	if err != nil {
		t.Fatalf("Load with nil tree should not error: %v", err)
	}
}

func TestSaveLoad_EmptyCatalog(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	cat := New(tree, pool, nil)
	err = cat.Save()
	if err != nil {
		t.Fatalf("Save empty catalog failed: %v", err)
	}
	cat2 := New(tree, pool, nil)
	err = cat2.Load()
	if err != nil {
		t.Fatalf("Load empty catalog failed: %v", err)
	}
}
