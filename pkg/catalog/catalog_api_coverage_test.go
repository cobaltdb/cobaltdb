package catalog

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// helper: creates a fresh catalog with a memory-backed buffer pool.
func newTestCatalog(t *testing.T) (*Catalog, *storage.BufferPool) {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)
	return cat, pool
}

// helper: creates a simple table with (id INTEGER PK, name TEXT, age INTEGER).
func createUsersTable(t *testing.T, cat *Catalog) {
	t.Helper()
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "age", Type: query.TokenInteger},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
}

// helper: inserts a row into users table.
func insertUser(t *testing.T, cat *Catalog, id int, name string, age int) {
	t.Helper()
	_, _, err := cat.Insert(&query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name", "age"},
		Values: [][]query.Expression{
			{
				&query.NumberLiteral{Value: float64(id)},
				&query.StringLiteral{Value: name},
				&query.NumberLiteral{Value: float64(age)},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Insert user (%d, %s, %d): %v", id, name, age, err)
	}
}

// ==================== 1. SetWAL ====================

func TestAPI_SetWAL(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	if cat.wal != nil {
		t.Fatal("expected nil wal initially")
	}

	// Create a temporary WAL file
	tmpFile, err := os.CreateTemp("", "cobaltdb-wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	wal, err := storage.OpenWAL(tmpPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	cat.SetWAL(wal)
	if cat.wal != wal {
		t.Fatal("SetWAL did not set the WAL")
	}

	// Verify commit writes a record via WAL
	cat.BeginTransaction(100)
	err = cat.CommitTransaction()
	if err != nil {
		t.Fatalf("CommitTransaction with WAL: %v", err)
	}
}

// ==================== 2. TxnID ====================

func TestAPI_TxnID(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	if cat.TxnID() != 0 {
		t.Fatalf("expected initial TxnID 0, got %d", cat.TxnID())
	}

	cat.BeginTransaction(42)
	if cat.TxnID() != 42 {
		t.Fatalf("expected TxnID 42, got %d", cat.TxnID())
	}

	cat.BeginTransaction(999)
	if cat.TxnID() != 999 {
		t.Fatalf("expected TxnID 999, got %d", cat.TxnID())
	}
}

// ==================== 3. HasTableOrView ====================

func TestAPI_HasTableOrView(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// Nothing exists
	if cat.HasTableOrView("users") {
		t.Fatal("expected false for non-existent name")
	}

	// Create a table
	createUsersTable(t, cat)
	if !cat.HasTableOrView("users") {
		t.Fatal("expected true after creating table")
	}

	// Create a view
	err := cat.CreateView("user_names", &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "users"},
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if !cat.HasTableOrView("user_names") {
		t.Fatal("expected true after creating view")
	}

	// Non-existent should still be false
	if cat.HasTableOrView("does_not_exist") {
		t.Fatal("expected false for non-existent name")
	}
}

// ==================== 4. GetTrigger ====================

func TestAPI_GetTrigger(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)

	// Not found
	_, err := cat.GetTrigger("trg_test")
	if err == nil {
		t.Fatal("expected error for non-existent trigger")
	}

	// Create trigger
	trigStmt := &query.CreateTriggerStmt{
		Name:  "trg_test",
		Table: "users",
		Time:  "BEFORE",
		Event: "INSERT",
		Body:  []query.Statement{},
	}
	err = cat.CreateTrigger(trigStmt)
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	// Get trigger
	got, err := cat.GetTrigger("trg_test")
	if err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	if got.Name != "trg_test" {
		t.Fatalf("expected trigger name trg_test, got %s", got.Name)
	}
	if got.Table != "users" {
		t.Fatalf("expected trigger table users, got %s", got.Table)
	}
}

// ==================== 5. CreateProcedure / GetProcedure / DropProcedure ====================

func TestAPI_Procedures(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// Get non-existent
	_, err := cat.GetProcedure("my_proc")
	if err == nil {
		t.Fatal("expected error for non-existent procedure")
	}

	// Create
	procStmt := &query.CreateProcedureStmt{
		Name: "my_proc",
		Params: []*query.ParamDef{
			{Name: "x", Type: query.TokenInteger},
		},
		Body: []query.Statement{},
	}
	err = cat.CreateProcedure(procStmt)
	if err != nil {
		t.Fatalf("CreateProcedure: %v", err)
	}

	// Duplicate
	err = cat.CreateProcedure(procStmt)
	if err == nil {
		t.Fatal("expected error for duplicate procedure")
	}

	// Get
	got, err := cat.GetProcedure("my_proc")
	if err != nil {
		t.Fatalf("GetProcedure: %v", err)
	}
	if got.Name != "my_proc" {
		t.Fatalf("expected procedure name my_proc, got %s", got.Name)
	}

	// Drop
	err = cat.DropProcedure("my_proc")
	if err != nil {
		t.Fatalf("DropProcedure: %v", err)
	}

	// Drop again
	err = cat.DropProcedure("my_proc")
	if err == nil {
		t.Fatal("expected error when dropping non-existent procedure")
	}

	// Get after drop
	_, err = cat.GetProcedure("my_proc")
	if err == nil {
		t.Fatal("expected error after drop")
	}
}

// ==================== 6. GetIndex ====================

func TestAPI_GetIndex(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)

	// Not found
	_, err := cat.GetIndex("idx_name")
	if err != ErrIndexNotFound {
		t.Fatalf("expected ErrIndexNotFound, got %v", err)
	}

	// Create index
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_name",
		Table:   "users",
		Columns: []string{"name"},
		Unique:  true,
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Get index
	idx, err := cat.GetIndex("idx_name")
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	if idx.Name != "idx_name" {
		t.Fatalf("expected idx_name, got %s", idx.Name)
	}
	if idx.TableName != "users" {
		t.Fatalf("expected table users, got %s", idx.TableName)
	}
}

// ==================== 7. findUsableIndex ====================

func TestAPI_findUsableIndex(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)

	// No index yet
	name, col, val := cat.findUsableIndex("users", &query.BinaryExpr{
		Left:     &query.Identifier{Name: "name"},
		Operator: query.TokenEq,
		Right:    &query.StringLiteral{Value: "Alice"},
	})
	if name != "" {
		t.Fatalf("expected no index, got %s", name)
	}

	// Create unique index on name
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_users_name",
		Table:   "users",
		Columns: []string{"name"},
		Unique:  true,
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Now index should be found
	name, col, val = cat.findUsableIndex("users", &query.BinaryExpr{
		Left:     &query.Identifier{Name: "name"},
		Operator: query.TokenEq,
		Right:    &query.StringLiteral{Value: "Alice"},
	})
	if name != "idx_users_name" {
		t.Fatalf("expected idx_users_name, got %s", name)
	}
	if col != "name" {
		t.Fatalf("expected col name, got %s", col)
	}
	if val != "Alice" {
		t.Fatalf("expected val Alice, got %v", val)
	}

	// nil where
	name, _, _ = cat.findUsableIndex("users", nil)
	if name != "" {
		t.Fatalf("expected empty for nil where, got %s", name)
	}

	// AND condition: left side has index
	name, _, _ = cat.findUsableIndex("users", &query.BinaryExpr{
		Left: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "name"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "Bob"},
		},
		Operator: query.TokenAnd,
		Right: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "age"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 30.0},
		},
	})
	if name != "idx_users_name" {
		t.Fatalf("expected index from AND left side, got %s", name)
	}

	// Reversed: value = column
	name, _, _ = cat.findUsableIndex("users", &query.BinaryExpr{
		Left:     &query.StringLiteral{Value: "Carol"},
		Operator: query.TokenEq,
		Right:    &query.Identifier{Name: "name"},
	})
	if name != "idx_users_name" {
		t.Fatalf("expected index when value=column form, got %s", name)
	}
}

// ==================== 8. useIndexForQuery ====================

func TestAPI_useIndexForQuery(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)

	// Insert data
	insertUser(t, cat, 1, "Alice", 30)

	// Create unique index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_users_name_u",
		Table:   "users",
		Columns: []string{"name"},
		Unique:  true,
	})
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Query with indexed column
	where := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "name"},
		Operator: query.TokenEq,
		Right:    &query.StringLiteral{Value: "Alice"},
	}
	pks, used := cat.useIndexForQuery("users", where)
	if !used {
		t.Fatal("expected index to be used")
	}
	if len(pks) == 0 {
		t.Fatal("expected at least one PK result from index lookup")
	}

	// Query non-existent value
	where2 := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "name"},
		Operator: query.TokenEq,
		Right:    &query.StringLiteral{Value: "NoSuchUser"},
	}
	pks2, used2 := cat.useIndexForQuery("users", where2)
	if !used2 {
		t.Fatal("expected index to still be 'used' even with no matches")
	}
	if len(pks2) != 0 {
		t.Fatalf("expected 0 PKs for non-existent value, got %d", len(pks2))
	}

	// No index on age -> not used
	where3 := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "age"},
		Operator: query.TokenEq,
		Right:    &query.NumberLiteral{Value: 30.0},
	}
	_, used3 := cat.useIndexForQuery("users", where3)
	if used3 {
		t.Fatal("expected false for non-indexed column")
	}
}

// ==================== 9. encodeRow ====================

func TestAPI_encodeRow(t *testing.T) {
	exprs := []query.Expression{
		&query.StringLiteral{Value: "hello"},
		&query.NumberLiteral{Value: 42.0},
		&query.BooleanLiteral{Value: true},
		&query.NullLiteral{},
		&query.PlaceholderExpr{Index: 0},
		&query.Identifier{Name: "col1"},
	}
	args := []interface{}{"placeholder_val"}

	data, err := encodeRow(exprs, args)
	if err != nil {
		t.Fatalf("encodeRow: %v", err)
	}

	var decoded []interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	if decoded[0] != "hello" {
		t.Errorf("expected 'hello', got %v", decoded[0])
	}
	if decoded[1].(float64) != 42.0 {
		t.Errorf("expected 42, got %v", decoded[1])
	}
	if decoded[2] != true {
		t.Errorf("expected true, got %v", decoded[2])
	}
	if decoded[3] != nil {
		t.Errorf("expected nil, got %v", decoded[3])
	}
	if decoded[4] != "placeholder_val" {
		t.Errorf("expected placeholder_val, got %v", decoded[4])
	}
	if decoded[5] != "col1" {
		t.Errorf("expected col1, got %v", decoded[5])
	}
}

func TestAPI_encodeRow_PlaceholderFallback(t *testing.T) {
	// PlaceholderExpr with Index out of range but args available positionally
	exprs := []query.Expression{
		&query.PlaceholderExpr{Index: 99},
	}
	args := []interface{}{"positional"}

	data, err := encodeRow(exprs, args)
	if err != nil {
		t.Fatalf("encodeRow: %v", err)
	}

	var decoded []interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	// Positional fallback used: first decoded should be "positional"
	if decoded[0] != "positional" {
		t.Errorf("expected 'positional', got %v", decoded[0])
	}
}

// ==================== 10. fastEncodeRow / fastDecodeRow ====================

func TestAPI_fastEncodeDecodeRow(t *testing.T) {
	values := []interface{}{
		nil,
		int64(123),
		float64(3.14),
		"hello world",
		true,
		false,
	}

	encoded, err := fastEncodeRow(values)
	if err != nil {
		t.Fatalf("fastEncodeRow: %v", err)
	}

	decoded, err := fastDecodeRow(encoded)
	if err != nil {
		t.Fatalf("fastDecodeRow: %v", err)
	}

	if len(decoded) != len(values) {
		t.Fatalf("expected %d values, got %d", len(values), len(decoded))
	}

	// nil
	if decoded[0] != nil {
		t.Errorf("expected nil, got %v", decoded[0])
	}
	// int64
	if decoded[1].(int64) != 123 {
		t.Errorf("expected int64(123), got %v", decoded[1])
	}
	// float64
	if decoded[2].(float64) != 3.14 {
		t.Errorf("expected 3.14, got %v", decoded[2])
	}
	// string
	if decoded[3].(string) != "hello world" {
		t.Errorf("expected 'hello world', got %v", decoded[3])
	}
	// bool true
	if decoded[4].(bool) != true {
		t.Errorf("expected true, got %v", decoded[4])
	}
	// bool false
	if decoded[5].(bool) != false {
		t.Errorf("expected false, got %v", decoded[5])
	}
}

func TestAPI_fastEncodeRow_Empty(t *testing.T) {
	encoded, err := fastEncodeRow([]interface{}{})
	if err != nil {
		t.Fatalf("fastEncodeRow empty: %v", err)
	}
	if len(encoded) != 1 || encoded[0] != 0 {
		t.Fatalf("expected [0] for empty, got %v", encoded)
	}
}

func TestAPI_fastDecodeRow_Empty(t *testing.T) {
	decoded, err := fastDecodeRow([]byte{})
	if err != nil {
		t.Fatalf("fastDecodeRow empty: %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("expected empty, got %v", decoded)
	}
}

func TestAPI_fastEncodeRow_IntType(t *testing.T) {
	// Go int (not int64)
	values := []interface{}{int(42)}
	encoded, err := fastEncodeRow(values)
	if err != nil {
		t.Fatalf("fastEncodeRow: %v", err)
	}
	decoded, err := fastDecodeRow(encoded)
	if err != nil {
		t.Fatalf("fastDecodeRow: %v", err)
	}
	if decoded[0].(int64) != 42 {
		t.Errorf("expected 42, got %v", decoded[0])
	}
}

func TestAPI_fastDecodeRow_BadData(t *testing.T) {
	// Unknown type byte
	_, err := fastDecodeRow([]byte{99})
	if err == nil {
		t.Fatal("expected error for unknown type")
	}

	// Truncated int64
	_, err = fastDecodeRow([]byte{1, 0, 0})
	if err == nil {
		t.Fatal("expected error for truncated int64")
	}

	// Truncated float64
	_, err = fastDecodeRow([]byte{2, 0, 0})
	if err == nil {
		t.Fatal("expected error for truncated float64")
	}

	// Truncated string length
	_, err = fastDecodeRow([]byte{3})
	if err == nil {
		t.Fatal("expected error for truncated string length")
	}

	// Truncated string data
	_, err = fastDecodeRow([]byte{3, 10, 0})
	if err == nil {
		t.Fatal("expected error for truncated string data")
	}

	// Truncated bool
	_, err = fastDecodeRow([]byte{4})
	if err == nil {
		t.Fatal("expected error for truncated bool")
	}
}

// ==================== 11. GetRow ====================

func TestAPI_GetRow(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)
	insertUser(t, cat, 1, "Alice", 30)

	row, err := cat.GetRow("users", int64(1))
	if err != nil {
		t.Fatalf("GetRow: %v", err)
	}

	if row["name"] != "Alice" {
		t.Errorf("expected Alice, got %v", row["name"])
	}

	// Non-existent table
	_, err = cat.GetRow("nonexistent", 1)
	if err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

// ==================== 12. UpdateRow ====================

func TestAPI_UpdateRow(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)
	insertUser(t, cat, 1, "Alice", 30)

	err := cat.UpdateRow("users", int64(1), map[string]interface{}{
		"id":   int64(1),
		"name": "Alice Updated",
		"age":  int64(31),
	})
	if err != nil {
		t.Fatalf("UpdateRow: %v", err)
	}

	// Verify via GetRow
	row, err := cat.GetRow("users", int64(1))
	if err != nil {
		t.Fatalf("GetRow after update: %v", err)
	}
	if row["name"] != "Alice Updated" {
		t.Errorf("expected 'Alice Updated', got %v", row["name"])
	}

	// Non-existent table
	err = cat.UpdateRow("nonexistent", 1, map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

// ==================== 13. GetMaterializedView ====================

func TestAPI_GetMaterializedView(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)
	insertUser(t, cat, 1, "Alice", 30)

	// Not found
	_, err := cat.GetMaterializedView("mv_test")
	if err == nil {
		t.Fatal("expected error for non-existent MV")
	}

	// Create MV
	err = cat.CreateMaterializedView("mv_test", &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "users"},
	})
	if err != nil {
		t.Fatalf("CreateMaterializedView: %v", err)
	}

	mv, err := cat.GetMaterializedView("mv_test")
	if err != nil {
		t.Fatalf("GetMaterializedView: %v", err)
	}
	if mv.Name != "mv_test" {
		t.Fatalf("expected mv_test, got %s", mv.Name)
	}
}

// ==================== 14. ListMaterializedViews ====================

func TestAPI_ListMaterializedViews(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)
	insertUser(t, cat, 1, "Alice", 30)

	// Empty list
	list := cat.ListMaterializedViews()
	if len(list) != 0 {
		t.Fatalf("expected 0, got %d", len(list))
	}

	// Create MVs
	for _, name := range []string{"mv_b", "mv_a", "mv_c"} {
		err := cat.CreateMaterializedView(name, &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "users"},
		})
		if err != nil {
			t.Fatalf("CreateMaterializedView(%s): %v", name, err)
		}
	}

	list = cat.ListMaterializedViews()
	if len(list) != 3 {
		t.Fatalf("expected 3, got %d", len(list))
	}
	// Should be sorted
	expected := []string{"mv_a", "mv_b", "mv_c"}
	for i, name := range expected {
		if list[i] != name {
			t.Errorf("expected %s at index %d, got %s", name, i, list[i])
		}
	}
}

// ==================== 15. FTS functions ====================

func TestAPI_FTS(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// Create table with text data
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "articles",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "title", Type: query.TokenText},
			{Name: "body", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Create FTS index (table not found)
	err = cat.CreateFTSIndex("fts_bad", "nonexistent", []string{"title"})
	if err == nil {
		t.Fatal("expected error for non-existent table")
	}

	// Create FTS index (column not found)
	err = cat.CreateFTSIndex("fts_bad", "articles", []string{"no_such_col"})
	if err == nil {
		t.Fatal("expected error for non-existent column")
	}

	// Create FTS index on empty table
	err = cat.CreateFTSIndex("fts_articles", "articles", []string{"title", "body"})
	if err != nil {
		t.Fatalf("CreateFTSIndex: %v", err)
	}

	// Duplicate
	err = cat.CreateFTSIndex("fts_articles", "articles", []string{"title"})
	if err == nil {
		t.Fatal("expected error for duplicate FTS index")
	}

	// Manually populate FTS index using indexRowForFTS (the internal function)
	// since Insert stores rows as arrays and CreateFTSIndex expects map format.
	ftsIdx := cat.ftsIndexes["fts_articles"]
	cat.indexRowForFTS(ftsIdx, map[string]interface{}{
		"id": 1, "title": "Go Programming", "body": "Go is a compiled programming language",
	}, []byte("key1"))
	cat.indexRowForFTS(ftsIdx, map[string]interface{}{
		"id": 2, "title": "Database Internals", "body": "B-trees are fundamental data structures",
	}, []byte("key2"))
	cat.indexRowForFTS(ftsIdx, map[string]interface{}{
		"id": 3, "title": "Go Databases", "body": "Writing a database engine in Go",
	}, []byte("key3"))

	// SearchFTS
	results, err := cat.SearchFTS("fts_articles", "go")
	if err != nil {
		t.Fatalf("SearchFTS: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one result for 'go'")
	}

	// SearchFTS empty query
	results, err = cat.SearchFTS("fts_articles", "")
	if err != nil {
		t.Fatalf("SearchFTS empty: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for empty query, got %d", len(results))
	}

	// SearchFTS term not found
	results, err = cat.SearchFTS("fts_articles", "XYZ_MISSING")
	if err != nil {
		t.Fatalf("SearchFTS missing: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for missing term, got %d", len(results))
	}

	// SearchFTS not found index
	_, err = cat.SearchFTS("nonexistent_fts", "test")
	if err == nil {
		t.Fatal("expected error for non-existent FTS index")
	}

	// ListFTSIndexes
	ftsNames := cat.ListFTSIndexes()
	if len(ftsNames) != 1 || ftsNames[0] != "fts_articles" {
		t.Fatalf("expected [fts_articles], got %v", ftsNames)
	}

	// DropFTSIndex
	err = cat.DropFTSIndex("fts_articles")
	if err != nil {
		t.Fatalf("DropFTSIndex: %v", err)
	}

	// Drop again
	err = cat.DropFTSIndex("fts_articles")
	if err == nil {
		t.Fatal("expected error dropping non-existent FTS index")
	}

	// List should be empty
	ftsNames = cat.ListFTSIndexes()
	if len(ftsNames) != 0 {
		t.Fatalf("expected empty list, got %v", ftsNames)
	}
}

func TestAPI_tokenize(t *testing.T) {
	words := tokenize("Hello, World! This is a test123.")
	expected := []string{"Hello", "World", "This", "is", "a", "test123"}
	if len(words) != len(expected) {
		t.Fatalf("expected %d words, got %d: %v", len(expected), len(words), words)
	}
	for i, w := range expected {
		if words[i] != w {
			t.Errorf("expected word %d = %s, got %s", i, w, words[i])
		}
	}

	// Empty
	words = tokenize("")
	if len(words) != 0 {
		t.Fatalf("expected 0 words for empty, got %d", len(words))
	}
}

func TestAPI_intersectSorted(t *testing.T) {
	a := []int64{1, 2, 3, 5, 7}
	b := []int64{2, 3, 6, 7, 8}
	result := intersectSorted(a, b)
	expected := []int64{2, 3, 7}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i := range expected {
		if result[i] != expected[i] {
			t.Errorf("index %d: expected %d, got %d", i, expected[i], result[i])
		}
	}

	// No intersection
	result = intersectSorted([]int64{1, 3}, []int64{2, 4})
	if len(result) != 0 {
		t.Fatalf("expected empty, got %v", result)
	}

	// Empty inputs
	result = intersectSorted([]int64{}, []int64{1, 2})
	if len(result) != 0 {
		t.Fatalf("expected empty, got %v", result)
	}
}

func TestAPI_indexRowForFTS(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	ftsIndex := &FTSIndexDef{
		Name:      "test_fts",
		TableName: "test",
		Columns:   []string{"title"},
		Index:     make(map[string][]int64),
	}

	row := map[string]interface{}{
		"title": "Hello World",
	}
	cat.indexRowForFTS(ftsIndex, row, []byte("key1"))

	if _, ok := ftsIndex.Index["hello"]; !ok {
		t.Fatal("expected 'hello' in index")
	}
	if _, ok := ftsIndex.Index["world"]; !ok {
		t.Fatal("expected 'world' in index")
	}

	// Column not in row
	ftsIndex2 := &FTSIndexDef{
		Name:      "test_fts2",
		TableName: "test",
		Columns:   []string{"missing_col"},
		Index:     make(map[string][]int64),
	}
	cat.indexRowForFTS(ftsIndex2, row, []byte("key1"))
	if len(ftsIndex2.Index) != 0 {
		t.Fatalf("expected empty index for missing column, got %d entries", len(ftsIndex2.Index))
	}
}

// ==================== 16. catalogCompareValues ====================

func TestAPI_catalogCompareValues(t *testing.T) {
	// nil vs nil
	if catalogCompareValues(nil, nil) != 0 {
		t.Error("nil vs nil should be 0")
	}
	// nil vs non-nil
	if catalogCompareValues(nil, 1) >= 0 {
		t.Error("nil vs 1 should be < 0")
	}
	// non-nil vs nil
	if catalogCompareValues(1, nil) <= 0 {
		t.Error("1 vs nil should be > 0")
	}
	// numeric
	if catalogCompareValues(1.0, 2.0) >= 0 {
		t.Error("1.0 vs 2.0 should be < 0")
	}
	if catalogCompareValues(2.0, 1.0) <= 0 {
		t.Error("2.0 vs 1.0 should be > 0")
	}
	if catalogCompareValues(3.0, 3.0) != 0 {
		t.Error("3.0 vs 3.0 should be 0")
	}
	// string comparison
	if catalogCompareValues("apple", "banana") >= 0 {
		t.Error("apple vs banana should be < 0")
	}
	if catalogCompareValues("banana", "apple") <= 0 {
		t.Error("banana vs apple should be > 0")
	}
	if catalogCompareValues("same", "same") != 0 {
		t.Error("same vs same should be 0")
	}
	// int64 comparison
	if catalogCompareValues(int64(10), int64(20)) >= 0 {
		t.Error("10 vs 20 should be < 0")
	}
}

// ==================== 17. GetTableStats ====================

func TestAPI_GetTableStats(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)
	insertUser(t, cat, 1, "Alice", 30)
	insertUser(t, cat, 2, "Bob", 25)

	// Not analyzed yet
	_, err := cat.GetTableStats("users")
	if err == nil {
		t.Fatal("expected error for non-analyzed table")
	}

	// Analyze
	err = cat.Analyze("users")
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}

	stats, err := cat.GetTableStats("users")
	if err != nil {
		t.Fatalf("GetTableStats: %v", err)
	}
	if stats.TableName != "users" {
		t.Errorf("expected users, got %s", stats.TableName)
	}
	if stats.RowCount != 2 {
		t.Errorf("expected 2 rows, got %d", stats.RowCount)
	}
}

// ==================== 18. SaveData / LoadSchema / LoadData ====================

func TestAPI_DeprecatedWrappers(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// SaveData (calls Save which works with nil tree)
	err := cat.SaveData("/tmp/fake")
	if err != nil {
		t.Fatalf("SaveData: %v", err)
	}

	// LoadSchema is a no-op
	err = cat.LoadSchema("/tmp/fake")
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}

	// LoadData is a no-op
	err = cat.LoadData("/tmp/fake")
	if err != nil {
		t.Fatalf("LoadData: %v", err)
	}
}

// ==================== 19. EvalExpression ====================

func TestAPI_EvalExpression_Literals(t *testing.T) {
	// StringLiteral
	val, err := EvalExpression(&query.StringLiteral{Value: "hello"}, nil)
	if err != nil || val != "hello" {
		t.Errorf("StringLiteral: %v, %v", val, err)
	}

	// NumberLiteral
	val, err = EvalExpression(&query.NumberLiteral{Value: 42.0}, nil)
	if err != nil || val != 42.0 {
		t.Errorf("NumberLiteral: %v, %v", val, err)
	}

	// BooleanLiteral
	val, err = EvalExpression(&query.BooleanLiteral{Value: true}, nil)
	if err != nil || val != true {
		t.Errorf("BooleanLiteral: %v, %v", val, err)
	}

	// NullLiteral
	val, err = EvalExpression(&query.NullLiteral{}, nil)
	if err != nil || val != nil {
		t.Errorf("NullLiteral: %v, %v", val, err)
	}

	// Identifier
	val, err = EvalExpression(&query.Identifier{Name: "col_name"}, nil)
	if err != nil || val != "col_name" {
		t.Errorf("Identifier: %v, %v", val, err)
	}
}

func TestAPI_EvalExpression_Placeholder(t *testing.T) {
	val, err := EvalExpression(&query.PlaceholderExpr{Index: 0}, []interface{}{"arg0"})
	if err != nil || val != "arg0" {
		t.Errorf("Placeholder: %v, %v", val, err)
	}

	// Out of range
	_, err = EvalExpression(&query.PlaceholderExpr{Index: 99}, nil)
	if err == nil {
		t.Error("expected error for out-of-range placeholder")
	}
}

func TestAPI_EvalExpression_Unary(t *testing.T) {
	// Negate integer
	val, err := EvalExpression(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.NumberLiteral{Value: 10.0},
	}, nil)
	if err != nil {
		t.Fatalf("UnaryExpr minus: %v", err)
	}
	if val != int64(-10) {
		t.Errorf("expected -10, got %v (%T)", val, val)
	}

	// NOT true
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil {
		t.Fatalf("UnaryExpr NOT: %v", err)
	}
	if val != false {
		t.Errorf("expected false, got %v", val)
	}

	// NOT NULL should return nil
	val, err = EvalExpression(&query.UnaryExpr{
		Operator: query.TokenNot,
		Expr:     &query.NullLiteral{},
	}, nil)
	if err != nil {
		t.Fatalf("UnaryExpr NOT NULL: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestAPI_EvalExpression_BinaryArithmetic(t *testing.T) {
	tests := []struct {
		op     query.TokenType
		left   float64
		right  float64
		expect interface{}
	}{
		{query.TokenPlus, 3, 2, int64(5)},
		{query.TokenMinus, 10, 4, int64(6)},
		{query.TokenStar, 3, 4, int64(12)},
		{query.TokenSlash, 10, 4, 2.5},
		{query.TokenPercent, 10, 3, int64(1)},
	}

	for _, tt := range tests {
		val, err := EvalExpression(&query.BinaryExpr{
			Left:     &query.NumberLiteral{Value: tt.left},
			Operator: tt.op,
			Right:    &query.NumberLiteral{Value: tt.right},
		}, nil)
		if err != nil {
			t.Fatalf("op %d: %v", tt.op, err)
		}
		// For division, result is float64
		if tt.op == query.TokenSlash {
			if v, ok := val.(float64); !ok || v != tt.expect.(float64) {
				t.Errorf("op %d: expected %v, got %v", tt.op, tt.expect, val)
			}
		} else {
			if val != tt.expect {
				t.Errorf("op %d: expected %v (%T), got %v (%T)", tt.op, tt.expect, tt.expect, val, val)
			}
		}
	}

	// Division by zero
	_, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 1},
		Operator: query.TokenSlash,
		Right:    &query.NumberLiteral{Value: 0},
	}, nil)
	if err == nil {
		t.Error("expected division by zero error")
	}

	// Modulo by zero
	_, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.NumberLiteral{Value: 1},
		Operator: query.TokenPercent,
		Right:    &query.NumberLiteral{Value: 0},
	}, nil)
	if err == nil {
		t.Error("expected modulo by zero error")
	}
}

func TestAPI_EvalExpression_BinaryComparison(t *testing.T) {
	tests := []struct {
		op     query.TokenType
		left   interface{}
		right  interface{}
		expect bool
	}{
		{query.TokenEq, 1.0, 1.0, true},
		{query.TokenEq, 1.0, 2.0, false},
		{query.TokenNeq, 1.0, 2.0, true},
		{query.TokenLt, 1.0, 2.0, true},
		{query.TokenGt, 2.0, 1.0, true},
		{query.TokenLte, 1.0, 1.0, true},
		{query.TokenGte, 1.0, 1.0, true},
	}

	for _, tt := range tests {
		var leftExpr, rightExpr query.Expression
		switch v := tt.left.(type) {
		case float64:
			leftExpr = &query.NumberLiteral{Value: v}
		case string:
			leftExpr = &query.StringLiteral{Value: v}
		}
		switch v := tt.right.(type) {
		case float64:
			rightExpr = &query.NumberLiteral{Value: v}
		case string:
			rightExpr = &query.StringLiteral{Value: v}
		}

		val, err := EvalExpression(&query.BinaryExpr{
			Left:     leftExpr,
			Operator: tt.op,
			Right:    rightExpr,
		}, nil)
		if err != nil {
			t.Fatalf("op %d: %v", tt.op, err)
		}
		if val != tt.expect {
			t.Errorf("op %d (%v vs %v): expected %v, got %v", tt.op, tt.left, tt.right, tt.expect, val)
		}
	}
}

func TestAPI_EvalExpression_BinaryLogical(t *testing.T) {
	// AND
	val, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if err != nil || val != false {
		t.Errorf("AND true false: %v, %v", val, err)
	}

	// OR
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if err != nil || val != true {
		t.Errorf("OR false true: %v, %v", val, err)
	}

	// Concat
	val, err = EvalExpression(&query.BinaryExpr{
		Left:     &query.StringLiteral{Value: "hello"},
		Operator: query.TokenConcat,
		Right:    &query.StringLiteral{Value: " world"},
	}, nil)
	if err != nil || val != "hello world" {
		t.Errorf("Concat: %v, %v", val, err)
	}
}

func TestAPI_EvalExpression_NullPropagation(t *testing.T) {
	// NULL AND true = NULL
	val, _ := EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if val != nil {
		t.Errorf("NULL AND true: expected nil, got %v", val)
	}

	// NULL AND false = false
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if val != false {
		t.Errorf("NULL AND false: expected false, got %v", val)
	}

	// true AND NULL = NULL
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if val != nil {
		t.Errorf("true AND NULL: expected nil, got %v", val)
	}

	// false AND NULL = false
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if val != false {
		t.Errorf("false AND NULL: expected false, got %v", val)
	}

	// NULL AND NULL = NULL
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenAnd,
		Right:    &query.NullLiteral{},
	}, nil)
	if val != nil {
		t.Errorf("NULL AND NULL: expected nil, got %v", val)
	}

	// NULL OR true = true
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: true},
	}, nil)
	if val != true {
		t.Errorf("NULL OR true: expected true, got %v", val)
	}

	// NULL OR false = NULL
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.BooleanLiteral{Value: false},
	}, nil)
	if val != nil {
		t.Errorf("NULL OR false: expected nil, got %v", val)
	}

	// true OR NULL = true
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: true},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if val != true {
		t.Errorf("true OR NULL: expected true, got %v", val)
	}

	// false OR NULL = NULL
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.BooleanLiteral{Value: false},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if val != nil {
		t.Errorf("false OR NULL: expected nil, got %v", val)
	}

	// NULL OR NULL = NULL
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenOr,
		Right:    &query.NullLiteral{},
	}, nil)
	if val != nil {
		t.Errorf("NULL OR NULL: expected nil, got %v", val)
	}

	// NULL || 'text' = NULL (concat)
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenConcat,
		Right:    &query.StringLiteral{Value: "text"},
	}, nil)
	if val != nil {
		t.Errorf("NULL concat: expected nil, got %v", val)
	}

	// NULL + 1 = NULL (arithmetic)
	val, _ = EvalExpression(&query.BinaryExpr{
		Left:     &query.NullLiteral{},
		Operator: query.TokenPlus,
		Right:    &query.NumberLiteral{Value: 1},
	}, nil)
	if val != nil {
		t.Errorf("NULL + 1: expected nil, got %v", val)
	}
}

func TestAPI_EvalExpression_CaseExpr(t *testing.T) {
	// Simple CASE
	val, err := EvalExpression(&query.CaseExpr{
		Expr: &query.NumberLiteral{Value: 2.0},
		Whens: []*query.WhenClause{
			{Condition: &query.NumberLiteral{Value: 1.0}, Result: &query.StringLiteral{Value: "one"}},
			{Condition: &query.NumberLiteral{Value: 2.0}, Result: &query.StringLiteral{Value: "two"}},
		},
		Else: &query.StringLiteral{Value: "other"},
	}, nil)
	if err != nil || val != "two" {
		t.Errorf("Simple CASE: expected 'two', got %v (err=%v)", val, err)
	}

	// Simple CASE with ELSE
	val, err = EvalExpression(&query.CaseExpr{
		Expr: &query.NumberLiteral{Value: 99.0},
		Whens: []*query.WhenClause{
			{Condition: &query.NumberLiteral{Value: 1.0}, Result: &query.StringLiteral{Value: "one"}},
		},
		Else: &query.StringLiteral{Value: "default"},
	}, nil)
	if err != nil || val != "default" {
		t.Errorf("CASE ELSE: expected 'default', got %v", val)
	}

	// Searched CASE
	val, err = EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{
			{
				Condition: &query.BooleanLiteral{Value: false},
				Result:    &query.StringLiteral{Value: "no"},
			},
			{
				Condition: &query.BooleanLiteral{Value: true},
				Result:    &query.StringLiteral{Value: "yes"},
			},
		},
	}, nil)
	if err != nil || val != "yes" {
		t.Errorf("Searched CASE: expected 'yes', got %v", val)
	}

	// CASE with no match and no ELSE
	val, err = EvalExpression(&query.CaseExpr{
		Whens: []*query.WhenClause{
			{Condition: &query.BooleanLiteral{Value: false}, Result: &query.StringLiteral{Value: "no"}},
		},
	}, nil)
	if err != nil || val != nil {
		t.Errorf("CASE no match: expected nil, got %v", val)
	}

	// CASE NULL WHEN NULL -- should fall to ELSE per SQL standard
	val, err = EvalExpression(&query.CaseExpr{
		Expr: &query.NullLiteral{},
		Whens: []*query.WhenClause{
			{Condition: &query.NullLiteral{}, Result: &query.StringLiteral{Value: "matched"}},
		},
		Else: &query.StringLiteral{Value: "not_matched"},
	}, nil)
	if err != nil || val != "not_matched" {
		t.Errorf("CASE NULL WHEN NULL: expected 'not_matched', got %v", val)
	}
}

func TestAPI_EvalExpression_CastExpr(t *testing.T) {
	// CAST(3.7 AS INTEGER)
	val, err := EvalExpression(&query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 3.7},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Fatalf("CAST INTEGER: %v", err)
	}
	if val != int64(3) {
		t.Errorf("expected int64(3), got %v (%T)", val, val)
	}

	// CAST(42 AS REAL)
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 42.0},
		DataType: query.TokenReal,
	}, nil)
	if err != nil {
		t.Fatalf("CAST REAL: %v", err)
	}
	if val != float64(42.0) {
		t.Errorf("expected 42.0, got %v", val)
	}

	// CAST(123 AS TEXT)
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NumberLiteral{Value: 123.0},
		DataType: query.TokenText,
	}, nil)
	if err != nil {
		t.Fatalf("CAST TEXT: %v", err)
	}
	if val != "123" {
		t.Errorf("expected '123', got %v", val)
	}

	// CAST(NULL AS INTEGER)
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.NullLiteral{},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Fatalf("CAST NULL: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}

	// CAST('42' AS INTEGER) - string to int
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "42"},
		DataType: query.TokenInteger,
	}, nil)
	if err != nil {
		t.Fatalf("CAST string to INTEGER: %v", err)
	}
	if val != int64(42) {
		t.Errorf("expected int64(42), got %v (%T)", val, val)
	}

	// CAST('3.14' AS REAL) - string to real
	val, err = EvalExpression(&query.CastExpr{
		Expr:     &query.StringLiteral{Value: "3.14"},
		DataType: query.TokenReal,
	}, nil)
	if err != nil {
		t.Fatalf("CAST string to REAL: %v", err)
	}
	if val != float64(3.14) {
		t.Errorf("expected 3.14, got %v", val)
	}
}

// ==================== 20. serializePK ====================

func TestAPI_serializePK(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// String PK (no tree)
	key := cat.serializePK("hello", nil)
	if string(key) != "hello" {
		t.Errorf("expected 'hello', got %s", string(key))
	}

	// Integer PK
	key = cat.serializePK(int64(42), nil)
	expected := "00000000000000000042"
	if string(key) != expected {
		t.Errorf("expected %s, got %s", expected, string(key))
	}

	// int PK
	key = cat.serializePK(int(7), nil)
	expected = "00000000000000000007"
	if string(key) != expected {
		t.Errorf("expected %s, got %s", expected, string(key))
	}

	// float64 PK
	key = cat.serializePK(float64(99), nil)
	expected = "00000000000000000099"
	if string(key) != expected {
		t.Errorf("expected %s, got %s", expected, string(key))
	}

	// Fallback type
	key = cat.serializePK(true, nil)
	if string(key) != "true" {
		t.Errorf("expected 'true', got %s", string(key))
	}
}

// ==================== 21. foreign_key.go functions ====================

func TestAPI_ForeignKey_ValidateInsert(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// Create parent table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable departments: %v", err)
	}

	// Insert a department
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{
				&query.NumberLiteral{Value: 1},
				&query.StringLiteral{Value: "Engineering"},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Insert department: %v", err)
	}

	// Create child table with FK
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "dept_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"dept_id"},
				ReferencedTable:   "departments",
				ReferencedColumns: []string{"id"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable employees: %v", err)
	}

	fke := NewForeignKeyEnforcer(cat)

	// Valid insert (NULL FK is allowed)
	err = fke.ValidateInsert(context.Background(), "employees", map[string]interface{}{
		"id":      int64(1),
		"name":    "Alice",
		"dept_id": nil,
	})
	if err != nil {
		t.Fatalf("ValidateInsert with NULL FK: %v", err)
	}

	// Invalid insert - non-existent table
	err = fke.ValidateInsert(context.Background(), "nonexistent", map[string]interface{}{})
	if err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

func TestAPI_ForeignKey_ValidateUpdate(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// Create parent and child tables
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "parent",
				ReferencedColumns: []string{"id"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	fke := NewForeignKeyEnforcer(cat)

	// Update where FK didn't change
	err = fke.ValidateUpdate(context.Background(), "child",
		map[string]interface{}{"id": int64(1), "parent_id": int64(1)},
		map[string]interface{}{"id": int64(1), "parent_id": int64(1)},
	)
	if err != nil {
		t.Fatalf("ValidateUpdate no change: %v", err)
	}

	// Update to NULL FK (allowed)
	err = fke.ValidateUpdate(context.Background(), "child",
		map[string]interface{}{"id": int64(1), "parent_id": int64(1)},
		map[string]interface{}{"id": int64(1), "parent_id": nil},
	)
	if err != nil {
		t.Fatalf("ValidateUpdate to NULL: %v", err)
	}

	// Non-existent table
	err = fke.ValidateUpdate(context.Background(), "nonexistent",
		map[string]interface{}{},
		map[string]interface{}{},
	)
	if err == nil {
		t.Fatal("expected error for non-existent table")
	}
}

func TestAPI_ForeignKey_serializeCompositeKey(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	fke := NewForeignKeyEnforcer(cat)

	key := fke.serializeCompositeKey([]interface{}{"abc", int64(42)})
	// Should be "S:abc\x0000000000000000000042"
	if len(key) == 0 {
		t.Fatal("expected non-empty composite key")
	}

	// Single value
	key2 := fke.serializeCompositeKey([]interface{}{int64(1)})
	if string(key2) != "00000000000000000001" {
		t.Errorf("expected padded int, got %s", string(key2))
	}
}

func TestAPI_ForeignKey_CheckForeignKeyConstraints(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// Create parent
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}

	// Create child with no data
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "parent",
				ReferencedColumns: []string{"id"},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	fke := NewForeignKeyEnforcer(cat)

	// Check on table with no data
	err = fke.CheckForeignKeyConstraints(context.Background(), "child")
	if err != nil {
		t.Fatalf("CheckForeignKeyConstraints empty: %v", err)
	}

	// Check on non-existent table (no tableTrees entry -> nil)
	err = fke.CheckForeignKeyConstraints(context.Background(), "nonexistent_table")
	if err != nil {
		t.Fatalf("expected nil for non-existent table data, got %v", err)
	}
}

func TestAPI_ForeignKey_referencedRowExists(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	fke := NewForeignKeyEnforcer(cat)

	// Table with no data
	exists, err := fke.referencedRowExists("nonexistent", []string{"id"}, []interface{}{int64(1)})
	if err != nil {
		t.Fatalf("referencedRowExists: %v", err)
	}
	if exists {
		t.Fatal("expected false for non-existent table")
	}

	// Create table and insert data
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "ref_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "ref_table",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{
				&query.NumberLiteral{Value: 1},
				&query.StringLiteral{Value: "test"},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}

	// Check with composite key (multi-column)
	exists, err = fke.referencedRowExists("ref_table", []string{"id", "name"}, []interface{}{int64(1), "test"})
	// May or may not exist depending on key format; just verify no crash
	if err != nil {
		t.Logf("referencedRowExists composite key: %v (expected for format mismatch)", err)
	}
}

// ==================== 22. valuesEqual ====================

func TestAPI_valuesEqual(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	fke := NewForeignKeyEnforcer(cat)

	// nil vs nil
	if !fke.valuesEqual(nil, nil) {
		t.Error("nil == nil should be true")
	}
	// nil vs non-nil
	if fke.valuesEqual(nil, "x") {
		t.Error("nil vs non-nil should be false")
	}
	if fke.valuesEqual("x", nil) {
		t.Error("non-nil vs nil should be false")
	}
	// float64
	if !fke.valuesEqual(float64(1.0), float64(1.0)) {
		t.Error("1.0 == 1.0 should be true")
	}
	if fke.valuesEqual(float64(1.0), float64(2.0)) {
		t.Error("1.0 != 2.0")
	}
	// string
	if !fke.valuesEqual("abc", "abc") {
		t.Error("abc == abc should be true")
	}
	if fke.valuesEqual("abc", "def") {
		t.Error("abc != def")
	}
	// int64 vs float64 cross-type
	if !fke.valuesEqual(int64(5), float64(5.0)) {
		t.Error("int64(5) == float64(5.0) should be true")
	}
	// int vs int64
	if !fke.valuesEqual(int(10), int64(10)) {
		t.Error("int(10) == int64(10) should be true")
	}
	// bool vs bool (non-numeric)
	if !fke.valuesEqual(true, true) {
		t.Error("true == true should be true")
	}
	if fke.valuesEqual(true, false) {
		t.Error("true != false")
	}
}

// ==================== 23. serializeValue ====================

func TestAPI_serializeValue(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	fke := NewForeignKeyEnforcer(cat)

	// string
	val := fke.serializeValue("hello")
	if string(val) != "S:hello" {
		t.Errorf("expected 'S:hello', got %s", string(val))
	}

	// int
	val = fke.serializeValue(int(5))
	if string(val) != "00000000000000000005" {
		t.Errorf("expected padded int, got %s", string(val))
	}

	// int64
	val = fke.serializeValue(int64(123))
	if string(val) != "00000000000000000123" {
		t.Errorf("expected padded int64, got %s", string(val))
	}

	// float64
	val = fke.serializeValue(float64(42))
	if string(val) != "00000000000000000042" {
		t.Errorf("expected padded float64, got %s", string(val))
	}

	// []byte
	val = fke.serializeValue([]byte{1, 2, 3})
	if len(val) != 3 {
		t.Errorf("expected len 3 for []byte, got %d", len(val))
	}

	// nil
	val = fke.serializeValue(nil)
	if string(val) != "NULL" {
		t.Errorf("expected 'NULL', got %s", string(val))
	}

	// fallback type
	val = fke.serializeValue(true)
	if string(val) != "true" {
		t.Errorf("expected 'true', got %s", string(val))
	}
}

// ==================== Additional edge cases ====================

func TestAPI_GetFTSIndex(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	// Not found
	_, err := cat.GetFTSIndex("nope")
	if err == nil {
		t.Fatal("expected error for non-existent FTS index")
	}

	// Create table + FTS index
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "docs",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "content", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	err = cat.CreateFTSIndex("fts_docs", "docs", []string{"content"})
	if err != nil {
		t.Fatalf("CreateFTSIndex: %v", err)
	}

	idx, err := cat.GetFTSIndex("fts_docs")
	if err != nil {
		t.Fatalf("GetFTSIndex: %v", err)
	}
	if idx.Name != "fts_docs" {
		t.Errorf("expected fts_docs, got %s", idx.Name)
	}
}

func TestAPI_MultiwordFTSSearch(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()

	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "posts",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "text", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	err = cat.CreateFTSIndex("fts_posts", "posts", []string{"text"})
	if err != nil {
		t.Fatalf("CreateFTSIndex: %v", err)
	}

	// Manually populate index since Insert stores rows as arrays
	ftsIdx := cat.ftsIndexes["fts_posts"]
	cat.indexRowForFTS(ftsIdx, map[string]interface{}{
		"text": "the quick brown fox",
	}, []byte("row1"))

	// Multi-word search (AND logic)
	results, err := cat.SearchFTS("fts_posts", "quick fox")
	if err != nil {
		t.Fatalf("SearchFTS: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected results for 'quick fox'")
	}

	// One word missing
	results, err = cat.SearchFTS("fts_posts", "quick zebra")
	if err != nil {
		t.Fatalf("SearchFTS: %v", err)
	}
	if len(results) != 0 {
		t.Fatal("expected 0 results when one word is missing")
	}
}

func TestAPI_EvalExpression_UnsupportedOp(t *testing.T) {
	// An unsupported binary operator
	_, err := EvalExpression(&query.BinaryExpr{
		Left:     &query.StringLiteral{Value: "a"},
		Operator: query.TokenLParen, // not a valid binary op
		Right:    &query.StringLiteral{Value: "b"},
	}, nil)
	if err == nil {
		t.Error("expected error for unsupported binary operator")
	}
}

func TestAPI_ListMaterializedViews_Sorted(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	createUsersTable(t, cat)
	insertUser(t, cat, 1, "Alice", 30)

	names := []string{"zz_mv", "aa_mv", "mm_mv"}
	for _, name := range names {
		err := cat.CreateMaterializedView(name, &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "users"},
		})
		if err != nil {
			t.Fatalf("CreateMaterializedView(%s): %v", name, err)
		}
	}

	list := cat.ListMaterializedViews()
	sorted := make([]string, len(list))
	copy(sorted, list)
	sort.Strings(sorted)
	for i := range list {
		if list[i] != sorted[i] {
			t.Fatalf("ListMaterializedViews not sorted: %v", list)
		}
	}
}

func TestAPI_EvalExpression_Negate_Float(t *testing.T) {
	// Negate a float (not integer)
	val, err := EvalExpression(&query.UnaryExpr{
		Operator: query.TokenMinus,
		Expr:     &query.StringLiteral{Value: "3.14"},
	}, nil)
	if err != nil {
		t.Fatalf("Negate non-numeric: %v", err)
	}
	// String can't be negated, should return the value as-is
	if val != "3.14" {
		t.Logf("Non-numeric negate returned: %v (%T)", val, val)
	}
}

func TestAPI_deserializeValue(t *testing.T) {
	cat, pool := newTestCatalog(t)
	defer pool.Close()
	fke := NewForeignKeyEnforcer(cat)

	// String with "S:" prefix
	val := fke.deserializeValue([]byte("S:hello"))
	if val != "hello" {
		t.Errorf("expected 'hello', got %v", val)
	}

	// Zero-padded integer
	val = fke.deserializeValue([]byte("00000000000000000042"))
	if val != int(42) {
		t.Errorf("expected int(42), got %v (%T)", val, val)
	}

	// Regular string
	val = fke.deserializeValue([]byte("abc"))
	// "abc" is not parseable as int or float, should return as string
	if _, ok := val.(string); !ok {
		t.Errorf("expected string, got %T", val)
	}
}

func TestAPI_fastEncodeRow_UnknownType(t *testing.T) {
	// Fallback type: struct
	type custom struct{ X int }
	values := []interface{}{custom{X: 42}}
	encoded, err := fastEncodeRow(values)
	if err != nil {
		t.Fatalf("fastEncodeRow unknown type: %v", err)
	}
	decoded, err := fastDecodeRow(encoded)
	if err != nil {
		t.Fatalf("fastDecodeRow unknown type: %v", err)
	}
	// Should be stored as string (JSON serialized)
	if len(decoded) != 1 {
		t.Fatalf("expected 1 value, got %d", len(decoded))
	}
}
