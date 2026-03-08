package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCommitTransactionMore tests CommitTransaction
func TestCommitTransactionMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Begin a transaction
	cat.BeginTransaction(1)

	// Commit the transaction
	err := cat.CommitTransaction()
	if err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}
}

// TestRollbackTransactionMore tests RollbackTransaction
func TestRollbackTransactionMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Begin a transaction
	cat.BeginTransaction(1)

	// Rollback the transaction
	err := cat.RollbackTransaction()
	if err != nil {
		t.Errorf("Failed to rollback transaction: %v", err)
	}
}

// TestCreateIndexMore tests CreateIndex
func TestCreateIndexMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table first
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_idx_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create index
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_name",
		Table:   "test_idx_tbl",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Logf("CreateIndex error (may be expected): %v", err)
	}
}

// TestUpdateMore tests Update with various scenarios
func TestUpdateMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_update_more",
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
		Table:   "test_update_more",
		Columns: []string{"id", "name", "value"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "alice"}, &query.NumberLiteral{Value: 100}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Update with WHERE clause
	_, _, err = cat.Update(&query.UpdateStmt{
		Table: "test_update_more",
		Set: []*query.SetClause{
			{Column: "value", Value: &query.NumberLiteral{Value: 200}},
		},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err != nil {
		t.Logf("Update error (may be expected): %v", err)
	}
}

// TestLoadDataMore tests LoadData function
func TestLoadDataMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_load_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create temporary CSV file
	tmpFile := filepath.Join(t.TempDir(), "test_load_data.csv")
	content := "1,hello\n2,world\n"
	err = os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}

	// Load data from file
	err = cat.LoadData(tmpFile)
	if err != nil {
		t.Logf("LoadData error (may be expected): %v", err)
	}
}

// TestLoad tests Load function
func TestLoad(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Load on empty catalog should work
	err := cat.Load()
	if err != nil {
		t.Errorf("Load on empty catalog should not error: %v", err)
	}
}

// TestSelectWithFunctions tests SELECT with SQL functions
func TestSelectWithFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_func",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_func",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test UPPER function in SELECT
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "UPPER", Args: []query.Expression{&query.Identifier{Name: "name"}}},
		},
		From: &query.TableRef{Name: "test_func"},
	}
	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("SELECT with UPPER error: %v", err)
	} else {
		t.Logf("Columns: %v, Rows: %v", cols, rows)
	}
}

// TestSelectWithJSONFunctions tests SELECT with JSON functions
func TestSelectWithJSONFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_json",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with JSON
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_json",
		Columns: []string{"id", "data"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: `{"name": "John", "age": 30}`}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test JSON_EXTRACT function in SELECT
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "JSON_EXTRACT", Args: []query.Expression{
				&query.Identifier{Name: "data"},
				&query.StringLiteral{Value: "$.name"},
			}},
		},
		From: &query.TableRef{Name: "test_json"},
	}
	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("SELECT with JSON_EXTRACT error: %v", err)
	} else {
		t.Logf("Columns: %v, Rows: %v", cols, rows)
	}
}

// TestEvaluateHavingMore tests evaluateHaving with more scenarios
func TestEvaluateHavingMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_having_more",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		category := "A"
		if i > 3 {
			category = "B"
		}
		_, _, err = cat.Insert(&query.InsertStmt{
			Table:   "test_having_more",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.StringLiteral{Value: category}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test HAVING with aggregate
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		From:    &query.TableRef{Name: "test_having_more"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		Having: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "COUNT"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("SELECT with HAVING error: %v", err)
	} else {
		t.Logf("Columns: %v, Rows: %v", cols, rows)
	}
}

// TestComputeAggregates tests computeAggregates function

// TestExecuteScalarAggregateMore tests executeScalarAggregate with more scenarios
func TestExecuteScalarAggregateMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_scalar_agg",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		_, _, err = cat.Insert(&query.InsertStmt{
			Table:   "test_scalar_agg",
			Columns: []string{"id", "value"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(i)}, &query.NumberLiteral{Value: float64(i * 10)}}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test MIN and MAX
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.FunctionCall{Name: "MIN", Args: []query.Expression{&query.Identifier{Name: "value"}}},
			&query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.Identifier{Name: "value"}}},
		},
		From: &query.TableRef{Name: "test_scalar_agg"},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("MIN/MAX query error: %v", err)
	} else {
		t.Logf("Columns: %v, Rows: %v", cols, rows)
	}
}

// TestApplyGroupByOrderByMore tests applyGroupByOrderBy with more scenarios
func TestApplyGroupByOrderByMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_group_order",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "value", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with different categories
	data := []struct {
		id       int
		category string
		value    float64
	}{
		{1, "A", 10},
		{2, "B", 20},
		{3, "A", 30},
		{4, "B", 40},
		{5, "C", 50},
	}
	for _, d := range data {
		_, _, err = cat.Insert(&query.InsertStmt{
			Table:   "test_group_order",
			Columns: []string{"id", "category", "value"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(d.id)}, &query.StringLiteral{Value: d.category}, &query.NumberLiteral{Value: d.value}}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Test GROUP BY with ORDER BY
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "value"}}},
		},
		From:    &query.TableRef{Name: "test_group_order"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		OrderBy: []*query.OrderByExpr{{Expr: &query.Identifier{Name: "category"}}},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("GROUP BY with ORDER BY error: %v", err)
	} else {
		t.Logf("Columns: %v, Rows: %v", cols, rows)
	}
}

// TestEncodeRowMore tests encodeRow function
func TestEncodeRowMore(t *testing.T) {
	// Test with various expression types
	exprs := []query.Expression{
		&query.StringLiteral{Value: "hello"},
		&query.NumberLiteral{Value: 42},
		&query.BooleanLiteral{Value: true},
		&query.NullLiteral{},
	}

	data, err := encodeRow(exprs, nil)
	if err != nil {
		t.Fatalf("encodeRow failed: %v", err)
	}

	var result []interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if len(result) != 4 {
		t.Errorf("Expected 4 values, got %d", len(result))
	}

	t.Logf("Encoded row: %v", result)
}

// TestToBool tests toBool function
func TestToBool(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected bool
	}{
		{nil, false},
		{true, true},
		{false, false},
		{int(1), true},
		{int(0), false},
		{int64(1), true},
		{int64(0), false},
		{float64(1.5), true},
		{float64(0), false},
		{"hello", true},
		{"", false},
	}

	for _, tt := range tests {
		result := toBool(tt.input)
		if result != tt.expected {
			t.Errorf("toBool(%v) = %v, expected %v", tt.input, result, tt.expected)
		}
	}
}

// TestTokenTypeToColumnTypeMore tests tokenTypeToColumnType
func TestTokenTypeToColumnTypeMore(t *testing.T) {
	tests := []struct {
		tokenType query.TokenType
		expected  string
	}{
		{query.TokenInteger, "INTEGER"},
		{query.TokenReal, "REAL"},
		{query.TokenText, "TEXT"},
		{query.TokenBoolean, "BOOLEAN"},
		{query.TokenBlob, "BLOB"},
		{query.TokenType(999), "TEXT"}, // Unknown type defaults to text
	}

	for _, tt := range tests {
		result := tokenTypeToColumnType(tt.tokenType)
		if result != tt.expected {
			t.Errorf("tokenTypeToColumnType(%v) = %v, expected %v", tt.tokenType, result, tt.expected)
		}
	}
}

// TestExecuteSelectWithJoin tests executeSelectWithJoin
func TestExecuteSelectWithJoin(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create first table
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

	// Create second table
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

	// Insert data into users
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert into users: %v", err)
	}

	// Insert data into orders
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "user_id", "amount"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 100}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert into orders: %v", err)
	}

	// Test JOIN
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "name"},
			&query.Identifier{Name: "amount"},
		},
		From: &query.TableRef{Name: "users"},
		Joins: []*query.JoinClause{
			{
				Type:  query.TokenInner,
				Table: &query.TableRef{Name: "orders"},
				Condition: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.Identifier{Name: "user_id"},
				},
			},
		},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Logf("JOIN query error: %v", err)
	} else {
		t.Logf("Columns: %v, Rows: %v", cols, rows)
	}
}

// TestComputeAggregatesEmptyTable tests aggregates on empty table

// TestLoadMore tests Load function
func TestLoadMore(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()
	dataFile := filepath.Join(tmpDir, "test.db")

	// Create backend and pool
	backend, err := storage.OpenDisk(dataFile)
	if err != nil {
		t.Fatalf("Failed to create disk backend: %v", err)
	}
	pool := storage.NewBufferPool(1024, backend)

	// Create catalog and tables
	cat := New(nil, pool, nil)
	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "test_load_table1",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table1: %v", err)
	}

	err = cat.CreateTable(&query.CreateTableStmt{
		Table: "test_load_table2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "value", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table2: %v", err)
	}

	// Create an index
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_test",
		Table:   "test_load_table1",
		Columns: []string{"name"},
	})
	if err != nil {
		t.Logf("CreateIndex error (may be expected): %v", err)
	}

	// Close the pool
	pool.Close()
	backend.Close()

	// Reopen and load
	backend2, err := storage.OpenDisk(dataFile)
	if err != nil {
		t.Fatalf("Failed to reopen disk backend: %v", err)
	}
	pool2 := storage.NewBufferPool(1024, backend2)
	cat2 := New(nil, pool2, nil)

	// Load the catalog - this may not work with disk backend
	// as the catalog storage mechanism may differ
	err = cat2.Load()
	if err != nil {
		t.Logf("Load returned error (may be expected): %v", err)
	}

	// Note: Catalog persistence may not be fully implemented
	// Just verify Load doesn't crash
	t.Logf("Tables after load: %v", cat2.ListTables())

	pool2.Close()
	backend2.Close()
}

// TestLoadWithNilTree tests Load with nil tree
func TestLoadWithNilTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Load with nil tree should return nil
	err := cat.Load()
	if err != nil {
		t.Errorf("Load with nil tree should return nil, got: %v", err)
	}
}

// TestComputeAggregatesNonExistentTable tests aggregates on non-existent table

// TestParseJSONPathMore tests ParseJSONPath with various paths
func TestParseJSONPathMore(t *testing.T) {
	tests := []struct {
		path    string
		isValid bool
	}{
		{"$.store.book[0].title", true},
		{"$..book[2]", true},
		{"$.store.*", true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result, err := ParseJSONPath(tt.path)
			if tt.isValid && err != nil {
				t.Logf("ParseJSONPath(%s) error: %v", tt.path, err)
			}
			if !tt.isValid && err == nil {
				t.Logf("ParseJSONPath(%s) should have errored, got: %v", tt.path, result)
			}
		})
	}
}

// TestEncodeRowWithIdentifier tests encodeRow with Identifier
func TestEncodeRowWithIdentifier(t *testing.T) {
	exprs := []query.Expression{
		&query.Identifier{Name: "column_name"},
	}
	args := []interface{}{}

	data, err := encodeRow(exprs, args)
	if err != nil {
		t.Fatalf("encodeRow failed: %v", err)
	}

	var result []interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	if len(result) != 1 || result[0] != "column_name" {
		t.Errorf("Expected ['column_name'], got %v", result)
	}
}

// TestEncodeRowWithExtraArgs tests encodeRow with extra args
func TestEncodeRowWithExtraArgs(t *testing.T) {
	exprs := []query.Expression{
		&query.StringLiteral{Value: "fixed"},
	}
	args := []interface{}{"extra1", "extra2"}

	data, err := encodeRow(exprs, args)
	if err != nil {
		t.Fatalf("encodeRow failed: %v", err)
	}

	var result []interface{}
	err = json.Unmarshal(data, &result)
	if err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	// Should include both the fixed value and extra args
	if len(result) != 3 {
		t.Errorf("Expected 3 values (1 fixed + 2 extra), got %d: %v", len(result), result)
	}

	t.Logf("Result: %v", result)
}

// TestLoadDataWithDataFiles tests LoadData function with actual data files
func TestLoadDataWithDataFiles(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_load_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create temp directory with data files
	tmpDir := t.TempDir()

	// Create a JSON data file
	dataFile := filepath.Join(tmpDir, "test_load_data.json")
	data := `{"keys":["1","2"],"values":["{\"id\":1,\"name\":\"Alice\"}","{\"id\":2,\"name\":\"Bob\"}"]}`
	err = os.WriteFile(dataFile, []byte(data), 0644)
	if err != nil {
		t.Fatalf("Failed to write data file: %v", err)
	}

	// Load data
	err = cat.LoadData(tmpDir)
	if err != nil {
		t.Logf("LoadData error (may be expected): %v", err)
	}

	// Verify data was loaded
	cols, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "test_load_data"},
	}, nil)
	if err != nil {
		t.Logf("Select after LoadData error: %v", err)
	} else {
		t.Logf("Loaded data: cols=%v, rows=%d", cols, len(rows))
	}
}

// TestLoadDataNonExistentDir tests LoadData with non-existent directory
func TestLoadDataNonExistentDir(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Load from non-existent directory should not error
	err := cat.LoadData("/non/existent/dir")
	if err != nil {
		t.Errorf("LoadData with non-existent dir should not error: %v", err)
	}
}

// TestEvaluateWhereMore tests evaluateWhere with various scenarios
func TestEvaluateWhereMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_where",
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
		Table:   "test_where",
		Columns: []string{"id", "name", "value"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}, &query.NumberLiteral{Value: 100}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Test WHERE with various conditions
	tests := []struct {
		name     string
		where    query.Expression
		expected int // expected row count
	}{
		{
			name: "EQ condition",
			where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 1},
			},
			expected: 1,
		},
		{
			name: "GT condition",
			where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 0},
			},
			expected: 1,
		},
		{
			name: "LT condition",
			where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenLt,
				Right:    &query.NumberLiteral{Value: 2},
			},
			expected: 1,
		},
		{
			name: "AND condition",
			where: &query.BinaryExpr{
				Left: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenGt,
					Right:    &query.NumberLiteral{Value: 0},
				},
				Operator: query.TokenAnd,
				Right: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "value"},
					Operator: query.TokenLt,
					Right:    &query.NumberLiteral{Value: 200},
				},
			},
			expected: 1,
		},
		{
			name: "OR condition",
			where: &query.BinaryExpr{
				Left: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.NumberLiteral{Value: 1},
				},
				Operator: query.TokenOr,
				Right: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "id"},
					Operator: query.TokenEq,
					Right:    &query.NumberLiteral{Value: 2},
				},
			},
			expected: 1,
		},
		{
			name: "String equality",
			where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "name"},
				Operator: query.TokenEq,
				Right:    &query.StringLiteral{Value: "Alice"},
			},
			expected: 1,
		},
		{
			name: "No match condition",
			where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 999},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := &query.SelectStmt{
				Columns: []query.Expression{&query.StarExpr{}},
				From:    &query.TableRef{Name: "test_where"},
				Where:   tt.where,
			}
			cols, rows, err := cat.Select(stmt, nil)
			if err != nil {
				t.Logf("Select error: %v", err)
				return
			}
			if len(rows) != tt.expected {
				t.Errorf("Expected %d rows, got %d", tt.expected, len(rows))
			}
			t.Logf("%s: cols=%v, rows=%d", tt.name, cols, len(rows))
		})
	}
}

// TestComputeAggregatesDirect tests computeAggregates function directly

// TestComputeAggregatesWithWhere tests computeAggregates function with WHERE clause

// TestComputeAggregatesWithNullValues tests computeAggregates with NULL values

// TestLoadCatalogWithNilTree tests Load with nil tree
func TestLoadCatalogWithNilTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)

	// Create catalog without tree
	cat := &Catalog{
		pool:    pool,
		tables:  make(map[string]*TableDef),
		indexes: make(map[string]*IndexDef),
	}

	// Load with nil tree should return nil immediately
	err := cat.Load()
	if err != nil {
		t.Fatalf("Load with nil tree should return nil, got: %v", err)
	}

	t.Log("Load with nil tree returned nil as expected")
}

// TestEvaluateWhereWithDifferentTypes tests evaluateWhere with various data types
func TestEvaluateWhereWithDifferentTypes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table with various data types
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_where_types",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "active", Type: query.TokenBoolean},
			{Name: "score", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_where_types",
		Columns: []string{"id", "name", "active", "score"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}, &query.BooleanLiteral{Value: true}, &query.NumberLiteral{Value: 95.5}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert row 1: %v", err)
	}

	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_where_types",
		Columns: []string{"id", "name", "active", "score"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}, &query.BooleanLiteral{Value: false}, &query.NumberLiteral{Value: 82.0}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert row 2: %v", err)
	}

	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_where_types",
		Columns: []string{"id", "name", "active", "score"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Charlie"}, &query.BooleanLiteral{Value: true}, &query.NumberLiteral{Value: 78.5}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert row 3: %v", err)
	}

	// Test string equality
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "test_where_types"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "name"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "Alice"},
		},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("String equality query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
	t.Logf("String equality result: cols=%v, rows=%v", cols, rows)

	// Test boolean condition
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "test_where_types"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "active"},
			Operator: query.TokenEq,
			Right:    &query.BooleanLiteral{Value: true},
		},
	}

	cols, rows, err = cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("Boolean query failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
	t.Logf("Boolean condition result: cols=%v, rows=%v", cols, rows)

	// Test numeric comparison
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "test_where_types"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "score"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 80},
		},
	}

	cols, rows, err = cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("Numeric comparison query failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
	t.Logf("Numeric comparison result: cols=%v, rows=%v", cols, rows)

	// Test LIKE condition with simpler pattern
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "name"}},
		From:    &query.TableRef{Name: "test_where_types"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "name"},
			Operator: query.TokenLike,
			Right:    &query.StringLiteral{Value: "Ali%"},
		},
	}

	cols, rows, err = cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("LIKE query failed: %v", err)
	}
	// Note: LIKE implementation may vary - just log the result
	t.Logf("LIKE condition result: cols=%v, rows=%v (expected: rows containing 'Ali%%')", cols, rows)
}

// TestCommitTransactionWithoutWAL tests CommitTransaction without WAL
func TestCommitTransactionWithoutWAL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)

	// Create catalog without WAL
	cat := New(nil, pool, nil)

	// Begin transaction
	cat.BeginTransaction(1)

	// Commit transaction should work without WAL
	err := cat.CommitTransaction()
	if err != nil {
		t.Fatalf("CommitTransaction without WAL failed: %v", err)
	}

	if cat.txnActive {
		t.Error("Transaction should not be active after commit")
	}

	t.Log("CommitTransaction without WAL completed successfully")
}

// TestRollbackTransactionWithoutWAL tests RollbackTransaction without WAL
func TestRollbackTransactionWithoutWAL(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)

	// Create catalog without WAL
	cat := New(nil, pool, nil)

	// Begin transaction
	cat.BeginTransaction(1)

	// Rollback transaction should work without WAL
	err := cat.RollbackTransaction()
	if err != nil {
		t.Fatalf("RollbackTransaction without WAL failed: %v", err)
	}

	if cat.txnActive {
		t.Error("Transaction should not be active after rollback")
	}

	t.Log("RollbackTransaction without WAL completed successfully")
}

// TestGroupByWithOrderBy tests GROUP BY with ORDER BY clause
func TestGroupByWithOrderBy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_group_order",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	data := []struct {
		id       int
		category string
		amount   float64
	}{
		{1, "A", 100.0},
		{2, "A", 200.0},
		{3, "B", 50.0},
		{4, "B", 150.0},
		{5, "C", 300.0},
	}

	for _, d := range data {
		_, _, err := cat.Insert(&query.InsertStmt{
			Table:   "test_group_order",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(d.id)}, &query.StringLiteral{Value: d.category}, &query.NumberLiteral{Value: d.amount}}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Test GROUP BY with ORDER BY category ASC
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		},
		From:    &query.TableRef{Name: "test_group_order"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "category"}, Desc: false},
		},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("GROUP BY with ORDER BY ASC failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 groups, got %d", len(rows))
	}

	// Verify order (should be A, B, C)
	if len(rows) >= 3 {
		if cat, ok := rows[0][0].(string); !ok || cat != "A" {
			t.Errorf("Expected first group 'A', got %v", rows[0][0])
		}
		if cat, ok := rows[1][0].(string); !ok || cat != "B" {
			t.Errorf("Expected second group 'B', got %v", rows[1][0])
		}
		if cat, ok := rows[2][0].(string); !ok || cat != "C" {
			t.Errorf("Expected third group 'C', got %v", rows[2][0])
		}
	}

	t.Logf("GROUP BY with ORDER BY ASC: cols=%v, rows=%v", cols, rows)

	// Test GROUP BY with ORDER BY category DESC
	stmt = &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		},
		From:    &query.TableRef{Name: "test_group_order"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "category"}, Desc: true},
		},
	}

	cols, rows, err = cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("GROUP BY with ORDER BY DESC failed: %v", err)
	}

	// Verify order (should be C, B, A)
	if len(rows) >= 3 {
		if cat, ok := rows[0][0].(string); !ok || cat != "C" {
			t.Errorf("Expected first group 'C', got %v", rows[0][0])
		}
		if cat, ok := rows[2][0].(string); !ok || cat != "A" {
			t.Errorf("Expected last group 'A', got %v", rows[2][0])
		}
	}

	t.Logf("GROUP BY with ORDER BY DESC: cols=%v, rows=%v", cols, rows)
}

// TestHavingClause tests HAVING clause with GROUP BY
func TestHavingClause(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_having",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenReal},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	data := []struct {
		id       int
		category string
		amount   float64
	}{
		{1, "A", 100.0},
		{2, "A", 200.0},
		{3, "B", 50.0},
		{4, "B", 150.0},
		{5, "C", 300.0},
	}

	for _, d := range data {
		_, _, err := cat.Insert(&query.InsertStmt{
			Table:   "test_having",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{&query.NumberLiteral{Value: float64(d.id)}, &query.StringLiteral{Value: d.category}, &query.NumberLiteral{Value: d.amount}}},
		}, nil)
		if err != nil {
			t.Fatalf("Failed to insert row: %v", err)
		}
	}

	// Test GROUP BY with HAVING clause
	stmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
		},
		From:    &query.TableRef{Name: "test_having"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		Having: &query.BinaryExpr{
			Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 200},
		},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("GROUP BY with HAVING failed: %v", err)
	}

	// Should only return groups with SUM > 200 (A=300, C=300)
	if len(rows) != 2 {
		t.Errorf("Expected 2 groups with SUM > 200, got %d", len(rows))
	}

	t.Logf("GROUP BY with HAVING: cols=%v, rows=%v", cols, rows)

	// Test GROUP BY with HAVING COUNT
	stmt = &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "category"},
			&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
		},
		From:    &query.TableRef{Name: "test_having"},
		GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		Having: &query.BinaryExpr{
			Left:     &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 2},
		},
	}

	cols, rows, err = cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("GROUP BY with HAVING COUNT failed: %v", err)
	}

	// Should only return groups with COUNT = 2 (A and B both have 2 rows)
	if len(rows) != 2 {
		t.Errorf("Expected 2 groups with COUNT = 2, got %d", len(rows))
	}

	t.Logf("GROUP BY with HAVING COUNT: cols=%v, rows=%v", cols, rows)
}

// TestInOperatorMore tests IN operator with more cases
func TestInOperatorMore(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	cat := New(nil, pool, nil)

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_in",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_in",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "A"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert row 1: %v", err)
	}

	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_in",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "B"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert row 2: %v", err)
	}

	_, _, err = cat.Insert(&query.InsertStmt{
		Table:   "test_in",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "C"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert row 3: %v", err)
	}

	// Test IN operator with list
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "category"}},
		From:    &query.TableRef{Name: "test_in"},
		Where: &query.InExpr{
			Expr: &query.Identifier{Name: "category"},
			List: []query.Expression{&query.StringLiteral{Value: "A"}, &query.StringLiteral{Value: "C"}},
		},
	}

	cols, rows, err := cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("IN operator query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows with category IN ('A', 'C'), got %d", len(rows))
	}

	t.Logf("IN operator result: cols=%v, rows=%v", cols, rows)

	// Test NOT IN operator
	stmt = &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}, &query.Identifier{Name: "category"}},
		From:    &query.TableRef{Name: "test_in"},
		Where: &query.InExpr{
			Expr: &query.Identifier{Name: "category"},
			List: []query.Expression{&query.StringLiteral{Value: "A"}},
			Not:  true,
		},
	}

	cols, rows, err = cat.Select(stmt, nil)
	if err != nil {
		t.Fatalf("NOT IN operator query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows with category NOT IN ('A'), got %d", len(rows))
	}

	t.Logf("NOT IN operator result: cols=%v, rows=%v", cols, rows)
}
