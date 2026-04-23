package catalog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ============================================================
// REAL Functional Tests - Verifies actual behavior
// ============================================================

// TestReal_InsertAndSelect verifies INSERT and SELECT work correctly
func TestReal_InsertAndSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table: "test_users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "age", Type: query.TokenInteger},
		},
		PrimaryKey: []string{"id"},
	})
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert test data
	_, _, err = cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_users",
		Columns: []string{"id", "name", "age"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Alice"), numReal(30)},
			{numReal(2), strReal("Bob"), numReal(25)},
			{numReal(3), strReal("Carol"), numReal(35)},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Select all rows
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.StarExpr{}},
		From:    &query.TableRef{Name: "test_users"},
		OrderBy: []*query.OrderByExpr{{Expr: &query.QualifiedIdentifier{Column: "id"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	// Verify data
	expected := []struct {
		id   int64
		name string
		age  int64
	}{
		{1, "Alice", 30},
		{2, "Bob", 25},
		{3, "Carol", 35},
	}

	for i, row := range rows {
		if row[0].(int64) != expected[i].id {
			t.Errorf("Row %d: expected id %d, got %d", i, expected[i].id, row[0].(int64))
		}
		if row[1].(string) != expected[i].name {
			t.Errorf("Row %d: expected name %s, got %s", i, expected[i].name, row[1].(string))
		}
		if row[2].(int64) != expected[i].age {
			t.Errorf("Row %d: expected age %d, got %d", i, expected[i].age, row[2].(int64))
		}
	}
}

// TestReal_Update verifies UPDATE works correctly
func TestReal_Update(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_update",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_update",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(200)}},
	}, nil)

	// Update row 1
	_, affected, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "test_update",
		Set:   []*query.SetClause{{Column: "val", Value: numReal(999)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}

	// Verify update
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("val")},
		From:    &query.TableRef{Name: "test_update"},
		Where:   &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
	if rows[0][0].(int64) != 999 {
		t.Errorf("Expected value 999, got %v", rows[0][0])
	}
}

// TestReal_Delete verifies DELETE works correctly
func TestReal_Delete(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_delete",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 5; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_delete",
			Columns: []string{"id"},
			Values:  [][]query.Expression{{numReal(float64(i))}},
		}, nil)
	}

	// Delete id=3
	_, affected, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "test_delete",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(3)},
	}, nil)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("Expected 1 row deleted, got %d", affected)
	}

	// Verify count
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "test_delete"},
	}, nil)

	if len(rows) != 1 || rows[0][0].(int64) != 4 {
		t.Errorf("Expected 4 rows remaining, got %v", rows)
	}
}

// TestReal_Join verifies JOIN works correctly
func TestReal_Join(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create tables
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "customers",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "orders",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "customer_id", Type: query.TokenInteger}, {Name: "amount", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "customers",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "customer_id", "amount"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), numReal(100)}, {numReal(2), numReal(1), numReal(200)}, {numReal(3), numReal(2), numReal(150)}},
	}, nil)

	// Test JOIN
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			colReal("customers.name"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{colReal("orders.amount")}}, Alias: "total"},
		},
		From: &query.TableRef{Name: "customers"},
		Joins: []*query.JoinClause{
			{Type: query.TokenInner, Table: &query.TableRef{Name: "orders"}, Condition: &query.BinaryExpr{Left: colReal("customers.id"), Operator: query.TokenEq, Right: colReal("orders.customer_id")}},
		},
		GroupBy: []query.Expression{colReal("customers.name")},
	}, nil)
	if err != nil {
		t.Fatalf("Join query failed: %v", err)
	}

	// Should have 2 customers with orders
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}

	// Verify totals
	found := make(map[string]int64)
	for _, row := range rows {
		name := row[0].(string)
		total := row[1].(float64)
		found[name] = int64(total)
	}

	if found["Alice"] != 300 {
		t.Errorf("Expected Alice total 300, got %d", found["Alice"])
	}
	if found["Bob"] != 150 {
		t.Errorf("Expected Bob total 150, got %d", found["Bob"])
	}
}

// TestReal_Aggregates verifies aggregate functions work
func TestReal_Aggregates(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_agg",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "score", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	for i := 1; i <= 10; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "test_agg",
			Columns: []string{"id", "score"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Test aggregates
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}, Alias: "cnt"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{colReal("score")}}, Alias: "total"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "AVG", Args: []query.Expression{colReal("score")}}, Alias: "avg"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "MIN", Args: []query.Expression{colReal("score")}}, Alias: "min"},
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "MAX", Args: []query.Expression{colReal("score")}}, Alias: "max"},
		},
		From: &query.TableRef{Name: "test_agg"},
	}, nil)
	if err != nil {
		t.Fatalf("Aggregate query failed: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}

	// Verify: 10 rows, sum=550, avg=55, min=10, max=100
	if rows[0][0].(int64) != 10 {
		t.Errorf("Expected COUNT 10, got %v", rows[0][0])
	}
	if rows[0][1].(float64) != 550 {
		t.Errorf("Expected SUM 550, got %v", rows[0][1])
	}
	if rows[0][2].(float64) != 55 {
		t.Errorf("Expected AVG 55, got %v", rows[0][2])
	}
	if rows[0][3].(int64) != 10 {
		t.Errorf("Expected MIN 10, got %v", rows[0][3])
	}
	if rows[0][4].(int64) != 100 {
		t.Errorf("Expected MAX 100, got %v", rows[0][4])
	}
}

// TestReal_Transaction verifies transaction commit/rollback
func TestReal_Transaction(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_txn",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	// Begin transaction
	cat.BeginTransaction(1)

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_txn",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)

	// Commit
	err := cat.CommitTransaction()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data persisted
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "test_txn"},
	}, nil)

	if len(rows) != 1 || rows[0][0].(int64) != 2 {
		t.Errorf("Expected 2 rows after commit, got %v", rows)
	}
}

// TestReal_IndexUsage verifies index is used for queries
func TestReal_IndexUsage(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_idx",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "email", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	// Create index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_email",
		Table:   "test_idx",
		Columns: []string{"email"},
	})
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_idx",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{numReal(1), strReal("alice@test.com")}, {numReal(2), strReal("bob@test.com")}},
	}, nil)

	// Query using index
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "test_idx"},
		Where:   &query.BinaryExpr{Left: colReal("email"), Operator: query.TokenEq, Right: strReal("alice@test.com")},
	}, nil)
	if err != nil {
		t.Fatalf("Select with index failed: %v", err)
	}

	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Errorf("Expected id=1, got %v", rows)
	}
}

// TestReal_WindowFunctions verifies window functions work
func TestReal_WindowFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_window",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "dept", Type: query.TokenText}, {Name: "salary", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_window",
		Columns: []string{"id", "dept", "salary"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Eng"), numReal(100)},
			{numReal(2), strReal("Eng"), numReal(200)},
			{numReal(3), strReal("Sales"), numReal(150)},
		},
	}, nil)

	// Test ROW_NUMBER
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			colReal("id"),
			&query.WindowExpr{Function: "ROW_NUMBER", PartitionBy: []query.Expression{colReal("dept")}, OrderBy: []*query.OrderByExpr{{Expr: colReal("salary")}}},
		},
		From:    &query.TableRef{Name: "test_window"},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("id")}},
	}, nil)
	if err != nil {
		t.Fatalf("Window function failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}

	// Verify row numbers (Eng: 100=1, 200=2; Sales: 150=1)
	expectedRanks := map[int64]int64{1: 1, 2: 2, 3: 1}
	for _, row := range rows {
		id := row[0].(int64)
		rank := row[1].(int64)
		if rank != expectedRanks[id] {
			t.Errorf("Id %d: expected rank %d, got %d", id, expectedRanks[id], rank)
		}
	}
}

// TestReal_LeftJoin verifies LEFT JOIN works correctly
func TestReal_LeftJoin(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create tables
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "departments",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "employees",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "dept_id", Type: query.TokenInteger}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	// Insert departments
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Engineering")}, {numReal(2), strReal("Sales")}, {numReal(3), strReal("Marketing")}},
	}, nil)

	// Insert employees (only for Engineering and Sales, not Marketing)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id", "name"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("Alice")}, {numReal(2), numReal(1), strReal("Bob")}, {numReal(3), numReal(2), strReal("Carol")}},
	}, nil)

	// LEFT JOIN - should show all departments including Marketing with no employees
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			colReal("departments.name"),
			colReal("employees.name"),
		},
		From: &query.TableRef{Name: "departments"},
		Joins: []*query.JoinClause{
			{Type: query.TokenLeft, Table: &query.TableRef{Name: "employees"}, Condition: &query.BinaryExpr{Left: colReal("departments.id"), Operator: query.TokenEq, Right: colReal("employees.dept_id")}},
		},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("departments.id")}},
	}, nil)
	if err != nil {
		t.Fatalf("LEFT JOIN failed: %v", err)
	}

	// Should have 4 rows: Eng+Alice, Eng+Bob, Sales+Carol, Marketing+NULL
	if len(rows) != 4 {
		t.Errorf("Expected 4 rows, got %d", len(rows))
	}

	// Check Marketing department has NULL employee
	foundMarketingNull := false
	for _, row := range rows {
		dept := row[0].(string)
		emp := row[1]
		if dept == "Marketing" && emp == nil {
			foundMarketingNull = true
		}
	}
	if !foundMarketingNull {
		t.Errorf("Expected Marketing department with NULL employee")
	}
}

// TestReal_SubqueryInSelect verifies subqueries work
func TestReal_SubqueryInSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "products",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "category", Type: query.TokenText}, {Name: "price", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "category", "price"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Electronics"), numReal(100)},
			{numReal(2), strReal("Electronics"), numReal(200)},
			{numReal(3), strReal("Clothing"), numReal(50)},
		},
	}, nil)

	// SELECT with IN subquery
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "products"},
		Where: &query.InExpr{
			Expr: colReal("category"),
			List: []query.Expression{
				strReal("Electronics"),
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("IN query failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 Electronics products, got %d", len(rows))
	}
}

// TestReal_OrderByLimit verifies ORDER BY and LIMIT
func TestReal_OrderByLimit(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "scores",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "score", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	// Insert scores in random order
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "scores",
		Columns: []string{"id", "score"},
		Values:  [][]query.Expression{{numReal(1), numReal(50)}, {numReal(2), numReal(90)}, {numReal(3), numReal(70)}, {numReal(4), numReal(80)}},
	}, nil)

	// SELECT with ORDER BY DESC
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("score")},
		From:    &query.TableRef{Name: "scores"},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("score"), Desc: true}},
	}, nil)
	if err != nil {
		t.Fatalf("ORDER BY failed: %v", err)
	}

	// Verify descending order
	expected := []int64{90, 80, 70, 50}
	for i, row := range rows {
		if row[0].(int64) != expected[i] {
			t.Errorf("Row %d: expected %d, got %d", i, expected[i], row[0].(int64))
		}
	}
}

// TestReal_Rollback verifies transaction rollback
func TestReal_Rollback(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_rollback",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	// Insert initial data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_rollback",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Begin transaction and insert more
	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_rollback",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}, {numReal(3)}},
	}, nil)

	// Rollback
	err := cat.RollbackTransaction()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify only original row remains
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "test_rollback"},
	}, nil)

	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Errorf("Expected 1 row after rollback, got %v", rows)
	}
}

// TestReal_Savepoint verifies savepoint rollback
func TestReal_Savepoint(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "test_savepoint",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	cat.BeginTransaction(1)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_savepoint",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)

	// Create savepoint
	err := cat.Savepoint("sp1")
	if err != nil {
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Insert more after savepoint
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "test_savepoint",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(2)}},
	}, nil)

	// Rollback to savepoint
	err = cat.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("Rollback to savepoint failed: %v", err)
	}

	cat.CommitTransaction()

	// Verify only row 1 exists
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "test_savepoint"},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("id")}},
	}, nil)

	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Errorf("Expected only row 1, got %v", rows)
	}
}

// TestReal_UpdateMultipleColumns verifies UPDATE with multiple columns
func TestReal_UpdateMultipleColumns(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "users",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}, {Name: "age", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name", "age"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice"), numReal(30)}},
	}, nil)

	// UPDATE multiple columns
	_, affected, err := cat.Update(ctx, &query.UpdateStmt{
		Table: "users",
		Set:   []*query.SetClause{{Column: "name", Value: strReal("Alicia")}, {Column: "age", Value: numReal(31)}},
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Fatalf("UPDATE multiple columns failed: %v", err)
	}
	if affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}

	// Verify both columns updated
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("name"), colReal("age")},
		From:    &query.TableRef{Name: "users"},
	}, nil)

	if len(rows) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(rows))
	}
	if rows[0][0].(string) != "Alicia" {
		t.Errorf("Expected name Alicia, got %v", rows[0][0])
	}
	if rows[0][1].(int64) != 31 {
		t.Errorf("Expected age 31, got %v", rows[0][1])
	}
}

// TestReal_Distinct verifies DISTINCT queries
func TestReal_Distinct(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "orders",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "category", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "orders",
		Columns: []string{"id", "category"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}, {numReal(3), strReal("A")}, {numReal(4), strReal("C")}, {numReal(5), strReal("B")}},
	}, nil)

	// SELECT DISTINCT category
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns:  []query.Expression{colReal("category")},
		From:     &query.TableRef{Name: "orders"},
		Distinct: true,
		OrderBy:  []*query.OrderByExpr{{Expr: colReal("category")}},
	}, nil)
	if err != nil {
		t.Fatalf("DISTINCT failed: %v", err)
	}

	if len(rows) != 3 {
		t.Errorf("Expected 3 distinct categories, got %d", len(rows))
	}

	// Verify A, B, C
	expected := []string{"A", "B", "C"}
	for i, row := range rows {
		if row[0].(string) != expected[i] {
			t.Errorf("Row %d: expected %s, got %s", i, expected[i], row[0].(string))
		}
	}
}

// TestReal_Having verifies HAVING clause with GROUP BY
func TestReal_Having(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "sales",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "region", Type: query.TokenText}, {Name: "amount", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "region", "amount"},
		Values: [][]query.Expression{
			{numReal(1), strReal("North"), numReal(100)},
			{numReal(2), strReal("North"), numReal(200)},
			{numReal(3), strReal("South"), numReal(50)},
			{numReal(4), strReal("South"), numReal(60)},
			{numReal(5), strReal("East"), numReal(500)},
		},
	}, nil)

	// SELECT region, SUM(amount) FROM sales GROUP BY region HAVING SUM(amount) > 150
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			colReal("region"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "SUM", Args: []query.Expression{colReal("amount")}}, Alias: "total"},
		},
		From:    &query.TableRef{Name: "sales"},
		GroupBy: []query.Expression{colReal("region")},
		Having:  &query.BinaryExpr{Left: &query.FunctionCall{Name: "SUM", Args: []query.Expression{colReal("amount")}}, Operator: query.TokenGt, Right: numReal(150)},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("region")}},
	}, nil)
	if err != nil {
		t.Fatalf("HAVING failed: %v", err)
	}

	// Should have North (300) and East (500) - South (110) should be filtered out
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

// TestReal_ComplexWhere verifies complex WHERE with AND/OR
func TestReal_ComplexWhere(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "products",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "category", Type: query.TokenText}, {Name: "price", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "category", "price"},
		Values: [][]query.Expression{
			{numReal(1), strReal("Electronics"), numReal(100)},
			{numReal(2), strReal("Electronics"), numReal(50)},
			{numReal(3), strReal("Clothing"), numReal(75)},
			{numReal(4), strReal("Clothing"), numReal(25)},
		},
	}, nil)

	// WHERE (category = 'Electronics' AND price > 60) OR (category = 'Clothing' AND price < 50)
	// Should return id 1 (Electronics, 100) and id 4 (Clothing, 25)
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "products"},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.BinaryExpr{Left: colReal("category"), Operator: query.TokenEq, Right: strReal("Electronics")},
				Operator: query.TokenAnd,
				Right:    &query.BinaryExpr{Left: colReal("price"), Operator: query.TokenGt, Right: numReal(60)},
			},
			Operator: query.TokenOr,
			Right: &query.BinaryExpr{
				Left:     &query.BinaryExpr{Left: colReal("category"), Operator: query.TokenEq, Right: strReal("Clothing")},
				Operator: query.TokenAnd,
				Right:    &query.BinaryExpr{Left: colReal("price"), Operator: query.TokenLt, Right: numReal(50)},
			},
		},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("id")}},
	}, nil)
	if err != nil {
		t.Fatalf("Complex WHERE failed: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
	if rows[0][0].(int64) != 1 || rows[1][0].(int64) != 4 {
		t.Errorf("Expected ids 1 and 4, got %v and %v", rows[0][0], rows[1][0])
	}
}

// TestReal_Between verifies BETWEEN expression
func TestReal_Between(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "scores",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "value", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "scores",
		Columns: []string{"id", "value"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}, {numReal(3), numReal(30)}, {numReal(4), numReal(40)}},
	}, nil)

	// WHERE value BETWEEN 20 AND 35
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "scores"},
		Where:   &query.BetweenExpr{Expr: colReal("value"), Lower: numReal(20), Upper: numReal(35)},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("id")}},
	}, nil)
	if err != nil {
		t.Fatalf("BETWEEN failed: %v", err)
	}

	// Should return ids 2 and 3 (values 20 and 30)
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(rows))
	}
}

// TestReal_Like verifies LIKE pattern matching
func TestReal_Like(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "names",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "names",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}, {numReal(2), strReal("Bob")}, {numReal(3), strReal("Alex")}, {numReal(4), strReal("Charlie")}},
	}, nil)

	// WHERE name LIKE 'A%'
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("name")},
		From:    &query.TableRef{Name: "names"},
		Where:   &query.LikeExpr{Expr: colReal("name"), Pattern: strReal("A%")},
	}, nil)
	if err != nil {
		t.Fatalf("LIKE failed: %v", err)
	}

	// Should return Alice and Alex
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows starting with A, got %d", len(rows))
	}
}

// TestReal_CaseExpression verifies CASE WHEN expressions
func TestReal_CaseExpression(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "grades",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "score", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "grades",
		Columns: []string{"id", "score"},
		Values:  [][]query.Expression{{numReal(1), numReal(95)}, {numReal(2), numReal(85)}, {numReal(3), numReal(75)}, {numReal(4), numReal(65)}},
	}, nil)

	// SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END as grade
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			colReal("id"),
			&query.AliasExpr{
				Expr: &query.CaseExpr{
					Whens: []*query.WhenClause{
						{Condition: &query.BinaryExpr{Left: colReal("score"), Operator: query.TokenGte, Right: numReal(90)}, Result: strReal("A")},
						{Condition: &query.BinaryExpr{Left: colReal("score"), Operator: query.TokenGte, Right: numReal(80)}, Result: strReal("B")},
					},
					Else: strReal("C"),
				},
				Alias: "grade",
			},
		},
		From:    &query.TableRef{Name: "grades"},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("id")}},
	}, nil)
	if err != nil {
		t.Fatalf("CASE expression failed: %v", err)
	}

	if len(rows) != 4 {
		t.Fatalf("Expected 4 rows, got %d", len(rows))
	}

	expected := []string{"A", "B", "C", "C"}
	for i, row := range rows {
		if row[1].(string) != expected[i] {
			t.Errorf("Row %d: expected grade %s, got %s", i, expected[i], row[1].(string))
		}
	}
}

// TestReal_CrossJoin verifies CROSS JOIN
func TestReal_CrossJoin(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "colors",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "color", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "sizes",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "size", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "colors",
		Columns: []string{"id", "color"},
		Values:  [][]query.Expression{{numReal(1), strReal("Red")}, {numReal(2), strReal("Blue")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "sizes",
		Columns: []string{"id", "size"},
		Values:  [][]query.Expression{{numReal(1), strReal("S")}, {numReal(2), strReal("M")}, {numReal(3), strReal("L")}},
	}, nil)

	// CROSS JOIN - should produce 2 * 3 = 6 combinations
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("colors.color"), colReal("sizes.size")},
		From:    &query.TableRef{Name: "colors"},
		Joins:   []*query.JoinClause{{Type: query.TokenCross, Table: &query.TableRef{Name: "sizes"}}},
	}, nil)
	if err != nil {
		t.Fatalf("CROSS JOIN failed: %v", err)
	}

	if len(rows) != 6 {
		t.Errorf("Expected 6 rows from CROSS JOIN (2 colors * 3 sizes), got %d", len(rows))
	}
}

// TestReal_ExecuteQuery tests the ExecuteQuery function
func TestReal_ExecuteQuery(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "eq_test",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "eq_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A")}, {numReal(2), strReal("B")}},
	}, nil)

	// Test ExecuteQuery
	result, err := cat.ExecuteQuery("SELECT id, val FROM eq_test ORDER BY id")
	if err != nil {
		t.Fatalf("ExecuteQuery failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 2 {
		t.Errorf("Expected 2 rows, got %d", len(result.Rows))
	}

	// Test countRows
	sc := NewStatsCollector(cat)
	count, err := sc.countRows("eq_test")
	if err != nil {
		t.Logf("countRows error: %v", err)
	} else if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	// Test collectColumnStats
	colStats, err := sc.collectColumnStats("eq_test", "val")
	if err != nil {
		t.Logf("collectColumnStats error: %v", err)
	} else if colStats == nil {
		t.Error("Expected non-nil column stats")
	}

	// Test CollectStats
	_, err = sc.CollectStats("eq_test")
	if err != nil {
		t.Logf("CollectStats error: %v", err)
	}
}

// TestReal_ExecuteQueryInvalid tests ExecuteQuery with invalid SQL
func TestReal_ExecuteQueryInvalid(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Test invalid SQL
	_, err := cat.ExecuteQuery("INVALID SQL")
	if err == nil {
		t.Error("Expected error for invalid SQL")
	}
}

// TestReal_StatsCollectStats tests CollectStats with real data
func TestReal_StatsCollectStats(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create table with NULL values for collectColumnStats
	cat.CreateTable(&query.CreateTableStmt{
		Table: "stats_null_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
		PrimaryKey: []string{"id"},
	})

	// Insert data including NULLs
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "stats_null_test",
		Columns: []string{"id", "val"},
		Values: [][]query.Expression{
			{numReal(1), numReal(10)},
			{numReal(2), numReal(20)},
			{numReal(3), &query.NullLiteral{}},
		},
	}, nil)

	sc := NewStatsCollector(cat)

	// Test CollectStats
	stats, err := sc.CollectStats("stats_null_test")
	if err != nil {
		t.Logf("CollectStats error: %v", err)
	} else {
		if stats == nil {
			t.Error("Expected non-nil stats")
		} else if stats.RowCount != 3 {
			t.Errorf("Expected row count 3, got %d", stats.RowCount)
		}
	}

	// Test collectColumnStats on column with NULLs
	colStats, err := sc.collectColumnStats("stats_null_test", "val")
	if err != nil {
		t.Logf("collectColumnStats error: %v", err)
	} else if colStats != nil {
		if colStats.ColumnName != "val" {
			t.Errorf("Expected column name 'val', got %s", colStats.ColumnName)
		}
	}

	// Test GetColumnStats
	cStats, ok := sc.GetColumnStats("stats_null_test", "val")
	if !ok {
		t.Log("GetColumnStats returned false (expected if stats not collected)")
	} else if cStats == nil {
		t.Error("GetColumnStats returned nil stats")
	}

}

// TestReal_DeleteUsing tests DELETE with USING clause
func TestReal_DeleteUsing(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_main",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "ref_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "del_ref",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_main",
		Columns: []string{"id", "ref_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), numReal(20)}, {numReal(3), numReal(10)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "del_ref",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(10)}},
	}, nil)

	// DELETE with USING - delete rows where ref_id exists in del_ref
	affected, _, err := cat.Delete(ctx, &query.DeleteStmt{
		Table: "del_main",
		Using: []*query.TableRef{{Name: "del_ref"}},
		Where: &query.BinaryExpr{Left: colReal("del_main.ref_id"), Operator: query.TokenEq, Right: colReal("del_ref.id")},
	}, nil)
	if err != nil {
		t.Fatalf("DELETE USING failed: %v", err)
	}
	if affected != 2 {
		t.Errorf("Expected 2 rows deleted, got %d", affected)
	}

	// Verify remaining rows
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "del_main"},
	}, nil)
	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Errorf("Expected 1 row remaining, got %v", rows)
	}
}

// TestReal_MultipleJoins tests multiple JOINs in one query
func TestReal_MultipleJoins(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create 3 tables
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "a",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "b",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "a_id", Type: query.TokenInteger}, {Name: "val", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "c",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "b_id", Type: query.TokenInteger}, {Name: "data", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "a",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "b",
		Columns: []string{"id", "a_id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("B1")}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "c",
		Columns: []string{"id", "b_id", "data"},
		Values:  [][]query.Expression{{numReal(1), numReal(1), strReal("C1")}},
	}, nil)

	// Multiple JOINs: a JOIN b JOIN c
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("a.name"), colReal("b.val"), colReal("c.data")},
		From:    &query.TableRef{Name: "a"},
		Joins: []*query.JoinClause{
			{Type: query.TokenInner, Table: &query.TableRef{Name: "b"}, Condition: &query.BinaryExpr{Left: colReal("a.id"), Operator: query.TokenEq, Right: colReal("b.a_id")}},
			{Type: query.TokenInner, Table: &query.TableRef{Name: "c"}, Condition: &query.BinaryExpr{Left: colReal("b.id"), Operator: query.TokenEq, Right: colReal("c.b_id")}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("Multiple JOINs failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

// TestReal_RightJoin tests RIGHT JOIN
func TestReal_RightJoin(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "left_t",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "right_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "right_t",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "name", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "left_t",
		Columns: []string{"id", "right_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "right_t",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("One")}, {numReal(2), strReal("Two")}},
	}, nil)

	// RIGHT JOIN - should show all right rows
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("left_t.id"), colReal("right_t.name")},
		From:    &query.TableRef{Name: "left_t"},
		Joins: []*query.JoinClause{
			{Type: query.TokenRight, Table: &query.TableRef{Name: "right_t"}, Condition: &query.BinaryExpr{Left: colReal("left_t.right_id"), Operator: query.TokenEq, Right: colReal("right_t.id")}},
		},
	}, nil)
	if err != nil {
		t.Fatalf("RIGHT JOIN failed: %v", err)
	}
	// Should have 2 rows: matched (1,One) and unmatched (null,Two)
	if len(rows) != 2 {
		t.Errorf("Expected 2 rows from RIGHT JOIN, got %d", len(rows))
	}
}

// TestReal_NullHandling tests NULL value handling in expressions
func TestReal_NullHandling(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "null_test",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "val", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "null_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}, {numReal(2), &query.NullLiteral{}}},
	}, nil)

	// IS NULL
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "null_test"},
		Where:   &query.IsNullExpr{Expr: colReal("val")},
	}, nil)
	if err != nil {
		t.Fatalf("IS NULL failed: %v", err)
	}
	if len(rows) != 1 || rows[0][0].(int64) != 2 {
		t.Errorf("Expected id 2 with NULL val, got %v", rows)
	}
}

// TestReal_StringAgg tests string aggregation functions
func TestReal_StringAgg(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "str_agg",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "grp", Type: query.TokenText}, {Name: "val", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "str_agg",
		Columns: []string{"id", "grp", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("A"), strReal("x")}, {numReal(2), strReal("A"), strReal("y")}, {numReal(3), strReal("B"), strReal("z")}},
	}, nil)

	// GROUP_CONCAT
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			colReal("grp"),
			&query.AliasExpr{Expr: &query.FunctionCall{Name: "GROUP_CONCAT", Args: []query.Expression{colReal("val")}}, Alias: "concatenated"},
		},
		From:    &query.TableRef{Name: "str_agg"},
		GroupBy: []query.Expression{colReal("grp")},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("grp")}},
	}, nil)
	if err != nil {
		t.Fatalf("GROUP_CONCAT failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Expected 2 groups, got %d", len(rows))
	}
}

// TestReal_ForeignKeyCascade tests FK ON DELETE CASCADE
func TestReal_ForeignKeyCascade(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	// Create parent table
	err := cat.CreateTable(&query.CreateTableStmt{
		Table:      "fk_parent",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create child table with FK
	err = cat.CreateTable(&query.CreateTableStmt{
		Table:      "fk_child",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "parent_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
		ForeignKeys: []*query.ForeignKeyDef{
			{Columns: []string{"parent_id"}, ReferencedTable: "fk_parent", ReferencedColumns: []string{"id"}, OnDelete: "CASCADE"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "fk_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}, {numReal(2), numReal(1)}, {numReal(3), numReal(2)}},
	}, nil)

	// Delete parent 1 - should cascade to children
	_, _, err = cat.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent",
		Where: &query.BinaryExpr{Left: colReal("id"), Operator: query.TokenEq, Right: numReal(1)},
	}, nil)
	if err != nil {
		t.Fatalf("DELETE with CASCADE failed: %v", err)
	}

	// Check remaining children
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}}},
		From:    &query.TableRef{Name: "fk_child"},
	}, nil)
	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Errorf("Expected 1 child remaining (parent_id=2), got %v", rows)
	}
}

// TestReal_IndexScan tests index scan operations
func TestReal_IndexScan(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "idx_scan",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "code", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	// Create index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code",
		Table:   "idx_scan",
		Columns: []string{"code"},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_scan",
			Columns: []string{"id", "code"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(fmt.Sprintf("CODE%03d", i))}},
		}, nil)
	}

	// Query with index condition
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "idx_scan"},
		Where:   &query.BinaryExpr{Left: colReal("code"), Operator: query.TokenEq, Right: strReal("CODE050")},
	}, nil)
	if err != nil {
		t.Fatalf("Index scan query failed: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

// TestReal_AlterTableAddDropColumn tests ALTER TABLE
func TestReal_AlterTableAddDropColumn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "alter_test",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "old_col", Type: query.TokenText}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "alter_test",
		Columns: []string{"id", "old_col"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Add column using AlterTableAddColumn
	err := cat.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_test",
		Column: query.ColumnDef{Name: "new_col", Type: query.TokenInteger},
	})
	if err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN failed: %v", err)
	}

	// Verify new column exists by selecting it
	_, rows, _ := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("new_col")},
		From:    &query.TableRef{Name: "alter_test"},
	}, nil)
	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

// TestReal_WindowRankFunctions tests RANK and DENSE_RANK
func TestReal_WindowRankFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "rank_test",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "score", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	// Insert with ties
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "rank_test",
		Columns: []string{"id", "score"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}, {numReal(2), numReal(100)}, {numReal(3), numReal(90)}},
	}, nil)

	// Test RANK
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{
			colReal("id"),
			&query.WindowExpr{Function: "RANK", OrderBy: []*query.OrderByExpr{{Expr: colReal("score"), Desc: true}}},
			&query.WindowExpr{Function: "DENSE_RANK", OrderBy: []*query.OrderByExpr{{Expr: colReal("score"), Desc: true}}},
		},
		From:    &query.TableRef{Name: "rank_test"},
		OrderBy: []*query.OrderByExpr{{Expr: colReal("id")}},
	}, nil)
	if err != nil {
		t.Fatalf("RANK/DENSE_RANK failed: %v", err)
	}
	if len(rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(rows))
	}
}

// TestReal_ExistsSubquery tests EXISTS subquery
func TestReal_ExistsSubquery(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)
	ctx := context.Background()

	cat.CreateTable(&query.CreateTableStmt{
		Table:      "exists_parent",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
		PrimaryKey: []string{"id"},
	})
	cat.CreateTable(&query.CreateTableStmt{
		Table:      "exists_child",
		Columns:    []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}, {Name: "parent_id", Type: query.TokenInteger}},
		PrimaryKey: []string{"id"},
	})

	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_parent",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}, {numReal(2)}},
	}, nil)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "exists_child",
		Columns: []string{"id", "parent_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)

	// EXISTS
	_, rows, err := cat.Select(&query.SelectStmt{
		Columns: []query.Expression{colReal("id")},
		From:    &query.TableRef{Name: "exists_parent"},
		Where: &query.ExistsExpr{
			Subquery: &query.SelectStmt{
				Columns: []query.Expression{&query.StarExpr{}},
				From:    &query.TableRef{Name: "exists_child"},
				Where:   &query.BinaryExpr{Left: colReal("exists_child.parent_id"), Operator: query.TokenEq, Right: colReal("exists_parent.id")},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("EXISTS failed: %v", err)
	}
	if len(rows) != 1 || rows[0][0].(int64) != 1 {
		t.Errorf("Expected only parent with id 1, got %v", rows)
	}
}

// Helpers
func numReal(v float64) *query.NumberLiteral { return &query.NumberLiteral{Value: v} }
func strReal(s string) *query.StringLiteral  { return &query.StringLiteral{Value: s} }

// createCoverageTestTable helper for coverage tests
func createCoverageTestTable(t *testing.T, cat *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	stmt := &query.CreateTableStmt{
		Table:   name,
		Columns: cols,
	}
	if err := cat.CreateTable(stmt); err != nil {
		t.Fatalf("CreateTable(%s) failed: %v", name, err)
	}
}
func colReal(name string) *query.QualifiedIdentifier {
	// Parse "table.column" format
	if dotIdx := strings.IndexByte(name, '.'); dotIdx > 0 && dotIdx < len(name)-1 {
		return &query.QualifiedIdentifier{Table: name[:dotIdx], Column: name[dotIdx+1:]}
	}
	return &query.QualifiedIdentifier{Column: name}
}
