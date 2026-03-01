package catalog

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestNewCatalog(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)

	catalog := New(nil, pool, nil)
	if catalog == nil {
		t.Fatal("Catalog is nil")
	}

	if len(catalog.tables) != 0 {
		t.Error("Expected empty tables map")
	}
}

func TestCreateTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	stmt := &query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText, NotNull: true},
		},
	}

	err := catalog.CreateTable(stmt)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	table, err := catalog.GetTable("users")
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}

	if table.Name != "users" {
		t.Errorf("Expected table name 'users', got %q", table.Name)
	}

	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}
}

func TestCreateDuplicateTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	stmt := &query.CreateTableStmt{Table: "test"}
	catalog.CreateTable(stmt)

	// Try to create again
	err := catalog.CreateTable(stmt)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

func TestGetNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	_, err := catalog.GetTable("nonexistent")
	if err == nil {
		t.Error("Expected error when getting non-existent table")
	}
}

func TestInsert(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table first
	createStmt := &query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	}
	catalog.CreateTable(createStmt)

	// Insert
	insertStmt := &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{
				&query.NumberLiteral{Value: 1},
				&query.StringLiteral{Value: "Alice"},
			},
		},
	}

	lastID, rowsAffected, err := catalog.Insert(insertStmt, nil)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}

	_ = lastID
}

func TestSelect(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	createStmt := &query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	}
	catalog.CreateTable(createStmt)

	// Insert data
	insertStmt := &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{
				&query.NumberLiteral{Value: 1},
				&query.StringLiteral{Value: "Item1"},
			},
		},
	}
	catalog.Insert(insertStmt, nil)

	// Select
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "name"},
		},
		From: &query.TableRef{Name: "items"},
	}

	columns, rows, err := catalog.Select(selectStmt, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(columns))
	}

	if len(rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(rows))
	}
}

func TestSelectStar(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	createStmt := &query.CreateTableStmt{
		Table: "data",
		Columns: []*query.ColumnDef{
			{Name: "a", Type: query.TokenInteger},
			{Name: "b", Type: query.TokenText},
		},
	}
	catalog.CreateTable(createStmt)

	// Select *
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.StarExpr{},
		},
		From: &query.TableRef{Name: "data"},
	}

	columns, _, err := catalog.Select(selectStmt, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(columns) != 2 {
		t.Errorf("Expected 2 columns with SELECT *, got %d", len(columns))
	}
}

func TestDropTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	createStmt := &query.CreateTableStmt{Table: "temp"}
	catalog.CreateTable(createStmt)

	// Drop table
	dropStmt := &query.DropTableStmt{Table: "temp"}
	err := catalog.DropTable(dropStmt)
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Verify it's gone
	_, err = catalog.GetTable("temp")
	if err == nil {
		t.Error("Expected error when getting dropped table")
	}
}

func TestListTables(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create multiple tables
	catalog.CreateTable(&query.CreateTableStmt{Table: "table1"})
	catalog.CreateTable(&query.CreateTableStmt{Table: "table2"})
	catalog.CreateTable(&query.CreateTableStmt{Table: "table3"})

	tables := catalog.ListTables()
	if len(tables) != 3 {
		t.Errorf("Expected 3 tables, got %d", len(tables))
	}
}

func TestCreateIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table first
	catalog.CreateTable(&query.CreateTableStmt{Table: "users"})

	// Create index
	indexStmt := &query.CreateIndexStmt{
		Index:  "idx_name",
		Table:  "users",
		Columns: []string{"name"},
	}

	err := catalog.CreateIndex(indexStmt)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	index, err := catalog.GetIndex("idx_name")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}

	if index.Name != "idx_name" {
		t.Errorf("Expected index name 'idx_name', got %q", index.Name)
	}
}

func TestAggregateFunctions(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
			{Name: "price", Type: query.TokenReal},
		},
	})

	// Insert test data
	for i := 1; i <= 5; i++ {
		catalog.Insert(&query.InsertStmt{
			Table:   "items",
			Columns: []string{"id", "name", "price"},
			Values: [][]query.Expression{
				{
					&query.NumberLiteral{Value: float64(i)},
					&query.StringLiteral{Value: fmt.Sprintf("Item%d", i)},
					&query.NumberLiteral{Value: float64(i * 10)},
				},
			},
		}, nil)
	}

	// Test COUNT(*)
	t.Run("CountStar", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			},
			From: &query.TableRef{Name: "items"},
		}
		columns, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("COUNT(*) failed: %v", err)
		}
		if len(rows) != 1 || len(rows[0]) != 1 {
			t.Errorf("Expected 1 row with 1 column, got %d rows with %d columns", len(rows), len(rows[0]))
		}
		if columns[0] != "COUNT(*)" {
			t.Errorf("Expected column name 'COUNT(*)', got %q", columns[0])
		}
		count := rows[0][0].(int64)
		if count != 5 {
			t.Errorf("Expected COUNT(*) = 5, got %d", count)
		}
	})

	// Test COUNT(column)
	t.Run("CountColumn", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.Identifier{Name: "price"}}},
			},
			From: &query.TableRef{Name: "items"},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("COUNT(price) failed: %v", err)
		}
		count := rows[0][0].(int64)
		if count != 5 {
			t.Errorf("Expected COUNT(price) = 5, got %d", count)
		}
	})

	// Test SUM
	t.Run("Sum", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "price"}}},
			},
			From: &query.TableRef{Name: "items"},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("SUM(price) failed: %v", err)
		}
		sum := rows[0][0].(float64)
		if sum != 150 { // 10+20+30+40+50
			t.Errorf("Expected SUM(price) = 150, got %f", sum)
		}
	})

	// Test AVG
	t.Run("Avg", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.Identifier{Name: "price"}}},
			},
			From: &query.TableRef{Name: "items"},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("AVG(price) failed: %v", err)
		}
		avg := rows[0][0].(float64)
		if avg != 30 { // (10+20+30+40+50)/5 = 30
			t.Errorf("Expected AVG(price) = 30, got %f", avg)
		}
	})

	// Test MIN
	t.Run("Min", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{Name: "MIN", Args: []query.Expression{&query.Identifier{Name: "price"}}},
			},
			From: &query.TableRef{Name: "items"},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("MIN(price) failed: %v", err)
		}
		min := rows[0][0].(float64)
		if min != 10 {
			t.Errorf("Expected MIN(price) = 10, got %f", min)
		}
	})

	// Test MAX
	t.Run("Max", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{Name: "MAX", Args: []query.Expression{&query.Identifier{Name: "price"}}},
			},
			From: &query.TableRef{Name: "items"},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("MAX(price) failed: %v", err)
		}
		max := rows[0][0].(float64)
		if max != 50 {
			t.Errorf("Expected MAX(price) = 50, got %f", max)
		}
	})

	// Test aggregate with WHERE clause
	t.Run("AggregateWithWhere", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "price"}}},
			},
			From: &query.TableRef{Name: "items"},
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 2},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("SUM with WHERE failed: %v", err)
		}
		sum := rows[0][0].(float64)
		if sum != 120 { // 30+40+50 = 120 (id > 2 means id = 3,4,5)
			t.Errorf("Expected SUM(price) = 120 for id > 2, got %f", sum)
		}
	})
}

func TestLikeOperator(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	catalog.Insert(&query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Amanda"}},
			{&query.NumberLiteral{Value: 4}, &query.StringLiteral{Value: "Charlie"}},
		},
	}, nil)

	// Test LIKE with %
	t.Run("LikePercent", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "users"},
			Where: &query.LikeExpr{
				Expr:    &query.Identifier{Name: "name"},
				Pattern: &query.StringLiteral{Value: "A%"},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("LIKE failed: %v", err)
		}
		// A% matches Alice and Amanda (both start with A)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	// Test LIKE with _
	t.Run("LikeUnderscore", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "users"},
			Where: &query.LikeExpr{
				Expr:    &query.Identifier{Name: "name"},
				Pattern: &query.StringLiteral{Value: "_lice"},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("LIKE failed: %v", err)
		}
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}

func TestInOperator(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert data
	catalog.Insert(&query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "One"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Two"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Three"}},
		},
	}, nil)

	// Test IN
	t.Run("In", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "items"},
			Where: &query.InExpr{
				Expr: &query.Identifier{Name: "id"},
				List: []query.Expression{
					&query.NumberLiteral{Value: 1},
					&query.NumberLiteral{Value: 3},
				},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("IN failed: %v", err)
		}
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	// Test NOT IN
	t.Run("NotIn", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "items"},
			Where: &query.InExpr{
				Expr: &query.Identifier{Name: "id"},
				List: []query.Expression{
					&query.NumberLiteral{Value: 1},
					&query.NumberLiteral{Value: 3},
				},
				Not: true,
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("NOT IN failed: %v", err)
		}
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}

func TestBetweenOperator(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "price", Type: query.TokenReal},
		},
	})

	// Insert data
	catalog.Insert(&query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "price"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 10}},
			{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 25}},
			{&query.NumberLiteral{Value: 3}, &query.NumberLiteral{Value: 50}},
			{&query.NumberLiteral{Value: 4}, &query.NumberLiteral{Value: 100}},
		},
	}, nil)

	// Test BETWEEN
	t.Run("Between", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "price"}},
			From:    &query.TableRef{Name: "items"},
			Where: &query.BetweenExpr{
				Expr:  &query.Identifier{Name: "price"},
				Lower: &query.NumberLiteral{Value: 20},
				Upper: &query.NumberLiteral{Value: 60},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("BETWEEN failed: %v", err)
		}
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	// Test NOT BETWEEN
	t.Run("NotBetween", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "price"}},
			From:    &query.TableRef{Name: "items"},
			Where: &query.BetweenExpr{
				Expr:  &query.Identifier{Name: "price"},
				Lower: &query.NumberLiteral{Value: 20},
				Upper: &query.NumberLiteral{Value: 60},
				Not:   true,
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("NOT BETWEEN failed: %v", err)
		}
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}

func TestOrderBy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
			{Name: "price", Type: query.TokenReal},
		},
	})

	// Insert data (unsorted)
	catalog.Insert(&query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "name", "price"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "Charlie"}, &query.NumberLiteral{Value: 30}},
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}, &query.NumberLiteral{Value: 10}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}, &query.NumberLiteral{Value: 20}},
		},
	}, nil)

	// Test ORDER BY ASC
	t.Run("OrderByAsc", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "items"},
			OrderBy: []*query.OrderByExpr{
				{Expr: &query.Identifier{Name: "name"}, Desc: false},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("ORDER BY failed: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("Expected 3 rows, got %d", len(rows))
		}
		if rows[0][0] != "Alice" || rows[1][0] != "Bob" || rows[2][0] != "Charlie" {
			t.Errorf("Expected Alice, Bob, Charlie but got %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
		}
	})

	// Test ORDER BY DESC
	t.Run("OrderByDesc", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "name"}},
			From:    &query.TableRef{Name: "items"},
			OrderBy: []*query.OrderByExpr{
				{Expr: &query.Identifier{Name: "name"}, Desc: true},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("ORDER BY DESC failed: %v", err)
		}
		if rows[0][0] != "Charlie" || rows[1][0] != "Bob" || rows[2][0] != "Alice" {
			t.Errorf("Expected Charlie, Bob, Alice but got %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
		}
	})
}

func TestLimitOffset(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	})

	// Insert 10 rows
	for i := 1; i <= 10; i++ {
		catalog.Insert(&query.InsertStmt{
			Table:   "items",
			Columns: []string{"id"},
			Values: [][]query.Expression{
				{&query.NumberLiteral{Value: float64(i)}},
			},
		}, nil)
	}

	// Test LIMIT
	t.Run("Limit", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "id"}},
			From:    &query.TableRef{Name: "items"},
			Limit:   &query.NumberLiteral{Value: 5},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("LIMIT failed: %v", err)
		}
		if len(rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(rows))
		}
	})

	// Test OFFSET
	t.Run("Offset", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{&query.Identifier{Name: "id"}},
			From:    &query.TableRef{Name: "items"},
			Limit:   &query.NumberLiteral{Value: 5},
			Offset:  &query.NumberLiteral{Value: 3},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("OFFSET failed: %v", err)
		}
		// Should get rows 4,5 (after skipping 3 and limiting to 5)
		if len(rows) != 5 {
			t.Errorf("Expected 5 rows, got %d", len(rows))
		}
	})
}

func TestDistinct(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "category", Type: query.TokenText},
		},
	})

	// Insert data with duplicates
	catalog.Insert(&query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "category"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "A"}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "B"}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "A"}},
			{&query.NumberLiteral{Value: 4}, &query.StringLiteral{Value: "C"}},
			{&query.NumberLiteral{Value: 5}, &query.StringLiteral{Value: "B"}},
		},
	}, nil)

	// Test DISTINCT
	t.Run("Distinct", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns:  []query.Expression{&query.Identifier{Name: "category"}},
			From:     &query.TableRef{Name: "items"},
			Distinct: true,
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("DISTINCT failed: %v", err)
		}
		if len(rows) != 3 {
			t.Errorf("Expected 3 distinct rows, got %d", len(rows))
		}
	})
}

func TestGroupBy(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table with category for grouping
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenReal},
		},
	})

	// Insert test data
	catalog.Insert(&query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "category", "amount"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "A"}, &query.NumberLiteral{Value: 100}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "B"}, &query.NumberLiteral{Value: 200}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "A"}, &query.NumberLiteral{Value: 150}},
			{&query.NumberLiteral{Value: 4}, &query.StringLiteral{Value: "B"}, &query.NumberLiteral{Value: 250}},
			{&query.NumberLiteral{Value: 5}, &query.StringLiteral{Value: "C"}, &query.NumberLiteral{Value: 50}},
		},
	}, nil)

	// Test GROUP BY category with COUNT
	t.Run("GroupByCount", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "category"},
				&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			},
			From:    &query.TableRef{Name: "sales"},
			GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		}
		columns, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("GROUP BY COUNT failed: %v", err)
		}
		if len(rows) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(rows))
		}
		// Categories should be A, B, C
		found := make(map[string]int64)
		for _, row := range rows {
			found[row[0].(string)] = row[1].(int64)
		}
		if found["A"] != 2 || found["B"] != 2 || found["C"] != 1 {
			t.Errorf("Expected counts A=2, B=2, C=1, got %v", found)
		}
		_ = columns
	})

	// Test GROUP BY category with SUM
	t.Run("GroupBySum", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "category"},
				&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
			From:    &query.TableRef{Name: "sales"},
			GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("GROUP BY SUM failed: %v", err)
		}
		if len(rows) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(rows))
		}
		found := make(map[string]float64)
		for _, row := range rows {
			found[row[0].(string)] = row[1].(float64)
		}
		if found["A"] != 250 || found["B"] != 450 || found["C"] != 50 {
			t.Errorf("Expected sums A=250, B=450, C=50, got %v", found)
		}
	})

	// Test GROUP BY category with AVG
	t.Run("GroupByAvg", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "category"},
				&query.FunctionCall{Name: "AVG", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
			From:    &query.TableRef{Name: "sales"},
			GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("GROUP BY AVG failed: %v", err)
		}
		if len(rows) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(rows))
		}
		found := make(map[string]float64)
		for _, row := range rows {
			found[row[0].(string)] = row[1].(float64)
		}
		// A: (100+150)/2 = 125, B: (200+250)/2 = 225, C: 50
		if found["A"] != 125 || found["B"] != 225 || found["C"] != 50 {
			t.Errorf("Expected avgs A=125, B=225, C=50, got %v", found)
		}
	})

	// Test GROUP BY with ORDER BY
	t.Run("GroupByWithOrderBy", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "category"},
				&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
			From:    &query.TableRef{Name: "sales"},
			GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
			OrderBy: []*query.OrderByExpr{
				{Expr: &query.Identifier{Name: "category"}, Desc: true},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("GROUP BY with ORDER BY failed: %v", err)
		}
		if len(rows) != 3 {
			t.Errorf("Expected 3 groups, got %d", len(rows))
		}
		// Should be ordered C, B, A (descending)
		if rows[0][0].(string) != "C" || rows[1][0].(string) != "B" || rows[2][0].(string) != "A" {
			t.Errorf("Expected order C, B, A, got %v, %v, %v", rows[0][0], rows[1][0], rows[2][0])
		}
	})

	// Test GROUP BY with LIMIT
	t.Run("GroupByWithLimit", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "category"},
				&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
			From:    &query.TableRef{Name: "sales"},
			GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
			Limit:   &query.NumberLiteral{Value: 2},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("GROUP BY with LIMIT failed: %v", err)
		}
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows with LIMIT, got %d", len(rows))
		}
	})
}

func TestHaving(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool, nil)

	// Create table with category for grouping
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "sales",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "category", Type: query.TokenText},
			{Name: "amount", Type: query.TokenReal},
		},
	})

	// Insert test data
	catalog.Insert(&query.InsertStmt{
		Table:   "sales",
		Columns: []string{"id", "category", "amount"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "A"}, &query.NumberLiteral{Value: 100}},
			{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "B"}, &query.NumberLiteral{Value: 200}},
			{&query.NumberLiteral{Value: 3}, &query.StringLiteral{Value: "A"}, &query.NumberLiteral{Value: 150}},
			{&query.NumberLiteral{Value: 4}, &query.StringLiteral{Value: "B"}, &query.NumberLiteral{Value: 250}},
			{&query.NumberLiteral{Value: 5}, &query.StringLiteral{Value: "C"}, &query.NumberLiteral{Value: 50}},
		},
	}, nil)

	// Test HAVING with COUNT
	t.Run("HavingCount", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "category"},
				&query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
			},
			From:    &query.TableRef{Name: "sales"},
			GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
			Having: &query.BinaryExpr{
				Left:     &query.FunctionCall{Name: "COUNT", Args: []query.Expression{&query.StarExpr{}}},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 1},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("HAVING COUNT failed: %v", err)
		}
		// Should return only groups with COUNT > 1 (A and B, not C)
		if len(rows) != 2 {
			t.Errorf("Expected 2 groups with COUNT > 1, got %d", len(rows))
		}
	})

	// Test HAVING with SUM
	t.Run("HavingSum", func(t *testing.T) {
		stmt := &query.SelectStmt{
			Columns: []query.Expression{
				&query.Identifier{Name: "category"},
				&query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
			},
			From:    &query.TableRef{Name: "sales"},
			GroupBy: []query.Expression{&query.Identifier{Name: "category"}},
			Having: &query.BinaryExpr{
				Left:     &query.FunctionCall{Name: "SUM", Args: []query.Expression{&query.Identifier{Name: "amount"}}},
				Operator: query.TokenGt,
				Right:    &query.NumberLiteral{Value: 300},
			},
		}
		_, rows, err := catalog.Select(stmt, nil)
		if err != nil {
			t.Fatalf("HAVING SUM failed: %v", err)
		}
		// Should return only groups with SUM > 300 (B with 450, not A with 250 or C with 50)
		if len(rows) != 1 {
			t.Errorf("Expected 1 group with SUM > 300, got %d", len(rows))
		}
		if len(rows) > 0 && rows[0][0].(string) != "B" {
			t.Errorf("Expected category B, got %v", rows[0][0])
		}
	})
}
