package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestNewCatalog(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)

	catalog := New(nil, pool)
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
	catalog := New(nil, pool)

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
	catalog := New(nil, pool)

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
	catalog := New(nil, pool)

	_, err := catalog.GetTable("nonexistent")
	if err == nil {
		t.Error("Expected error when getting non-existent table")
	}
}

func TestInsert(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

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
	catalog := New(nil, pool)

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
	catalog := New(nil, pool)

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
	catalog := New(nil, pool)

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
	catalog := New(nil, pool)

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
	catalog := New(nil, pool)

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
