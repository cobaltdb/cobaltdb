package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestUpdate(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	// Create table
	createStmt := &query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	}
	catalog.CreateTable(createStmt)

	// Insert data
	insertStmt := &query.InsertStmt{
		Table:   "users",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}},
		},
	}
	catalog.Insert(insertStmt, nil)

	// Update data
	updateStmt := &query.UpdateStmt{
		Table: "users",
		Set: []*query.SetClause{
			{
				Column: "name",
				Value:  &query.StringLiteral{Value: "Bob"},
			},
		},
	}

	_, rowsAffected, err := catalog.Update(updateStmt, nil)
	if err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	if rowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", rowsAffected)
	}
}

func TestDelete(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	// Create table
	createStmt := &query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	}
	catalog.CreateTable(createStmt)

	// Insert data
	insertStmt := &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}},
			{&query.NumberLiteral{Value: 2}},
		},
	}
	catalog.Insert(insertStmt, nil)

	// Delete all
	deleteStmt := &query.DeleteStmt{Table: "items"}

	_, rowsAffected, err := catalog.Delete(deleteStmt, nil)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	if rowsAffected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", rowsAffected)
	}

	// Verify deletion
	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "items"},
	}

	_, rows, err := catalog.Select(selectStmt, nil)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if len(rows) != 0 {
		t.Errorf("Expected 0 rows after delete, got %d", len(rows))
	}
}

func TestDropIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	// Create table
	createStmt := &query.CreateTableStmt{
		Table:   "users",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger}},
	}
	catalog.CreateTable(createStmt)

	// Create index
	createIdxStmt := &query.CreateIndexStmt{
		Index:   "idx_id",
		Table:   "users",
		Columns: []string{"id"},
	}
	catalog.CreateIndex(createIdxStmt)

	// Drop index
	err := catalog.DropIndex("idx_id")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// Verify index is dropped
	_, err = catalog.GetIndex("idx_id")
	if err == nil {
		t.Error("Expected error when getting dropped index")
	}
}

func TestDropNonExistentIndex(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	err := catalog.DropIndex("nonexistent")
	if err == nil {
		t.Error("Expected error when dropping non-existent index")
	}
}

func TestUpdateNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	updateStmt := &query.UpdateStmt{Table: "nonexistent"}
	_, _, err := catalog.Update(updateStmt, nil)
	if err == nil {
		t.Error("Expected error when updating non-existent table")
	}
}

func TestDeleteNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	deleteStmt := &query.DeleteStmt{Table: "nonexistent"}
	_, _, err := catalog.Delete(deleteStmt, nil)
	if err == nil {
		t.Error("Expected error when deleting from non-existent table")
	}
}

func TestSelectWithoutFrom(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    nil,
	}

	_, _, err := catalog.Select(selectStmt, nil)
	if err == nil {
		t.Error("Expected error when selecting without FROM clause")
	}
}

func TestSelectNonExistentTable(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)
	catalog := New(nil, pool)

	selectStmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "nonexistent"},
	}

	_, _, err := catalog.Select(selectStmt, nil)
	if err == nil {
		t.Error("Expected error when selecting from non-existent table")
	}
}

func TestEvalExpressionTypes(t *testing.T) {
	// Test all expression types
	tests := []struct {
		name string
		expr query.Expression
	}{
		{"string", &query.StringLiteral{Value: "test"}},
		{"number", &query.NumberLiteral{Value: 42}},
		{"boolean", &query.BooleanLiteral{Value: true}},
		{"null", &query.NullLiteral{}},
		{"placeholder", &query.PlaceholderExpr{Index: 0}},
		{"identifier", &query.Identifier{Name: "col"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var args []interface{}
			if tt.name == "placeholder" {
				args = []interface{}{"value"}
			}

			_, err := evalExpression(tt.expr, args)
			if err != nil {
				t.Errorf("Failed to eval %s: %v", tt.name, err)
			}
		})
	}
}

func TestEvalPlaceholderOutOfRange(t *testing.T) {
	expr := &query.PlaceholderExpr{Index: 10}
	_, err := evalExpression(expr, []interface{}{"one"})
	if err == nil {
		t.Error("Expected error for placeholder index out of range")
	}
}
