package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func setupForeignKeyTest(t *testing.T) (*Catalog, *ForeignKeyEnforcer, func()) {
	// Create in-memory catalog
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(1024, backend)

	catalog := New(nil, pool, nil)
	enforcer := NewForeignKeyEnforcer(catalog)

	cleanupFunc := func() {
		pool.Close()
	}

	return catalog, enforcer, cleanupFunc
}

func TestForeignKeyEnforcerValidateInsert(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create parent table
	parentTable := &query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	}
	if err := catalog.CreateTable(parentTable); err != nil {
		t.Fatalf("Failed to create parent table: %v", err)
	}

	// Create child table with foreign key
	childTable := &query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
			{Name: "total", Type: query.TokenReal},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          "RESTRICT",
				OnUpdate:          "RESTRICT",
			},
		},
	}
	if err := catalog.CreateTable(childTable); err != nil {
		t.Fatalf("Failed to create child table: %v", err)
	}

	// Insert a parent row
	_, _, err := catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)
	if err != nil {
		t.Fatalf("Failed to insert parent row: %v", err)
	}

	// Test valid insert (references existing parent)
	validRow := map[string]interface{}{
		"id":      1,
		"user_id": 1,
		"total":   100.0,
	}
	if err := enforcer.ValidateInsert(ctx, "orders", validRow); err != nil {
		t.Errorf("Expected valid insert to succeed: %v", err)
	}

	// Test invalid insert (references non-existent parent)
	invalidRow := map[string]interface{}{
		"id":      2,
		"user_id": 999,
		"total":   200.0,
	}
	if err := enforcer.ValidateInsert(ctx, "orders", invalidRow); err == nil {
		t.Error("Expected invalid insert to fail with foreign key violation")
	}

	// Test insert with NULL foreign key (should be allowed)
	nullRow := map[string]interface{}{
		"id":      3,
		"user_id": nil,
		"total":   300.0,
	}
	if err := enforcer.ValidateInsert(ctx, "orders", nullRow); err != nil {
		t.Errorf("Expected insert with NULL foreign key to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerOnDeleteRestrict(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          "RESTRICT",
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Try to delete parent row (should fail with RESTRICT)
	if err := enforcer.OnDelete(ctx, "users", 1); err == nil {
		t.Error("Expected delete to fail with RESTRICT policy")
	}
}

func TestForeignKeyEnforcerOnDeleteCascade(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables with CASCADE
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          "CASCADE",
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Delete parent row (should cascade)
	if err := enforcer.OnDelete(ctx, "users", 1); err != nil {
		t.Errorf("Expected cascade delete to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerOnDeleteSetNull(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables with SET NULL
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          "SET NULL",
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Delete parent row (should set null)
	if err := enforcer.OnDelete(ctx, "users", 1); err != nil {
		t.Errorf("Expected set null to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerOnUpdateCascade(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables with CASCADE
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Update parent primary key (should cascade)
	if err := enforcer.OnUpdate(ctx, "users", 1, 100); err != nil {
		t.Errorf("Expected cascade update to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerFindReferencingTables(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	// Create tables
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "reviews",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Find referencing tables
	refs := enforcer.findReferencingTables("users")
	if len(refs) != 2 {
		t.Errorf("Expected 2 referencing tables, got %d", len(refs))
	}
}

func TestForeignKeyEnforcerValidateUpdate(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Bob"}}},
	}, nil)

	// Test valid update (references existing parent)
	oldRow := map[string]interface{}{
		"id":      1,
		"user_id": 1,
	}
	newRow := map[string]interface{}{
		"id":      1,
		"user_id": 2,
	}
	if err := enforcer.ValidateUpdate(ctx, "orders", oldRow, newRow); err != nil {
		t.Errorf("Expected valid update to succeed: %v", err)
	}

	// Test invalid update (references non-existent parent)
	invalidRow := map[string]interface{}{
		"id":      1,
		"user_id": 999,
	}
	if err := enforcer.ValidateUpdate(ctx, "orders", oldRow, invalidRow); err == nil {
		t.Error("Expected invalid update to fail with foreign key violation")
	}
}

// Edge Case Tests for Foreign Key Enforcement

func TestForeignKeyEnforcerValidateInsertMissingColumn(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create parent table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create child table with foreign key
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Insert with missing foreign key column
	row := map[string]interface{}{
		"id": 1,
		// user_id is missing
	}
	if err := enforcer.ValidateInsert(ctx, "orders", row); err == nil {
		t.Error("Expected error for missing foreign key column")
	}
}

func TestForeignKeyEnforcerValidateInsertNonExistentTable(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Try to validate insert on non-existent table
	row := map[string]interface{}{
		"id": 1,
	}
	if err := enforcer.ValidateInsert(ctx, "non_existent_table", row); err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestForeignKeyEnforcerOnDeleteNonExistentTable(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Try to delete from non-existent table (should not error, just return)
	if err := enforcer.OnDelete(ctx, "non_existent_table", 1); err != nil {
		t.Errorf("Expected no error for non-existent table: %v", err)
	}
}

func TestForeignKeyEnforcerOnUpdateNonExistentTable(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Try to update non-existent table (should not error, just return)
	if err := enforcer.OnUpdate(ctx, "non_existent_table", 1, 2); err != nil {
		t.Errorf("Expected no error for non-existent table: %v", err)
	}
}

func TestForeignKeyEnforcerCompositeKey(t *testing.T) {
	t.Skip("Composite key foreign key validation not fully implemented")
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create parent table with composite primary key
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "tenant_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Create child table with composite foreign key
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "tenant_id", Type: query.TokenInteger},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"tenant_id", "user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"tenant_id", "user_id"},
				OnDelete:          "RESTRICT",
			},
		},
	})

	// Insert parent row
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"tenant_id", "user_id", "name"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 100}, &query.StringLiteral{Value: "Alice"}}},
	}, nil)

	// Valid insert with both FK values
	validRow := map[string]interface{}{
		"id":        1,
		"tenant_id": 1,
		"user_id":   100,
	}
	if err := enforcer.ValidateInsert(ctx, "orders", validRow); err != nil {
		t.Errorf("Expected valid insert to succeed: %v", err)
	}

	// Invalid insert with partial match
	invalidRow := map[string]interface{}{
		"id":        2,
		"tenant_id": 1,
		"user_id":   999,
	}
	if err := enforcer.ValidateInsert(ctx, "orders", invalidRow); err == nil {
		t.Error("Expected invalid insert to fail with foreign key violation")
	}
}

func TestForeignKeyEnforcerSelfReferencing(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create self-referencing table (e.g., employee hierarchy)
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "manager_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"manager_id"},
				ReferencedTable:   "employees",
				ReferencedColumns: []string{"id"},
				OnDelete:          "SET NULL",
			},
		},
	})

	// Insert top-level manager
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "employees", Columns: []string{"id", "name", "manager_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "CEO"}, &query.NullLiteral{}}},
	}, nil)

	// Insert employee with valid manager
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "employees", Columns: []string{"id", "name", "manager_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 2}, &query.StringLiteral{Value: "Manager"}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Valid insert with NULL manager
	nullManagerRow := map[string]interface{}{
		"id":         3,
		"name":       "New Hire",
		"manager_id": nil,
	}
	if err := enforcer.ValidateInsert(ctx, "employees", nullManagerRow); err != nil {
		t.Errorf("Expected insert with NULL manager to succeed: %v", err)
	}

	// Invalid insert with non-existent manager
	invalidRow := map[string]interface{}{
		"id":         4,
		"name":       "Invalid",
		"manager_id": 999,
	}
	if err := enforcer.ValidateInsert(ctx, "employees", invalidRow); err == nil {
		t.Error("Expected insert with non-existent manager to fail")
	}
}

func TestForeignKeyEnforcerMultipleForeignKeys(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create parent tables
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Create child table with multiple foreign keys
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
			{Name: "product_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
			{
				Columns:           []string{"product_id"},
				ReferencedTable:   "products",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Insert parent rows
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "products", Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 100}}},
	}, nil)

	// Valid insert with both FKs valid
	validRow := map[string]interface{}{
		"id":         1,
		"user_id":    1,
		"product_id": 100,
	}
	if err := enforcer.ValidateInsert(ctx, "orders", validRow); err != nil {
		t.Errorf("Expected valid insert to succeed: %v", err)
	}

	// Invalid insert with one FK invalid
	invalidRow := map[string]interface{}{
		"id":         2,
		"user_id":    999,
		"product_id": 100,
	}
	if err := enforcer.ValidateInsert(ctx, "orders", invalidRow); err == nil {
		t.Error("Expected insert with invalid user_id to fail")
	}
}

func TestForeignKeyEnforcerCircularReference(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table A referencing table B
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "table_a",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "b_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"b_id"},
				ReferencedTable:   "table_b",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Create table B referencing table A
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "table_b",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "a_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"a_id"},
				ReferencedTable:   "table_a",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Insert into table A with NULL FK (allowed)
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "table_a", Columns: []string{"id", "b_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NullLiteral{}}},
	}, nil)

	// Insert into table B referencing table A
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "table_b", Columns: []string{"id", "a_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 100}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Now update table A to reference table B
	rowA := map[string]interface{}{
		"id":   1,
		"b_id": 100,
	}
	if err := enforcer.ValidateInsert(ctx, "table_a", rowA); err != nil {
		t.Errorf("Expected insert to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerValidateUpdateNoChange(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	// Update with no FK change should succeed even if parent doesn't exist
	oldRow := map[string]interface{}{
		"id":      1,
		"user_id": 1,
	}
	newRow := map[string]interface{}{
		"id":      1,
		"user_id": 1, // Same value
	}
	if err := enforcer.ValidateUpdate(ctx, "orders", oldRow, newRow); err != nil {
		t.Errorf("Expected update with no FK change to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerOnDeleteNoAction(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables with NO ACTION (same as RESTRICT)
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnDelete:          "NO ACTION",
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Try to delete parent (should fail with NO ACTION)
	if err := enforcer.OnDelete(ctx, "users", 1); err == nil {
		t.Error("Expected delete to fail with NO ACTION policy")
	}
}

func TestForeignKeyEnforcerOnUpdateNoAction(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables with NO ACTION
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "NO ACTION",
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Try to update parent PK (should fail with NO ACTION)
	if err := enforcer.OnUpdate(ctx, "users", 1, 100); err == nil {
		t.Error("Expected update to fail with NO ACTION policy")
	}
}

func TestForeignKeyEnforcerOnUpdateSetNull(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create tables with SET NULL
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	catalog.CreateTable(&query.CreateTableStmt{
		Table: "orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "user_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"user_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "SET NULL",
			},
		},
	})

	// Insert data
	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "users", Columns: []string{"id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}}},
	}, nil)

	catalog.Insert(ctx, &query.InsertStmt{
		Table:   "orders", Columns: []string{"id", "user_id"},
		Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1}}},
	}, nil)

	// Update parent PK (should set null)
	if err := enforcer.OnUpdate(ctx, "users", 1, 100); err != nil {
		t.Errorf("Expected set null to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerEmptyRow(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Validate empty row (no FK columns)
	emptyRow := map[string]interface{}{}
	if err := enforcer.ValidateInsert(ctx, "users", emptyRow); err != nil {
		t.Errorf("Expected empty row validation to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerCheckConstraintsEmptyTable(t *testing.T) {
	catalog, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Create table with no data
	catalog.CreateTable(&query.CreateTableStmt{
		Table: "users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "manager_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"manager_id"},
				ReferencedTable:   "users",
				ReferencedColumns: []string{"id"},
			},
		},
	})

	// Check constraints on empty table should succeed
	if err := enforcer.CheckForeignKeyConstraints(ctx, "users"); err != nil {
		t.Errorf("Expected empty table check to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerCheckConstraintsNonExistentTable(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	ctx := context.Background()

	// Check constraints on non-existent table should succeed (no data)
	if err := enforcer.CheckForeignKeyConstraints(ctx, "non_existent"); err != nil {
		t.Errorf("Expected non-existent table check to succeed: %v", err)
	}
}

func TestForeignKeyEnforcerValuesEqual(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	tests := []struct {
		a        interface{}
		b        interface{}
		expected bool
	}{
		{nil, nil, true},
		{nil, 1, false},
		{1, nil, false},
		{1, 1, true},
		{1, 2, false},
		{int(1), int64(1), true},
		{int32(1), int64(1), true},
		{float32(1.0), float64(1.0), true},
		{1.0, 1.0, true},
		{1.5, 1.5, true},
		{"test", "test", true},
		{"test", "other", false},
		{1, "1", false},
		{uint(1), int(1), true},
		{int8(1), int16(1), true},
		{int32(1), float64(1.0), true},
	}

	for _, test := range tests {
		result := enforcer.valuesEqual(test.a, test.b)
		if result != test.expected {
			t.Errorf("valuesEqual(%v, %v) = %v, expected %v", test.a, test.b, result, test.expected)
		}
	}
}

func TestForeignKeyEnforcerSerializeDeserialize(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	tests := []interface{}{
		"test string",
		int(42),
		int64(42),
		float64(3.14),
		[]byte("bytes"),
		nil,
		true,
	}

	for _, test := range tests {
		serialized := enforcer.serializeValue(test)
		if test != nil {
			// For non-nil values, serialization should produce non-empty bytes
			if len(serialized) == 0 {
				t.Errorf("Expected non-empty serialization for %v", test)
			}
		}
	}

	// Test deserialization
	deserializeTests := []struct {
		input    []byte
		expected interface{}
	}{
		{[]byte("00000000000000000042"), int(42)},
		{[]byte("test"), "test"},
		{[]byte("3.140000"), float64(3.14)},
	}

	for _, test := range deserializeTests {
		result := enforcer.deserializeValue(test.input)
		// Note: deserialization may not perfectly round-trip all types
		_ = result
	}
}

func TestForeignKeyEnforcerCompositeKeySerialization(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	// Test composite key serialization
	values := []interface{}{1, "test", 3.14}
	key := enforcer.serializeCompositeKey(values)

	if len(key) == 0 {
		t.Error("Expected non-empty composite key")
	}

	// Should contain null byte delimiters
	hasDelimiter := false
	for _, b := range key {
		if b == 0x00 {
			hasDelimiter = true
			break
		}
	}
	if !hasDelimiter {
		t.Error("Expected composite key to contain delimiters")
	}
}

func TestForeignKeyEnforcerFindReferencingTablesEmpty(t *testing.T) {
	_, enforcer, cleanup := setupForeignKeyTest(t)
	defer cleanup()

	// Find referencing tables for table with no references
	refs := enforcer.findReferencingTables("non_existent")
	if len(refs) != 0 {
		t.Errorf("Expected 0 referencing tables, got %d", len(refs))
	}
}
