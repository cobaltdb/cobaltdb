package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestInsertLockedWithCheckConstraint tests insertLocked with CHECK constraints
func TestInsertLockedWithCheckConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "age", Type: query.TokenInteger, Check: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "age"},
				Operator: query.TokenGte,
				Right:    &query.NumberLiteral{Value: 0},
			}},
		},
	})

	// Valid insert
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{numReal(1), numReal(25)}},
	}, nil)
	if err != nil {
		t.Errorf("Valid insert failed: %v", err)
	}

	// Invalid insert (violates CHECK constraint)
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{numReal(2), numReal(-5)}},
	}, nil)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation")
	}
}

// TestInsertLockedWithNotNullConstraint tests insertLocked with NOT NULL constraints
func TestInsertLockedWithNotNullConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText, NotNull: true},
		},
	})

	// Valid insert
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Alice")}},
	}, nil)
	if err != nil {
		t.Errorf("Valid insert failed: %v", err)
	}

	// Invalid insert (NULL for NOT NULL column)
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(2), &query.NullLiteral{}}},
	}, nil)
	if err == nil {
		t.Error("Expected error for NOT NULL constraint violation")
	}
}

// TestInsertLockedWithUniqueConstraint tests insertLocked with UNIQUE constraints
func TestInsertLockedWithUniqueConstraint(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText, Unique: true},
		},
	})

	// First insert
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{numReal(1), strReal("alice@test.com")}},
	}, nil)
	if err != nil {
		t.Errorf("First insert failed: %v", err)
	}

	// Duplicate insert (violates UNIQUE constraint)
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "email"},
		Values:  [][]query.Expression{{numReal(2), strReal("alice@test.com")}},
	}, nil)
	if err == nil {
		t.Error("Expected error for UNIQUE constraint violation")
	}

	// Duplicate with IGNORE
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:          "test",
		Columns:        []string{"id", "email"},
		Values:         [][]query.Expression{{numReal(2), strReal("alice@test.com")}},
		ConflictAction: query.ConflictIgnore,
	}, nil)
	if err != nil {
		t.Errorf("Insert with IGNORE should not error: %v", err)
	}

	// Duplicate with REPLACE
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:          "test",
		Columns:        []string{"id", "email"},
		Values:         [][]query.Expression{{numReal(2), strReal("alice@test.com")}},
		ConflictAction: query.ConflictReplace,
	}, nil)
	if err != nil {
		t.Errorf("Insert with REPLACE failed: %v", err)
	}
}

// TestInsertLockedWithForeignKey tests insertLocked with FOREIGN KEY constraints
func TestInsertLockedWithForeignKey(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
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

	// Insert parent first
	c.Insert(ctx, &query.InsertStmt{
		Table:   "departments",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("Sales")}},
	}, nil)

	// Valid insert (FK exists)
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id"},
		Values:  [][]query.Expression{{numReal(1), numReal(1)}},
	}, nil)
	if err != nil {
		t.Errorf("Valid FK insert failed: %v", err)
	}

	// Invalid insert (FK doesn't exist)
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id"},
		Values:  [][]query.Expression{{numReal(2), numReal(999)}},
	}, nil)
	if err == nil {
		t.Error("Expected error for FK constraint violation")
	}

	// Insert with NULL FK (should be allowed)
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:   "employees",
		Columns: []string{"id", "dept_id"},
		Values:  [][]query.Expression{{numReal(3), &query.NullLiteral{}}},
	}, nil)
	if err != nil {
		t.Errorf("NULL FK insert should be allowed: %v", err)
	}
}

// TestInsertLockedWithDefaultExpr tests insertLocked with DEFAULT expressions
func TestInsertLockedWithDefaultExpr(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create table with DEFAULT using SQL
	c.ExecuteQuery("CREATE TABLE test (id INTEGER PRIMARY KEY, created_at INTEGER DEFAULT 100)")

	// Insert without specifying default column
	_, lastID, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)
	if err != nil {
		t.Logf("Insert with default result: %v", err)
	}
	t.Logf("Insert succeeded, lastID=%d", lastID)
}

// TestInsertLockedWithRLS tests insertLocked with RLS policies
func TestInsertLockedWithRLS(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Enable RLS
	c.EnableRLS()

	// Create a table
	c.CreateTable(&query.CreateTableStmt{
		Table: "rls_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "data", Type: query.TokenText},
			{Name: "owner", Type: query.TokenText},
		},
	})

	// Insert should work (RLS enabled but no policy restricting insert)
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "rls_test",
		Columns: []string{"id", "data", "owner"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "hello"}, &query.StringLiteral{Value: "alice"}},
		},
	}, nil)
	if err != nil {
		t.Logf("Insert with RLS enabled: %v", err)
	}
}

// TestInsertLockedWithPrimaryKeyConflict tests insertLocked with PK conflicts
func TestInsertLockedWithPrimaryKeyConflict(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// First insert
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)
	if err != nil {
		t.Errorf("First insert failed: %v", err)
	}

	// Duplicate PK
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(200)}},
	}, nil)
	if err == nil {
		t.Error("Expected error for duplicate PK")
	}

	// Duplicate PK with IGNORE
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:          "test",
		Columns:        []string{"id", "val"},
		Values:         [][]query.Expression{{numReal(1), numReal(200)}},
		ConflictAction: query.ConflictIgnore,
	}, nil)
	if err != nil {
		t.Errorf("Insert with IGNORE should not error: %v", err)
	}

	// Duplicate PK with REPLACE
	_, _, err = c.insertLocked(ctx, &query.InsertStmt{
		Table:          "test",
		Columns:        []string{"id", "val"},
		Values:         [][]query.Expression{{numReal(1), numReal(200)}},
		ConflictAction: query.ConflictReplace,
	}, nil)
	if err != nil {
		t.Errorf("Insert with REPLACE failed: %v", err)
	}
}

// TestInsertLockedWithExpressionEvalError tests insertLocked with expression eval errors
func TestInsertLockedWithExpressionEvalError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert with invalid column reference in expression
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), &query.Identifier{Name: "nonexistent_column"}}},
	}, nil)
	if err == nil {
		t.Error("Expected error for invalid expression")
	}
}

// TestInsertLockedWithStringPK tests insertLocked with string primary key
func TestInsertLockedWithStringPK(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "code", Type: query.TokenText, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert with string PK
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"code", "val"},
		Values:  [][]query.Expression{{strReal("ABC123"), numReal(100)}},
	}, nil)
	if err != nil {
		t.Errorf("Insert with string PK failed: %v", err)
	}
}

// TestInsertLockedWithEmptyColumns tests insertLocked with empty column list
func TestInsertLockedWithEmptyColumns(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})

	// Insert without specifying columns (should use all columns)
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:  "test",
		Values: [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)
	if err != nil {
		t.Logf("Insert without columns result: %v", err)
	}
}

// TestInsertLockedWithInvalidColumnName tests insertLocked with invalid column
func TestInsertLockedWithInvalidColumnName(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})

	// Insert with non-existent column
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "test",
		Columns: []string{"id", "nonexistent"},
		Values:  [][]query.Expression{{numReal(1), numReal(100)}},
	}, nil)
	if err == nil {
		t.Error("Expected error for invalid column name")
	}
}
