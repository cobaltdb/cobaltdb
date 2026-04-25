package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestBuildCompositePKEdgeCases tests buildCompositePK error paths
func TestBuildCompositePKEdgeCases(t *testing.T) {
	// No primary key
	table := &TableDef{
		Name:       "test",
		Columns:    []ColumnDef{{Name: "id", Type: "INTEGER"}},
		PrimaryKey: []string{},
	}
	if _, ok := buildCompositePK(table, []interface{}{1}); ok {
		t.Error("Expected false for empty PrimaryKey")
	}

	// PK column index out of range
	table2 := &TableDef{
		Name:       "test",
		Columns:    []ColumnDef{{Name: "id", Type: "INTEGER"}},
		PrimaryKey: []string{"missing_col"},
	}
	if _, ok := buildCompositePK(table2, []interface{}{1}); ok {
		t.Error("Expected false for missing column index")
	}

	// Nil PK value
	table3 := &TableDef{
		Name:       "test",
		Columns:    []ColumnDef{{Name: "id", Type: "INTEGER"}},
		PrimaryKey: []string{"id"},
	}
	if _, ok := buildCompositePK(table3, []interface{}{nil}); ok {
		t.Error("Expected false for nil PK value")
	}

	// Single-column PK success
	table4 := &TableDef{
		Name:       "test",
		Columns:    []ColumnDef{{Name: "id", Type: "INTEGER"}},
		PrimaryKey: []string{"id"},
	}
	key, ok := buildCompositePK(table4, []interface{}{42})
	if !ok {
		t.Fatal("Expected true for valid single-column PK")
	}
	if key != formatKey(42) {
		t.Errorf("Expected formatted key, got %s", key)
	}
}

// TestInsertLockedForeignTable tests insert into foreign table
func TestInsertLockedForeignTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Register a foreign table
	c.tables["foreign_t"] = &TableDef{
		Name:    "foreign_t",
		Type:    "foreign",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}},
	}

	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "foreign_t",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{num(1)}},
	}, nil)
	if err == nil {
		t.Error("Expected error inserting into foreign table")
	}
}

// TestInsertLockedSelectIntType tests INSERT...SELECT returning int
func TestInsertLockedSelectIntType(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE src_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO src_t (id) VALUES (1)")
	c.ExecuteQuery("CREATE TABLE dst_t (id INTEGER PRIMARY KEY)")

	// Use INSERT...SELECT
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "dst_t",
		Columns: []string{"id"},
		Select: &query.SelectStmt{
			Columns: []query.Expression{&query.ColumnRef{Column: "id"}},
			From:    &query.TableRef{Name: "src_t"},
		},
	}, nil)
	if err != nil {
		t.Logf("INSERT...SELECT error: %v", err)
	}
}

// TestGetInsertTargetTreeNotFound tests getInsertTargetTree when tree is missing
func TestGetInsertTargetTreeNotFound(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.tables["ghost"] = &TableDef{
		Name:    "ghost",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}},
	}
	// Do NOT add to tableTrees

	_, _, err := c.getInsertTargetTree(c.tables["ghost"], &query.InsertStmt{Table: "ghost"}, nil)
	if err == nil {
		t.Error("Expected error for missing table tree")
	}
}

// TestInsertLockedPartitionError tests insert into partitioned table with errors
func TestInsertLockedPartitionError(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE part_t (id INTEGER PRIMARY KEY, region TEXT) PARTITION BY LIST(region) (PARTITION p1 VALUES ('east'), PARTITION p2 VALUES ('west'))")

	// Insert with NULL partition value
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "part_t",
		Columns: []string{"id", "region"},
		Values:  [][]query.Expression{{num(1), &query.NullLiteral{}}},
	}, nil)
	if err == nil {
		t.Error("Expected error for NULL partition value")
	}
}

// TestInsertLockedCheckConstraintBoost tests CHECK constraint failure
func TestInsertLockedCheckConstraintBoost(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE check_t (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0))")

	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "check_t",
		Columns: []string{"id", "age"},
		Values:  [][]query.Expression{{num(1), num(-5)}},
	}, nil)
	if err == nil {
		t.Error("Expected error for CHECK constraint violation")
	}
}

// TestInsertLockedCompositePKNil tests composite PK with nil value
func TestInsertLockedCompositePKNil(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE comp_t (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")

	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "comp_t",
		Columns: []string{"a", "b"},
		Values:  [][]query.Expression{{num(1), &query.NullLiteral{}}},
	}, nil)
	if err == nil {
		t.Error("Expected error for nil composite PK value")
	}
}

// TestInsertLockedRLSDenied tests RLS policy denying INSERT
func TestInsertLockedRLSDenied(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE rls_t (id INTEGER PRIMARY KEY, owner TEXT)")
	c.EnableRLS()
	c.rlsManager.EnableTable("rls_t")
	policy := &security.Policy{
		Name:       "rls_insert_deny",
		TableName:  "rls_t",
		Type:       security.PolicyInsert,
		Expression: "owner = 'bob'",
		Enabled:    true,
	}
	if err := c.rlsManager.CreatePolicy(policy); err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	ctx = context.WithValue(ctx, "cobaltdb_user", "alice")
	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "rls_t",
		Columns: []string{"id", "owner"},
		Values:  [][]query.Expression{{num(1), strReal("alice")}},
	}, nil)
	if err == nil {
		t.Error("Expected error for RLS denied INSERT")
	}
}

// TestInsertLockedFKFound tests foreign key found path
func TestInsertLockedFKFound(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.ExecuteQuery("CREATE TABLE parent_t (id INTEGER PRIMARY KEY)")
	c.ExecuteQuery("INSERT INTO parent_t (id) VALUES (1)")
	c.ExecuteQuery("CREATE TABLE child_t (id INTEGER PRIMARY KEY, pid INTEGER)")

	// Set up FK manually
	c.tables["child_t"].ForeignKeys = []ForeignKeyDef{{
		Columns:           []string{"pid"},
		ReferencedTable:   "parent_t",
		ReferencedColumns: []string{"id"},
	}}

	_, _, err := c.insertLocked(ctx, &query.InsertStmt{
		Table:   "child_t",
		Columns: []string{"id", "pid"},
		Values:  [][]query.Expression{{num(1), num(1)}},
	}, nil)
	if err != nil {
		t.Errorf("Expected success for valid FK insert, got %v", err)
	}
}
