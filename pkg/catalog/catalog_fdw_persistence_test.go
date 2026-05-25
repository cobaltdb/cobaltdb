package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestForeignTablePersistenceLoad(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()
	c.fdwRegistry = nil

	if err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table:   "persist_ft",
		Wrapper: "csv",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
		Options: map[string]string{"file": "/tmp/persist.csv"},
	}); err != nil {
		t.Fatalf("CreateForeignTable: %v", err)
	}
	if err := c.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}

	reloaded := New(c.tree, pool, nil)
	if err := reloaded.Load(); err != nil {
		t.Fatalf("Load: %v", err)
	}
	ft, err := reloaded.GetForeignTable("persist_ft")
	if err != nil {
		t.Fatalf("GetForeignTable after Load: %v", err)
	}
	if ft.Wrapper != "csv" || ft.Options["file"] != "/tmp/persist.csv" {
		t.Fatalf("unexpected foreign table metadata: %+v", ft)
	}
	if len(ft.Columns) != 2 || ft.Columns[0].Name != "id" || ft.Columns[1].Name != "name" {
		t.Fatalf("unexpected foreign table columns: %+v", ft.Columns)
	}
}

func TestCreateForeignTableRollsBack(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()
	c.fdwRegistry = nil

	c.BeginTransaction(1)
	if err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table:   "rollback_create_ft",
		Wrapper: "csv",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger}},
	}); err != nil {
		t.Fatalf("CreateForeignTable: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	if _, err := c.GetForeignTable("rollback_create_ft"); err != ErrTableNotFound {
		t.Fatalf("foreign table should be absent after rollback, got %v", err)
	}
	if _, err := c.tree.Get([]byte("ft:rollback_create_ft")); err == nil {
		t.Fatal("foreign table metadata should be absent after rollback")
	}
}

func TestDropForeignTableRollsBack(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()
	c.fdwRegistry = nil

	if err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table:   "rollback_drop_ft",
		Wrapper: "csv",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger}},
		Options: map[string]string{"file": "/tmp/rollback.csv"},
	}); err != nil {
		t.Fatalf("CreateForeignTable: %v", err)
	}

	c.BeginTransaction(1)
	if err := c.DropForeignTable("rollback_drop_ft"); err != nil {
		t.Fatalf("DropForeignTable: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	ft, err := c.GetForeignTable("rollback_drop_ft")
	if err != nil {
		t.Fatalf("foreign table should be restored after rollback: %v", err)
	}
	if ft.Options["file"] != "/tmp/rollback.csv" {
		t.Fatalf("unexpected restored metadata: %+v", ft)
	}
	if _, err := c.tree.Get([]byte("ft:rollback_drop_ft")); err != nil {
		t.Fatalf("foreign table metadata should be restored after rollback: %v", err)
	}
}

func TestCreateForeignTableIfNotExists(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()
	c.fdwRegistry = nil

	stmt := &query.CreateForeignTableStmt{
		IfNotExists: true,
		Table:       "ifne_ft",
		Wrapper:     "csv",
		Columns:     []*query.ColumnDef{{Name: "id", Type: query.TokenInteger}},
	}
	if err := c.CreateForeignTable(stmt); err != nil {
		t.Fatalf("CreateForeignTable first call: %v", err)
	}
	if err := c.CreateForeignTable(stmt); err != nil {
		t.Fatalf("CreateForeignTable IF NOT EXISTS duplicate: %v", err)
	}
}
