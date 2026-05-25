package catalog

import (
	"errors"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
)

type deleteFailTree struct {
	btree.TreeStore
	err error
}

func (t *deleteFailTree) Delete(key []byte) error {
	return t.err
}

func TestDDLDropMetadataDeleteFailureKeepsCatalogState(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	deleteErr := errors.New("delete failed")
	c.tree = &deleteFailTree{TreeStore: c.tree, err: deleteErr}

	c.tables["ddl_delete_t"] = &TableDef{Name: "ddl_delete_t"}
	c.indexes["ddl_delete_idx"] = &IndexDef{Name: "ddl_delete_idx", TableName: "ddl_delete_t"}
	c.views["ddl_delete_v"] = &query.SelectStmt{}
	c.viewSQL["ddl_delete_v"] = "CREATE VIEW ddl_delete_v AS SELECT 1"
	c.triggers["ddl_delete_trg"] = &query.CreateTriggerStmt{Name: "ddl_delete_trg", Table: "ddl_delete_t"}
	c.triggerSQL["ddl_delete_trg"] = "CREATE TRIGGER ddl_delete_trg AFTER INSERT ON ddl_delete_t BEGIN SELECT 1; END"
	c.procedures["ddl_delete_proc"] = &query.CreateProcedureStmt{Name: "ddl_delete_proc"}
	c.procedureSQL["ddl_delete_proc"] = "CREATE PROCEDURE ddl_delete_proc() BEGIN SELECT 1; END"
	c.materializedViews["ddl_delete_mv"] = &MaterializedViewDef{Name: "ddl_delete_mv"}
	c.materializedViewSQL["ddl_delete_mv"] = "CREATE MATERIALIZED VIEW ddl_delete_mv AS SELECT 1"

	cases := []struct {
		name      string
		drop      func() error
		stillHere func() bool
	}{
		{
			name: "table",
			drop: func() error {
				return c.DropTable(&query.DropTableStmt{Table: "ddl_delete_t"})
			},
			stillHere: func() bool { _, ok := c.tables["ddl_delete_t"]; return ok },
		},
		{
			name:      "index",
			drop:      func() error { return c.DropIndex("ddl_delete_idx") },
			stillHere: func() bool { _, ok := c.indexes["ddl_delete_idx"]; return ok },
		},
		{
			name:      "view",
			drop:      func() error { return c.DropView("ddl_delete_v") },
			stillHere: func() bool { _, ok := c.views["ddl_delete_v"]; return ok },
		},
		{
			name:      "trigger",
			drop:      func() error { return c.DropTrigger("ddl_delete_trg") },
			stillHere: func() bool { _, ok := c.triggers["ddl_delete_trg"]; return ok },
		},
		{
			name:      "procedure",
			drop:      func() error { return c.DropProcedure("ddl_delete_proc") },
			stillHere: func() bool { _, ok := c.procedures["ddl_delete_proc"]; return ok },
		},
		{
			name: "materialized view",
			drop: func() error {
				return c.DropMaterializedView("ddl_delete_mv", false)
			},
			stillHere: func() bool { _, ok := c.materializedViews["ddl_delete_mv"]; return ok },
		},
	}

	for _, tc := range cases {
		err := tc.drop()
		if err == nil || !strings.Contains(err.Error(), "delete") {
			t.Fatalf("%s: expected metadata delete error, got %v", tc.name, err)
		}
		if !tc.stillHere() {
			t.Fatalf("%s: catalog state was removed after metadata delete failure", tc.name)
		}
	}
}

func TestAlterTableRenameMetadataDeleteFailureKeepsOldName(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	c.tables["ddl_rename_old"] = &TableDef{Name: "ddl_rename_old"}
	c.tree = &deleteFailTree{TreeStore: c.tree, err: errors.New("delete failed")}

	err := c.AlterTableRename(&query.AlterTableStmt{
		Table:   "ddl_rename_old",
		NewName: "ddl_rename_new",
	})
	if err == nil || !strings.Contains(err.Error(), "delete") {
		t.Fatalf("expected metadata delete error, got %v", err)
	}
	if _, ok := c.tables["ddl_rename_old"]; !ok {
		t.Fatal("old table name should remain after metadata delete failure")
	}
	if _, ok := c.tables["ddl_rename_new"]; ok {
		t.Fatal("new table name should not appear after metadata delete failure")
	}
}
