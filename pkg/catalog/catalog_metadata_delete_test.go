package catalog

import (
	"encoding/json"
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

type putFailTree struct {
	btree.TreeStore
	err error
}

func (t *putFailTree) Put(key, value []byte) error {
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
	c.foreignTables["ddl_delete_ft"] = &ForeignTableDef{TableName: "ddl_delete_ft", Wrapper: "csv"}

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
		{
			name:      "foreign table",
			drop:      func() error { return c.DropForeignTable("ddl_delete_ft") },
			stillHere: func() bool { _, ok := c.foreignTables["ddl_delete_ft"]; return ok },
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

func TestDropTableDeletesIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_table_idx_meta",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "drop_table_idx_meta_email_idx",
		Table:   "drop_table_idx_meta",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if _, err := c.tree.Get([]byte("idx:drop_table_idx_meta_email_idx")); err != nil {
		t.Fatalf("expected index metadata before DROP TABLE: %v", err)
	}

	if err := c.DropTable(&query.DropTableStmt{Table: "drop_table_idx_meta"}); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if _, err := c.tree.Get([]byte("idx:drop_table_idx_meta_email_idx")); !errors.Is(err, btree.ErrKeyNotFound) {
		t.Fatalf("DROP TABLE left stale index metadata, get err=%v", err)
	}
	if indexes := c.GetTableIndexes("drop_table_idx_meta"); len(indexes) != 0 {
		t.Fatalf("DROP TABLE left in-memory indexes: %#v", indexes)
	}
}

func TestRollbackDropTableRestoresIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_drop_table_idx_meta",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "rollback_drop_table_idx_meta_email_idx",
		Table:   "rollback_drop_table_idx_meta",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	c.BeginTransaction(1)
	if err := c.DropTable(&query.DropTableStmt{Table: "rollback_drop_table_idx_meta"}); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if _, err := c.tree.Get([]byte("idx:rollback_drop_table_idx_meta_email_idx")); !errors.Is(err, btree.ErrKeyNotFound) {
		t.Fatalf("DROP TABLE inside transaction left index metadata before rollback, get err=%v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	if _, err := c.GetTable("rollback_drop_table_idx_meta"); err != nil {
		t.Fatalf("table not restored after rollback: %v", err)
	}
	if _, err := c.tree.Get([]byte("idx:rollback_drop_table_idx_meta_email_idx")); err != nil {
		t.Fatalf("index metadata not restored after rollback: %v", err)
	}
	if indexes := c.GetTableIndexes("rollback_drop_table_idx_meta"); len(indexes) != 1 {
		t.Fatalf("index definitions not restored after rollback: %#v", indexes)
	}
}

func TestAlterTableDropColumnDeletesIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_col_idx_meta",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
			{Name: "name", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "drop_col_idx_meta_email_idx",
		Table:   "drop_col_idx_meta",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := c.AlterTableDropColumn(&query.AlterTableStmt{Table: "drop_col_idx_meta", Column: query.ColumnDef{Name: "email"}}); err != nil {
		t.Fatalf("AlterTableDropColumn: %v", err)
	}
	if _, err := c.tree.Get([]byte("idx:drop_col_idx_meta_email_idx")); !errors.Is(err, btree.ErrKeyNotFound) {
		t.Fatalf("DROP COLUMN left stale index metadata, get err=%v", err)
	}
	if indexes := c.GetTableIndexes("drop_col_idx_meta"); len(indexes) != 0 {
		t.Fatalf("DROP COLUMN left in-memory index metadata: %#v", indexes)
	}
}

func TestRollbackDropColumnRestoresIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_drop_col_idx_meta",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "rollback_drop_col_idx_meta_email_idx",
		Table:   "rollback_drop_col_idx_meta",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	c.BeginTransaction(1)
	if err := c.AlterTableDropColumn(&query.AlterTableStmt{Table: "rollback_drop_col_idx_meta", Column: query.ColumnDef{Name: "email"}}); err != nil {
		t.Fatalf("AlterTableDropColumn: %v", err)
	}
	if _, err := c.tree.Get([]byte("idx:rollback_drop_col_idx_meta_email_idx")); !errors.Is(err, btree.ErrKeyNotFound) {
		t.Fatalf("DROP COLUMN inside transaction left index metadata before rollback, get err=%v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	if _, err := c.tree.Get([]byte("idx:rollback_drop_col_idx_meta_email_idx")); err != nil {
		t.Fatalf("index metadata not restored after rollback: %v", err)
	}
	if indexes := c.GetTableIndexes("rollback_drop_col_idx_meta"); len(indexes) != 1 {
		t.Fatalf("index definition not restored after rollback: %#v", indexes)
	}
}

func TestAlterTableDropColumnRejectsForeignKeyColumns(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "drop_fk_col_parent",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_fk_col_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Name:              "drop_fk_col_child_parent_fk",
			Columns:           []string{"parent_id"},
			ReferencedTable:   "drop_fk_col_parent",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	err := c.AlterTableDropColumn(&query.AlterTableStmt{Table: "drop_fk_col_child", Column: query.ColumnDef{Name: "parent_id"}})
	if err == nil || !strings.Contains(err.Error(), "drop_fk_col_child_parent_fk") {
		t.Fatalf("expected local FK column drop rejection, got %v", err)
	}
	err = c.AlterTableDropColumn(&query.AlterTableStmt{Table: "drop_fk_col_parent", Column: query.ColumnDef{Name: "id"}})
	if err == nil || !strings.Contains(err.Error(), "PRIMARY KEY") {
		t.Fatalf("expected primary-key referenced column drop rejection, got %v", err)
	}

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_ref_col_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable referenced parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_ref_col_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_code", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Name:              "drop_ref_col_child_parent_fk",
			Columns:           []string{"parent_code"},
			ReferencedTable:   "drop_ref_col_parent",
			ReferencedColumns: []string{"code"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable referenced child: %v", err)
	}
	err = c.AlterTableDropColumn(&query.AlterTableStmt{Table: "drop_ref_col_parent", Column: query.ColumnDef{Name: "code"}})
	if err == nil || !strings.Contains(err.Error(), "drop_ref_col_child_parent_fk") {
		t.Fatalf("expected referenced FK column drop rejection, got %v", err)
	}
}

func TestAlterTableRenameColumnPersistsIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rename_col_idx_meta",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "rename_col_idx_meta_email_idx",
		Table:   "rename_col_idx_meta",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rename_col_idx_meta", OldName: "email", NewName: "contact_email"}); err != nil {
		t.Fatalf("AlterTableRenameColumn: %v", err)
	}
	data, err := c.tree.Get([]byte("idx:rename_col_idx_meta_email_idx"))
	if err != nil {
		t.Fatalf("index metadata missing after rename: %v", err)
	}
	var idx IndexDef
	if err := json.Unmarshal(data, &idx); err != nil {
		t.Fatalf("unmarshal index metadata: %v", err)
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "contact_email" {
		t.Fatalf("index metadata columns = %v, want [contact_email]", idx.Columns)
	}
}

func TestRollbackRenameColumnRestoresIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_rename_col_idx_meta",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "rollback_rename_col_idx_meta_email_idx",
		Table:   "rollback_rename_col_idx_meta",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	c.BeginTransaction(1)
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rollback_rename_col_idx_meta", OldName: "email", NewName: "contact_email"}); err != nil {
		t.Fatalf("AlterTableRenameColumn: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	data, err := c.tree.Get([]byte("idx:rollback_rename_col_idx_meta_email_idx"))
	if err != nil {
		t.Fatalf("index metadata missing after rollback: %v", err)
	}
	var idx IndexDef
	if err := json.Unmarshal(data, &idx); err != nil {
		t.Fatalf("unmarshal index metadata: %v", err)
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "email" {
		t.Fatalf("index metadata columns after rollback = %v, want [email]", idx.Columns)
	}
}

func TestAlterTableRenameColumnPersistsForeignKeyColumns(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "rename_fk_col_parent",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rename_fk_col_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "rename_fk_col_parent",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rename_fk_col_child", OldName: "parent_id", NewName: "account_id"}); err != nil {
		t.Fatalf("AlterTableRenameColumn child: %v", err)
	}
	child, err := c.GetTable("rename_fk_col_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if got := child.ForeignKeys[0].Columns[0]; got != "account_id" {
		t.Fatalf("in-memory FK column = %q, want account_id", got)
	}
	data, err := c.tree.Get([]byte("tbl:rename_fk_col_child"))
	if err != nil {
		t.Fatalf("child metadata missing: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal child metadata: %v", err)
	}
	if got := persisted.ForeignKeys[0].Columns[0]; got != "account_id" {
		t.Fatalf("persisted FK column = %q, want account_id", got)
	}
}

func TestAlterTableRenameReferencedColumnPersistsForeignKeys(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rename_ref_col_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rename_ref_col_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_code", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_code"},
			ReferencedTable:   "rename_ref_col_parent",
			ReferencedColumns: []string{"code"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rename_ref_col_parent", OldName: "code", NewName: "account_code"}); err != nil {
		t.Fatalf("AlterTableRenameColumn parent: %v", err)
	}
	child, err := c.GetTable("rename_ref_col_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if got := child.ForeignKeys[0].ReferencedColumns[0]; got != "account_code" {
		t.Fatalf("in-memory referenced FK column = %q, want account_code", got)
	}
	data, err := c.tree.Get([]byte("tbl:rename_ref_col_child"))
	if err != nil {
		t.Fatalf("child metadata missing: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal child metadata: %v", err)
	}
	if got := persisted.ForeignKeys[0].ReferencedColumns[0]; got != "account_code" {
		t.Fatalf("persisted referenced FK column = %q, want account_code", got)
	}
}

func TestRollbackRenameColumnRestoresForeignKeyColumns(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "rollback_rename_fk_col_parent",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_rename_fk_col_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "rollback_rename_fk_col_parent",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	c.BeginTransaction(1)
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rollback_rename_fk_col_child", OldName: "parent_id", NewName: "account_id"}); err != nil {
		t.Fatalf("AlterTableRenameColumn child: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}

	child, err := c.GetTable("rollback_rename_fk_col_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if got := child.ForeignKeys[0].Columns[0]; got != "parent_id" {
		t.Fatalf("in-memory FK column after rollback = %q, want parent_id", got)
	}
	data, err := c.tree.Get([]byte("tbl:rollback_rename_fk_col_child"))
	if err != nil {
		t.Fatalf("child metadata missing after rollback: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal child metadata: %v", err)
	}
	if got := persisted.ForeignKeys[0].Columns[0]; got != "parent_id" {
		t.Fatalf("persisted FK column after rollback = %q, want parent_id", got)
	}
}

func TestRollbackRenameReferencedColumnRestoresForeignKeys(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_rename_ref_col_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "code", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_rename_ref_col_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_code", Type: query.TokenText},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_code"},
			ReferencedTable:   "rollback_rename_ref_col_parent",
			ReferencedColumns: []string{"code"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	c.BeginTransaction(1)
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rollback_rename_ref_col_parent", OldName: "code", NewName: "account_code"}); err != nil {
		t.Fatalf("AlterTableRenameColumn parent: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}

	child, err := c.GetTable("rollback_rename_ref_col_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if got := child.ForeignKeys[0].ReferencedColumns[0]; got != "code" {
		t.Fatalf("in-memory referenced FK column after rollback = %q, want code", got)
	}
	data, err := c.tree.Get([]byte("tbl:rollback_rename_ref_col_child"))
	if err != nil {
		t.Fatalf("child metadata missing after rollback: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal child metadata: %v", err)
	}
	if got := persisted.ForeignKeys[0].ReferencedColumns[0]; got != "code" {
		t.Fatalf("persisted referenced FK column after rollback = %q, want code", got)
	}
}

func TestAlterTableDropColumnRejectsCheckReferences(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_check_col",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "balance", Type: query.TokenInteger},
			{Name: "minimum", Type: query.TokenInteger},
		},
		CheckConstraints: []query.CheckConstraintDef{{
			Name: "drop_check_col_balance_ck",
			Expr: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "balance"},
				Operator: query.TokenGte,
				Right:    &query.Identifier{Name: "minimum"},
			},
		}},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	err := c.AlterTableDropColumn(&query.AlterTableStmt{Table: "drop_check_col", Column: query.ColumnDef{Name: "minimum"}})
	if err == nil || !strings.Contains(err.Error(), "drop_check_col_balance_ck") {
		t.Fatalf("expected CHECK referenced-column drop rejection, got %v", err)
	}
	if _, err := c.GetTable("drop_check_col"); err != nil {
		t.Fatalf("table should remain after failed drop: %v", err)
	}
}

func TestAlterTableRenameColumnPersistsCheckReferences(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rename_check_col",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "balance", Type: query.TokenInteger},
			{Name: "minimum", Type: query.TokenInteger},
		},
		CheckConstraints: []query.CheckConstraintDef{{
			Name: "rename_check_col_balance_ck",
			Expr: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "balance"},
				Operator: query.TokenGte,
				Right:    &query.Identifier{Name: "minimum"},
			},
		}},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rename_check_col", OldName: "balance", NewName: "current_balance"}); err != nil {
		t.Fatalf("AlterTableRenameColumn: %v", err)
	}

	table, err := c.GetTable("rename_check_col")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if got := table.Checks[0].CheckStr; got != "(current_balance >= minimum)" {
		t.Fatalf("in-memory CHECK SQL = %q, want renamed column", got)
	}
	data, err := c.tree.Get([]byte("tbl:rename_check_col"))
	if err != nil {
		t.Fatalf("table metadata missing: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal table metadata: %v", err)
	}
	if got := persisted.Checks[0].CheckStr; got != "(current_balance >= minimum)" {
		t.Fatalf("persisted CHECK SQL = %q, want renamed column", got)
	}
}

func TestRollbackRenameColumnRestoresCheckReferences(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_rename_check_col",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "balance", Type: query.TokenInteger},
			{Name: "minimum", Type: query.TokenInteger},
		},
		CheckConstraints: []query.CheckConstraintDef{{
			Name: "rollback_rename_check_col_balance_ck",
			Expr: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "balance"},
				Operator: query.TokenGte,
				Right:    &query.Identifier{Name: "minimum"},
			},
		}},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	c.BeginTransaction(1)
	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{Table: "rollback_rename_check_col", OldName: "balance", NewName: "current_balance"}); err != nil {
		t.Fatalf("AlterTableRenameColumn: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}

	table, err := c.GetTable("rollback_rename_check_col")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	if got := table.Checks[0].CheckStr; got != "(balance >= minimum)" {
		t.Fatalf("in-memory CHECK SQL after rollback = %q, want original column", got)
	}
	data, err := c.tree.Get([]byte("tbl:rollback_rename_check_col"))
	if err != nil {
		t.Fatalf("table metadata missing after rollback: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal table metadata: %v", err)
	}
	if got := persisted.Checks[0].CheckStr; got != "(balance >= minimum)" {
		t.Fatalf("persisted CHECK SQL after rollback = %q, want original column", got)
	}
}

func TestAlterTableRenamePersistsIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rename_table_idx_meta_old",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "rename_table_idx_meta_email_idx",
		Table:   "rename_table_idx_meta_old",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	if err := c.AlterTableRename(&query.AlterTableStmt{Table: "rename_table_idx_meta_old", NewName: "rename_table_idx_meta_new"}); err != nil {
		t.Fatalf("AlterTableRename: %v", err)
	}
	data, err := c.tree.Get([]byte("idx:rename_table_idx_meta_email_idx"))
	if err != nil {
		t.Fatalf("index metadata missing after table rename: %v", err)
	}
	var idx IndexDef
	if err := json.Unmarshal(data, &idx); err != nil {
		t.Fatalf("unmarshal index metadata: %v", err)
	}
	if idx.TableName != "rename_table_idx_meta_new" {
		t.Fatalf("index metadata table = %q, want rename_table_idx_meta_new", idx.TableName)
	}
}

func TestRollbackRenameTableRestoresIndexMetadata(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_rename_table_idx_meta_old",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "rollback_rename_table_idx_meta_email_idx",
		Table:   "rollback_rename_table_idx_meta_old",
		Columns: []string{"email"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}
	c.BeginTransaction(1)
	if err := c.AlterTableRename(&query.AlterTableStmt{Table: "rollback_rename_table_idx_meta_old", NewName: "rollback_rename_table_idx_meta_new"}); err != nil {
		t.Fatalf("AlterTableRename: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	data, err := c.tree.Get([]byte("idx:rollback_rename_table_idx_meta_email_idx"))
	if err != nil {
		t.Fatalf("index metadata missing after rollback: %v", err)
	}
	var idx IndexDef
	if err := json.Unmarshal(data, &idx); err != nil {
		t.Fatalf("unmarshal index metadata: %v", err)
	}
	if idx.TableName != "rollback_rename_table_idx_meta_old" {
		t.Fatalf("index metadata table after rollback = %q, want rollback_rename_table_idx_meta_old", idx.TableName)
	}
}

func TestAlterTableRenamePersistsForeignKeyReferences(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "rename_fk_parent_old",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rename_fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "rename_fk_parent_old",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}
	if err := c.AlterTableRename(&query.AlterTableStmt{Table: "rename_fk_parent_old", NewName: "rename_fk_parent_new"}); err != nil {
		t.Fatalf("AlterTableRename: %v", err)
	}
	child, err := c.GetTable("rename_fk_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if got := child.ForeignKeys[0].ReferencedTable; got != "rename_fk_parent_new" {
		t.Fatalf("in-memory FK referenced table = %q, want rename_fk_parent_new", got)
	}
	data, err := c.tree.Get([]byte("tbl:rename_fk_child"))
	if err != nil {
		t.Fatalf("child table metadata missing after parent rename: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal child table metadata: %v", err)
	}
	if got := persisted.ForeignKeys[0].ReferencedTable; got != "rename_fk_parent_new" {
		t.Fatalf("persisted FK referenced table = %q, want rename_fk_parent_new", got)
	}
}

func TestRollbackRenameTableRestoresForeignKeyReferences(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "rollback_rename_fk_parent_old",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "rollback_rename_fk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "rollback_rename_fk_parent_old",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}
	c.BeginTransaction(1)
	if err := c.AlterTableRename(&query.AlterTableStmt{Table: "rollback_rename_fk_parent_old", NewName: "rollback_rename_fk_parent_new"}); err != nil {
		t.Fatalf("AlterTableRename: %v", err)
	}
	if err := c.RollbackTransaction(); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	child, err := c.GetTable("rollback_rename_fk_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if got := child.ForeignKeys[0].ReferencedTable; got != "rollback_rename_fk_parent_old" {
		t.Fatalf("in-memory FK referenced table after rollback = %q, want rollback_rename_fk_parent_old", got)
	}
	data, err := c.tree.Get([]byte("tbl:rollback_rename_fk_child"))
	if err != nil {
		t.Fatalf("child table metadata missing after rollback: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal child table metadata: %v", err)
	}
	if got := persisted.ForeignKeys[0].ReferencedTable; got != "rollback_rename_fk_parent_old" {
		t.Fatalf("persisted FK referenced table after rollback = %q, want rollback_rename_fk_parent_old", got)
	}
}

func TestDropReferencedTableIsRejected(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "drop_ref_parent",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_ref_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Name:              "drop_ref_child_parent_fk",
			Columns:           []string{"parent_id"},
			ReferencedTable:   "drop_ref_parent",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	err := c.DropTable(&query.DropTableStmt{Table: "drop_ref_parent"})
	if err == nil || !strings.Contains(err.Error(), "drop_ref_child_parent_fk") {
		t.Fatalf("expected referenced-table drop rejection, got %v", err)
	}
	if _, err := c.GetTable("drop_ref_parent"); err != nil {
		t.Fatalf("referenced parent table should remain after failed drop: %v", err)
	}
}

func TestDropSelfReferencingTableAllowed(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "drop_self_ref",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "drop_self_ref",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.DropTable(&query.DropTableStmt{Table: "drop_self_ref"}); err != nil {
		t.Fatalf("self-referencing table should be droppable: %v", err)
	}
	if _, err := c.GetTable("drop_self_ref"); err == nil {
		t.Fatal("self-referencing table remained after drop")
	}
}

func TestCreateForeignTableMetadataStoreFailureRollsBackCatalogState(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()
	c.fdwRegistry = nil

	c.tree = &putFailTree{TreeStore: c.tree, err: errors.New("put failed")}
	err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table:   "ddl_foreign_create_fail",
		Wrapper: "csv",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "put failed") {
		t.Fatalf("expected metadata store error, got %v", err)
	}
	if _, ok := c.foreignTables["ddl_foreign_create_fail"]; ok {
		t.Fatal("foreign table should not remain in catalog after metadata store failure")
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

func TestCreateTableMetadataStoreFailureRollsBackCatalogState(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	c.tree = &putFailTree{TreeStore: c.tree, err: errors.New("put failed")}
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "ddl_create_fail",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "put failed") {
		t.Fatalf("expected metadata store error, got %v", err)
	}
	if _, ok := c.tables["ddl_create_fail"]; ok {
		t.Fatal("table should not remain in catalog after metadata store failure")
	}
	if _, ok := c.tableTrees["ddl_create_fail"]; ok {
		t.Fatal("table tree should not remain after metadata store failure")
	}
}

func TestCreateIndexMetadataStoreFailureRollsBackCatalogState(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "ddl_index_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	c.tree = &putFailTree{TreeStore: c.tree, err: errors.New("put failed")}
	err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "ddl_index_fail",
		Table:   "ddl_index_base",
		Columns: []string{"name"},
	})
	if err == nil || !strings.Contains(err.Error(), "put failed") {
		t.Fatalf("expected metadata store error, got %v", err)
	}
	if _, ok := c.indexes["ddl_index_fail"]; ok {
		t.Fatal("index should not remain in catalog after metadata store failure")
	}
	if _, ok := c.indexTrees["ddl_index_fail"]; ok {
		t.Fatal("index tree should not remain after metadata store failure")
	}
}
