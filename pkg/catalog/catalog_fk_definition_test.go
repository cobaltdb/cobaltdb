package catalog

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestCreateTableRejectsMismatchedForeignKeyColumns(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "fk_def_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "tenant_id", Type: query.TokenInteger},
		},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}

	err := c.CreateTable(&query.CreateTableStmt{
		Table: "fk_def_child_bad",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "tenant_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id", "tenant_id"},
			ReferencedTable:   "fk_def_parent",
			ReferencedColumns: []string{"id"},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "column count mismatch") {
		t.Fatalf("expected FK column count mismatch, got %v", err)
	}
	if _, err := c.GetTable("fk_def_child_bad"); err == nil {
		t.Fatal("invalid FK table remained in catalog")
	}
}

func TestCreateTableNormalizesForeignKeyPrimaryKeyReference(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "fk_def_pk_parent",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "fk_def_pk_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:         []string{"parent_id"},
			ReferencedTable: "fk_def_pk_parent",
		}},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	child, err := c.GetTable("fk_def_pk_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if got := child.ForeignKeys[0].ReferencedColumns; len(got) != 1 || got[0] != "id" {
		t.Fatalf("in-memory referenced columns = %v, want [id]", got)
	}
	data, err := c.tree.Get([]byte("tbl:fk_def_pk_child"))
	if err != nil {
		t.Fatalf("child metadata missing: %v", err)
	}
	var persisted TableDef
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal child metadata: %v", err)
	}
	if got := persisted.ForeignKeys[0].ReferencedColumns; len(got) != 1 || got[0] != "id" {
		t.Fatalf("persisted referenced columns = %v, want [id]", got)
	}
}

func TestForeignKeyCascadeUpdatePreservesVersionedRowsAndNonUniqueIndexes(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE fk_cascade_idx_parent (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE fk_cascade_idx_child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES fk_cascade_idx_parent(id) ON UPDATE CASCADE)"); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX idx_fk_cascade_child_parent ON fk_cascade_idx_child(parent_id)"); err != nil {
		t.Fatalf("create child index: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO fk_cascade_idx_parent VALUES (1)"); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO fk_cascade_idx_child VALUES (10, 1)"); err != nil {
		t.Fatalf("insert child: %v", err)
	}

	c.BeginTransaction(301)
	if _, err := c.ExecuteQuery("UPDATE fk_cascade_idx_parent SET id = 2 WHERE id = 1"); err != nil {
		t.Fatalf("cascade update: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	childTree := c.tableTrees["fk_cascade_idx_child"]
	childData, err := childTree.Get([]byte(formatKey(10)))
	if err != nil {
		t.Fatalf("get child row: %v", err)
	}
	if len(childData) == 0 || childData[0] != '{' {
		t.Fatalf("cascade update stored child row in non-versioned format: %s", string(childData))
	}
	childRow, err := decodeVersionedRow(childData, 2)
	if err != nil {
		t.Fatalf("decode child row: %v", err)
	}
	if childRow.Version.CreatedAt == 0 {
		t.Fatal("cascade update should preserve version metadata")
	}
	if len(childRow.Data) != 2 || childRow.Data[1] != int64(2) {
		t.Fatalf("unexpected child row after cascade update: %+v", childRow.Data)
	}

	idxTree := c.indexTrees["idx_fk_cascade_child_parent"]
	oldKey := []byte(typeTaggedKey(int64(1)) + "\x00" + formatKey(10))
	if _, err := idxTree.Get(oldKey); err == nil {
		t.Fatal("old non-unique child index entry remained after FK cascade update")
	}
	newKey := []byte(typeTaggedKey(int64(2)) + "\x00" + formatKey(10))
	if pk, err := idxTree.Get(newKey); err != nil || string(pk) != formatKey(10) {
		t.Fatalf("new non-unique child index entry missing after FK cascade update: pk=%q err=%v", string(pk), err)
	}

	result, err := c.ExecuteQuery("SELECT id FROM fk_cascade_idx_child WHERE parent_id = 2")
	if err != nil {
		t.Fatalf("indexed child select: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(10) {
		t.Fatalf("indexed child lookup returned %+v", result.Rows)
	}
}

func TestAlterTableAddForeignKeyRejectsMismatchedColumns(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table:   "alter_fk_def_parent",
		Columns: []*query.ColumnDef{{Name: "id", Type: query.TokenInteger, PrimaryKey: true}},
	}); err != nil {
		t.Fatalf("CreateTable parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "alter_fk_def_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
			{Name: "tenant_id", Type: query.TokenInteger},
		},
	}); err != nil {
		t.Fatalf("CreateTable child: %v", err)
	}

	err := c.AlterTableAddForeignKeyConstraint(context.Background(), &query.AlterTableStmt{
		Table:          "alter_fk_def_child",
		ConstraintName: "alter_fk_def_bad_fk",
		ForeignKey: &query.ForeignKeyDef{
			Columns:           []string{"parent_id", "tenant_id"},
			ReferencedTable:   "alter_fk_def_parent",
			ReferencedColumns: []string{"id"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "column count mismatch") {
		t.Fatalf("expected FK column count mismatch, got %v", err)
	}
	child, err := c.GetTable("alter_fk_def_child")
	if err != nil {
		t.Fatalf("GetTable child: %v", err)
	}
	if len(child.ForeignKeys) != 0 {
		t.Fatalf("invalid FK was added to catalog: %+v", child.ForeignKeys)
	}
}
