package catalog

import (
	"errors"
	"strings"
	"testing"
)

func TestUndoAlterAddColumnSurfacesMetadataPersistenceFailure(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	c.tables["undo_add_col"] = &TableDef{
		Name:    "undo_add_col",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "tmp", Type: "TEXT"}},
	}
	c.tree = &putFailTree{TreeStore: c.tree, err: errors.New("metadata put failed")}

	err := c.applyUndoEntry(undoEntry{
		action:     undoAlterAddColumn,
		tableName:  "undo_add_col",
		oldColumns: []ColumnDef{{Name: "id", Type: "INTEGER"}},
	}, "rollback")
	if err == nil || !strings.Contains(err.Error(), "metadata put failed") || !strings.Contains(err.Error(), "undo_add_col") {
		t.Fatalf("expected undo metadata persistence error, got %v", err)
	}
}

func TestUndoAlterRenameColumnSurfacesMetadataPersistenceFailure(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	c.tables["undo_rename_col"] = &TableDef{
		Name:       "undo_rename_col",
		Columns:    []ColumnDef{{Name: "id", Type: "INTEGER"}, {Name: "new_name", Type: "TEXT"}},
		PrimaryKey: []string{"id"},
	}
	c.tree = &putFailTree{TreeStore: c.tree, err: errors.New("metadata put failed")}

	err := c.applyUndoEntry(undoEntry{
		action:    undoAlterRenameColumn,
		tableName: "undo_rename_col",
		oldName:   "old_name",
		newName:   "new_name",
	}, "rollback")
	if err == nil || !strings.Contains(err.Error(), "metadata put failed") || !strings.Contains(err.Error(), "undo_rename_col") {
		t.Fatalf("expected undo metadata persistence error, got %v", err)
	}
}
