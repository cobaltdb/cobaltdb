package catalog

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestUpdateReturnsCorruptRowError(t *testing.T) {
	ctx := context.Background()
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_corrupt_row (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_corrupt_row (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["upd_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	affected, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_corrupt_row",
		Set:   []*query.SetClause{{Column: "name", Value: strReal("bob")}},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "upd_corrupt_row") {
		t.Fatalf("expected corrupt row update error, affected=%d err=%v", affected, err)
	}
	if affected != 0 {
		t.Fatalf("expected no updates after corrupt row error, got %d", affected)
	}
}

func TestDeleteReturnsCorruptRowError(t *testing.T) {
	ctx := context.Background()
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE del_corrupt_row (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO del_corrupt_row (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["del_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	affected, _, err := c.Delete(ctx, &query.DeleteStmt{Table: "del_corrupt_row"}, nil)
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "del_corrupt_row") {
		t.Fatalf("expected corrupt row delete error, affected=%d err=%v", affected, err)
	}
	if affected != 0 {
		t.Fatalf("expected no deletes after corrupt row error, got %d", affected)
	}
}

func TestInsertUniqueCheckReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "ins_unique_corrupt_row",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO ins_unique_corrupt_row (id, email) VALUES (1, 'a@example.com')"); err != nil {
		t.Fatalf("insert seed: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["ins_unique_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	_, err := c.ExecuteQuery("INSERT INTO ins_unique_corrupt_row (id, email) VALUES (2, 'b@example.com')")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "ins_unique_corrupt_row") {
		t.Fatalf("expected corrupt row unique check error, got %v", err)
	}
}

func TestInsertForeignKeyCheckReturnsCorruptReferencedRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "ins_fk_corrupt_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	}); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "ins_fk_corrupt_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "ins_fk_corrupt_parent",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO ins_fk_corrupt_parent (id) VALUES (1)"); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["ins_fk_corrupt_parent"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt parent row: %v", err)
	}

	_, err := c.ExecuteQuery("INSERT INTO ins_fk_corrupt_child (id, parent_id) VALUES (1, 1)")
	if err == nil || !strings.Contains(err.Error(), "failed to decode referenced row") || !strings.Contains(err.Error(), "ins_fk_corrupt_parent") {
		t.Fatalf("expected corrupt referenced row FK error, got %v", err)
	}
}

func TestUpdateUniqueCheckReturnsCorruptRowError(t *testing.T) {
	ctx := context.Background()
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "upd_unique_corrupt_row",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText, Unique: true},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_unique_corrupt_row (id, email) VALUES (1, 'a@example.com')"); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_unique_corrupt_row (id, email) VALUES (2, 'b@example.com')"); err != nil {
		t.Fatalf("insert corrupt candidate: %v", err)
	}
	corruptKey := fmt.Sprintf("%020d", 2)
	if err := c.tableTrees["upd_unique_corrupt_row"].Put([]byte(corruptKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	affected, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_unique_corrupt_row",
		Set:   []*query.SetClause{{Column: "email", Value: strReal("c@example.com")}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "upd_unique_corrupt_row") {
		t.Fatalf("expected corrupt row unique update error, affected=%d err=%v", affected, err)
	}
}

func TestUpdateForeignKeyCheckReturnsCorruptReferencedRowError(t *testing.T) {
	ctx := context.Background()
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "upd_fk_corrupt_parent",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	}); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "upd_fk_corrupt_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "upd_fk_corrupt_parent",
			ReferencedColumns: []string{"id"},
		}},
	}); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_fk_corrupt_parent (id) VALUES (1)"); err != nil {
		t.Fatalf("insert parent 1: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_fk_corrupt_parent (id) VALUES (2)"); err != nil {
		t.Fatalf("insert parent 2: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_fk_corrupt_child (id, parent_id) VALUES (1, 1)"); err != nil {
		t.Fatalf("insert child: %v", err)
	}
	corruptKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["upd_fk_corrupt_parent"].Put([]byte(corruptKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt parent row: %v", err)
	}

	affected, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_fk_corrupt_child",
		Set:   []*query.SetClause{{Column: "parent_id", Value: numReal(2)}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "failed to decode referenced row") || !strings.Contains(err.Error(), "upd_fk_corrupt_parent") {
		t.Fatalf("expected corrupt referenced row FK update error, affected=%d err=%v", affected, err)
	}
}
