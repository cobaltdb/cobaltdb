package catalog

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestInsertRollbackReportsRowCleanupFailure(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "insert_rb_fail",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText, NotNull: true},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	c.tableTrees["insert_rb_fail"] = &deleteFailTree{
		TreeStore: c.tableTrees["insert_rb_fail"],
		err:       errors.New("delete failed"),
	}

	_, _, err := c.Insert(context.Background(), &query.InsertStmt{
		Table:   "insert_rb_fail",
		Columns: []string{"id", "name"},
		Values: [][]query.Expression{
			{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "ok"}},
			{&query.NumberLiteral{Value: 2}, &query.NullLiteral{}},
		},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "rollback failed") || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected rollback delete failure, got %v", err)
	}
}
