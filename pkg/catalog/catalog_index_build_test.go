package catalog

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestPopulateIndexReturnsPutError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "idx_populate_err",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "email", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if _, _, err := c.Insert(context.Background(), &query.InsertStmt{
		Table:   "idx_populate_err",
		Columns: []string{"id", "email"},
		Values: [][]query.Expression{{
			&query.NumberLiteral{Value: 1},
			&query.StringLiteral{Value: "a@example.com"},
		}},
	}, nil); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	table := c.tables["idx_populate_err"]
	indexDef := &IndexDef{Name: "idx_populate_err_email", TableName: "idx_populate_err", Columns: []string{"email"}}
	indexErr := errors.New("index put failed")
	err := c.populateIndexLocked(&putFailTree{TreeStore: c.tree, err: indexErr}, indexDef, table, c.tableTrees["idx_populate_err"])
	if err == nil || !strings.Contains(err.Error(), "index put failed") {
		t.Fatalf("expected index put error, got %v", err)
	}
}
