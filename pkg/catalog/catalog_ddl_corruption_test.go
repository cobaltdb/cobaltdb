package catalog

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestAlterTableAddColumnReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE alter_add_corrupt_row (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO alter_add_corrupt_row (id, name) VALUES (1, 'alice')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["alter_add_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	err := c.AlterTableAddColumn(&query.AlterTableStmt{
		Table: "alter_add_corrupt_row",
		Column: query.ColumnDef{
			Name:    "age",
			Type:    query.TokenInteger,
			Default: &query.NumberLiteral{Value: 0},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "alter_add_corrupt_row") {
		t.Fatalf("expected corrupt row alter add error, got %v", err)
	}

	table, err := c.GetTable("alter_add_corrupt_row")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if table.GetColumnIndex("age") >= 0 {
		t.Fatal("failed ALTER ADD COLUMN should not mutate table schema")
	}
}

func TestAlterTableDropColumnReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE alter_drop_corrupt_row (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO alter_drop_corrupt_row (id, name, age) VALUES (1, 'alice', 30)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	pkKey := fmt.Sprintf("%020d", 1)
	if err := c.tableTrees["alter_drop_corrupt_row"].Put([]byte(pkKey), []byte("not json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	err := c.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alter_drop_corrupt_row",
		NewName: "age",
		Column:  query.ColumnDef{Name: "age"},
	})
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "alter_drop_corrupt_row") {
		t.Fatalf("expected corrupt row alter drop error, got %v", err)
	}

	table, err := c.GetTable("alter_drop_corrupt_row")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}
	if table.GetColumnIndex("age") < 0 {
		t.Fatal("failed ALTER DROP COLUMN should not mutate table schema")
	}
}
