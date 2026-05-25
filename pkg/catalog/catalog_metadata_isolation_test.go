package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newMetadataIsolationCatalog(t *testing.T) (*Catalog, *storage.BufferPool) {
	t.Helper()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, err := btree.NewBTree(pool)
	if err != nil {
		pool.Close()
		t.Fatalf("NewBTree: %v", err)
	}
	return New(tree, pool, nil), pool
}

func TestGetTableReturnsIsolatedDefinition(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "meta_users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	table, err := c.GetTable("meta_users")
	if err != nil {
		t.Fatalf("GetTable: %v", err)
	}
	table.Columns[0].Name = "mutated"
	table.PrimaryKey[0] = "mutated_pk"

	tableAgain, err := c.GetTable("meta_users")
	if err != nil {
		t.Fatalf("GetTable second read: %v", err)
	}
	if tableAgain.Columns[0].Name != "id" {
		t.Fatalf("GetTable returned mutable columns: %+v", tableAgain.Columns)
	}
	if tableAgain.PrimaryKey[0] != "id" {
		t.Fatalf("GetTable returned mutable primary key: %+v", tableAgain.PrimaryKey)
	}
	if tableAgain.GetColumnIndex("id") != 0 {
		t.Fatalf("GetTable clone did not rebuild column index cache")
	}
}

func TestGetIndexReturnsIsolatedDefinition(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "meta_idx_users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_meta_name",
		Table:   "meta_idx_users",
		Columns: []string{"name"},
	}); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	index, err := c.GetIndex("idx_meta_name")
	if err != nil {
		t.Fatalf("GetIndex: %v", err)
	}
	index.Columns[0] = "mutated"

	indexAgain, err := c.GetIndex("idx_meta_name")
	if err != nil {
		t.Fatalf("GetIndex second read: %v", err)
	}
	if indexAgain.Columns[0] != "name" {
		t.Fatalf("GetIndex returned mutable columns: %+v", indexAgain.Columns)
	}
}

func TestGetForeignTableReturnsIsolatedDefinition(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()
	c.fdwRegistry = nil

	if err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table:   "meta_ft",
		Wrapper: "csv",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
		},
		Options: map[string]string{"file": "/tmp/meta.csv"},
	}); err != nil {
		t.Fatalf("CreateForeignTable: %v", err)
	}

	ft, err := c.GetForeignTable("meta_ft")
	if err != nil {
		t.Fatalf("GetForeignTable: %v", err)
	}
	ft.Columns[0].Name = "mutated"
	ft.Options["file"] = "/tmp/mutated.csv"

	ftAgain, err := c.GetForeignTable("meta_ft")
	if err != nil {
		t.Fatalf("GetForeignTable second read: %v", err)
	}
	if ftAgain.Columns[0].Name != "id" {
		t.Fatalf("GetForeignTable returned mutable columns: %+v", ftAgain.Columns)
	}
	if ftAgain.Options["file"] != "/tmp/meta.csv" {
		t.Fatalf("GetForeignTable returned mutable options: %+v", ftAgain.Options)
	}
}

func TestGetMaterializedViewReturnsIsolatedDefinition(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	c.materializedViews["mv_meta"] = &MaterializedViewDef{
		Name:  "mv_meta",
		Query: &query.SelectStmt{Columns: []query.Expression{&query.Identifier{Name: "id"}}},
		Data: []map[string]interface{}{
			{"id": int64(1), "payload": []byte("alpha")},
		},
	}

	mv, err := c.GetMaterializedView("mv_meta")
	if err != nil {
		t.Fatalf("GetMaterializedView: %v", err)
	}
	mv.Query.Columns[0] = &query.Identifier{Name: "mutated"}
	mv.Data[0]["id"] = int64(2)
	mv.Data[0]["payload"].([]byte)[0] = 'z'

	mvAgain, err := c.GetMaterializedView("mv_meta")
	if err != nil {
		t.Fatalf("GetMaterializedView second read: %v", err)
	}
	if mvAgain.Query.Columns[0].(*query.Identifier).Name != "id" {
		t.Fatalf("GetMaterializedView returned mutable query: %+v", mvAgain.Query.Columns[0])
	}
	if mvAgain.Data[0]["id"] != int64(1) {
		t.Fatalf("GetMaterializedView returned mutable data map: %+v", mvAgain.Data)
	}
	if string(mvAgain.Data[0]["payload"].([]byte)) != "alpha" {
		t.Fatalf("GetMaterializedView returned mutable row bytes: %+v", mvAgain.Data[0]["payload"])
	}
}

func TestTriggerAndProcedureGettersReturnIsolatedDefinitions(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "meta_events",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if err := c.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "tr_meta",
		Table: "meta_events",
		Time:  "BEFORE",
		Event: "INSERT",
		Body:  []query.Statement{&query.CommitStmt{}},
	}); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	trigger, err := c.GetTrigger("tr_meta")
	if err != nil {
		t.Fatalf("GetTrigger: %v", err)
	}
	trigger.Body[0] = &query.RollbackStmt{}

	triggerAgain, err := c.GetTrigger("tr_meta")
	if err != nil {
		t.Fatalf("GetTrigger second read: %v", err)
	}
	if _, ok := triggerAgain.Body[0].(*query.CommitStmt); !ok {
		t.Fatalf("GetTrigger returned mutable body: %T", triggerAgain.Body[0])
	}

	if err := c.CreateProcedure(&query.CreateProcedureStmt{
		Name:   "proc_meta",
		Params: []*query.ParamDef{{Name: "p_id", Type: query.TokenInteger}},
		Body:   []query.Statement{&query.CommitStmt{}},
	}); err != nil {
		t.Fatalf("CreateProcedure: %v", err)
	}
	proc, err := c.GetProcedure("proc_meta")
	if err != nil {
		t.Fatalf("GetProcedure: %v", err)
	}
	proc.Params[0].Name = "mutated"
	proc.Body[0] = &query.RollbackStmt{}

	procAgain, err := c.GetProcedure("proc_meta")
	if err != nil {
		t.Fatalf("GetProcedure second read: %v", err)
	}
	if procAgain.Params[0].Name != "p_id" {
		t.Fatalf("GetProcedure returned mutable params: %+v", procAgain.Params[0])
	}
	if _, ok := procAgain.Body[0].(*query.CommitStmt); !ok {
		t.Fatalf("GetProcedure returned mutable body: %T", procAgain.Body[0])
	}
}
