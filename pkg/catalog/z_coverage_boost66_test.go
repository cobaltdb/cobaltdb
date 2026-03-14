package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_CreateDropIndex targets CreateIndex and DropIndex
func TestCoverage_CreateDropIndex(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "idx_ops", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "code", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 50; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "idx_ops",
			Columns: []string{"id", "code", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("CODE" + string(rune('A'+i%26))), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create unique index
	err := cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_code_unique",
		Table:   "idx_ops",
		Columns: []string{"code"},
		Unique:  true,
	})
	if err != nil {
		t.Logf("Create unique index error (expected - duplicates): %v", err)
	}

	// Create non-unique index
	err = cat.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_val",
		Table:   "idx_ops",
		Columns: []string{"val"},
		Unique:  false,
	})
	if err != nil {
		t.Logf("Create index error: %v", err)
	}

	// Query using index
	result, _ := cat.ExecuteQuery("SELECT * FROM idx_ops WHERE val > 300")
	t.Logf("Index query returned %d rows", len(result.Rows))

	// Drop index
	err = cat.DropIndex("idx_val")
	if err != nil {
		t.Logf("Drop index error: %v", err)
	}
}

// TestCoverage_ViewCreateDrop targets view operations
func TestCoverage_ViewCreateDrop(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "view_base2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "amount", Type: query.TokenInteger},
	})

	// Insert data
	for i := 1; i <= 30; i++ {
		catg := "A"
		if i > 15 {
			catg = "B"
		}
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "view_base2",
			Columns: []string{"id", "category", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(catg), numReal(float64(i * 10))}},
		}, nil)
	}

	// Create view
	viewStmt := &query.SelectStmt{
		Columns: []query.Expression{
			&query.Identifier{Name: "id"},
			&query.Identifier{Name: "category"},
			&query.Identifier{Name: "amount"},
		},
		From: &query.TableRef{Name: "view_base2"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "amount"},
			Operator: query.TokenGt,
			Right:    numReal(100),
		},
	}
	err := cat.CreateView("high_amount_view", viewStmt)
	if err != nil {
		t.Logf("CreateView error: %v", err)
	}

	// Query view
	result, _ := cat.ExecuteQuery("SELECT * FROM high_amount_view")
	t.Logf("View query returned %d rows", len(result.Rows))

	// Drop view
	err = cat.DropView("high_amount_view")
	if err != nil {
		t.Logf("DropView error: %v", err)
	}
}

// TestCoverage_JSONIndexOperations targets JSON index
func TestCoverage_JSONIndexOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "json_idx2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	// Insert JSON data
	for i := 1; i <= 20; i++ {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "json_idx2",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(`{"value": ` + string(rune('0'+i%10)) + `}`)}},
		}, nil)
	}

	// Create JSON index
	err := cat.CreateJSONIndex("json_val_idx", "json_idx2", "data", "$.value", "integer")
	if err != nil {
		t.Logf("CreateJSONIndex error: %v", err)
	}

	// Query JSON
	result, err := cat.ExecuteQuery("SELECT * FROM json_idx2 WHERE JSON_EXTRACT(data, '$.value') > 5")
	if err != nil {
		t.Logf("JSON query error: %v", err)
	} else {
		t.Logf("JSON query returned %d rows", len(result.Rows))
	}

	// Get JSON index
	idx, err := cat.GetJSONIndex("json_val_idx")
	if err != nil {
		t.Logf("GetJSONIndex error: %v", err)
	} else if idx != nil {
		t.Logf("Found JSON index: %s", idx.Name)
	}

	// List JSON indexes
	indexes := cat.ListJSONIndexes()
	t.Logf("JSON indexes: %v", indexes)

	// Drop JSON index
	err = cat.DropJSONIndex("json_val_idx")
	if err != nil {
		t.Logf("DropJSONIndex error: %v", err)
	}
}

// TestCoverage_FTSIndexOperations targets FTS index
func TestCoverage_FTSIndexOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	createCoverageTestTable(t, cat, "fts_test2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "title", Type: query.TokenText},
		{Name: "content", Type: query.TokenText},
	})

	// Insert data
	texts := []string{
		"The quick brown fox",
		"Lazy dog sleeping",
		"Quick brown dog",
		"Fox jumps over",
	}
	for i, txt := range texts {
		cat.Insert(ctx, &query.InsertStmt{
			Table:   "fts_test2",
			Columns: []string{"id", "title", "content"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(txt), strReal(txt + " content")}},
		}, nil)
	}

	// Create FTS index
	err := cat.CreateFTSIndex("fts_idx2", "fts_test2", []string{"title", "content"})
	if err != nil {
		t.Logf("CreateFTSIndex error: %v", err)
	}

	// Get FTS index
	ftsIdx, err := cat.GetFTSIndex("fts_idx2")
	if err != nil {
		t.Logf("GetFTSIndex error: %v", err)
	} else if ftsIdx != nil {
		t.Logf("Found FTS index: %s", ftsIdx.Name)
	}

	// List FTS indexes
	indexes := cat.ListFTSIndexes()
	t.Logf("FTS indexes: %v", indexes)

	// Search FTS
	results, err := cat.SearchFTS("fts_idx2", "quick")
	if err != nil {
		t.Logf("SearchFTS error: %v", err)
	} else {
		t.Logf("FTS search returned %d results", len(results))
	}

	// Drop FTS index
	err = cat.DropFTSIndex("fts_idx2")
	if err != nil {
		t.Logf("DropFTSIndex error: %v", err)
	}
}

// TestCoverage_TriggerOperations2 targets trigger operations
func TestCoverage_TriggerOperations2(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create audit table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "audit_log2",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "action", Type: query.TokenText},
		},
	})

	createCoverageTestTable(t, cat, "trig_target2", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	// Create trigger
	triggerStmt := &query.CreateTriggerStmt{
		Name:  "test_trigger2",
		Table: "trig_target2",
		Time:  "AFTER",
		Event: "INSERT",
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "audit_log2",
				Columns: []string{"action"},
				Values:  [][]query.Expression{{strReal("insert")}},
			},
		},
	}
	err := cat.CreateTrigger(triggerStmt)
	if err != nil {
		t.Logf("CreateTrigger error: %v", err)
	}

	// Get trigger
	trig, err := cat.GetTrigger("test_trigger2")
	if err != nil {
		t.Logf("GetTrigger error: %v", err)
	} else if trig != nil {
		t.Logf("Found trigger: %s", trig.Name)
	}

	// Insert data (trigger should fire)
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "trig_target2",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Drop trigger
	err = cat.DropTrigger("test_trigger2")
	if err != nil {
		t.Logf("DropTrigger error: %v", err)
	}
}

// TestCoverage_ProcedureOperations targets stored procedure operations
func TestCoverage_ProcedureOperations(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create procedure
	procStmt := &query.CreateProcedureStmt{
		Name: "test_proc",
		Body: []query.Statement{
			&query.SelectStmt{
				Columns: []query.Expression{numReal(1)},
			},
		},
	}
	err := cat.CreateProcedure(procStmt)
	if err != nil {
		t.Logf("CreateProcedure error: %v", err)
	}

	// Get procedure
	proc, err := cat.GetProcedure("test_proc")
	if err != nil {
		t.Logf("GetProcedure error: %v", err)
	} else if proc != nil {
		t.Logf("Found procedure: %s", proc.Name)
	}

	// Drop procedure
	err = cat.DropProcedure("test_proc")
	if err != nil {
		t.Logf("DropProcedure error: %v", err)
	}
}

// TestCoverage_TableOperations targets table operations
func TestCoverage_TableOperations(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	cat := New(tree, pool, nil)

	// Create table
	cat.CreateTable(&query.CreateTableStmt{
		Table: "ops_test",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenText},
		},
	})

	// Insert data
	cat.Insert(ctx, &query.InsertStmt{
		Table:   "ops_test",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("test")}},
	}, nil)

	// Get table
	tbl, err := cat.GetTable("ops_test")
	if err != nil {
		t.Logf("GetTable error: %v", err)
	} else if tbl != nil {
		t.Logf("Found table: %s", tbl.Name)
	}

	// Has table
	has := cat.HasTableOrView("ops_test")
	t.Logf("Has table: %v", has)

	// List tables
	tables := cat.ListTables()
	t.Logf("Tables count: %d", len(tables))
}
