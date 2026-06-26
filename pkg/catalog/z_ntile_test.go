package catalog

import (
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func createCatalogForNtile(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	catalogTree, _ := btree.NewBTree(pool)
	return &Catalog{
		tables:            make(map[string]*TableDef),
		tableTrees:        make(map[string]btree.TreeStore),
		indexes:           make(map[string]*IndexDef),
		indexTrees:        make(map[string]btree.TreeStore),
		tree:              catalogTree,
		pool:              pool,
		views:             make(map[string]*query.SelectStmt),
		triggers:          make(map[string]*query.CreateTriggerStmt),
		procedures:        make(map[string]*query.CreateProcedureStmt),
		materializedViews: make(map[string]*MaterializedViewDef),
		ftsIndexes:        make(map[string]*FTSIndexDef),
		jsonIndexes:       make(map[string]*JSONIndexDef),
		vectorIndexes:     make(map[string]*VectorIndexDef),
		stats:             make(map[string]*StatsTableStats),
	}
}

func TestNtile_Basic(t *testing.T) {
	c := createCatalogForNtile(t)

	_, err := c.ExecuteQuery("CREATE TABLE ntile_test (id INTEGER PRIMARY KEY, grp INTEGER, val INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert using numeric values to avoid string quoting issues
	for i := 1; i <= 4; i++ {
		grp := 1
		if i > 2 {
			grp = 2
		}
		sql := fmt.Sprintf("INSERT INTO ntile_test VALUES (%d, %d, %d)", i, grp, i*10)
		_, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	result, err := c.ExecuteQuery("SELECT id, NTILE(2) OVER (ORDER BY id) AS bucket FROM ntile_test")
	if err != nil {
		t.Fatalf("NTILE query: %v", err)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}

	// NTILE(2) with 4 rows ordered by id should give: 1,1,2,2
	expected := []int64{1, 1, 2, 2}
	for i := range result.Rows {
		bucketIdx := len(result.Rows[i]) - 1
		if bucketIdx < 0 || bucketIdx >= len(result.Rows[i]) {
			t.Fatalf("row %d: bucket index out of range: %v", i, result.Rows[i][bucketIdx])
		}
		got, ok := result.Rows[i][bucketIdx].(int64)
		if !ok {
			t.Errorf("row %d: expected int64 bucket, got %T", i, result.Rows[i][bucketIdx])
		}
		if got != expected[i] {
			t.Errorf("row %d: expected NTILE=%d, got %d", i, expected[i], got)
		}
	}
}

func TestNtile_RemainderDistribution(t *testing.T) {
	c := createCatalogForNtile(t)

	if _, err := c.ExecuteQuery("CREATE TABLE ntile_rem (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 10; i++ {
		if _, err := c.ExecuteQuery(fmt.Sprintf("INSERT INTO ntile_rem VALUES (%d)", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	result, err := c.ExecuteQuery("SELECT id, NTILE(4) OVER (ORDER BY id) AS bucket FROM ntile_rem")
	if err != nil {
		t.Fatalf("NTILE query: %v", err)
	}
	// 10 rows / 4 buckets: remainder 2 → first two buckets have 3 rows, last two have 2.
	// Expected bucket sequence: 1,1,1,2,2,2,3,3,4,4
	expected := []int64{1, 1, 1, 2, 2, 2, 3, 3, 4, 4}
	if len(result.Rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(result.Rows))
	}
	for i := range result.Rows {
		bucketIdx := len(result.Rows[i]) - 1
		got, ok := result.Rows[i][bucketIdx].(int64)
		if !ok {
			t.Fatalf("row %d: expected int64 bucket, got %T", i, result.Rows[i][bucketIdx])
		}
		if got != expected[i] {
			t.Errorf("row %d: expected NTILE=%d, got %d", i, expected[i], got)
		}
	}
}

func TestLastValue_RunningFrame(t *testing.T) {
	c := createCatalogForNtile(t)

	if _, err := c.ExecuteQuery("CREATE TABLE lv_test (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 4; i++ {
		if _, err := c.ExecuteQuery(fmt.Sprintf("INSERT INTO lv_test VALUES (%d)", i)); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// With ORDER BY and the default frame, LAST_VALUE is the running last value
	// (current row), i.e. 1,2,3,4 — not the partition max for every row.
	result, err := c.ExecuteQuery("SELECT id, LAST_VALUE(id) OVER (ORDER BY id) AS lv FROM lv_test")
	if err != nil {
		t.Fatalf("LAST_VALUE query: %v", err)
	}
	expected := []int64{1, 2, 3, 4}
	if len(result.Rows) != len(expected) {
		t.Fatalf("expected %d rows, got %d", len(expected), len(result.Rows))
	}
	for i := range result.Rows {
		lvIdx := len(result.Rows[i]) - 1
		got, ok := result.Rows[i][lvIdx].(int64)
		if !ok {
			t.Fatalf("row %d: expected int64, got %T", i, result.Rows[i][lvIdx])
		}
		if got != expected[i] {
			t.Errorf("row %d: expected LAST_VALUE=%d, got %d", i, expected[i], got)
		}
	}
}

func TestNtile_WithPartition(t *testing.T) {
	c := createCatalogForNtile(t)

	_, err := c.ExecuteQuery("CREATE TABLE ntile_part (id INTEGER PRIMARY KEY, grp INTEGER, val INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	for i := 1; i <= 6; i++ {
		grp := 1
		if i > 3 {
			grp = 2
		}
		sql := fmt.Sprintf("INSERT INTO ntile_part VALUES (%d, %d, %d)", i, grp, i*10)
		_, err := c.ExecuteQuery(sql)
		if err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	result, err := c.ExecuteQuery("SELECT id, grp, NTILE(2) OVER (PARTITION BY grp ORDER BY id) AS bucket FROM ntile_part")
	if err != nil {
		t.Fatalf("NTILE partition query: %v", err)
	}
	if len(result.Rows) != 6 {
		t.Fatalf("expected 6 rows, got %d", len(result.Rows))
	}

	t.Logf("NTILE with partition results:")
	for i, row := range result.Rows {
		t.Logf("  row %d: %v", i, row)
	}
}
