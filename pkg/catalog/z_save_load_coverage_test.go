package catalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// newEmptyCatalog creates a catalog with initialized maps for save/load testing
func newEmptyCatalog() *Catalog {
	return &Catalog{
		tables:            make(map[string]*TableDef),
		tableTrees:        make(map[string]*btree.BTree),
		indexes:           make(map[string]*IndexDef),
		indexTrees:        make(map[string]*btree.BTree),
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

// createTestCatalogForSaveLoad creates a catalog with tables for save/load tests
func createTestCatalogForSaveLoad(t *testing.T) *Catalog {
	t.Helper()
	cat := newEmptyCatalog()

	// Create users table
	usersTable := &TableDef{
		Name:    "users",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER", PrimaryKey: true}, {Name: "name", Type: "TEXT"}, {Name: "age", Type: "INTEGER"}},
	}
	usersTable.buildColumnIndexCache()
	cat.tables["users"] = usersTable

	// Create orders table
	ordersTable := &TableDef{
		Name:    "orders",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER", PrimaryKey: true}, {Name: "user_id", Type: "INTEGER"}, {Name: "amount", Type: "REAL"}},
	}
	ordersTable.buildColumnIndexCache()
	cat.tables["orders"] = ordersTable

	return cat
}

func TestSaveDataAndLoadSchemaLoadData(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)

	// Save catalog
	err := cat.SaveData(tmpDir)
	if err != nil {
		t.Fatalf("SaveData: %v", err)
	}

	// Verify schema.json exists
	if _, err := os.Stat(filepath.Join(tmpDir, "schema.json")); os.IsNotExist(err) {
		t.Fatal("schema.json not created")
	}

	// Load into fresh catalog
	cat2 := newEmptyCatalog()

	err = cat2.LoadSchema(tmpDir)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}

	// Verify tables loaded
	if len(cat2.tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(cat2.tables))
	}
	if _, ok := cat2.tables["users"]; !ok {
		t.Fatal("users table not loaded")
	}
	if _, ok := cat2.tables["orders"]; !ok {
		t.Fatal("orders table not loaded")
	}

	// Verify columns
	users := cat2.tables["users"]
	if len(users.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(users.Columns))
	}
	if users.Columns[0].Name != "id" {
		t.Errorf("expected first column 'id', got %s", users.Columns[0].Name)
	}

	// LoadData (no table data files) — should succeed
	err = cat2.LoadData(tmpDir)
	if err != nil {
		t.Fatalf("LoadData no data: %v", err)
	}
}

func TestLoadSchemaNonexistentDir(t *testing.T) {
	cat := newEmptyCatalog()
	err := cat.LoadSchema("/nonexistent/path/that/does/not/exist")
	if err != nil {
		t.Fatalf("LoadSchema on nonexistent dir should return nil, got: %v", err)
	}
}

func TestLoadSchemaEmptyDir(t *testing.T) {
	tmpDir := t.TempDir()
	cat := newEmptyCatalog()
	err := cat.LoadSchema(tmpDir)
	if err != nil {
		t.Fatalf("LoadSchema on empty dir: %v", err)
	}
}

func TestLoadDataNonexistentDir(t *testing.T) {
	cat := newEmptyCatalog()
	err := cat.LoadData("/nonexistent/path/that/does/not/exist")
	if err != nil {
		t.Fatalf("LoadData on nonexistent dir should succeed, got: %v", err)
	}
}

func TestSaveDataInvalidDir(t *testing.T) {
	cat := newEmptyCatalog()
	err := cat.SaveData("/dev/null/impossible/path")
	if err == nil {
		t.Fatal("expected error for invalid dir")
	}
}

func TestLoadSchemaWithDefaults(t *testing.T) {
	tmpDir := t.TempDir()
	cat := newEmptyCatalog()

	// Create table with default and check expression
	tableDef := &TableDef{
		Name: "products",
		Columns: []ColumnDef{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "price", Type: "REAL", Default: "99.99"},
			{Name: "active", Type: "INTEGER", CheckStr: "price > 0"},
		},
	}
	tableDef.buildColumnIndexCache()
	cat.tables["products"] = tableDef

	err := cat.SaveData(tmpDir)
	if err != nil {
		t.Fatalf("SaveData: %v", err)
	}

	// Load into fresh catalog
	cat2 := newEmptyCatalog()
	err = cat2.LoadSchema(tmpDir)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}

	products := cat2.tables["products"]
	if products == nil {
		t.Fatal("products table not loaded")
	}
	if products.Columns[1].Default != "99.99" {
		t.Errorf("expected default '99.99', got %s", products.Columns[1].Default)
	}
	if products.Columns[2].CheckStr != "price > 0" {
		t.Errorf("expected check 'price > 0', got %s", products.Columns[2].CheckStr)
	}
}

func TestSaveDataLoadDataRoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)

	err := cat.SaveData(tmpDir)
	if err != nil {
		t.Fatalf("SaveData: %v", err)
	}

	cat2 := newEmptyCatalog()
	err = cat2.LoadSchema(tmpDir)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}

	err = cat2.LoadData(tmpDir)
	if err != nil {
		t.Fatalf("LoadData: %v", err)
	}

	if len(cat2.tables) != len(cat.tables) {
		t.Fatalf("table count mismatch: expected %d, got %d", len(cat.tables), len(cat2.tables))
	}
}

func TestLoadDataCorruptFile(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)

	err := os.WriteFile(filepath.Join(tmpDir, "users.json"), []byte("{invalid json"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = cat.LoadData(tmpDir)
	if err == nil {
		t.Fatal("expected error for corrupt JSON data file")
	}
}

func TestLoadSchemaCorruptJSON(t *testing.T) {
	tmpDir := t.TempDir()
	cat := newEmptyCatalog()

	err := os.WriteFile(filepath.Join(tmpDir, "schema.json"), []byte("{not valid json"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = cat.LoadSchema(tmpDir)
	if err == nil {
		t.Fatal("expected error for corrupt schema.json")
	}
}

func TestLoadDataWithNilPool(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a catalog with pool and insert data, then save
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	c.Insert(context.Background(), &query.InsertStmt{
		Table:   "items",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("a")}},
	}, nil)

	err := c.SaveData(tmpDir)
	if err != nil {
		t.Fatalf("SaveData: %v", err)
	}

	// Load into fresh catalog without pool (tableTrees will be missing)
	cat2 := newEmptyCatalog()
	err = cat2.LoadSchema(tmpDir)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}

	// LoadData with missing tree and nil pool should continue without error
	err = cat2.LoadData(tmpDir)
	if err != nil {
		t.Fatalf("LoadData with nil pool should succeed, got: %v", err)
	}
}

func TestLoadDataKeysLongerThanValues(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)

	// Write a data file where keys array is longer than values array.
	// Values must be base64-encoded because [][]byte marshals to base64 strings.
	data := []byte(`{"keys":["AQ==","Ag==","Aw=="],"values":["YQ==","Yg=="]}`)
	err := os.WriteFile(filepath.Join(tmpDir, "users.json"), data, 0644)
	if err != nil {
		t.Fatal(err)
	}

	err = cat.LoadData(tmpDir)
	if err != nil {
		t.Fatalf("LoadData keys>values should succeed, got: %v", err)
	}
}

func TestLoadSchemaWithPool(t *testing.T) {
	tmpDir := t.TempDir()

	// Create catalog with pool, table, and data, then save
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})
	c.Insert(context.Background(), &query.InsertStmt{
		Table:   "products",
		Columns: []string{"id", "name"},
		Values:  [][]query.Expression{{numReal(1), strReal("widget")}},
	}, nil)

	err := c.SaveData(tmpDir)
	if err != nil {
		t.Fatalf("SaveData: %v", err)
	}

	// Load into fresh catalog that has a pool so trees are created
	cat2 := newEmptyCatalog()
	cat2.pool = pool

	err = cat2.LoadSchema(tmpDir)
	if err != nil {
		t.Fatalf("LoadSchema with pool: %v", err)
	}

	if _, ok := cat2.tableTrees["products"]; !ok {
		t.Fatal("expected products tree to be created when pool is present")
	}
}

func TestSaveNormalPath(t *testing.T) {
	c := newTestCatalog(t)
	c.CreateTable(&query.CreateTableStmt{
		Table: "t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	err := c.Save()
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
}

func TestLoadNormalPath(t *testing.T) {
	c := newTestCatalog(t)
	c.CreateTable(&query.CreateTableStmt{
		Table: "t",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	})
	c.Insert(context.Background(), &query.InsertStmt{
		Table:   "t",
		Columns: []string{"id"},
		Values:  [][]query.Expression{{numReal(1)}},
	}, nil)
	err := c.Save()
	if err != nil {
		t.Fatal(err)
	}
	err = c.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if _, ok := c.tables["t"]; !ok {
		t.Fatal("expected table t to be loaded")
	}
}

