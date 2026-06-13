package catalog

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

type shortCatalogWriter struct {
	limit   int
	written int
}

func (w *shortCatalogWriter) Write(p []byte) (int, error) {
	remaining := w.limit - w.written
	if remaining <= 0 {
		return 0, nil
	}
	if len(p) > remaining {
		w.written += remaining
		return remaining, nil
	}
	w.written += len(p)
	return len(p), nil
}

// newEmptyCatalog creates a catalog with initialized maps for save/load testing
func newEmptyCatalog() *Catalog {
	return &Catalog{
		tables:            make(map[string]*TableDef),
		tableTrees:        make(map[string]btree.TreeStore),
		indexes:           make(map[string]*IndexDef),
		indexTrees:        make(map[string]btree.TreeStore),
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
	usersTree, err := btree.NewBTree(storage.NewBufferPool(4096, storage.NewMemory()))
	if err != nil {
		t.Fatalf("failed to create users tree: %v", err)
	}
	if err := usersTree.Put([]byte("user:1"), []byte("alice")); err != nil {
		t.Fatalf("failed to seed users tree: %v", err)
	}
	cat.tableTrees["users"] = usersTree

	// Create orders table
	ordersTable := &TableDef{
		Name:    "orders",
		Columns: []ColumnDef{{Name: "id", Type: "INTEGER", PrimaryKey: true}, {Name: "user_id", Type: "INTEGER"}, {Name: "amount", Type: "REAL"}},
	}
	ordersTable.buildColumnIndexCache()
	cat.tables["orders"] = ordersTable
	ordersTree, err := btree.NewBTree(storage.NewBufferPool(4096, storage.NewMemory()))
	if err != nil {
		t.Fatalf("failed to create orders tree: %v", err)
	}
	cat.tableTrees["orders"] = ordersTree

	return cat
}

func TestSaveDataAndLoadSchemaLoadData(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)
	if err := os.WriteFile(filepath.Join(tmpDir, "schema.json"), []byte("stale"), 0644); err != nil {
		t.Fatalf("seed schema.json: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "users.json"), []byte("stale"), 0644); err != nil {
		t.Fatalf("seed users.json: %v", err)
	}

	// Save catalog
	err := cat.SaveData(tmpDir)
	if err != nil {
		t.Fatalf("SaveData: %v", err)
	}

	// Verify schema.json exists
	if _, err := os.Stat(filepath.Join(tmpDir, "schema.json")); os.IsNotExist(err) {
		t.Fatal("schema.json not created")
	}
	for _, fileName := range []string{"schema.json", "users.json"} {
		info, err := os.Stat(filepath.Join(tmpDir, fileName))
		if err != nil {
			t.Fatalf("stat %s: %v", fileName, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("%s permissions = %v, want 0600", fileName, info.Mode().Perm())
		}
		matches, err := filepath.Glob(filepath.Join(tmpDir, "."+fileName+".tmp-*"))
		if err != nil {
			t.Fatalf("glob temp files for %s: %v", fileName, err)
		}
		if len(matches) != 0 {
			t.Fatalf("SaveData left temporary files for %s: %v", fileName, matches)
		}
	}

	// Load into fresh catalog
	cat2 := newEmptyCatalog()
	cat2.pool = storage.NewBufferPool(4096, storage.NewMemory())

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

	// LoadData should import the exported table data files.
	err = cat2.LoadData(tmpDir)
	if err != nil {
		t.Fatalf("LoadData: %v", err)
	}
}

func TestWriteCatalogDataFullRejectsShortWrite(t *testing.T) {
	writer := &shortCatalogWriter{limit: 3}

	n, err := writeCatalogDataFull(writer, []byte("abcdef"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeCatalogDataFull short write error = %v, want %v", err, io.ErrShortWrite)
	}
	if n != 3 {
		t.Fatalf("writeCatalogDataFull wrote %d bytes, want 3", n)
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

func TestSaveDataRejectsSymlinkDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target")
	linkDir := filepath.Join(tmpDir, "export")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("Mkdir target: %v", err)
	}
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	cat := newEmptyCatalog()
	err := cat.SaveData(linkDir)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("SaveData symlink dir error = %v, want symlink rejection", err)
	}
	if _, err := os.Stat(filepath.Join(targetDir, "schema.json")); !os.IsNotExist(err) {
		t.Fatalf("SaveData wrote through symlink, stat error = %v", err)
	}
}

func TestLoadSchemaRejectsSymlinkDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target")
	linkDir := filepath.Join(tmpDir, "import")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("Mkdir target: %v", err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, "schema.json"), []byte(`{"tables":{},"vectorIndexes":{}}`), 0600); err != nil {
		t.Fatalf("WriteFile schema: %v", err)
	}
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	cat := newEmptyCatalog()
	err := cat.LoadSchema(linkDir)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("LoadSchema symlink dir error = %v, want symlink rejection", err)
	}
}

func TestLoadDataRejectsSymlinkDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "target")
	linkDir := filepath.Join(tmpDir, "import")
	if err := os.Mkdir(targetDir, 0750); err != nil {
		t.Fatalf("Mkdir target: %v", err)
	}
	if err := os.Symlink(targetDir, linkDir); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	cat := createTestCatalogForSaveLoad(t)
	err := cat.LoadData(linkDir)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("LoadData symlink dir error = %v, want symlink rejection", err)
	}
}

func TestPrepareCatalogDataDirCreatesRestrictiveDirectory(t *testing.T) {
	exportDir := filepath.Join(t.TempDir(), "export")
	if err := prepareCatalogDataDir(exportDir, true); err != nil {
		t.Fatalf("prepareCatalogDataDir: %v", err)
	}

	info, err := os.Stat(exportDir)
	if err != nil {
		t.Fatalf("Stat export dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0750 {
		t.Fatalf("export dir mode = %o, want 750", got)
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
	cat2.pool = storage.NewBufferPool(4096, storage.NewMemory())
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
	cat2.pool = storage.NewBufferPool(4096, storage.NewMemory())
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

func TestLoadSchemaRejectsOversizedDataFile(t *testing.T) {
	tmpDir := t.TempDir()
	cat := newEmptyCatalog()
	schemaPath := filepath.Join(tmpDir, "schema.json")
	file, err := os.OpenFile(schemaPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if err := file.Truncate(maxCatalogDataFileBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate schema: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close schema: %v", err)
	}

	err = cat.LoadSchema(tmpDir)
	if err == nil || !strings.Contains(err.Error(), "catalog data file is too large") {
		t.Fatalf("expected oversized schema rejection, got %v", err)
	}
}

func TestLoadSchemaRejectsUnsafeDataFile(t *testing.T) {
	tmpDir := t.TempDir()
	cat := newEmptyCatalog()
	schemaPath := filepath.Join(tmpDir, "schema.json")
	targetPath := filepath.Join(tmpDir, "target-schema.json")
	if err := os.WriteFile(targetPath, []byte(`{"tables":{}}`), 0600); err != nil {
		t.Fatalf("write target schema: %v", err)
	}
	if err := os.Symlink(targetPath, schemaPath); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err := cat.LoadSchema(tmpDir)
	if err == nil {
		t.Fatal("expected symlink schema file to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}

	if err := os.Remove(schemaPath); err != nil {
		t.Fatalf("remove schema symlink: %v", err)
	}
	if err := os.Mkdir(schemaPath, 0750); err != nil {
		t.Fatalf("mkdir schema path: %v", err)
	}
	err = cat.LoadSchema(tmpDir)
	if err == nil {
		t.Fatal("expected directory schema file to be rejected")
	}
	if !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("expected regular file rejection, got %v", err)
	}
}

func TestLoadDataRejectsUnsafeDataFile(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)
	dataPath := filepath.Join(tmpDir, "users.json")
	targetPath := filepath.Join(tmpDir, "target-users.json")
	if err := os.WriteFile(targetPath, []byte(`{"keys":[],"values":[]}`), 0600); err != nil {
		t.Fatalf("write target data: %v", err)
	}
	if err := os.Symlink(targetPath, dataPath); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}

	err := cat.LoadData(tmpDir)
	if err == nil {
		t.Fatal("expected symlink data file to be rejected")
	}
	if !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestLoadDataRejectsOversizedDataFile(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)
	dataPath := filepath.Join(tmpDir, "users.json")
	file, err := os.OpenFile(dataPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatalf("create data: %v", err)
	}
	if err := file.Truncate(maxCatalogDataFileBytes + 1); err != nil {
		_ = file.Close()
		t.Fatalf("truncate data: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close data: %v", err)
	}

	err = cat.LoadData(tmpDir)
	if err == nil || !strings.Contains(err.Error(), "catalog data file is too large") {
		t.Fatalf("expected oversized data rejection, got %v", err)
	}
}

func TestReadCatalogDataFileUsesValidatedOpenedFile(t *testing.T) {
	tmpDir := t.TempDir()
	dataPath := filepath.Join(tmpDir, "data.json")
	want := []byte(`{"keys":[],"values":[]}`)
	if err := os.WriteFile(dataPath, want, 0644); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	got, err := readCatalogDataFile(dataPath)
	if err != nil {
		t.Fatalf("readCatalogDataFile failed: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("readCatalogDataFile = %q, want %q", got, want)
	}
	info, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("stat data file: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("data file permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestLoadDataRestrictsExistingFilePermissions(t *testing.T) {
	tmpDir := t.TempDir()
	cat := createTestCatalogForSaveLoad(t)
	dataPath := filepath.Join(tmpDir, "users.json")
	if err := os.WriteFile(dataPath, []byte(`{"keys":[],"values":[]}`), 0644); err != nil {
		t.Fatalf("write data: %v", err)
	}

	if err := cat.LoadData(tmpDir); err != nil {
		t.Fatalf("LoadData failed: %v", err)
	}
	info, err := os.Stat(dataPath)
	if err != nil {
		t.Fatalf("stat data: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("data permissions = %v, want 0600", info.Mode().Perm())
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

	// LoadData with missing tree and nil pool cannot safely import table rows.
	err = cat2.LoadData(tmpDir)
	if err == nil || !strings.Contains(err.Error(), "has no tree") {
		t.Fatalf("expected missing tree error with nil pool, got %v", err)
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
	if err == nil || !strings.Contains(err.Error(), "mismatched key/value counts") {
		t.Fatalf("expected mismatched key/value count error, got %v", err)
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
