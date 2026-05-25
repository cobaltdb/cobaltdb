package catalog

import (
	"encoding/json"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"os"
	"strings"
	"time"
)

func (c *Catalog) ListTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tables := make([]string, 0, len(c.tables))
	for name := range c.tables {
		tables = append(tables, name)
	}
	return tables
}

func (c *Catalog) Save() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Save table definitions to catalog tree
	for _, tableDef := range c.tables {
		if err := c.storeTableDef(tableDef); err != nil {
			return fmt.Errorf("failed to save table definition %s: %w", tableDef.Name, err)
		}
	}

	for _, indexDef := range c.indexes {
		if err := c.storeIndexDef(indexDef); err != nil {
			return fmt.Errorf("failed to save index definition %s: %w", indexDef.Name, err)
		}
	}

	for viewName, viewQuery := range c.views {
		sql := c.viewSQL[viewName]
		if strings.TrimSpace(sql) == "" {
			sql = createViewSQL(viewName, viewQuery)
		}
		if err := c.storeViewDef(viewName, sql); err != nil {
			return fmt.Errorf("failed to save view definition %s: %w", viewName, err)
		}
	}

	for triggerName, trigger := range c.triggers {
		sql := c.triggerSQL[triggerName]
		if strings.TrimSpace(sql) == "" {
			sql = createTriggerSQL(trigger)
		}
		if err := c.storeTriggerDef(triggerName, sql); err != nil {
			return fmt.Errorf("failed to save trigger definition %s: %w", triggerName, err)
		}
	}

	for procedureName, procedure := range c.procedures {
		sql := c.procedureSQL[procedureName]
		if strings.TrimSpace(sql) == "" {
			sql = createProcedureSQL(procedure)
		}
		if err := c.storeProcedureDef(procedureName, sql); err != nil {
			return fmt.Errorf("failed to save procedure definition %s: %w", procedureName, err)
		}
	}

	for materializedViewName, materializedView := range c.materializedViews {
		sql := c.materializedViewSQL[materializedViewName]
		if strings.TrimSpace(sql) == "" {
			sql = createMaterializedViewSQL(materializedViewName, materializedView.Query)
		}
		if err := c.storeMaterializedViewDef(materializedViewName, sql, materializedView); err != nil {
			return fmt.Errorf("failed to save materialized view definition %s: %w", materializedViewName, err)
		}
	}

	// Save vector index definitions
	for _, vid := range c.vectorIndexes {
		if err := c.storeVectorIndexDef(vid); err != nil {
			return fmt.Errorf("failed to save vector index definition %s: %w", vid.Name, err)
		}
	}

	// Flush the catalog B+Tree's in-memory data to its page
	if c.tree != nil {
		if err := c.tree.Flush(); err != nil {
			return fmt.Errorf("failed to flush catalog tree: %w", err)
		}
	}

	// Flush all table B+Trees to their pages
	if err := c.flushTableTreesLocked(); err != nil {
		return fmt.Errorf("failed to flush table trees: %w", err)
	}

	if err := c.flushIndexTreesLocked(); err != nil {
		return fmt.Errorf("failed to flush index trees: %w", err)
	}

	// Flush buffer pool to ensure all pages are written to disk
	if c.pool != nil {
		if err := c.pool.FlushAll(); err != nil {
			return fmt.Errorf("failed to flush buffer pool: %w", err)
		}
	}

	return nil
}

type persistedSQLDef struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

type persistedMaterializedViewDef struct {
	Name        string                   `json:"name"`
	SQL         string                   `json:"sql"`
	Data        []map[string]interface{} `json:"data"`
	LastRefresh int64                    `json:"last_refresh"`
}

func (c *Catalog) storeViewDef(name string, sql string) error {
	return c.storeSQLDef("view:"+name, name, sql)
}

func (c *Catalog) storeTriggerDef(name string, sql string) error {
	return c.storeSQLDef("trg:"+name, name, sql)
}

func (c *Catalog) storeProcedureDef(name string, sql string) error {
	return c.storeSQLDef("proc:"+name, name, sql)
}

func (c *Catalog) storeMaterializedViewDef(name string, sql string, mv *MaterializedViewDef) error {
	def := persistedMaterializedViewDef{
		Name: name,
		SQL:  strings.TrimSpace(sql),
	}
	if mv != nil {
		def.Data = mv.Data
		def.LastRefresh = mv.LastRefresh.UnixNano()
	}
	data, err := json.Marshal(def)
	if err != nil {
		return err
	}
	if c.tree != nil {
		return c.tree.Put([]byte("mv:"+name), data)
	}
	return nil
}

func (c *Catalog) storeSQLDef(key string, name string, sql string) error {
	data, err := json.Marshal(persistedSQLDef{Name: name, SQL: strings.TrimSpace(sql)})
	if err != nil {
		return err
	}
	if c.tree != nil {
		return c.tree.Put([]byte(key), data)
	}
	return nil
}

func (c *Catalog) storeVectorIndexDef(vid *VectorIndexDef) error {
	key := []byte("vec:" + vid.Name)
	data, err := json.Marshal(vid)
	if err != nil {
		return err
	}
	if c.tree != nil {
		return c.tree.Put(key, data)
	}
	return nil
}

func (c *Catalog) Load() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tree == nil {
		return nil
	}
	if c.tables == nil {
		c.tables = make(map[string]*TableDef)
	}
	if c.tableTrees == nil {
		c.tableTrees = make(map[string]btree.TreeStore)
	}
	if c.indexes == nil {
		c.indexes = make(map[string]*IndexDef)
	}
	if c.indexTrees == nil {
		c.indexTrees = make(map[string]btree.TreeStore)
	}
	if c.views == nil {
		c.views = make(map[string]*query.SelectStmt)
	}
	if c.triggers == nil {
		c.triggers = make(map[string]*query.CreateTriggerStmt)
	}
	if c.procedures == nil {
		c.procedures = make(map[string]*query.CreateProcedureStmt)
	}
	if c.materializedViews == nil {
		c.materializedViews = make(map[string]*MaterializedViewDef)
	}

	// Load table definitions from catalog tree
	// Table data is loaded on-demand via the buffer pool
	iter, err := c.tree.Scan([]byte("tbl:"), []byte("tbl;"))
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.HasNext() {
		keyStr, value, err := iter.NextString()
		if err != nil {
			break
		}

		// Parse key to get table name
		if !strings.HasPrefix(keyStr, "tbl:") {
			continue
		}
		tableName := strings.TrimPrefix(keyStr, "tbl:")

		// Unmarshal table definition
		var tableDef TableDef
		if err := json.Unmarshal(value, &tableDef); err != nil {
			continue
		}

		// Restore DEFAULT and CHECK expressions from persisted strings
		for i := range tableDef.Columns {
			if tableDef.Columns[i].Default != "" && tableDef.Columns[i].defaultExpr == nil {
				if parsed, err := query.ParseExpression(tableDef.Columns[i].Default); err == nil {
					tableDef.Columns[i].defaultExpr = parsed
				}
			}
			if tableDef.Columns[i].CheckStr != "" && tableDef.Columns[i].Check == nil {
				if parsed, err := query.ParseExpression(tableDef.Columns[i].CheckStr); err == nil {
					tableDef.Columns[i].Check = parsed
				}
			}
		}

		c.tables[tableName] = &tableDef

		// Create or open B+Tree for the table
		if tableDef.RootPageID != 0 {
			tree, err := btree.OpenBTreeStrict(c.pool, tableDef.RootPageID)
			if err != nil {
				return fmt.Errorf("load catalog: failed to open tree for table %s: %w", tableName, err)
			}
			c.tableTrees[tableName] = tree
		} else {
			tree, err := btree.NewBTree(c.pool)
			if err != nil {
				continue
			}
			tableDef.RootPageID = tree.RootPageID()
			c.tableTrees[tableName] = tree
		}

		// Build column index cache
		tableDef.buildColumnIndexCache()
	}

	// Load regular B-tree index definitions after tables so orphaned/corrupt
	// metadata cannot resurrect indexes for missing tables.
	idxIter, err := c.tree.Scan([]byte("idx:"), []byte("idx;"))
	if err == nil {
		for idxIter.HasNext() {
			keyStr, value, err := idxIter.NextString()
			if err != nil {
				break
			}
			if !strings.HasPrefix(keyStr, "idx:") {
				continue
			}

			var indexDef IndexDef
			if err := json.Unmarshal(value, &indexDef); err != nil {
				continue
			}
			if indexDef.Name == "" {
				indexDef.Name = strings.TrimPrefix(keyStr, "idx:")
			}
			table, tableExists := c.tables[indexDef.TableName]
			if !tableExists {
				continue
			}

			var indexTree btree.TreeStore
			if indexDef.RootPageID != 0 {
				indexTree, err = btree.OpenBTreeStrict(c.pool, indexDef.RootPageID)
				if err != nil {
					return fmt.Errorf("load catalog: failed to open index %s: %w", indexDef.Name, err)
				}
			} else {
				indexTree, err = btree.NewBTree(c.pool)
				if err != nil {
					return fmt.Errorf("load catalog: failed to create index %s: %w", indexDef.Name, err)
				}
				indexDef.RootPageID = indexTree.RootPageID()
				if tableTree := c.tableTrees[indexDef.TableName]; tableTree != nil {
					c.populateIndexLocked(indexTree, &indexDef, table, tableTree)
				}
			}

			c.indexes[indexDef.Name] = &indexDef
			c.indexTrees[indexDef.Name] = indexTree
		}
		idxIter.Close()
	}

	viewIter, err := c.tree.Scan([]byte("view:"), []byte("view;"))
	if err == nil {
		if c.viewSQL == nil {
			c.viewSQL = make(map[string]string)
		}
		for viewIter.HasNext() {
			keyStr, value, err := viewIter.NextString()
			if err != nil {
				break
			}
			if !strings.HasPrefix(keyStr, "view:") {
				continue
			}
			def := decodePersistedSQLDef(keyStr, "view:", value)
			if def.SQL == "" {
				continue
			}
			parsed, err := query.Parse(def.SQL)
			if err != nil {
				continue
			}
			viewStmt, ok := parsed.(*query.CreateViewStmt)
			if !ok || viewStmt.Query == nil {
				continue
			}
			name := viewStmt.Name
			if name == "" {
				name = def.Name
			}
			c.views[name] = viewStmt.Query
			c.viewSQL[name] = strings.TrimSpace(def.SQL)
		}
		viewIter.Close()
	}

	triggerIter, err := c.tree.Scan([]byte("trg:"), []byte("trg;"))
	if err == nil {
		if c.triggerSQL == nil {
			c.triggerSQL = make(map[string]string)
		}
		for triggerIter.HasNext() {
			keyStr, value, err := triggerIter.NextString()
			if err != nil {
				break
			}
			if !strings.HasPrefix(keyStr, "trg:") {
				continue
			}
			def := decodePersistedSQLDef(keyStr, "trg:", value)
			if def.SQL == "" {
				continue
			}
			parsed, err := query.Parse(def.SQL)
			if err != nil {
				continue
			}
			triggerStmt, ok := parsed.(*query.CreateTriggerStmt)
			if !ok {
				continue
			}
			if _, tableOK := c.tables[triggerStmt.Table]; !tableOK {
				if _, viewOK := c.views[triggerStmt.Table]; !viewOK {
					continue
				}
			}
			name := triggerStmt.Name
			if name == "" {
				name = def.Name
			}
			triggerStmt.RawSQL = strings.TrimSpace(def.SQL)
			c.triggers[name] = triggerStmt
			c.triggerSQL[name] = triggerStmt.RawSQL
		}
		triggerIter.Close()
	}

	procedureIter, err := c.tree.Scan([]byte("proc:"), []byte("proc;"))
	if err == nil {
		if c.procedureSQL == nil {
			c.procedureSQL = make(map[string]string)
		}
		for procedureIter.HasNext() {
			keyStr, value, err := procedureIter.NextString()
			if err != nil {
				break
			}
			if !strings.HasPrefix(keyStr, "proc:") {
				continue
			}
			def := decodePersistedSQLDef(keyStr, "proc:", value)
			if def.SQL == "" {
				continue
			}
			parsed, err := query.Parse(def.SQL)
			if err != nil {
				continue
			}
			procedureStmt, ok := parsed.(*query.CreateProcedureStmt)
			if !ok {
				continue
			}
			name := procedureStmt.Name
			if name == "" {
				name = def.Name
			}
			procedureStmt.RawSQL = strings.TrimSpace(def.SQL)
			c.procedures[name] = procedureStmt
			c.procedureSQL[name] = procedureStmt.RawSQL
		}
		procedureIter.Close()
	}

	materializedViewIter, err := c.tree.Scan([]byte("mv:"), []byte("mv;"))
	if err == nil {
		if c.materializedViewSQL == nil {
			c.materializedViewSQL = make(map[string]string)
		}
		for materializedViewIter.HasNext() {
			keyStr, value, err := materializedViewIter.NextString()
			if err != nil {
				break
			}
			if !strings.HasPrefix(keyStr, "mv:") {
				continue
			}
			var def persistedMaterializedViewDef
			if err := json.Unmarshal(value, &def); err != nil {
				continue
			}
			if def.Name == "" {
				def.Name = strings.TrimPrefix(keyStr, "mv:")
			}
			def.SQL = strings.TrimSpace(def.SQL)
			if def.SQL == "" {
				continue
			}
			parsed, err := query.Parse(def.SQL)
			if err != nil {
				continue
			}
			materializedViewStmt, ok := parsed.(*query.CreateMaterializedViewStmt)
			if !ok || materializedViewStmt.Query == nil {
				continue
			}
			name := materializedViewStmt.Name
			if name == "" {
				name = def.Name
			}
			lastRefresh := time.Time{}
			if def.LastRefresh != 0 {
				lastRefresh = time.Unix(0, def.LastRefresh)
			}
			c.materializedViews[name] = &MaterializedViewDef{
				Name:        name,
				Query:       materializedViewStmt.Query,
				Data:        def.Data,
				LastRefresh: lastRefresh,
			}
			c.materializedViewSQL[name] = def.SQL
		}
		materializedViewIter.Close()
	}

	// Load vector index definitions from catalog tree
	vecIter, err := c.tree.Scan([]byte("vec:"), []byte("vec;"))
	if err == nil {
		for vecIter.HasNext() {
			key, value, err := vecIter.Next()
			if err != nil {
				break
			}
			keyStr := string(key)
			if !strings.HasPrefix(keyStr, "vec:") {
				continue
			}
			var vid VectorIndexDef
			if err := json.Unmarshal(value, &vid); err != nil {
				continue
			}
			if vid.HNSW != nil {
				vid.HNSW.RebuildEntryPoint()
			}
			c.vectorIndexes[vid.Name] = &vid
		}
		vecIter.Close()
	}

	return nil
}

// SaveData exports all table schemas and row data as JSON files to dir.
// It writes a schema.json containing all table definitions and one <table>.json
// per table containing key/value data.
func (c *Catalog) SaveData(dir string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := os.MkdirAll(dir, 0750); err != nil {
		return fmt.Errorf("save data: cannot create directory %s: %w", dir, err)
	}

	// Export schema
	schema := map[string]interface{}{
		"tables":        c.tables,
		"vectorIndexes": c.vectorIndexes,
	}
	schemaData, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("save data: failed to marshal schema: %w", err)
	}
	schemaPath, err := catalogDataFilePath(dir, "schema.json")
	if err != nil {
		return fmt.Errorf("save data: invalid schema path: %w", err)
	}
	if err := os.WriteFile(schemaPath, schemaData, 0600); err != nil { // #nosec G304 - path is validated to stay inside the export directory.
		return fmt.Errorf("save data: failed to write schema.json: %w", err)
	}

	// Export each table's data
	for tableName, tree := range c.tableTrees {
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("save data: failed to scan table %s: %w", tableName, err)
		}

		var keys [][]byte
		var values [][]byte
		for iter.HasNext() {
			k, v, iterErr := iter.Next()
			if iterErr != nil {
				iter.Close()
				return fmt.Errorf("save data: failed to read from table %s: %w", tableName, iterErr)
			}
			if k == nil {
				break
			}
			keys = append(keys, k)
			values = append(values, v)
		}
		iter.Close()

		tableData := map[string]interface{}{
			"keys":   keys,
			"values": values,
		}
		data, err := json.Marshal(tableData)
		if err != nil {
			return fmt.Errorf("save data: failed to marshal table %s: %w", tableName, err)
		}
		dataPath, err := catalogDataFilePath(dir, tableName+".json")
		if err != nil {
			return fmt.Errorf("save data: invalid data path for table %s: %w", tableName, err)
		}
		if err := os.WriteFile(dataPath, data, 0600); err != nil { // #nosec G304 - path is validated to stay inside the export directory.
			return fmt.Errorf("save data: failed to write %s.json: %w", tableName, err)
		}
	}

	return nil
}

// LoadSchema reads schema.json from dir and recreates the table definitions
// and their B+Trees in the catalog.
func (c *Catalog) LoadSchema(dir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	schemaPath, err := catalogDataFilePath(dir, "schema.json")
	if err != nil {
		return fmt.Errorf("load schema: invalid schema path: %w", err)
	}
	data, err := os.ReadFile(schemaPath) // #nosec G304 - path is validated to stay inside the import directory.
	if err != nil {
		if os.IsNotExist(err) {
			return nil // no schema file, nothing to load
		}
		return fmt.Errorf("load schema: failed to read %s: %w", schemaPath, err)
	}

	var schema struct {
		Tables        map[string]*TableDef       `json:"tables"`
		VectorIndexes map[string]*VectorIndexDef `json:"vectorIndexes"`
	}
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("load schema: failed to parse %s: %w", schemaPath, err)
	}

	for name, tableDef := range schema.Tables {
		// Restore DEFAULT and CHECK expressions from persisted strings
		for i := range tableDef.Columns {
			if tableDef.Columns[i].Default != "" && tableDef.Columns[i].defaultExpr == nil {
				if parsed, parseErr := query.ParseExpression(tableDef.Columns[i].Default); parseErr == nil {
					tableDef.Columns[i].defaultExpr = parsed
				}
			}
			if tableDef.Columns[i].CheckStr != "" && tableDef.Columns[i].Check == nil {
				if parsed, parseErr := query.ParseExpression(tableDef.Columns[i].CheckStr); parseErr == nil {
					tableDef.Columns[i].Check = parsed
				}
			}
		}
		tableDef.buildColumnIndexCache()

		c.tables[name] = tableDef

		// Create or open B+Tree for the table
		if tableDef.RootPageID != 0 && c.pool != nil {
			tree, err := btree.OpenBTreeStrict(c.pool, tableDef.RootPageID)
			if err != nil {
				tree, err = btree.NewBTree(c.pool)
				if err != nil {
					return fmt.Errorf("load schema: failed to create replacement tree for %s: %w", name, err)
				}
				tableDef.RootPageID = tree.RootPageID()
			}
			c.tableTrees[name] = tree
		} else {
			var tree *btree.BTree
			if c.pool != nil {
				tree, err = btree.NewBTree(c.pool)
				if err != nil {
					return fmt.Errorf("load schema: failed to create tree for %s: %w", name, err)
				}
				tableDef.RootPageID = tree.RootPageID()
			}
			if tree != nil {
				c.tableTrees[name] = tree
			}
		}

		// Persist to catalog tree
		if c.tree != nil {
			_ = c.storeTableDef(tableDef)
		}
	}

	// Restore vector indexes and rebuild entry points
	for name, vid := range schema.VectorIndexes {
		if vid.HNSW != nil {
			vid.HNSW.RebuildEntryPoint()
		}
		c.vectorIndexes[name] = vid
	}

	return nil
}

// LoadData reads <table>.json data files from dir and inserts rows into the tables.
// Each file contains keys/values arrays that are put directly into the table's B+Tree.
func (c *Catalog) LoadData(dir string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for tableName := range c.tables {
		dataPath, err := catalogDataFilePath(dir, tableName+".json")
		if err != nil {
			return fmt.Errorf("load data: invalid data path for table %s: %w", tableName, err)
		}
		data, err := os.ReadFile(dataPath) // #nosec G304 - path is validated to stay inside the import directory.
		if err != nil {
			if os.IsNotExist(err) {
				continue // no data file for this table
			}
			return fmt.Errorf("load data: failed to read %s: %w", dataPath, err)
		}

		var tableData struct {
			Keys   [][]byte `json:"keys"`
			Values [][]byte `json:"values"`
		}
		if err := json.Unmarshal(data, &tableData); err != nil {
			return fmt.Errorf("load data: failed to parse %s: %w", dataPath, err)
		}

		tree, exists := c.tableTrees[tableName]
		if !exists {
			// Create tree if missing
			if c.pool != nil {
				tree, err = btree.NewBTree(c.pool)
				if err != nil {
					return fmt.Errorf("load data: failed to create tree for %s: %w", tableName, err)
				}
				c.tableTrees[tableName] = tree
			} else {
				continue
			}
		}

		for i, key := range tableData.Keys {
			if i >= len(tableData.Values) {
				break
			}
			if err := tree.Put(key, tableData.Values[i]); err != nil {
				return fmt.Errorf("load data: failed to insert into %s: %w", tableName, err)
			}
		}
	}

	return nil
}

func (c *Catalog) Vacuum() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for name := range c.tableTrees {
		if err := c.vacuumTreeLocked(name); err != nil {
			return err
		}
	}

	// Compact index trees (indexes don't have soft-deleted entries)
	for name, tree := range c.indexTrees {
		if err := c.compactIndexTreeLocked(name, tree); err != nil {
			return err
		}
	}

	return nil
}

// VacuumTable vacuums a specific table and its partitions.
func (c *Catalog) VacuumTable(tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	treeNames := table.getPartitionTreeNames()
	for _, name := range treeNames {
		if err := c.vacuumTreeLocked(name); err != nil {
			return err
		}
	}
	return nil
}

// vacuumTreeLocked rebuilds a single table tree, skipping soft-deleted rows.
// Must be called with c.mu held (write lock).
func (c *Catalog) vacuumTreeLocked(name string) error {
	tree, exists := c.tableTrees[name]
	if !exists {
		return nil
	}

	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return fmt.Errorf("vacuum: failed to scan table %s: %w", name, err)
	}

	type entry struct {
		key   []byte
		value []byte
	}
	var entries []entry
	var liveCount int64
	for iter.HasNext() {
		key, value, iterErr := iter.Next()
		if iterErr != nil {
			iter.Close()
			return fmt.Errorf("vacuum: failed to read entry from table %s: %w", name, iterErr)
		}
		if key == nil {
			break
		}
		// Skip soft-deleted rows
		if bytesContainDeletedAt(value) {
			continue
		}
		entries = append(entries, entry{key: key, value: value})
		liveCount++
	}
	iter.Close()

	if len(entries) == 0 {
		c.vacuumMu.Lock()
		c.deadTuples[name] = 0
		c.liveTuples[name] = 0
		c.vacuumMu.Unlock()
		return nil
	}

	newTree, err := btree.NewBTree(c.pool)
	if err != nil {
		return fmt.Errorf("vacuum: failed to create new tree for table %s: %w", name, err)
	}
	for _, e := range entries {
		if err := newTree.Put(e.key, e.value); err != nil {
			return fmt.Errorf("vacuum: failed to copy entry in table %s: %w", name, err)
		}
	}

	c.tableTrees[name] = newTree

	c.ensureVacuumMaps()
	c.vacuumMu.Lock()
	c.deadTuples[name] = 0
	c.liveTuples[name] = liveCount
	c.vacuumMu.Unlock()

	return nil
}

// compactIndexTreeLocked rebuilds an index tree.
// Must be called with c.mu held (write lock).
func (c *Catalog) compactIndexTreeLocked(name string, tree btree.TreeStore) error {
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return fmt.Errorf("vacuum: failed to scan index %s: %w", name, err)
	}

	type entry struct {
		key   []byte
		value []byte
	}
	var entries []entry
	for iter.HasNext() {
		key, value, iterErr := iter.Next()
		if iterErr != nil {
			iter.Close()
			return fmt.Errorf("vacuum: failed to read entry from index %s: %w", name, iterErr)
		}
		if key == nil {
			break
		}
		entries = append(entries, entry{key: key, value: value})
	}
	iter.Close()

	if len(entries) == 0 {
		return nil
	}

	newTree, err := btree.NewBTree(c.pool)
	if err != nil {
		return fmt.Errorf("vacuum: failed to create new tree for index %s: %w", name, err)
	}
	for _, e := range entries {
		if err := newTree.Put(e.key, e.value); err != nil {
			return fmt.Errorf("vacuum: failed to copy entry in index %s: %w", name, err)
		}
	}

	c.indexTrees[name] = newTree
	return nil
}

// GetDeadTupleRatio returns the ratio of dead tuples to total tuples for a table.
// A ratio of 0.0 means no dead tuples; 1.0 means all tuples are dead.
func (c *Catalog) GetDeadTupleRatio(tableName string) float64 {
	c.vacuumMu.RLock()
	defer c.vacuumMu.RUnlock()

	dead := int64(0)
	live := int64(0)

	// Sum across main table and all partition trees
	table, err := c.GetTable(tableName)
	if err != nil {
		// Fallback to exact tree name
		dead = c.deadTuples[tableName]
		live = c.liveTuples[tableName]
		total := dead + live
		if total == 0 {
			return 0
		}
		return float64(dead) / float64(total)
	}

	for _, name := range table.getPartitionTreeNames() {
		dead += c.deadTuples[name]
		live += c.liveTuples[name]
	}

	total := dead + live
	if total == 0 {
		return 0
	}
	return float64(dead) / float64(total)
}

// ListTablesNeedingVacuum returns table names whose dead tuple ratio exceeds threshold.
func (c *Catalog) ListTablesNeedingVacuum(threshold float64) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []string
	seen := make(map[string]bool)
	for name := range c.tableTrees {
		// Strip partition suffix to get base table name
		baseName := name
		if idx := strings.Index(name, ":"); idx > 0 {
			baseName = name[:idx]
		}
		if seen[baseName] {
			continue
		}
		seen[baseName] = true
		if c.GetDeadTupleRatio(baseName) >= threshold {
			result = append(result, baseName)
		}
	}
	return result
}

func (c *Catalog) Analyze(tableName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return fmt.Errorf("table %s has no data", tableName)
	}

	// Use Scan to iterate over all entries
	iter, err := tree.Scan(nil, nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	var rowCount int64
	// Analyze each column - first pass: collect all values
	columnValues := make(map[string][]interface{})
	nullCounts := make(map[string]int64)

	for iter.HasNext() {
		_, value, iterErr := iter.Next()
		if iterErr != nil {
			return fmt.Errorf("analyze: failed to read row: %w", iterErr)
		}
		if value == nil {
			break
		}
		rowCount++
		vrow, err := decodeVersionedRow(value, len(table.Columns))
		if err != nil {
			continue
		}
		rowSlice := vrow.Data
		for i, col := range table.Columns {
			if i >= len(rowSlice) || rowSlice[i] == nil {
				nullCounts[col.Name]++
			} else {
				columnValues[col.Name] = append(columnValues[col.Name], rowSlice[i])
			}
		}
	}

	stats := &StatsTableStats{
		TableName:    tableName,
		RowCount:     uint64(rowCount),
		ColumnStats:  make(map[string]*ColumnStats),
		LastAnalyzed: time.Now(),
	}

	// Analyze each column
	for _, col := range table.Columns {
		values := columnValues[col.Name]
		colStats := &ColumnStats{
			ColumnName: col.Name,
		}

		// Count distinct values
		valueSet := make(map[string]bool)
		for _, val := range values {
			valueSet[ValueToStringKey(val)] = true
		}

		distinctCount, err := catalogUint64Len(len(valueSet), "distinct value count")
		if err != nil {
			return err
		}
		nullCount, err := catalogUint64Count(nullCounts[col.Name], "null count")
		if err != nil {
			return err
		}
		colStats.DistinctCount = distinctCount
		colStats.NullCount = nullCount

		// Find min/max
		if len(values) > 0 {
			minVal := values[0]
			maxVal := values[0]
			for _, val := range values[1:] {
				if catalogCompareValues(val, minVal) < 0 {
					minVal = val
				}
				if catalogCompareValues(val, maxVal) > 0 {
					maxVal = val
				}
			}
			colStats.MinValue = minVal
			colStats.MaxValue = maxVal
		}

		stats.ColumnStats[col.Name] = colStats
	}

	c.stats[tableName] = stats
	return nil
}
