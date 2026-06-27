package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
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
		if tableDef.Temporary {
			continue
		}
		if err := c.storeTableDef(tableDef); err != nil {
			return fmt.Errorf("failed to save table definition %s: %w", tableDef.Name, err)
		}
	}

	for _, indexDef := range c.indexes {
		if indexDef.Temporary {
			continue
		}
		if err := c.storeIndexDef(indexDef); err != nil {
			return fmt.Errorf("failed to save index definition %s: %w", indexDef.Name, err)
		}
	}

	for foreignTableName, foreignTableDef := range c.foreignTables {
		if err := c.storeForeignTableDef(foreignTableDef); err != nil {
			return fmt.Errorf("failed to save foreign table definition %s: %w", foreignTableName, err)
		}
	}

	for viewName, viewQuery := range c.views {
		if c.viewTemporary[viewName] {
			continue
		}
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

	// Save full-text and JSON index definitions (incl. their inverted indexes),
	// otherwise they were silently lost on disk reopen.
	for _, fts := range c.ftsIndexes {
		if err := c.storeFTSIndexDef(fts); err != nil {
			return fmt.Errorf("failed to save full-text index definition %s: %w", fts.Name, err)
		}
	}
	for _, ji := range c.jsonIndexes {
		if err := c.storeJSONIndexDef(ji); err != nil {
			return fmt.Errorf("failed to save JSON index definition %s: %w", ji.Name, err)
		}
	}

	if c.enableRLS && c.rlsManager != nil {
		for _, tableName := range c.rlsManager.ListEnabledTables() {
			if err := c.storeRLSEnabledTable(tableName); err != nil {
				return fmt.Errorf("failed to save RLS enabled table %s: %w", tableName, err)
			}
		}
		for _, policy := range c.rlsManager.ListPolicies() {
			if err := c.storeRLSPolicyDef(policy); err != nil {
				return fmt.Errorf("failed to save RLS policy %s on %s: %w", policy.Name, policy.TableName, err)
			}
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
	Columns     []string                 `json:"columns"`
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

func (c *Catalog) deleteCatalogDef(key string) error {
	if c.tree == nil {
		return nil
	}
	if err := c.tree.Delete([]byte(key)); err != nil && !errors.Is(err, btree.ErrKeyNotFound) {
		return err
	}
	return nil
}

func (c *Catalog) storeMaterializedViewDef(name string, sql string, mv *MaterializedViewDef) error {
	def := persistedMaterializedViewDef{
		Name: name,
		SQL:  strings.TrimSpace(sql),
	}
	if mv != nil {
		def.Columns = cloneStringSlice(mv.Columns)
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

func (c *Catalog) storeFTSIndexDef(fts *FTSIndexDef) error {
	data, err := json.Marshal(fts)
	if err != nil {
		return err
	}
	if c.tree != nil {
		return c.tree.Put([]byte("fts:"+fts.Name), data)
	}
	return nil
}

func (c *Catalog) storeJSONIndexDef(ji *JSONIndexDef) error {
	data, err := json.Marshal(ji)
	if err != nil {
		return err
	}
	if c.tree != nil {
		return c.tree.Put([]byte("json:"+ji.Name), data)
	}
	return nil
}

func (c *Catalog) storeForeignTableDef(ft *ForeignTableDef) error {
	data, err := json.Marshal(ft)
	if err != nil {
		return err
	}
	if c.tree != nil {
		return c.tree.Put([]byte("ft:"+ft.TableName), data)
	}
	return nil
}

func (c *Catalog) storeRLSEnabledTable(tableName string) error {
	if c.tree != nil {
		return c.tree.Put([]byte("rlst:"+strings.ToLower(tableName)), []byte(`{"enabled":true}`))
	}
	return nil
}

func (c *Catalog) storeRLSPolicyDef(policy *security.Policy) error {
	if policy == nil {
		return nil
	}
	data, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	if c.tree != nil {
		key := "rlsp:" + strings.ToLower(policy.TableName) + ":" + strings.ToLower(policy.Name)
		return c.tree.Put([]byte(key), data)
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
	if c.foreignTables == nil {
		c.foreignTables = make(map[string]*ForeignTableDef)
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
			return fmt.Errorf("load catalog: failed to read table metadata: %w", err)
		}

		// Parse key to get table name
		if !strings.HasPrefix(keyStr, "tbl:") {
			continue
		}
		tableName := strings.TrimPrefix(keyStr, "tbl:")

		// Unmarshal table definition
		var tableDef TableDef
		if err := json.Unmarshal(value, &tableDef); err != nil {
			return fmt.Errorf("load catalog: failed to parse table metadata %s: %w", tableName, err)
		}

		// Restore DEFAULT and CHECK expressions from persisted strings
		for i := range tableDef.Columns {
			if tableDef.Columns[i].Default != "" && tableDef.Columns[i].defaultExpr == nil {
				parsed, err := query.ParseExpression(tableDef.Columns[i].Default)
				if err != nil {
					return fmt.Errorf(
						"load catalog: failed to parse default expression for table %s column %s: %w",
						tableName,
						tableDef.Columns[i].Name,
						err,
					)
				}
				tableDef.Columns[i].defaultExpr = parsed
			}
			if tableDef.Columns[i].CheckStr != "" && tableDef.Columns[i].Check == nil {
				parsed, err := query.ParseExpression(tableDef.Columns[i].CheckStr)
				if err != nil {
					return fmt.Errorf(
						"load catalog: failed to parse check expression for table %s column %s: %w",
						tableName,
						tableDef.Columns[i].Name,
						err,
					)
				}
				tableDef.Columns[i].Check = parsed
			}
		}
		for i := range tableDef.Checks {
			if tableDef.Checks[i].CheckStr != "" && tableDef.Checks[i].Check == nil {
				parsed, err := query.ParseExpression(tableDef.Checks[i].CheckStr)
				if err != nil {
					return fmt.Errorf(
						"load catalog: failed to parse check constraint for table %s constraint %s: %w",
						tableName,
						tableDef.Checks[i].Name,
						err,
					)
				}
				tableDef.Checks[i].Check = parsed
			}
		}

		// Create or open B+Tree for the table
		var tableTree btree.TreeStore
		if tableDef.RootPageID != 0 {
			tree, err := btree.OpenBTreeStrict(c.pool, tableDef.RootPageID)
			if err != nil {
				return fmt.Errorf("load catalog: failed to open tree for table %s: %w", tableName, err)
			}
			tableTree = tree
		} else {
			tree, err := btree.NewBTree(c.pool)
			if err != nil {
				return fmt.Errorf("load catalog: failed to create tree for table %s: %w", tableName, err)
			}
			tableDef.RootPageID = tree.RootPageID()
			tableTree = tree
		}

		// Build column index cache
		tableDef.buildColumnIndexCache()
		c.tables[tableName] = &tableDef
		c.tableTrees[tableName] = tableTree
	}

	// Load regular B-tree index definitions after tables so orphaned/corrupt
	// metadata cannot resurrect indexes for missing tables.
	idxIter, err := c.tree.Scan([]byte("idx:"), []byte("idx;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan index metadata: %w", err)
	}
	defer idxIter.Close()
	for idxIter.HasNext() {
		keyStr, value, err := idxIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read index metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "idx:") {
			continue
		}

		indexName := strings.TrimPrefix(keyStr, "idx:")
		var indexDef IndexDef
		if err := json.Unmarshal(value, &indexDef); err != nil {
			return fmt.Errorf("load catalog: failed to parse index metadata %s: %w", indexName, err)
		}
		if indexDef.Name == "" {
			indexDef.Name = indexName
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
				if err := c.populateIndexLocked(indexTree, &indexDef, table, tableTree); err != nil {
					return fmt.Errorf("load catalog: failed to populate index %s: %w", indexDef.Name, err)
				}
			}
		}

		c.indexes[indexDef.Name] = &indexDef
		c.indexTrees[indexDef.Name] = indexTree
	}

	foreignTableIter, err := c.tree.Scan([]byte("ft:"), []byte("ft;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan foreign table metadata: %w", err)
	}
	defer foreignTableIter.Close()
	for foreignTableIter.HasNext() {
		keyStr, value, err := foreignTableIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read foreign table metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "ft:") {
			continue
		}
		foreignTableName := strings.TrimPrefix(keyStr, "ft:")
		var ft ForeignTableDef
		if err := json.Unmarshal(value, &ft); err != nil {
			return fmt.Errorf("load catalog: failed to parse foreign table metadata %s: %w", foreignTableName, err)
		}
		if ft.TableName == "" {
			ft.TableName = foreignTableName
		}
		if _, tableExists := c.tables[ft.TableName]; tableExists {
			continue
		}
		c.foreignTables[ft.TableName] = &ft
	}

	viewIter, err := c.tree.Scan([]byte("view:"), []byte("view;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan view metadata: %w", err)
	}
	defer viewIter.Close()
	if c.viewSQL == nil {
		c.viewSQL = make(map[string]string)
	}
	if c.viewTemporary == nil {
		c.viewTemporary = make(map[string]bool)
	}
	for viewIter.HasNext() {
		keyStr, value, err := viewIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read view metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "view:") {
			continue
		}
		viewName := strings.TrimPrefix(keyStr, "view:")
		def, err := decodePersistedSQLDef(keyStr, "view:", value)
		if err != nil {
			return fmt.Errorf("load catalog: failed to parse view metadata %s: %w", viewName, err)
		}
		if def.SQL == "" {
			return fmt.Errorf("load catalog: missing SQL for view metadata %s", viewName)
		}
		parsed, err := query.Parse(def.SQL)
		if err != nil {
			return fmt.Errorf("load catalog: failed to parse view SQL %s: %w", viewName, err)
		}
		viewStmt, ok := parsed.(*query.CreateViewStmt)
		if !ok || viewStmt.Query == nil {
			return fmt.Errorf("load catalog: invalid view metadata %s", viewName)
		}
		name := viewStmt.Name
		if name == "" {
			name = def.Name
		}
		c.views[name] = viewStmt.Query
		c.viewSQL[name] = strings.TrimSpace(def.SQL)
		c.viewTemporary[name] = false
	}

	triggerIter, err := c.tree.Scan([]byte("trg:"), []byte("trg;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan trigger metadata: %w", err)
	}
	defer triggerIter.Close()
	if c.triggerSQL == nil {
		c.triggerSQL = make(map[string]string)
	}
	for triggerIter.HasNext() {
		keyStr, value, err := triggerIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read trigger metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "trg:") {
			continue
		}
		triggerName := strings.TrimPrefix(keyStr, "trg:")
		def, err := decodePersistedSQLDef(keyStr, "trg:", value)
		if err != nil {
			return fmt.Errorf("load catalog: failed to parse trigger metadata %s: %w", triggerName, err)
		}
		if def.SQL == "" {
			return fmt.Errorf("load catalog: missing SQL for trigger metadata %s", triggerName)
		}
		parsed, err := query.Parse(def.SQL)
		if err != nil {
			return fmt.Errorf("load catalog: failed to parse trigger SQL %s: %w", triggerName, err)
		}
		triggerStmt, ok := parsed.(*query.CreateTriggerStmt)
		if !ok {
			return fmt.Errorf("load catalog: invalid trigger metadata %s", triggerName)
		}
		if _, tableOK := c.tables[triggerStmt.Table]; !tableOK {
			if _, viewOK := c.views[triggerStmt.Table]; !viewOK {
				return fmt.Errorf("load catalog: trigger %s references missing table or view %s", triggerName, triggerStmt.Table)
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

	procedureIter, err := c.tree.Scan([]byte("proc:"), []byte("proc;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan procedure metadata: %w", err)
	}
	defer procedureIter.Close()
	if c.procedureSQL == nil {
		c.procedureSQL = make(map[string]string)
	}
	for procedureIter.HasNext() {
		keyStr, value, err := procedureIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read procedure metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "proc:") {
			continue
		}
		procedureName := strings.TrimPrefix(keyStr, "proc:")
		def, err := decodePersistedSQLDef(keyStr, "proc:", value)
		if err != nil {
			return fmt.Errorf("load catalog: failed to parse procedure metadata %s: %w", procedureName, err)
		}
		if def.SQL == "" {
			return fmt.Errorf("load catalog: missing SQL for procedure metadata %s", procedureName)
		}
		parsed, err := query.Parse(def.SQL)
		if err != nil {
			return fmt.Errorf("load catalog: failed to parse procedure SQL %s: %w", procedureName, err)
		}
		procedureStmt, ok := parsed.(*query.CreateProcedureStmt)
		if !ok {
			return fmt.Errorf("load catalog: invalid procedure metadata %s", procedureName)
		}
		name := procedureStmt.Name
		if name == "" {
			name = def.Name
		}
		procedureStmt.RawSQL = strings.TrimSpace(def.SQL)
		c.procedures[name] = procedureStmt
		c.procedureSQL[name] = procedureStmt.RawSQL
	}

	materializedViewIter, err := c.tree.Scan([]byte("mv:"), []byte("mv;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan materialized view metadata: %w", err)
	}
	defer materializedViewIter.Close()
	if c.materializedViewSQL == nil {
		c.materializedViewSQL = make(map[string]string)
	}
	for materializedViewIter.HasNext() {
		keyStr, value, err := materializedViewIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read materialized view metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "mv:") {
			continue
		}
		materializedViewName := strings.TrimPrefix(keyStr, "mv:")
		var def persistedMaterializedViewDef
		if err := json.Unmarshal(value, &def); err != nil {
			return fmt.Errorf("load catalog: failed to parse materialized view metadata %s: %w", materializedViewName, err)
		}
		if def.Name == "" {
			def.Name = materializedViewName
		}
		def.SQL = strings.TrimSpace(def.SQL)
		if def.SQL == "" {
			return fmt.Errorf("load catalog: missing SQL for materialized view metadata %s", materializedViewName)
		}
		parsed, err := query.Parse(def.SQL)
		if err != nil {
			return fmt.Errorf("load catalog: failed to parse materialized view SQL %s: %w", materializedViewName, err)
		}
		materializedViewStmt, ok := parsed.(*query.CreateMaterializedViewStmt)
		if !ok || materializedViewStmt.Query == nil {
			return fmt.Errorf("load catalog: invalid materialized view metadata %s", materializedViewName)
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
			Columns:     cloneStringSlice(def.Columns),
			Query:       materializedViewStmt.Query,
			Data:        def.Data,
			LastRefresh: lastRefresh,
		}
		c.materializedViewSQL[name] = def.SQL
	}

	// Load vector index definitions from catalog tree
	vecIter, err := c.tree.Scan([]byte("vec:"), []byte("vec;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan vector index metadata: %w", err)
	}
	defer vecIter.Close()
	for vecIter.HasNext() {
		key, value, err := vecIter.Next()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read vector index metadata: %w", err)
		}
		keyStr := string(key)
		if !strings.HasPrefix(keyStr, "vec:") {
			continue
		}
		vectorIndexName := strings.TrimPrefix(keyStr, "vec:")
		var vid VectorIndexDef
		if err := json.Unmarshal(value, &vid); err != nil {
			return fmt.Errorf("load catalog: failed to parse vector index metadata %s: %w", vectorIndexName, err)
		}
		if vid.Name == "" {
			vid.Name = vectorIndexName
		}
		if vid.HNSW != nil {
			vid.HNSW.RebuildEntryPoint()
		}
		c.vectorIndexes[vid.Name] = &vid
	}

	// Load full-text index definitions.
	ftsIter, err := c.tree.Scan([]byte("fts:"), []byte("fts;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan full-text index metadata: %w", err)
	}
	defer ftsIter.Close()
	for ftsIter.HasNext() {
		key, value, err := ftsIter.Next()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read full-text index metadata: %w", err)
		}
		keyStr := string(key)
		if !strings.HasPrefix(keyStr, "fts:") {
			continue
		}
		name := strings.TrimPrefix(keyStr, "fts:")
		var fts FTSIndexDef
		if err := json.Unmarshal(value, &fts); err != nil {
			return fmt.Errorf("load catalog: failed to parse full-text index metadata %s: %w", name, err)
		}
		if fts.Name == "" {
			fts.Name = name
		}
		if fts.Index == nil {
			fts.Index = make(map[string][]int64)
		}
		c.ftsIndexes[fts.Name] = &fts
	}

	// Load JSON index definitions.
	jsonIter, err := c.tree.Scan([]byte("json:"), []byte("json;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan JSON index metadata: %w", err)
	}
	defer jsonIter.Close()
	for jsonIter.HasNext() {
		key, value, err := jsonIter.Next()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read JSON index metadata: %w", err)
		}
		keyStr := string(key)
		if !strings.HasPrefix(keyStr, "json:") {
			continue
		}
		name := strings.TrimPrefix(keyStr, "json:")
		var ji JSONIndexDef
		if err := json.Unmarshal(value, &ji); err != nil {
			return fmt.Errorf("load catalog: failed to parse JSON index metadata %s: %w", name, err)
		}
		if ji.Name == "" {
			ji.Name = name
		}
		if ji.Index == nil {
			ji.Index = make(map[string][]int64)
		}
		c.jsonIndexes[ji.Name] = &ji
	}

	rlsTableIter, err := c.tree.Scan([]byte("rlst:"), []byte("rlst;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan RLS table metadata: %w", err)
	}
	defer rlsTableIter.Close()
	for rlsTableIter.HasNext() {
		keyStr, _, err := rlsTableIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read RLS table metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "rlst:") {
			continue
		}
		tableName := strings.TrimPrefix(keyStr, "rlst:")
		if !c.hasTableLocked(tableName) {
			continue
		}
		if c.rlsManager == nil {
			c.rlsManager = security.NewManager()
		}
		c.enableRLS = true
		c.rlsManager.EnableTable(tableName)
	}

	rlsPolicyIter, err := c.tree.Scan([]byte("rlsp:"), []byte("rlsp;"))
	if err != nil {
		return fmt.Errorf("load catalog: failed to scan RLS policy metadata: %w", err)
	}
	defer rlsPolicyIter.Close()
	for rlsPolicyIter.HasNext() {
		keyStr, value, err := rlsPolicyIter.NextString()
		if err != nil {
			return fmt.Errorf("load catalog: failed to read RLS policy metadata: %w", err)
		}
		if !strings.HasPrefix(keyStr, "rlsp:") {
			continue
		}
		var policy security.Policy
		if err := json.Unmarshal(value, &policy); err != nil {
			return fmt.Errorf("load catalog: failed to parse RLS policy metadata %s: %w", keyStr, err)
		}
		if policy.Name == "" || policy.TableName == "" {
			return fmt.Errorf("load catalog: invalid RLS policy metadata %s", keyStr)
		}
		if !c.hasTableLocked(policy.TableName) {
			continue
		}
		if c.rlsManager == nil {
			c.rlsManager = security.NewManager()
		}
		c.enableRLS = true
		if err := c.rlsManager.CreatePolicy(&policy); err != nil {
			return fmt.Errorf("load catalog: failed to restore RLS policy %s on %s: %w", policy.Name, policy.TableName, err)
		}
	}

	return nil
}

func (c *Catalog) hasTableLocked(tableName string) bool {
	if _, exists := c.tables[tableName]; exists {
		return true
	}
	for existing := range c.tables {
		if strings.EqualFold(existing, tableName) {
			return true
		}
	}
	return false
}

// SaveData exports all table schemas and row data as JSON files to dir.
// It writes a schema.json containing all table definitions and one <table>.json
// per table containing key/value data.
func (c *Catalog) SaveData(dir string) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := prepareCatalogDataDir(dir, true); err != nil {
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
	if err := writeCatalogDataFileAtomic(schemaPath, schemaData, 0600); err != nil {
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
		if err := writeCatalogDataFileAtomic(dataPath, data, 0600); err != nil {
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

	if err := prepareCatalogDataDir(dir, false); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("load schema: invalid data directory: %w", err)
	}

	schemaPath, err := catalogDataFilePath(dir, "schema.json")
	if err != nil {
		return fmt.Errorf("load schema: invalid schema path: %w", err)
	}
	data, err := readCatalogDataFile(schemaPath)
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
				parsed, parseErr := query.ParseExpression(tableDef.Columns[i].Default)
				if parseErr != nil {
					return fmt.Errorf(
						"load schema: failed to parse default expression for table %s column %s: %w",
						name,
						tableDef.Columns[i].Name,
						parseErr,
					)
				}
				tableDef.Columns[i].defaultExpr = parsed
			}
			if tableDef.Columns[i].CheckStr != "" && tableDef.Columns[i].Check == nil {
				parsed, parseErr := query.ParseExpression(tableDef.Columns[i].CheckStr)
				if parseErr != nil {
					return fmt.Errorf(
						"load schema: failed to parse check expression for table %s column %s: %w",
						name,
						tableDef.Columns[i].Name,
						parseErr,
					)
				}
				tableDef.Columns[i].Check = parsed
			}
		}
		for i := range tableDef.Checks {
			if tableDef.Checks[i].CheckStr != "" && tableDef.Checks[i].Check == nil {
				parsed, parseErr := query.ParseExpression(tableDef.Checks[i].CheckStr)
				if parseErr != nil {
					return fmt.Errorf(
						"load schema: failed to parse check constraint for table %s constraint %s: %w",
						name,
						tableDef.Checks[i].Name,
						parseErr,
					)
				}
				tableDef.Checks[i].Check = parsed
			}
		}
		tableDef.buildColumnIndexCache()

		// Create or open B+Tree for the table
		var tableTree btree.TreeStore
		if tableDef.RootPageID != 0 && c.pool != nil {
			tree, err := btree.OpenBTreeStrict(c.pool, tableDef.RootPageID)
			if err != nil {
				tree, err = btree.NewBTree(c.pool)
				if err != nil {
					return fmt.Errorf("load schema: failed to create replacement tree for %s: %w", name, err)
				}
				tableDef.RootPageID = tree.RootPageID()
			}
			tableTree = tree
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
				tableTree = tree
			}
		}

		// Persist to catalog tree
		if c.tree != nil {
			if err := c.storeTableDef(tableDef); err != nil {
				return fmt.Errorf("load schema: failed to persist table definition %s: %w", name, err)
			}
		}

		c.tables[name] = tableDef
		if tableTree != nil {
			c.tableTrees[name] = tableTree
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

	if err := prepareCatalogDataDir(dir, false); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("load data: invalid data directory: %w", err)
	}

	for tableName := range c.tables {
		dataPath, err := catalogDataFilePath(dir, tableName+".json")
		if err != nil {
			return fmt.Errorf("load data: invalid data path for table %s: %w", tableName, err)
		}
		data, err := readCatalogDataFile(dataPath)
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
		if len(tableData.Keys) != len(tableData.Values) {
			return fmt.Errorf(
				"load data: mismatched key/value counts for table %s: %d keys, %d values",
				tableName,
				len(tableData.Keys),
				len(tableData.Values),
			)
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
				return fmt.Errorf("load data: table %s has no tree and catalog has no buffer pool", tableName)
			}
		}

		for i, key := range tableData.Keys {
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

	// Persist the new root page. Vacuum allocates a fresh tree (new root page),
	// but Load() reopens a table from its persisted TableDef.RootPageID. Without
	// updating + persisting the root, a reopen loads the OLD pre-vacuum tree —
	// resurrecting soft-deleted rows and losing every write made after the
	// vacuum (AutoVacuum is on by default, so this happens silently). Only the
	// non-partitioned single-tree case is keyed in c.tables here.
	if td, ok := c.tables[name]; ok {
		td.RootPageID = newTree.RootPageID()
		if err := c.storeTableDef(td); err != nil {
			return fmt.Errorf("vacuum: failed to persist new root for table %s: %w", name, err)
		}
	}

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
	// Persist the new index root page (same reopen hazard as table vacuum).
	if idx, ok := c.indexes[name]; ok {
		idx.RootPageID = newTree.RootPageID()
		if err := c.storeIndexDef(idx); err != nil {
			return fmt.Errorf("vacuum: failed to persist new root for index %s: %w", name, err)
		}
	}
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

// Analyze computes per-column statistics (row count, null count, distinct count,
// min/max values) for a table. The scan phase runs WITHOUT holding c.mu so that
// concurrent DML is not blocked. The catalog lock is held only briefly: once to
// read the tree reference, and once to write the computed stats. The trade-off
// is that if the table is dropped between the scan and the stats write, the
// stats write is silently skipped — this is safe because a dropped table's stats
// are irrelevant.
func (c *Catalog) Analyze(tableName string) error {
	// Phase 1: acquire catalog lock to read table/tree reference.
	c.mu.Lock()
	table, err := c.getTableLocked(tableName)
	if err != nil {
		c.mu.Unlock()
		return err
	}
	tree, exists := c.tableTrees[tableName]
	if !exists {
		c.mu.Unlock()
		return fmt.Errorf("table %s has no data", tableName)
	}
	columns := table.Columns
	c.mu.Unlock()

	// Phase 2: scan WITHOUT holding c.mu — allows concurrent DML.
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
		vrow, err := decodeVersionedRow(value, len(columns))
		if err != nil {
			return fmt.Errorf("analyze: failed to decode row in table %s: %w", tableName, err)
		}
		rowCount++
		rowSlice := vrow.Data
		for i, col := range columns {
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

	// Phase 3: write stats under catalog lock. If the table was dropped while the
	// scan was running, the tree lookup will fail and we skip the write.
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.tableTrees[tableName]; !exists {
		return nil // table dropped during scan; stats are irrelevant
	}
	c.stats[tableName] = stats
	return nil
}
