package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"sort"
	"strings"
	"time"
)

func (c *Catalog) CreateMaterializedView(name string, selectStmt *query.SelectStmt, ifNotExists bool) error {
	return c.CreateMaterializedViewSQL(name, selectStmt, ifNotExists, "")
}

func (c *Catalog) CreateMaterializedViewSQL(name string, selectStmt *query.SelectStmt, ifNotExists bool, sql string) error {
	c.mu.Lock()
	if _, exists := c.materializedViews[name]; exists {
		c.mu.Unlock()
		if ifNotExists {
			return nil // Silently succeed
		}
		return fmt.Errorf("materialized view %s already exists", name)
	}

	c.mu.Unlock()
	// Execute the query to get initial data (outside lock since Select takes its own lock)
	columns, rows, err := c.Select(selectStmt, nil)
	if err != nil {
		return fmt.Errorf("failed to execute materialized view query: %w", err)
	}

	// Convert rows to map format
	data := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		rowMap := make(map[string]interface{})
		for j, col := range columns {
			if j < len(row) {
				rowMap[col] = row[j]
			}
		}
		data[i] = rowMap
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if _, exists := c.materializedViews[name]; exists {
		if ifNotExists {
			return nil
		}
		return fmt.Errorf("materialized view %s already exists", name)
	}
	if c.materializedViewSQL == nil {
		c.materializedViewSQL = make(map[string]string)
	}
	if strings.TrimSpace(sql) == "" {
		sql = createMaterializedViewSQL(name, selectStmt)
	}
	c.materializedViews[name] = &MaterializedViewDef{
		Name:        name,
		Columns:     cloneStringSlice(columns),
		Query:       selectStmt,
		Data:        data,
		LastRefresh: time.Now(),
	}
	c.materializedViewSQL[name] = strings.TrimSpace(sql)
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:               undoCreateMaterializedView,
			materializedViewName: name,
			materializedViewSQL:  c.materializedViewSQL[name],
		})
	}

	return nil
}

func (c *Catalog) DropMaterializedView(name string, ifExists bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	mv, exists := c.materializedViews[name]
	if !exists {
		if ifExists {
			return nil // Silently succeed
		}
		return fmt.Errorf("materialized view %s not found", name)
	}

	if err := c.deleteCatalogDef("mv:" + name); err != nil {
		return fmt.Errorf("failed to delete materialized view metadata %s: %w", name, err)
	}
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:               undoDropMaterializedView,
			materializedViewName: name,
			materializedViewDef:  cloneMaterializedViewDef(mv),
			materializedViewSQL:  c.materializedViewSQL[name],
		})
	}
	delete(c.materializedViews, name)
	delete(c.materializedViewSQL, name)
	// Clear cached query result so subsequent SELECTs fail as expected.
	if c.cteResults != nil {
		delete(c.cteResults, toLowerFast(name))
	}
	return nil
}

func (c *Catalog) RefreshMaterializedView(name string) error {
	c.mu.RLock()
	mv, exists := c.materializedViews[name]
	if !exists {
		c.mu.RUnlock()
		return fmt.Errorf("materialized view %s not found", name)
	}
	queryStmt := mv.Query
	c.mu.RUnlock()

	// Re-execute the query (outside lock since Select takes its own lock)
	columns, rows, err := c.Select(queryStmt, nil)
	if err != nil {
		return fmt.Errorf("failed to refresh materialized view: %w", err)
	}

	// Convert rows to map format
	data := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		rowMap := make(map[string]interface{})
		for j, col := range columns {
			if j < len(row) {
				rowMap[col] = row[j]
			}
		}
		data[i] = rowMap
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	mv.Data = data
	mv.Columns = cloneStringSlice(columns)
	mv.LastRefresh = time.Now()
	// Clear cached query result so subsequent SELECTs see fresh data.
	if c.cteResults != nil {
		delete(c.cteResults, toLowerFast(name))
	}

	return nil
}

func (c *Catalog) GetMaterializedView(name string) (*MaterializedViewDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	mv, err := c.getMaterializedViewLocked(name)
	if err != nil {
		return nil, err
	}
	return cloneMaterializedViewDef(mv), nil
}

// getMaterializedViewLocked is the lock-free internal version. Must be called with mu held.
func (c *Catalog) getMaterializedViewLocked(name string) (*MaterializedViewDef, error) {
	mv, exists := c.materializedViews[name]
	if !exists {
		return nil, fmt.Errorf("materialized view %s not found", name)
	}
	return mv, nil
}

func (c *Catalog) ListMaterializedViews() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.materializedViews))
	for name := range c.materializedViews {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
