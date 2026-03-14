package catalog

import (
	"fmt"
	"sort"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"time"
)

func (c *Catalog) CreateMaterializedView(name string, selectStmt *query.SelectStmt, ifNotExists bool) error {
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
	c.materializedViews[name] = &MaterializedViewDef{
		Name:        name,
		Query:       selectStmt,
		Data:        data,
		LastRefresh: time.Now(),
	}

	return nil
}

func (c *Catalog) DropMaterializedView(name string, ifExists bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.materializedViews[name]; !exists {
		if ifExists {
			return nil // Silently succeed
		}
		return fmt.Errorf("materialized view %s not found", name)
	}

	delete(c.materializedViews, name)
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
	mv.Data = data
	mv.LastRefresh = time.Now()

	return nil
}

func (c *Catalog) GetMaterializedView(name string) (*MaterializedViewDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
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
