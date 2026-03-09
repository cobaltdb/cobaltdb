package catalog

import (
	"fmt"
	"sort"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"time"
)

func (c *Catalog) CreateMaterializedView(name string, selectStmt *query.SelectStmt) error {
	if _, exists := c.materializedViews[name]; exists {
		return fmt.Errorf("materialized view %s already exists", name)
	}

	// Execute the query to get initial data
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

	c.materializedViews[name] = &MaterializedViewDef{
		Name:        name,
		Query:       selectStmt,
		Data:        data,
		LastRefresh: time.Now(),
	}

	return nil
}

func (c *Catalog) DropMaterializedView(name string) error {
	if _, exists := c.materializedViews[name]; !exists {
		return fmt.Errorf("materialized view %s not found", name)
	}

	delete(c.materializedViews, name)
	return nil
}

func (c *Catalog) RefreshMaterializedView(name string) error {
	mv, exists := c.materializedViews[name]
	if !exists {
		return fmt.Errorf("materialized view %s not found", name)
	}

	// Re-execute the query
	columns, rows, err := c.Select(mv.Query, nil)
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

	mv.Data = data
	mv.LastRefresh = time.Now()

	return nil
}

func (c *Catalog) GetMaterializedView(name string) (*MaterializedViewDef, error) {
	mv, exists := c.materializedViews[name]
	if !exists {
		return nil, fmt.Errorf("materialized view %s not found", name)
	}
	return mv, nil
}

func (c *Catalog) ListMaterializedViews() []string {
	names := make([]string, 0, len(c.materializedViews))
	for name := range c.materializedViews {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}