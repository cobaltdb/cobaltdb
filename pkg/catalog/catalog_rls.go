package catalog

import (
	"context"
	"github.com/cobaltdb/cobaltdb/pkg/security"
)

func (c *Catalog) ApplyRLSFilter(ctx context.Context, tableName string, columns []string, rows [][]interface{}, user string, roles []string) ([]string, [][]interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return columns, rows, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return columns, rows, nil
	}

	// Convert rows to map format for RLS evaluation
	mapRows := make([]map[string]interface{}, len(rows))
	for i, row := range rows {
		mapRow := make(map[string]interface{})
		for j, col := range columns {
			if j < len(row) {
				mapRow[col] = row[j]
			}
		}
		mapRows[i] = mapRow
	}

	// Apply RLS filtering - use provided context for proper user/tenant propagation
	filtered, err := c.rlsManager.FilterRows(ctx, tableName, security.PolicySelect, mapRows, user, roles)
	if err != nil {
		return nil, nil, err
	}

	// Convert back to row format
	filteredRows := make([][]interface{}, len(filtered))
	for i, mapRow := range filtered {
		row := make([]interface{}, len(columns))
		for j, col := range columns {
			row[j] = mapRow[col]
		}
		filteredRows[i] = row
	}

	return columns, filteredRows, nil
}

func (c *Catalog) CheckRLSForInsert(ctx context.Context, tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(ctx, tableName, security.PolicyInsert, row, user, roles)
}

func (c *Catalog) CheckRLSForUpdate(ctx context.Context, tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(ctx, tableName, security.PolicyUpdate, row, user, roles)
}

func (c *Catalog) CheckRLSForDelete(ctx context.Context, tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(ctx, tableName, security.PolicyDelete, row, user, roles)
}