package catalog

import (
	"context"

	"github.com/cobaltdb/cobaltdb/pkg/security"
)

// rowToMap converts a row slice to a column-name-keyed map.
func rowToMap(columns []ColumnDef, row []interface{}) map[string]interface{} {
	m := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(row) {
			m[col.Name] = row[i]
		}
	}
	return m
}

// rlsContext extracts user and roles from the context.
func rlsContext(ctx context.Context) (user string, roles []string) {
	user, _ = ctx.Value("cobaltdb_user").(string)
	roles, _ = ctx.Value("cobaltdb_roles").([]string)
	return
}

// checkRowAccessLocked checks RLS access for the given operation.
// Caller must hold the catalog lock. Returns (allowed, error).
func (c *Catalog) checkRowAccessLocked(ctx context.Context, tableName string, columns []ColumnDef, row []interface{}, operation security.PolicyType) (bool, error) {
	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}
	user, roles := rlsContext(ctx)
	if user == "" {
		return true, nil
	}
	rowMap := rowToMap(columns, row)
	return c.rlsManager.CheckAccess(ctx, tableName, operation, rowMap, user, roles)
}

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

// applyRLSFilterInternal is a lock-free version of ApplyRLSFilter for use
// within methods that already hold the catalog lock (selectLocked, etc.).
func (c *Catalog) applyRLSFilterInternal(ctx context.Context, tableName string, columns []string, rows [][]interface{}, user string, roles []string) ([]string, [][]interface{}, error) {
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

	// Apply RLS filtering
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

// checkRLSForInsertInternal is a lock-free version of CheckRLSForInsert.
//nolint:unused // used by coverage tests
func (c *Catalog) checkRLSForInsertInternal(ctx context.Context, tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(ctx, tableName, security.PolicyInsert, row, user, roles)
}

// checkRLSForUpdateInternal is a lock-free version of CheckRLSForUpdate.
//nolint:unused // used by coverage tests
func (c *Catalog) checkRLSForUpdateInternal(ctx context.Context, tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(ctx, tableName, security.PolicyUpdate, row, user, roles)
}

// checkRLSForDeleteInternal is a lock-free version of CheckRLSForDelete.
//nolint:unused // used by coverage tests
func (c *Catalog) checkRLSForDeleteInternal(ctx context.Context, tableName string, row map[string]interface{}, user string, roles []string) (bool, error) {
	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}

	if !c.rlsManager.IsEnabled(tableName) {
		return true, nil
	}

	return c.rlsManager.CheckAccess(ctx, tableName, security.PolicyDelete, row, user, roles)
}
