package catalog

import (
	"context"
	"fmt"
	"sort"
	"strings"

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
//
// It prefers the typed keys exported by pkg/security (security.RLSUserKey,
// security.RLSRolesKey) and falls back to the legacy "cobaltdb_user" /
// "cobaltdb_roles" string keys for backward compatibility with older callers
// and tests. Callers should prefer the typed keys.
func rlsContext(ctx context.Context) (user string, roles []string) {
	if ctx == nil {
		return "", nil
	}
	if v, ok := ctx.Value(security.RLSUserKey).(string); ok && v != "" {
		user = v
	} else {
		user, _ = ctx.Value("cobaltdb_user").(string)
	}
	if v, ok := ctx.Value(security.RLSRolesKey).([]string); ok && len(v) > 0 {
		roles = v
	} else {
		roles, _ = ctx.Value("cobaltdb_roles").([]string)
	}
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

func (c *Catalog) filterRowsForSelectRLSLocked(ctx context.Context, tableName string, columns []ColumnDef, rows [][]interface{}) ([][]interface{}, error) {
	if !c.enableRLS || c.rlsManager == nil || !c.rlsManager.IsEnabled(tableName) {
		return rows, nil
	}
	if ctx == nil {
		ctx = c.rlsCtx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	user, _ := rlsContext(ctx)
	if user == "" {
		return rows, nil
	}

	filtered := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		allowed, err := c.checkRowAccessLocked(ctx, tableName, columns, row, security.PolicySelect)
		if err != nil {
			return nil, err
		}
		if allowed {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

// checkRowCheckLocked checks RLS WITH CHECK predicates for rows being written.
// Caller must hold the catalog lock.
func (c *Catalog) checkRowCheckLocked(ctx context.Context, tableName string, columns []ColumnDef, row []interface{}, operation security.PolicyType) (bool, error) {
	if !c.enableRLS || c.rlsManager == nil {
		return true, nil
	}
	user, roles := rlsContext(ctx)
	if user == "" {
		return true, nil
	}
	rowMap := rowToMap(columns, row)
	return c.rlsManager.CheckAccessWithCheck(ctx, tableName, operation, rowMap, user, roles)
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

	// Convert back to row format. Fail closed if a column the policy is
	// supposed to preserve is missing from the filtered map — silently
	// substituting nil would degrade a policy error into a wrong result.
	filteredRows := make([][]interface{}, len(filtered))
	for i, mapRow := range filtered {
		row := make([]interface{}, len(columns))
		for j, col := range columns {
			val, ok := mapRow[col]
			if !ok {
				return nil, nil, fmt.Errorf("rls: filtered row %d missing column %q", i, col)
			}
			row[j] = val
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

func (c *Catalog) ListRLSPolicies() []*security.Policy {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.enableRLS || c.rlsManager == nil {
		return nil
	}
	policies := c.rlsManager.ListPolicies()
	sort.Slice(policies, func(i, j int) bool {
		leftTable := strings.ToLower(policies[i].TableName)
		rightTable := strings.ToLower(policies[j].TableName)
		if leftTable != rightTable {
			return leftTable < rightTable
		}
		return strings.ToLower(policies[i].Name) < strings.ToLower(policies[j].Name)
	})
	return policies
}

func (c *Catalog) ListRLSEnabledTables() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.enableRLS || c.rlsManager == nil {
		return nil
	}
	return c.rlsManager.ListEnabledTables()
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

	// Convert back to row format. Fail closed if a column the policy is
	// supposed to preserve is missing from the filtered map — silently
	// substituting nil would degrade a policy error into a wrong result.
	filteredRows := make([][]interface{}, len(filtered))
	for i, mapRow := range filtered {
		row := make([]interface{}, len(columns))
		for j, col := range columns {
			val, ok := mapRow[col]
			if !ok {
				return nil, nil, fmt.Errorf("rls: filtered row %d missing column %q", i, col)
			}
			row[j] = val
		}
		filteredRows[i] = row
	}

	return columns, filteredRows, nil
}

// checkRLSForInsertInternal is a lock-free version of CheckRLSForInsert.
//
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
//
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
//
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
