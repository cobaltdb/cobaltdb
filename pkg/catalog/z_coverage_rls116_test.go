package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/security"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newTestCatalogRLS(t *testing.T) *Catalog {
	t.Helper()
	pool := storage.NewBufferPool(1024, storage.NewMemory())
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatal(err)
	}
	return New(tree, pool, nil)
}

// TestApplyRLSFilterInternal116 tests applyRLSFilterInternal with RLS enabled
func TestApplyRLSFilterInternal116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	// Enable RLS
	c.EnableRLS()

	// Create test table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "rls_test_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "owner", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Enable RLS on table
	c.rlsManager.EnableTable("rls_test_table")

	// Create a policy that filters by owner
	policy := &security.Policy{
		Name:       "owner_policy",
		TableName:  "rls_test_table",
		Type:       security.PolicySelect,
		Expression: "owner = CURRENT_USER",
		Enabled:    true,
	}
	err = c.rlsManager.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Test data
	columns := []string{"id", "name", "owner"}
	rows := [][]interface{}{
		{1, "Alice", "admin"},
		{2, "Bob", "user1"},
		{3, "Charlie", "user1"},
		{4, "David", "admin"},
	}

	// Create context with user
	ctx = context.WithValue(ctx, security.RLSUserKey, "user1")

	// Test applyRLSFilterInternal
	filteredCols, filteredRows, err := c.applyRLSFilterInternal(ctx, "rls_test_table", columns, rows, "user1", nil)
	if err != nil {
		t.Fatalf("applyRLSFilterInternal failed: %v", err)
	}

	if len(filteredRows) != 2 {
		t.Errorf("Expected 2 rows for user1, got %d", len(filteredRows))
	}

	// Verify column names are preserved
	if len(filteredCols) != len(columns) {
		t.Errorf("Expected %d columns, got %d", len(columns), len(filteredCols))
	}
}

// TestApplyRLSFilterInternalDisabled116 tests when RLS is disabled
func TestApplyRLSFilterInternalDisabled116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	// Don't enable RLS
	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	// Should return all rows when RLS is disabled
	filteredCols, filteredRows, err := c.applyRLSFilterInternal(ctx, "test_table", columns, rows, "user1", nil)
	if err != nil {
		t.Fatalf("applyRLSFilterInternal failed: %v", err)
	}

	if len(filteredRows) != 2 {
		t.Errorf("Expected 2 rows when RLS disabled, got %d", len(filteredRows))
	}

	if len(filteredCols) != len(columns) {
		t.Errorf("Expected %d columns, got %d", len(columns), len(filteredCols))
	}
}

// TestApplyRLSFilterInternalTableNotEnabled116 tests when table doesn't have RLS enabled
func TestApplyRLSFilterInternalTableNotEnabled116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	// Enable RLS but don't enable on specific table
	c.EnableRLS()

	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	// Should return all rows when table doesn't have RLS enabled
	filteredCols, filteredRows, err := c.applyRLSFilterInternal(ctx, "test_table", columns, rows, "user1", nil)
	if err != nil {
		t.Fatalf("applyRLSFilterInternal failed: %v", err)
	}

	if len(filteredRows) != 2 {
		t.Errorf("Expected 2 rows when table RLS not enabled, got %d", len(filteredRows))
	}

	_ = filteredCols
}

// TestApplyRLSFilterInternalEmptyRows116 tests with empty rows
func TestApplyRLSFilterInternalEmptyRows116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	c.EnableRLS()
	c.rlsManager.EnableTable("test_table")

	columns := []string{"id", "name"}
	rows := [][]interface{}{}

	filteredCols, filteredRows, err := c.applyRLSFilterInternal(ctx, "test_table", columns, rows, "user1", nil)
	if err != nil {
		t.Fatalf("applyRLSFilterInternal failed: %v", err)
	}

	if len(filteredRows) != 0 {
		t.Errorf("Expected 0 rows, got %d", len(filteredRows))
	}

	_ = filteredCols
}

// TestCheckRLSForInsertInternal116 tests checkRLSForInsertInternal
func TestCheckRLSForInsertInternal116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	c.EnableRLS()

	// Create test table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "rls_insert_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "owner", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	c.rlsManager.EnableTable("rls_insert_table")

	// Create insert policy
	policy := &security.Policy{
		Name:       "insert_policy",
		TableName:  "rls_insert_table",
		Type:       security.PolicyInsert,
		Expression: "owner = CURRENT_USER",
		Enabled:    true,
	}
	err = c.rlsManager.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	// Test with matching owner
	ctx = context.WithValue(ctx, security.RLSUserKey, "user1")
	row := map[string]interface{}{
		"id":    1,
		"name":  "Test",
		"owner": "user1",
	}

	allowed, err := c.checkRLSForInsertInternal(ctx, "rls_insert_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForInsertInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected insert to be allowed for matching owner")
	}

	// Test with non-matching owner
	row2 := map[string]interface{}{
		"id":    2,
		"name":  "Test2",
		"owner": "user2",
	}

	allowed, err = c.checkRLSForInsertInternal(ctx, "rls_insert_table", row2, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForInsertInternal failed: %v", err)
	}
	if allowed {
		t.Error("Expected insert to be denied for non-matching owner")
	}
}

// TestCheckRLSForUpdateInternal116 tests checkRLSForUpdateInternal
func TestCheckRLSForUpdateInternal116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	c.EnableRLS()

	// Create test table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "rls_update_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "owner", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	c.rlsManager.EnableTable("rls_update_table")

	// Create update policy
	policy := &security.Policy{
		Name:       "update_policy",
		TableName:  "rls_update_table",
		Type:       security.PolicyUpdate,
		Expression: "owner = CURRENT_USER",
		Enabled:    true,
	}
	err = c.rlsManager.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	ctx = context.WithValue(ctx, security.RLSUserKey, "user1")

	// Test with matching owner
	row := map[string]interface{}{
		"id":    1,
		"name":  "Test",
		"owner": "user1",
	}

	allowed, err := c.checkRLSForUpdateInternal(ctx, "rls_update_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForUpdateInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected update to be allowed for matching owner")
	}

	// Test with non-matching owner
	row2 := map[string]interface{}{
		"id":    2,
		"name":  "Test2",
		"owner": "admin",
	}

	allowed, err = c.checkRLSForUpdateInternal(ctx, "rls_update_table", row2, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForUpdateInternal failed: %v", err)
	}
	if allowed {
		t.Error("Expected update to be denied for non-matching owner")
	}
}

// TestCheckRLSForDeleteInternal116 tests checkRLSForDeleteInternal
func TestCheckRLSForDeleteInternal116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	c.EnableRLS()

	// Create test table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "rls_delete_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "owner", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	c.rlsManager.EnableTable("rls_delete_table")

	// Create delete policy
	policy := &security.Policy{
		Name:       "delete_policy",
		TableName:  "rls_delete_table",
		Type:       security.PolicyDelete,
		Expression: "owner = CURRENT_USER",
		Enabled:    true,
	}
	err = c.rlsManager.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	ctx = context.WithValue(ctx, security.RLSUserKey, "user1")

	// Test with matching owner
	row := map[string]interface{}{
		"id":    1,
		"name":  "Test",
		"owner": "user1",
	}

	allowed, err := c.checkRLSForDeleteInternal(ctx, "rls_delete_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForDeleteInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected delete to be allowed for matching owner")
	}

	// Test with non-matching owner
	row2 := map[string]interface{}{
		"id":    2,
		"name":  "Test2",
		"owner": "admin",
	}

	allowed, err = c.checkRLSForDeleteInternal(ctx, "rls_delete_table", row2, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForDeleteInternal failed: %v", err)
	}
	if allowed {
		t.Error("Expected delete to be denied for non-matching owner")
	}
}

// TestCheckRLSInternalDisabled116 tests internal functions when RLS disabled
func TestCheckRLSInternalDisabled116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	// Don't enable RLS
	row := map[string]interface{}{
		"id":   1,
		"name": "Test",
	}

	// All should return true when RLS is disabled
	allowed, err := c.checkRLSForInsertInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForInsertInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected insert to be allowed when RLS disabled")
	}

	allowed, err = c.checkRLSForUpdateInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForUpdateInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected update to be allowed when RLS disabled")
	}

	allowed, err = c.checkRLSForDeleteInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForDeleteInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected delete to be allowed when RLS disabled")
	}
}

// TestCheckRLSInternalTableNotEnabled116 tests when table RLS not enabled
func TestCheckRLSInternalTableNotEnabled116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	// Enable RLS but not on specific table
	c.EnableRLS()

	row := map[string]interface{}{
		"id":   1,
		"name": "Test",
	}

	// All should return true when table RLS not enabled
	allowed, err := c.checkRLSForInsertInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForInsertInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected insert to be allowed when table RLS not enabled")
	}

	allowed, err = c.checkRLSForUpdateInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForUpdateInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected update to be allowed when table RLS not enabled")
	}

	allowed, err = c.checkRLSForDeleteInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForDeleteInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected delete to be allowed when table RLS not enabled")
	}
}

// TestApplyRLSFilterInternalWithRoles116 tests applyRLSFilterInternal with roles
func TestApplyRLSFilterInternalWithRoles116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	c.EnableRLS()

	// Create test table
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "rls_role_table",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
			{Name: "role_col", Type: query.TokenText},
		},
	})
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	c.rlsManager.EnableTable("rls_role_table")

	// Create policy with role check
	policy := &security.Policy{
		Name:       "role_policy",
		TableName:  "rls_role_table",
		Type:       security.PolicySelect,
		Expression: "role_col = CURRENT_ROLE",
		Enabled:    true,
	}
	err = c.rlsManager.CreatePolicy(policy)
	if err != nil {
		t.Fatalf("Failed to create policy: %v", err)
	}

	columns := []string{"id", "name", "role_col"}
	rows := [][]interface{}{
		{1, "Alice", "admin"},
		{2, "Bob", "user"},
		{3, "Charlie", "admin"},
	}

	// Create context with role
	ctx = context.WithValue(ctx, security.RLSRoleKey, "admin")

	filteredCols, filteredRows, err := c.applyRLSFilterInternal(ctx, "rls_role_table", columns, rows, "user1", []string{"admin"})
	if err != nil {
		t.Fatalf("applyRLSFilterInternal failed: %v", err)
	}

	if len(filteredRows) != 2 {
		t.Errorf("Expected 2 rows for admin role, got %d", len(filteredRows))
	}

	if len(filteredCols) != len(columns) {
		t.Errorf("Expected %d columns, got %d", len(columns), len(filteredCols))
	}
}

// TestApplyRLSFilterInternalNilManager116 tests with nil rlsManager
func TestApplyRLSFilterInternalNilManager116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	// Set enableRLS to true but don't create manager
	c.enableRLS = true
	c.rlsManager = nil

	columns := []string{"id", "name"}
	rows := [][]interface{}{
		{1, "Alice"},
		{2, "Bob"},
	}

	// Should return all rows when manager is nil
	filteredCols, filteredRows, err := c.applyRLSFilterInternal(ctx, "test_table", columns, rows, "user1", nil)
	if err != nil {
		t.Fatalf("applyRLSFilterInternal failed: %v", err)
	}

	if len(filteredRows) != 2 {
		t.Errorf("Expected 2 rows when manager nil, got %d", len(filteredRows))
	}

	if len(filteredCols) != len(columns) {
		t.Errorf("Expected %d columns, got %d", len(columns), len(filteredCols))
	}
}

// TestCheckRLSInternalNilManager116 tests check functions with nil manager
func TestCheckRLSInternalNilManager116(t *testing.T) {
	c := newTestCatalogRLS(t)
	ctx := context.Background()

	// Set enableRLS to true but don't create manager
	c.enableRLS = true
	c.rlsManager = nil

	row := map[string]interface{}{
		"id":   1,
		"name": "Test",
	}

	// All should return true when manager is nil
	allowed, err := c.checkRLSForInsertInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForInsertInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected insert to be allowed when manager nil")
	}

	allowed, err = c.checkRLSForUpdateInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForUpdateInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected update to be allowed when manager nil")
	}

	allowed, err = c.checkRLSForDeleteInternal(ctx, "test_table", row, "user1", nil)
	if err != nil {
		t.Fatalf("checkRLSForDeleteInternal failed: %v", err)
	}
	if !allowed {
		t.Error("Expected delete to be allowed when manager nil")
	}
}
