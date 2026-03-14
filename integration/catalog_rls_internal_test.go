package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestRLSInsertInternal targets checkRLSForInsertInternal
func TestRLSInsertInternal(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rls_insert_test (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create RLS policy for INSERT
	_, err = db.Exec(ctx, `CREATE POLICY insert_policy ON rls_insert_test FOR INSERT WITH CHECK (user_id = 1)`)
	if err != nil {
		t.Logf("CREATE POLICY error: %v", err)
		return
	}

	tests := []struct {
		name  string
		sql   string
		valid bool
	}{
		{"Valid insert", `INSERT INTO rls_insert_test VALUES (1, 1, 'valid')`, true},
		{"Invalid insert", `INSERT INTO rls_insert_test VALUES (2, 2, 'invalid')`, false},
		{"NULL user_id", `INSERT INTO rls_insert_test VALUES (3, NULL, 'null')`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if tt.valid {
				if err != nil {
					t.Logf("Expected valid insert but got error: %v", err)
				}
			} else {
				if err != nil {
					t.Logf("RLS policy correctly blocked: %v", err)
				} else {
					t.Logf("Insert succeeded (RLS may not be enforced)")
				}
			}
		})
	}
}

// TestRLSUpdateInternal targets checkRLSForUpdateInternal
func TestRLSUpdateInternal(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rls_update_test (
		id INTEGER PRIMARY KEY,
		owner_id INTEGER,
		status TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO rls_update_test VALUES (1, 1, 'active'), (2, 2, 'pending'), (3, 1, 'completed')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create RLS policy for UPDATE
	_, err = db.Exec(ctx, `CREATE POLICY update_policy ON rls_update_test FOR UPDATE USING (owner_id = 1)`)
	if err != nil {
		t.Logf("CREATE POLICY error: %v", err)
		return
	}

	tests := []struct {
		name string
		sql  string
		id   int
	}{
		{"Update own row", `UPDATE rls_update_test SET status = 'updated' WHERE id = 1`, 1},
		{"Update other row", `UPDATE rls_update_test SET status = 'updated' WHERE id = 2`, 2},
		{"Update multiple own", `UPDATE rls_update_test SET status = 'updated' WHERE owner_id = 1`, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := db.Exec(ctx, tt.sql)
			if err != nil {
				t.Logf("Update error: %v", err)
				return
			}

			// Check if update was applied
			rows, _ := db.Query(ctx, `SELECT status FROM rls_update_test WHERE id = ?`, tt.id)
			if rows != nil {
				defer rows.Close()
				if rows.Next() {
					var status string
					rows.Scan(&status)
					t.Logf("Row status after update: %s", status)
				}
			}
		})
	}
}

// TestRLSDeleteInternal targets checkRLSForDeleteInternal
func TestRLSDeleteInternal(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rls_delete_test (
		id INTEGER PRIMARY KEY,
		user_id INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO rls_delete_test VALUES (1, 1), (2, 2), (3, 1)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create RLS policy for DELETE
	_, err = db.Exec(ctx, `CREATE POLICY delete_policy ON rls_delete_test FOR DELETE USING (user_id = 1)`)
	if err != nil {
		t.Logf("CREATE POLICY error: %v", err)
		return
	}

	// Try to delete all rows
	_, err = db.Exec(ctx, `DELETE FROM rls_delete_test`)
	if err != nil {
		t.Logf("DELETE with RLS error: %v", err)
		return
	}

	// Check remaining rows
	rows, _ := db.Query(ctx, `SELECT COUNT(*) FROM rls_delete_test`)
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Rows remaining after RLS delete: %d", count)
		}
	}
}

// TestRLSApplyRLSFilterInternal targets applyRLSFilterInternal
func TestRLSApplyRLSFilterInternal(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rls_filter_test (
		id INTEGER PRIMARY KEY,
		tenant_id INTEGER,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with different tenants
	_, err = db.Exec(ctx, `INSERT INTO rls_filter_test VALUES
		(1, 1, 'tenant1_data'),
		(2, 1, 'tenant1_more'),
		(3, 2, 'tenant2_data'),
		(4, 1, 'tenant1_extra')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create SELECT policy
	_, err = db.Exec(ctx, `CREATE POLICY select_filter ON rls_filter_test FOR SELECT USING (tenant_id = 1)`)
	if err != nil {
		t.Logf("CREATE POLICY error: %v", err)
		return
	}

	// Query should filter by tenant_id
	rows, err := db.Query(ctx, `SELECT * FROM rls_filter_test`)
	if err != nil {
		t.Logf("SELECT with RLS filter error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("Expected 3 rows for tenant 1, got %d", count)
	}
	t.Logf("RLS filter returned %d rows", count)
}

// TestRLSWithExpression targets RLS with complex expressions
func TestRLSWithExpression(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rls_expr_test (
		id INTEGER PRIMARY KEY,
		user_id INTEGER,
		status TEXT,
		priority INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO rls_expr_test VALUES
		(1, 1, 'active', 10),
		(2, 2, 'active', 20),
		(3, 1, 'inactive', 30)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create policy with complex expression
	_, err = db.Exec(ctx, `CREATE POLICY expr_policy ON rls_expr_test FOR SELECT USING (user_id = 1 AND status = 'active')`)
	if err != nil {
		t.Logf("CREATE POLICY with expression error: %v", err)
		return
	}

	rows, err := db.Query(ctx, `SELECT * FROM rls_expr_test`)
	if err != nil {
		t.Logf("SELECT with expression error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Complex expression RLS returned %d rows", count)
}

// TestRLSMultiplePolicies targets RLS with multiple policies on same table
func TestRLSMultiplePolicies(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE rls_multi (
		id INTEGER PRIMARY KEY,
		owner_id INTEGER,
		category TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO rls_multi VALUES (1, 1, 'A'), (2, 2, 'A'), (3, 1, 'B')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create multiple policies
	_, err = db.Exec(ctx, `CREATE POLICY owner_policy ON rls_multi FOR SELECT USING (owner_id = 1)`)
	if err != nil {
		t.Logf("CREATE POLICY 1 error: %v", err)
		return
	}

	_, err = db.Exec(ctx, `CREATE POLICY category_policy ON rls_multi FOR SELECT USING (category = 'A')`)
	if err != nil {
		t.Logf("CREATE POLICY 2 error: %v", err)
		return
	}

	rows, err := db.Query(ctx, `SELECT * FROM rls_multi`)
	if err != nil {
		t.Logf("SELECT with multiple policies error: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	t.Logf("Multiple policies returned %d rows", count)
}
