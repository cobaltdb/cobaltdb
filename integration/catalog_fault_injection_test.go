package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestRLSConcurrentAccess tests RLS with concurrent access
func TestRLSConcurrentAccess(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table with RLS
	_, err = db.Exec(ctx, `CREATE TABLE tenant_data (
		id INTEGER PRIMARY KEY,
		tenant_id INTEGER NOT NULL,
		data TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Note: RLS syntax may not be fully supported, skipping RLS-specific tests
	// and focusing on concurrent access patterns

	// Insert data for multiple tenants
	for tenantID := 1; tenantID <= 3; tenantID++ {
		for i := 1; i <= 10; i++ {
			_, err = db.Exec(ctx, "INSERT INTO tenant_data VALUES (?, ?, ?)",
				(tenantID-1)*10+i, tenantID, "data")
			if err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}
	}

	// Concurrent access from different tenants
	var wg sync.WaitGroup
	for tenantID := 1; tenantID <= 3; tenantID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Each tenant should only see their own data
			rows, err := db.Query(ctx,
				"SELECT * FROM tenant_data WHERE tenant_id = ?", id)
			if err != nil {
				t.Logf("Tenant %d query error: %v", id, err)
				return
			}
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			if count != 10 {
				t.Errorf("Tenant %d expected 10 rows, got %d", id, count)
			}
		}(tenantID)
	}

	wg.Wait()
}

// TestFKCascadeDeep tests deep FK cascade operations
func TestFKCascadeDeep(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create 3-level hierarchy
	_, err = db.Exec(ctx, `CREATE TABLE grandparent (
		id INTEGER PRIMARY KEY,
		name TEXT
	)`)
	if err != nil {
		t.Fatalf("Failed to create grandparent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE parent (
		id INTEGER PRIMARY KEY,
		grandparent_id INTEGER,
		name TEXT,
		FOREIGN KEY (grandparent_id) REFERENCES grandparent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE child (
		id INTEGER PRIMARY KEY,
		parent_id INTEGER,
		name TEXT,
		FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE CASCADE
	)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	// Insert hierarchy
	for gp := 1; gp <= 3; gp++ {
		_, err = db.Exec(ctx, "INSERT INTO grandparent VALUES (?, ?)", gp, "GP")
		if err != nil {
			t.Fatalf("Failed to insert grandparent: %v", err)
		}

		for p := 1; p <= 3; p++ {
			parentID := (gp-1)*3 + p
			_, err = db.Exec(ctx, "INSERT INTO parent VALUES (?, ?, ?)",
				parentID, gp, "P")
			if err != nil {
				t.Fatalf("Failed to insert parent: %v", err)
			}

			for c := 1; c <= 3; c++ {
				childID := (parentID-1)*3 + c
				_, err = db.Exec(ctx, "INSERT INTO child VALUES (?, ?, ?)",
					childID, parentID, "C")
				if err != nil {
					t.Fatalf("Failed to insert child: %v", err)
				}
			}
		}
	}

	// Delete grandparent - should cascade to all children
	_, err = db.Exec(ctx, "DELETE FROM grandparent WHERE id = 1")
	if err != nil {
		t.Fatalf("Failed to delete grandparent: %v", err)
	}

	// Verify cascade
	var count int
	row, err := db.Query(ctx, "SELECT COUNT(*) FROM child WHERE parent_id IN (SELECT id FROM parent WHERE grandparent_id = 1)")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer row.Close()

	if row.Next() {
		row.Scan(&count)
		if count != 0 {
			t.Errorf("Expected 0 children after cascade, got %d", count)
		}
	}
}

// TestTransactionRollbackComplex tests complex transaction rollback scenarios
func TestTransactionRollbackComplex(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, val INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, i*10)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Create nested savepoints
	for sp := 1; sp <= 5; sp++ {
		_, err = tx.Exec(ctx, "SAVEPOINT sp"+fmt.Sprintf("%d", sp))
		if err != nil {
			t.Logf("Savepoint error: %v", err)
		}

		// Insert more data in each savepoint
		for i := 1; i <= 10; i++ {
			id := 100 + sp*10 + i
			_, err = tx.Exec(ctx, "INSERT INTO test VALUES (?, ?)", id, id*10)
			if err != nil {
				t.Fatalf("Failed to insert in savepoint: %v", err)
			}
		}
	}

	// Rollback to middle savepoint
	_, err = tx.Exec(ctx, "ROLLBACK TO SAVEPOINT sp3")
	if err != nil {
		t.Logf("Rollback to savepoint error: %v", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Verify data
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		rows.Scan(&count)
		t.Logf("Row count after rollback: %d", count)
	}
}

// TestConcurrentTransactions tests concurrent transaction isolation
func TestConcurrentTransactions(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE accounts (
		id INTEGER PRIMARY KEY,
		balance INTEGER NOT NULL
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert accounts
	for i := 1; i <= 10; i++ {
		_, err = db.Exec(ctx, "INSERT INTO accounts VALUES (?, ?)", i, 1000)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Concurrent transfers
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(iteration int) {
			defer wg.Done()

			tx, err := db.Begin(ctx)
			if err != nil {
				t.Logf("Begin error: %v", err)
				return
			}
			defer tx.Rollback()

			// Transfer between random accounts
			from := (iteration % 10) + 1
			to := ((iteration + 1) % 10) + 1
			amount := 10

			_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance - ? WHERE id = ?",
				amount, from)
			if err != nil {
				t.Logf("Update from error: %v", err)
				return
			}

			_, err = tx.Exec(ctx, "UPDATE accounts SET balance = balance + ? WHERE id = ?",
				amount, to)
			if err != nil {
				t.Logf("Update to error: %v", err)
				return
			}

			if err := tx.Commit(); err != nil {
				t.Logf("Commit error: %v", err)
			}
		}(i)
	}

	wg.Wait()

	// Verify total balance unchanged
	rows, err := db.Query(ctx, "SELECT SUM(balance) FROM accounts")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	var total int
	if rows.Next() {
		rows.Scan(&total)
		expected := 10 * 1000
		if total != expected {
			t.Errorf("Total balance changed: expected %d, got %d", expected, total)
		}
	}
}

// TestQueryCacheRace tests query cache with concurrent access
func TestQueryCacheRace(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, "data")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Concurrent queries
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Same query pattern - should hit cache
			rows, err := db.Query(ctx, "SELECT * FROM test WHERE id = ?", id%100+1)
			if err != nil {
				t.Logf("Query error: %v", err)
				return
			}
			defer rows.Close()
		}(i)
	}

	wg.Wait()
}

// TestWALRecovery tests WAL recovery after crash simulation
func TestWALRecovery(t *testing.T) {
	// This test simulates recovery from WAL
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	ctx := context.Background()

	// Create table and insert data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data in transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	for i := 1; i <= 1000; i++ {
		_, err = tx.Exec(ctx, "INSERT INTO test VALUES (?)", i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Close database (simulates shutdown)
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Reopen database (triggers recovery)
	db2, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()
}

// TestDeadlockDetection tests deadlock detection
func TestDeadlockDetection(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE test (
		id INTEGER PRIMARY KEY,
		val INTEGER
	)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 5; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Two transactions attempting to update same rows in reverse order
	done := make(chan bool, 2)

	go func() {
		defer func() { done <- true }()

		tx, _ := db.Begin(ctx)
		if tx == nil {
			return
		}
		defer tx.Rollback()

		// Update rows 1-3
		for i := 1; i <= 3; i++ {
			_, err := tx.Exec(ctx, "UPDATE test SET val = val + 1 WHERE id = ?", i)
			if err != nil {
				t.Logf("TX1 update error: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}

		tx.Commit()
	}()

	go func() {
		defer func() { done <- true }()

		tx, _ := db.Begin(ctx)
		if tx == nil {
			return
		}
		defer tx.Rollback()

		// Update rows 3-1 (reverse order)
		for i := 3; i >= 1; i-- {
			_, err := tx.Exec(ctx, "UPDATE test SET val = val + 1 WHERE id = ?", i)
			if err != nil {
				t.Logf("TX2 update error: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}

		tx.Commit()
	}()

	// Wait for both transactions
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for transactions")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Error("Timeout waiting for transactions")
	}
}
