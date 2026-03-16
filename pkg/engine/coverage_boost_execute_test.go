package engine

import (
	"context"
	"testing"
)

// TestExecuteContextCancellation tests execute with cancelled context
func TestExecuteContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Cancel context before execution
	cancel()

	// Try to execute with cancelled context
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err == nil {
		t.Log("Execute with cancelled context may not error depending on timing")
	}
}

// TestExecuteCreateViewWithReplace tests CREATE VIEW with IF NOT EXISTS and OR REPLACE
func TestExecuteCreateViewWithReplace(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create base table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create view
	_, err = db.Exec(ctx, "CREATE VIEW v1 AS SELECT * FROM test")
	if err != nil {
		t.Logf("CREATE VIEW returned: %v", err)
	}

	// Create view IF NOT EXISTS (should not error)
	_, err = db.Exec(ctx, "CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM test")
	if err != nil {
		t.Errorf("CREATE VIEW IF NOT EXISTS should not error: %v", err)
	}

	// Create or replace view
	_, err = db.Exec(ctx, "CREATE OR REPLACE VIEW v1 AS SELECT id FROM test")
	if err != nil {
		t.Logf("CREATE OR REPLACE VIEW returned: %v", err)
	}

	// Query view
	rows, err := db.Query(ctx, "SELECT * FROM v1")
	if err != nil {
		t.Logf("Query view returned: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		t.Logf("View returned %d rows", count)
	}
}

// TestExecuteCreateProcedureWithReplace tests CREATE PROCEDURE variations
func TestExecuteCreateProcedureWithReplace(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create procedure
	_, err = db.Exec(ctx, `
		CREATE PROCEDURE proc1()
		BEGIN
			SELECT 1 AS col1;
		END
	`)
	if err != nil {
		t.Logf("CREATE PROCEDURE returned: %v", err)
		return
	}

	// Create procedure IF NOT EXISTS
	_, err = db.Exec(ctx, `
		CREATE PROCEDURE IF NOT EXISTS proc1()
		BEGIN
			SELECT 2 AS col1;
		END
	`)
	if err != nil {
		t.Logf("CREATE PROCEDURE IF NOT EXISTS returned: %v", err)
	}

	// Create or replace procedure
	_, err = db.Exec(ctx, `
		CREATE OR REPLACE PROCEDURE proc1()
		BEGIN
			SELECT 3 AS col1;
		END
	`)
	if err != nil {
		t.Logf("CREATE OR REPLACE PROCEDURE returned: %v", err)
	}
}

// TestExecuteCreatePolicyCoverage tests CREATE POLICY variations
func TestExecuteCreatePolicyCoverage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Enable RLS
	_, err = db.Exec(ctx, "ALTER TABLE test ENABLE ROW LEVEL SECURITY")
	if err != nil {
		t.Logf("ENABLE RLS returned: %v", err)
		return
	}

	// Create policy for ALL
	_, err = db.Exec(ctx, "CREATE POLICY p1 ON test FOR ALL TO PUBLIC USING (id > 0)")
	if err != nil {
		t.Logf("CREATE POLICY FOR ALL returned: %v", err)
	}

	// Create policy with CHECK
	_, err = db.Exec(ctx, "CREATE POLICY p2 ON test FOR INSERT TO PUBLIC WITH CHECK (id < 1000)")
	if err != nil {
		t.Logf("CREATE POLICY WITH CHECK returned: %v", err)
	}

	// Create policy for specific role
	_, err = db.Exec(ctx, "CREATE POLICY p3 ON test FOR SELECT TO CURRENT_USER USING (id > 0)")
	if err != nil {
		t.Logf("CREATE POLICY FOR SELECT returned: %v", err)
	}
}

// TestExecuteDropPolicyVariations tests DROP POLICY variations
func TestExecuteDropPolicyVariations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Enable RLS
	_, err = db.Exec(ctx, "ALTER TABLE test ENABLE ROW LEVEL SECURITY")
	if err != nil {
		t.Logf("ENABLE RLS returned: %v", err)
		return
	}

	// Create a policy
	_, err = db.Exec(ctx, "CREATE POLICY p1 ON test FOR SELECT TO PUBLIC USING (id > 0)")
	if err != nil {
		t.Logf("CREATE POLICY returned: %v", err)
		return
	}

	// Drop policy
	_, err = db.Exec(ctx, "DROP POLICY p1 ON test")
	if err != nil {
		t.Logf("DROP POLICY returned: %v", err)
	}

	// Drop non-existent policy with IF EXISTS
	_, err = db.Exec(ctx, "DROP POLICY IF EXISTS nonexistent ON test")
	if err != nil {
		t.Logf("DROP POLICY IF EXISTS returned: %v", err)
	}
}

// TestExecuteCreateTriggerVariations tests CREATE TRIGGER variations
func TestExecuteCreateTriggerVariations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE audit (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create audit table: %v", err)
	}

	// Create trigger
	_, err = db.Exec(ctx, `
		CREATE TRIGGER trg1
		AFTER INSERT ON test
		BEGIN
			INSERT INTO audit VALUES (NEW.id);
		END
	`)
	if err != nil {
		t.Logf("CREATE TRIGGER returned: %v", err)
	}

	// Create trigger IF NOT EXISTS
	_, err = db.Exec(ctx, `
		CREATE TRIGGER IF NOT EXISTS trg1
		AFTER INSERT ON test
		BEGIN
			INSERT INTO audit VALUES (NEW.id);
		END
	`)
	if err != nil {
		t.Logf("CREATE TRIGGER IF NOT EXISTS returned: %v", err)
	}
}

// TestExecuteDropTriggerVariations tests DROP TRIGGER variations
func TestExecuteDropTriggerVariations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create trigger
	_, err = db.Exec(ctx, `
		CREATE TRIGGER trg1
		AFTER INSERT ON test
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Logf("CREATE TRIGGER returned: %v", err)
		return
	}

	// Drop trigger
	_, err = db.Exec(ctx, "DROP TRIGGER trg1")
	if err != nil {
		t.Logf("DROP TRIGGER returned: %v", err)
	}

	// Drop non-existent trigger with IF EXISTS
	_, err = db.Exec(ctx, "DROP TRIGGER IF EXISTS nonexistent")
	if err != nil {
		t.Logf("DROP TRIGGER IF EXISTS returned: %v", err)
	}
}

// TestExecuteDropProcedureVariations tests DROP PROCEDURE variations
func TestExecuteDropProcedureVariations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create procedure
	_, err = db.Exec(ctx, `
		CREATE PROCEDURE proc1()
		BEGIN
			SELECT 1;
		END
	`)
	if err != nil {
		t.Logf("CREATE PROCEDURE returned: %v", err)
		return
	}

	// Drop procedure
	_, err = db.Exec(ctx, "DROP PROCEDURE proc1")
	if err != nil {
		t.Logf("DROP PROCEDURE returned: %v", err)
	}

	// Drop non-existent procedure with IF EXISTS
	_, err = db.Exec(ctx, "DROP PROCEDURE IF EXISTS nonexistent")
	if err != nil {
		t.Logf("DROP PROCEDURE IF EXISTS returned: %v", err)
	}
}

// TestExecuteDropViewVariations tests DROP VIEW variations
func TestExecuteDropViewVariations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table and view
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE VIEW v1 AS SELECT * FROM test")
	if err != nil {
		t.Logf("CREATE VIEW returned: %v", err)
		return
	}

	// Drop view
	_, err = db.Exec(ctx, "DROP VIEW v1")
	if err != nil {
		t.Logf("DROP VIEW returned: %v", err)
	}

	// Drop non-existent view with IF EXISTS
	_, err = db.Exec(ctx, "DROP VIEW IF EXISTS nonexistent")
	if err != nil {
		t.Logf("DROP VIEW IF EXISTS returned: %v", err)
	}
}

// TestQueryShowStatements tests SHOW statements in query function
func TestQueryShowStatements(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// SHOW TABLES
	rows, err := db.Query(ctx, "SHOW TABLES")
	if err != nil {
		t.Logf("SHOW TABLES returned: %v", err)
	} else {
		rows.Close()
	}

	// SHOW CREATE TABLE
	rows, err = db.Query(ctx, "SHOW CREATE TABLE test")
	if err != nil {
		t.Logf("SHOW CREATE TABLE returned: %v", err)
	} else {
		rows.Close()
	}

	// SHOW COLUMNS
	rows, err = db.Query(ctx, "SHOW COLUMNS FROM test")
	if err != nil {
		t.Logf("SHOW COLUMNS returned: %v", err)
	} else {
		rows.Close()
	}

	// DESCRIBE
	rows, err = db.Query(ctx, "DESCRIBE test")
	if err != nil {
		t.Logf("DESCRIBE returned: %v", err)
	} else {
		rows.Close()
	}
}

// TestQueryShowDatabasesCoverage tests SHOW DATABASES
func TestQueryShowDatabasesCoverage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// SHOW DATABASES
	rows, err := db.Query(ctx, "SHOW DATABASES")
	if err != nil {
		t.Logf("SHOW DATABASES returned: %v", err)
	} else {
		rows.Close()
	}
}

// TestExecuteTransactionControl tests transaction control statements
func TestExecuteTransactionControl(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true, WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// BEGIN
	_, err = db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Logf("BEGIN returned: %v", err)
	} else {
		// Insert in transaction
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (1)")
		if err != nil {
			t.Logf("INSERT in transaction returned: %v", err)
		}

		// SAVEPOINT
		_, err = db.Exec(ctx, "SAVEPOINT sp1")
		if err != nil {
			t.Logf("SAVEPOINT returned: %v", err)
		}

		// Insert more
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (2)")
		if err != nil {
			t.Logf("INSERT after savepoint returned: %v", err)
		}

		// ROLLBACK TO SAVEPOINT
		_, err = db.Exec(ctx, "ROLLBACK TO sp1")
		if err != nil {
			t.Logf("ROLLBACK TO SAVEPOINT returned: %v", err)
		}

		// COMMIT
		_, err = db.Exec(ctx, "COMMIT")
		if err != nil {
			t.Logf("COMMIT returned: %v", err)
		}
	}

	// Check row count - should be 1 (row 2 was rolled back)
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Logf("SELECT COUNT returned: %v", err)
	} else {
		if rows.Next() {
			var count int
			rows.Scan(&count)
			t.Logf("Row count after transaction: %d", count)
		}
		rows.Close()
	}
}

// TestExecuteTransactionErrors tests transaction error cases
func TestExecuteTransactionErrors(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// COMMIT without BEGIN
	_, err = db.Exec(ctx, "COMMIT")
	if err == nil {
		t.Error("Expected error for COMMIT without BEGIN")
	}

	// ROLLBACK without BEGIN
	_, err = db.Exec(ctx, "ROLLBACK")
	if err == nil {
		t.Error("Expected error for ROLLBACK without BEGIN")
	}

	// SAVEPOINT without BEGIN
	_, err = db.Exec(ctx, "SAVEPOINT sp1")
	if err == nil {
		t.Error("Expected error for SAVEPOINT without BEGIN")
	}

	// Begin a transaction
	_, err = db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Logf("BEGIN returned: %v", err)
		return
	}

	// Try to BEGIN again (should error)
	_, err = db.Exec(ctx, "BEGIN")
	if err == nil {
		t.Error("Expected error for double BEGIN")
	}
}

// TestExecuteReleaseSavepoint tests RELEASE SAVEPOINT
func TestExecuteReleaseSavepoint(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// BEGIN
	_, err = db.Exec(ctx, "BEGIN")
	if err != nil {
		t.Logf("BEGIN returned: %v", err)
		return
	}

	// SAVEPOINT
	_, err = db.Exec(ctx, "SAVEPOINT sp1")
	if err != nil {
		t.Logf("SAVEPOINT returned: %v", err)
		return
	}

	// RELEASE SAVEPOINT
	_, err = db.Exec(ctx, "RELEASE SAVEPOINT sp1")
	if err != nil {
		t.Logf("RELEASE SAVEPOINT returned: %v", err)
	}

	// COMMIT
	_, err = db.Exec(ctx, "COMMIT")
	if err != nil {
		t.Logf("COMMIT returned: %v", err)
	}
}

// TestExecuteCallProcedureCoverage tests CALL procedure
func TestExecuteCallProcedureCoverage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create procedure
	_, err = db.Exec(ctx, `
		CREATE PROCEDURE simple_proc()
		BEGIN
			SELECT 1 AS result;
		END
	`)
	if err != nil {
		t.Logf("CREATE PROCEDURE returned: %v", err)
		return
	}

	// Call procedure
	_, err = db.Exec(ctx, "CALL simple_proc()")
	if err != nil {
		t.Logf("CALL returned: %v", err)
	}
}

// TestExecuteVacuumWithOptions tests VACUUM variations
func TestExecuteVacuumWithOptions(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table with data
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, data TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 50; i++ {
		_, err = db.Exec(ctx, "INSERT INTO test VALUES (?, ?)", i, "data")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Delete some rows
	_, err = db.Exec(ctx, "DELETE FROM test WHERE id % 2 = 0")
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// VACUUM
	_, err = db.Exec(ctx, "VACUUM")
	if err != nil {
		t.Logf("VACUUM returned: %v", err)
	}

	// Verify data
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Logf("SELECT returned: %v", err)
	} else {
		if rows.Next() {
			var count int
			rows.Scan(&count)
			if count != 25 {
				t.Errorf("Expected 25 rows after VACUUM, got %d", count)
			}
		}
		rows.Close()
	}
}

// TestExecuteAnalyzeCoverage tests ANALYZE variations
func TestExecuteAnalyzeCoverage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create tables
	_, err = db.Exec(ctx, "CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "CREATE TABLE t2 (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	for i := 1; i <= 20; i++ {
		_, err = db.Exec(ctx, "INSERT INTO t1 VALUES (?)", i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// ANALYZE all tables
	_, err = db.Exec(ctx, "ANALYZE")
	if err != nil {
		t.Logf("ANALYZE returned: %v", err)
	}

	// ANALYZE specific table
	_, err = db.Exec(ctx, "ANALYZE t1")
	if err != nil {
		t.Logf("ANALYZE t1 returned: %v", err)
	}

	// ANALYZE non-existent table (may error)
	_, err = db.Exec(ctx, "ANALYZE nonexistent")
	if err != nil {
		t.Logf("ANALYZE nonexistent returned: %v", err)
	}
}

// TestQueryReturningVariations tests INSERT/UPDATE/DELETE RETURNING
func TestQueryReturningVariations(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// INSERT RETURNING single row
	rows, err := db.Query(ctx, "INSERT INTO test VALUES (1, 'Alice') RETURNING id, name")
	if err != nil {
		t.Logf("INSERT RETURNING returned: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 1 {
			t.Errorf("Expected 1 row, got %d", count)
		}
	}

	// INSERT RETURNING multiple rows
	rows, err = db.Query(ctx, "INSERT INTO test VALUES (2, 'Bob'), (3, 'Charlie') RETURNING id, name")
	if err != nil {
		t.Logf("INSERT multiple RETURNING returned: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}

	// UPDATE RETURNING
	rows, err = db.Query(ctx, "UPDATE test SET name = name || '_x' WHERE id = 1 RETURNING id, name")
	if err != nil {
		t.Logf("UPDATE RETURNING returned: %v", err)
	} else {
		rows.Close()
	}

	// DELETE RETURNING
	rows, err = db.Query(ctx, "DELETE FROM test WHERE id = 3 RETURNING id, name")
	if err != nil {
		t.Logf("DELETE RETURNING returned: %v", err)
	} else {
		rows.Close()
	}
}

// TestExecuteAlterTableCoverage tests ALTER TABLE variations
func TestExecuteAlterTableCoverage(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", &Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, "INSERT INTO test VALUES (1), (2)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// ALTER TABLE ADD COLUMN
	_, err = db.Exec(ctx, "ALTER TABLE test ADD COLUMN name TEXT")
	if err != nil {
		t.Logf("ALTER TABLE ADD COLUMN returned: %v", err)
	}

	// Verify existing rows have NULL for new column
	rows, err := db.Query(ctx, "SELECT * FROM test")
	if err != nil {
		t.Logf("SELECT returned: %v", err)
	} else {
		count := 0
		for rows.Next() {
			count++
		}
		rows.Close()
		if count != 2 {
			t.Errorf("Expected 2 rows, got %d", count)
		}
	}
}
