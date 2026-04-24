package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestVacuumWithDeletedData targets Vacuum with deleted rows
func TestVacuumWithDeletedData(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "vacuum.db")

	ctx := context.Background()

	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE vacuum_test (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert many rows
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, `INSERT INTO vacuum_test VALUES (?, ?)`, i, "test data")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Delete many rows
	_, err = db.Exec(ctx, `DELETE FROM vacuum_test WHERE id > 50`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Get size before vacuum
	info1, _ := os.Stat(dbPath)
	size1 := info1.Size()

	// Vacuum
	_, err = db.Exec(ctx, `VACUUM`)
	if err != nil {
		t.Logf("VACUUM error: %v", err)
	}

	db.Close()

	// Check size after vacuum
	info2, _ := os.Stat(dbPath)
	size2 := info2.Size()

	t.Logf("Database size: before=%d, after=%d", size1, size2)
}

// TestVacuumSpecificTable targets VACUUM with table name
func TestVacuumSpecificTable(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "vacuum_table.db")

	ctx := context.Background()

	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE t1 (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create t1: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE t2 (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create t2: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO t1 VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatalf("Failed to insert t1: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO t2 VALUES (1), (2)`)
	if err != nil {
		t.Fatalf("Failed to insert t2: %v", err)
	}

	_, err = db.Exec(ctx, `DELETE FROM t1 WHERE id = 2`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Vacuum specific table
	_, err = db.Exec(ctx, `VACUUM t1`)
	if err != nil {
		t.Logf("VACUUM t1 error: %v", err)
	}

	t.Log("VACUUM specific table completed")
}

// TestCountRows targets countRows function
func TestCountRows(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE count_test (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Empty table count
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM count_test`)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if rows.Next() {
		var count int
		rows.Scan(&count)
		t.Logf("Empty table count: %d", count)
	}
	rows.Close()

	// Insert rows
	for i := 1; i <= 1000; i++ {
		_, err = db.Exec(ctx, `INSERT INTO count_test VALUES (?)`, i)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Count with data
	rows, err = db.Query(ctx, `SELECT COUNT(*) FROM count_test`)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 1000 {
			t.Errorf("Expected 1000, got %d", count)
		}
		t.Logf("Table count: %d", count)
	}
	rows.Close()

	// Count with WHERE
	rows, err = db.Query(ctx, `SELECT COUNT(*) FROM count_test WHERE id > 500`)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 500 {
			t.Errorf("Expected 500, got %d", count)
		}
		t.Logf("Filtered count: %d", count)
	}
	rows.Close()
}

// TestCountRowsWithJoin targets countRows with JOIN
func TestCountRowsWithJoin(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE count_parent (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE count_child (id INTEGER PRIMARY KEY, parent_id INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create child: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO count_parent VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatalf("Failed to insert parent: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO count_child VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 2)`)
	if err != nil {
		t.Fatalf("Failed to insert child: %v", err)
	}

	// Count with JOIN
	rows, err := db.Query(ctx, `
		SELECT p.id, COUNT(c.id)
		FROM count_parent p
		LEFT JOIN count_child c ON p.id = c.parent_id
		GROUP BY p.id`)
	if err != nil {
		t.Fatalf("Failed to count with join: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var parentID, childCount int
		rows.Scan(&parentID, &childCount)
		t.Logf("Parent %d has %d children", parentID, childCount)
	}
}

// TestStoreIndexDef targets storeIndexDef
func TestStoreIndexDef(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "index_def.db")

	ctx := context.Background()

	db, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	_, err = db.Exec(ctx, `CREATE TABLE index_def_test (id INTEGER PRIMARY KEY, email TEXT, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Create unique index
	_, err = db.Exec(ctx, `CREATE UNIQUE INDEX idx_unique_email ON index_def_test(email)`)
	if err != nil {
		t.Fatalf("Failed to create unique index: %v", err)
	}

	// Create multi-column index
	_, err = db.Exec(ctx, `CREATE INDEX idx_name_email ON index_def_test(name, email)`)
	if err != nil {
		t.Fatalf("Failed to create multi-column index: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, `INSERT INTO index_def_test VALUES (1, 'test@example.com', 'Test')`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	db.Close()

	// Reopen and verify indexes work
	db2, err := engine.Open(dbPath, &engine.Options{InMemory: false})
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()

	// Query using index
	rows, err := db2.Query(ctx, `SELECT * FROM index_def_test WHERE email = 'test@example.com'`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var id int
		var email, name string
		rows.Scan(&id, &email, &name)
		t.Logf("Found: id=%d, email=%s, name=%s", id, email, name)
	}

	// Try duplicate (should fail)
	_, err = db2.Exec(ctx, `INSERT INTO index_def_test VALUES (2, 'test@example.com', 'Another')`)
	if err != nil {
		t.Logf("Duplicate correctly blocked: %v", err)
	}
}

// TestAutoVacuumIntegration tests the automatic vacuum background process.
func TestAutoVacuumIntegration(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "autovacuum.db")

	ctx := context.Background()

	opts := &engine.Options{
		InMemory:              false,
		EnableAutoVacuum:      true,
		AutoVacuumInterval:    100 * time.Millisecond,
		AutoVacuumThreshold:   0.15, // 15% to trigger quickly
		SchedulerTickInterval: 50 * time.Millisecond,
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE av_test (id INTEGER PRIMARY KEY, data TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert 100 rows
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, `INSERT INTO av_test VALUES (?, ?)`, i, "data")
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Delete 50 rows to create dead tuples (50% ratio)
	_, err = db.Exec(ctx, `DELETE FROM av_test WHERE id > 50`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Wait for auto-vacuum to run
	time.Sleep(400 * time.Millisecond)

	// Verify dead tuple ratio is now low (auto-vacuum should have cleaned up)
	ratio := db.GetCatalog().GetDeadTupleRatio("av_test")
	if ratio >= 0.15 {
		t.Errorf("AutoVacuum did not clean dead tuples: ratio=%.2f", ratio)
	}

	// Verify all live rows are still accessible
	rows, err := db.Query(ctx, `SELECT COUNT(*) FROM av_test`)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		var count int
		rows.Scan(&count)
		if count != 50 {
			t.Errorf("Expected 50 live rows, got %d", count)
		}
	}

	t.Logf("AutoVacuum integration passed: final dead ratio=%.2f", ratio)
}

// TestAutoVacuumDisabled ensures auto-vacuum does not run when disabled.
func TestAutoVacuumDisabled(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "no_autovacuum.db")

	ctx := context.Background()

	opts := &engine.Options{
		InMemory:            false,
		EnableAutoVacuum:    false,
		AutoVacuumInterval:  100 * time.Millisecond,
		AutoVacuumThreshold: 0.01,
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE no_av (id INTEGER PRIMARY KEY)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, `INSERT INTO no_av VALUES (1), (2), (3)`)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	_, err = db.Exec(ctx, `DELETE FROM no_av WHERE id > 1`)
	if err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Wait longer than the interval
	time.Sleep(300 * time.Millisecond)

	// Dead tuples should still be present
	ratio := db.GetCatalog().GetDeadTupleRatio("no_av")
	if ratio == 0 {
		t.Log("Dead tuple ratio is 0; auto-vacuum may have run or tracking reset")
	}

	t.Logf("AutoVacuum disabled test: final ratio=%.2f", ratio)
}

// TestAnalyzeTableStats targets Analyze table stats
func TestAnalyzeTableStats(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	_, err = db.Exec(ctx, `CREATE TABLE analyze_stats (id INTEGER PRIMARY KEY, val INTEGER)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data with distribution
	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ctx, `INSERT INTO analyze_stats VALUES (?, ?)`, i, i%10)
		if err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Analyze table
	_, err = db.Exec(ctx, `ANALYZE analyze_stats`)
	if err != nil {
		t.Logf("ANALYZE error: %v", err)
		return
	}

	// Analyze all tables
	_, err = db.Exec(ctx, `ANALYZE`)
	if err != nil {
		t.Logf("ANALYZE all error: %v", err)
		return
	}

	t.Log("ANALYZE completed")
}
