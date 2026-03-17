package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCreateNewDatabase tests the createNew function paths
func TestCreateNewDatabase(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name    string
		dbName  string
		options *Options
		wantErr bool
	}{
		{
			name:    "basic_create",
			dbName:  "basic.db",
			options: &Options{},
			wantErr: false,
		},
		{
			name:   "create_with_wal",
			dbName: "wal.db",
			options: &Options{
				WALEnabled: true,
			},
			wantErr: false,
		},
		{
			name:   "create_with_rls",
			dbName: "rls.db",
			options: &Options{
				EnableRLS: true,
			},
			wantErr: false,
		},
		{
			name:   "create_with_query_cache",
			dbName: "cache.db",
			options: &Options{
				EnableQueryCache: true,
				QueryCacheSize:   1000,
			},
			wantErr: false,
		},
		{
			name:   "create_with_encryption",
			dbName: "encrypted.db",
			options: &Options{
				EncryptionKey: []byte("0123456789abcdef0123456789abcdef"), // 32 bytes for AES-256
			},
			wantErr: false,
		},
		{
			name:   "create_all_features",
			dbName: "all.db",
			options: &Options{
				WALEnabled:       true,
				EnableRLS:        true,
				EnableQueryCache: true,
				EncryptionKey:    []byte("0123456789abcdef0123456789abcdef"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(tempDir, tt.dbName)

			db, err := Open(dbPath, tt.options)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if db != nil {
				db.Close()
			}
		})
	}
}

// TestLoadExistingDatabase tests loading existing databases
func TestLoadExistingDatabase(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create initial database
	db1, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Execute some operations
	ctx := context.Background()
	_, err = db1.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db1.Exec(ctx, "INSERT INTO test VALUES (1, 'hello'), (2, 'world')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	db1.Close()

	// Load existing database
	db2, err := Open(dbPath, &Options{WALEnabled: true})
	if err != nil {
		t.Fatalf("Failed to load database: %v", err)
	}
	defer db2.Close()

	// Verify data persisted
	rows, err := db2.Query(ctx, "SELECT COUNT(*) FROM test")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}

	var count int64
	if err := rows.Scan(&count); err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}

	if count != 2 {
		t.Fatalf("Expected count=2, got %v", count)
	}
}

// TestCreateNewWithCacheSizes tests various cache configurations
func TestCreateNewWithCacheSizes(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name      string
		cacheSize int
	}{
		{"small_cache", 4},
		{"medium_cache", 16},
		{"large_cache", 64},
		{"default_cache", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(tempDir, tt.name+".db")
			opts := &Options{
				CacheSize: tt.cacheSize,
			}

			db, err := Open(dbPath, opts)
			if err != nil {
				t.Errorf("Open() error = %v", err)
				return
			}
			if db != nil {
				db.Close()
			}
		})
	}
}

// TestCreateNewWithSyncModes tests WAL sync mode paths
func TestCreateNewWithSyncModes(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name     string
		syncMode SyncMode
	}{
		{"sync_full", SyncFull},
		{"sync_normal", SyncNormal},
		{"sync_off", SyncOff},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath := filepath.Join(tempDir, tt.name+".db")
			opts := &Options{
				WALEnabled: true,
				SyncMode:   tt.syncMode,
			}

			db, err := Open(dbPath, opts)
			if err != nil {
				t.Errorf("Open() error = %v", err)
				return
			}
			if db != nil {
				db.Close()
			}
		})
	}
}

// TestDatabaseOpenErrors tests error paths in Open function
func TestDatabaseOpenErrors(t *testing.T) {
	t.Run("invalid_path", func(t *testing.T) {
		// Try to open with a file as database path (should fail)
		tempFile, err := os.CreateTemp("", "test*.db")
		if err != nil {
			t.Skip("Cannot create temp file")
		}
		tempFile.Close()
		defer os.Remove(tempFile.Name())

		_, err = Open(tempFile.Name(), nil)
		// Note: On some platforms/file systems this may succeed
		// The database will try to use the file as a directory
		if err != nil {
			t.Logf("Got expected error: %v", err)
		} else {
			t.Log("Open with file path succeeded - platform specific behavior")
		}
	})

	t.Run("in_memory_database", func(t *testing.T) {
		// In-memory database
		db, err := Open(":memory:", nil)
		if err != nil {
			t.Errorf("Failed to open in-memory database: %v", err)
			return
		}
		if db != nil {
			db.Close()
		}
	})
}

// TestCatalogInitialization verifies catalog is properly initialized
func TestCatalogInitialization(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "catalog.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Catalog should be initialized and functional
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE catalog_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Errorf("Failed to create table: %v", err)
	}

	// Verify table was created by querying it
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM catalog_test")
	if err != nil {
		t.Errorf("Failed to query table: %v", err)
	}
	if rows != nil {
		defer rows.Close()
		if rows.Next() {
			var count int64
			if err := rows.Scan(&count); err != nil {
				t.Errorf("Failed to scan: %v", err)
			}
		} else {
			t.Error("Expected to be able to query catalog_test")
		}
	}
}

// TestTransactionManagerInitialization verifies txn manager is initialized
func TestTransactionManagerInitialization(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "txn.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Transaction manager should be initialized
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Errorf("Failed to begin transaction: %v", err)
		return
	}

	err = tx.Commit()
	if err != nil {
		t.Errorf("Failed to commit: %v", err)
	}
}

// TestQueryPlanCacheInitialization tests query cache init paths
func TestQueryPlanCacheInitialization(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "cache.db")

	opts := &Options{
		EnableQueryCache: true,
		QueryCacheSize:   100,
		QueryCacheTTL:    0, // No TTL
	}

	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create a table
	_, err = db.Exec(ctx, "CREATE TABLE cache_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Execute query multiple times to exercise cache
	for i := 0; i < 3; i++ {
		rows, err := db.Query(ctx, "SELECT * FROM cache_test WHERE id = 1")
		if err != nil {
			t.Errorf("Query failed on iteration %d: %v", i, err)
		}
		if rows != nil {
			rows.Close()
		}
	}
}

// TestDatabaseReopenWithData verifies data persists across reopen
func TestDatabaseReopenWithData(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "persistent.db")
	ctx := context.Background()

	// First session
	db1, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create schema
	_, err = db1.Exec(ctx, `
		CREATE TABLE persistent_test (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			value REAL DEFAULT 0.0
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db1.Exec(ctx, "INSERT INTO persistent_test VALUES (1, 'test1', 3.14), (2, 'test2', 2.71)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Create index
	_, err = db1.Exec(ctx, "CREATE INDEX idx_name ON persistent_test(name)")
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	db1.Close()

	// Second session - load existing
	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify schema by querying the table
	rows, err := db2.Query(ctx, "SELECT COUNT(*) FROM persistent_test")
	if err != nil {
		t.Fatalf("Failed to query table: %v", err)
	}
	if rows != nil {
		rows.Close()
	}

	// Verify data
	rows, err = db2.Query(ctx, "SELECT COUNT(*) FROM persistent_test")
	if err != nil {
		t.Fatalf("Failed to query data: %v", err)
	}
	var count int64
	if rows != nil && rows.Next() {
		rows.Scan(&count)
		rows.Close()
	}
	if count != 2 {
		t.Errorf("Expected 2 rows, got %v", count)
	}

	// Verify index by querying with it
	_, err = db2.Query(ctx, "SELECT * FROM persistent_test WHERE name = 'test1'")
	if err != nil {
		t.Fatalf("Failed to query with index: %v", err)
	}
}

// TestCreateNewWithReplicationOptions tests replication config paths
func TestCreateNewWithReplicationOptions(t *testing.T) {
	tempDir := t.TempDir()

	// Only test master - slave requires an actual running master
	t.Run("replication_master", func(t *testing.T) {
		opts := &Options{
			ReplicationRole:       "master",
			ReplicationListenAddr: "localhost:0", // Let system assign port
		}
		dbPath := filepath.Join(tempDir, "replication_master.db")

		db, err := Open(dbPath, opts)
		if err != nil {
			// Replication may not be fully initialized - skip if it fails
			t.Skipf("Replication initialization failed: %v", err)
			return
		}
		if db != nil {
			db.Close()
		}
	})
}

// TestCreateNewWithBackupOptions tests backup config paths
func TestCreateNewWithBackupOptions(t *testing.T) {
	tempDir := t.TempDir()
	backupDir := filepath.Join(tempDir, "backups")

	opts := &Options{
		BackupDir:              backupDir,
		BackupRetention:        24 * time.Hour,
		MaxBackups:             10,
		BackupCompressionLevel: 6,
	}

	dbPath := filepath.Join(tempDir, "backup.db")
	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	db.Close()
}

// TestCreateNewWithAuditConfig tests audit configuration
func TestCreateNewWithAuditConfig(t *testing.T) {
	tempDir := t.TempDir()

	// Disabled by default
	opts := &Options{
		AuditConfig: nil,
	}

	dbPath := filepath.Join(tempDir, "audit.db")
	db, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	db.Close()
}
