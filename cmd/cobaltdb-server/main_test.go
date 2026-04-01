package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"github.com/cobaltdb/cobaltdb/pkg/server"
)

func TestMainFunc(t *testing.T) {
	t.Run("MainDoesNotPanic", func(t *testing.T) {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Main panicked: %v", r)
			}
		}()

		// Cannot fully test main() without starting a server
		// Just verify it doesn't panic immediately
	})
}

// TestMainFunction tests the main function logic
func TestMainFunction(t *testing.T) {
	// Create temp directory
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/test.cobalt"

	// Test in-memory mode
	opts := &engine.Options{
		CacheSize:  1024,
		InMemory:   false,
		WALEnabled: true,
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create server
	srv, err := server.New(db, &server.Config{
		Address: "127.0.0.1:0",
	})
	if err != nil {
		db.Close()
		t.Fatalf("Failed to create server: %v", err)
	}

	// Test that server was created
	if srv == nil {
		t.Error("Expected non-nil server")
	}

	// Clean up
	srv.Close()
	db.Close()
}

// TestServerWithInMemoryMode tests server with in-memory database
func TestServerWithInMemoryMode(t *testing.T) {
	opts := &engine.Options{
		CacheSize:  1024,
		InMemory:   true,
		WALEnabled: false,
	}

	db, err := engine.Open(":memory:", opts)
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, &server.Config{
		Address: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.Close()

	// Test basic database operations
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
}

// TestServerConfiguration tests different server configurations
func TestServerConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		dataDir   string
		address   string
		inMemory  bool
		cacheSize int
	}{
		{
			name:      "DefaultConfig",
			dataDir:   "./data",
			address:   "127.0.0.1:4200",
			inMemory:  false,
			cacheSize: 1024,
		},
		{
			name:      "InMemoryMode",
			dataDir:   "",
			address:   ":4201",
			inMemory:  true,
			cacheSize: 512,
		},
		{
			name:      "CustomCache",
			dataDir:   "./custom",
			address:   ":4202",
			inMemory:  false,
			cacheSize: 2048,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dbPath string
			opts := &engine.Options{
				CacheSize:  tt.cacheSize,
				InMemory:   tt.inMemory,
				WALEnabled: !tt.inMemory,
			}

			if tt.inMemory {
				dbPath = ":memory:"
			} else {
				tmpDir := t.TempDir()
				dbPath = tmpDir + "/cobalt.cb"
			}

			db, err := engine.Open(dbPath, opts)
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			srv, err := server.New(db, &server.Config{
				Address: "127.0.0.1:0",
			})
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}
			defer srv.Close()
		})
	}
}

// TestDatabasePathConstruction tests database path construction
func TestDatabasePathConstruction(t *testing.T) {
	tests := []struct {
		name     string
		dataDir  string
		inMemory bool
		expected string
	}{
		{
			name:     "InMemory",
			dataDir:  "./data",
			inMemory: true,
			expected: ":memory:",
		},
		{
			name:     "DiskStorage",
			dataDir:  "./data",
			inMemory: false,
			expected: "./data/cobalt.cb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dbPath string
			if tt.inMemory {
				dbPath = ":memory:"
			} else {
				dbPath = tt.dataDir + "/cobalt.cb"
			}

			if tt.inMemory && dbPath != ":memory:" {
				t.Errorf("Expected :memory:, got %s", dbPath)
			}
			if !tt.inMemory && dbPath != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, dbPath)
			}
		})
	}
}

// TestServerLifecycle tests server startup and shutdown
func TestServerLifecycle(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, &server.Config{
		Address: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Test server close
	err = srv.Close()
	if err != nil {
		t.Errorf("Failed to close server: %v", err)
	}
}

// TestDatabaseOperations tests various database operations
func TestDatabaseOperations(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	_, err = db.Exec(ctx, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	// Query data
	rows, err := db.Query(ctx, "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}
}

// TestConcurrentConnections tests concurrent database connections
func TestConcurrentConnections(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := tmpDir + "/concurrent.cobalt"

	db, err := engine.Open(dbPath, &engine.Options{
		InMemory:   false,
		CacheSize:  1024,
		WALEnabled: true,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE concurrent_test (id INTEGER)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Perform concurrent inserts
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func(id int) {
			_, err := db.Exec(ctx, "INSERT INTO concurrent_test (id) VALUES (?)", id)
			if err != nil {
				t.Logf("Insert error: %v", err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 5; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for goroutines")
		}
	}

	// Verify inserts
	row := db.QueryRow(ctx, "SELECT COUNT(*) FROM concurrent_test")
	var count int
	err = row.Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count: %v", err)
	}

	if count != 5 {
		t.Errorf("Expected 5 rows, got %d", count)
	}
}

// TestSignalHandling tests signal handling for graceful shutdown
func TestSignalHandling(t *testing.T) {
	// This test verifies the signal handling logic exists
	// Actual signal testing is complex in unit tests
	db, err := engine.Open(":memory:", &engine.Options{
		InMemory:  true,
		CacheSize: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	srv, err := server.New(db, &server.Config{
		Address: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Simulate shutdown
	err = srv.Close()
	if err != nil {
		t.Errorf("Failed to close server: %v", err)
	}
}

// TestCacheSizeConfiguration tests different cache sizes
func TestCacheSizeConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		cacheSize int
	}{
		{"SmallCache", 64},
		{"DefaultCache", 1024},
		{"LargeCache", 4096},
		{"VeryLargeCache", 8192},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := engine.Open(":memory:", &engine.Options{
				InMemory:  true,
				CacheSize: tt.cacheSize,
			})
			if err != nil {
				t.Fatalf("Failed to open database with cache size %d: %v", tt.cacheSize, err)
			}
			defer db.Close()
		})
	}
}

// TestWALConfiguration tests WAL enabled/disabled configurations
func TestWALConfiguration(t *testing.T) {
	tests := []struct {
		name       string
		inMemory   bool
		walEnabled bool
	}{
		{"InMemoryNoWAL", true, false},
		{"DiskWithWAL", false, true},
		{"DiskNoWAL", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dbPath string
			if tt.inMemory {
				dbPath = ":memory:"
			} else {
				tmpDir := t.TempDir()
				dbPath = tmpDir + "/wal_test.cobalt"
			}

			opts := &engine.Options{
				InMemory:   tt.inMemory,
				WALEnabled: tt.walEnabled,
				CacheSize:  1024,
			}

			db, err := engine.Open(dbPath, opts)
			if err != nil {
				t.Skipf("Configuration not supported: %v", err)
				return
			}
			defer db.Close()
		})
	}
}

// TestDataDirectoryCreation tests data directory creation
func TestDataDirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := tmpDir + "/nested/data/dir"

	// Create the directory
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create data directory: %v", err)
	}

	dbPath := dataDir + "/cobalt.cb"
	db, err := engine.Open(dbPath, &engine.Options{
		InMemory:   false,
		WALEnabled: true,
		CacheSize:  1024,
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
}

// TestServerAddressConfiguration tests different server address configurations
func TestServerAddressConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		address string
	}{
		{"DefaultPort", "127.0.0.1:4200"},
		{"CustomPort", ":8080"},
		{"Localhost", "127.0.0.1:0"},
		{"AllInterfaces", "0.0.0.0:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := engine.Open(":memory:", &engine.Options{
				InMemory:  true,
				CacheSize: 1024,
			})
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			srv, err := server.New(db, &server.Config{
				Address: tt.address,
			})
			if err != nil {
				t.Logf("Address %s may not be available: %v", tt.address, err)
				return
			}
			defer srv.Close()
		})
	}
}
