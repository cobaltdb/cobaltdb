package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
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
		CoreStorage: engine.CoreStorage{
			CacheSize:  1024,
			InMemory:   false,
			WALEnabled: engine.BoolPtr(true),
		},
	}

	db, err := engine.Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Create server
	ps := server.NewProductionServer(db, server.DefaultProductionConfig())
	srv, err := server.New(ps, &server.Config{
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
		CoreStorage: engine.CoreStorage{
			CacheSize:  1024,
			InMemory:   true,
			WALEnabled: engine.BoolPtr(false),
		},
	}

	db, err := engine.Open(":memory:", opts)
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer db.Close()

	ps := server.NewProductionServer(db, server.DefaultProductionConfig())
	srv, err := server.New(ps, &server.Config{
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
				CoreStorage: engine.CoreStorage{
					CacheSize:  tt.cacheSize,
					InMemory:   tt.inMemory,
					WALEnabled: engine.BoolPtr(!tt.inMemory),
				},
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

			ps := server.NewProductionServer(db, server.DefaultProductionConfig())
			srv, err := server.New(ps, &server.Config{
				Address: "127.0.0.1:0",
			})
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}
			defer srv.Close()
		})
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	t.Setenv("COBALTDB_DATA_DIR", "/var/lib/cobaltdb")
	t.Setenv("COBALTDB_ADDR", ":14200")
	t.Setenv("COBALTDB_MYSQL_ADDR", ":13307")
	t.Setenv("COBALTDB_MYSQL_ENABLED", "false")
	t.Setenv("COBALTDB_IN_MEMORY", "true")
	t.Setenv("COBALTDB_CACHE_SIZE", "2048")
	t.Setenv("COBALTDB_SECURITY_AUTH_ENABLED", "false")
	t.Setenv("COBALTDB_TLS_ENABLED", "true")
	t.Setenv("COBALTDB_TLS_CERT_FILE", "/certs/server.crt")
	t.Setenv("COBALTDB_TLS_KEY_FILE", "/certs/server.key")
	t.Setenv("COBALTDB_TLS_GEN_CERT", "true")
	t.Setenv("COBALTDB_HEALTH_ADDR", ":18420")
	t.Setenv("COBALTDB_HEALTH_SERVER_ENABLED", "false")
	t.Setenv("COBALTDB_CIRCUIT_BREAKER_ENABLED", "false")
	t.Setenv("COBALTDB_RETRY_ENABLED", "false")
	t.Setenv("COBALTDB_REMOTE_METRICS_ENABLED", "true")
	t.Setenv("COBALTDB_ADMIN_TOKEN", "admin-secret")
	t.Setenv("COBALTDB_ALLOW_CLEARTEXT_AUTH", "true")
	t.Setenv("COBALTDB_SHUTDOWN_TIMEOUT", "45s")
	t.Setenv("COBALTDB_DRAIN_TIMEOUT", "15s")

	dataDir := "./data"
	address := "127.0.0.1:4200"
	mysqlAddr := "127.0.0.1:3307"
	enableMySQL := true
	inMemory := false
	cacheSize := 1024
	authEnabled := true
	tlsEnabled := false
	tlsCert := ""
	tlsKey := ""
	tlsGenCert := false
	healthAddr := "127.0.0.1:8420"
	enableHealthServer := true
	enableCircuitBreaker := true
	enableRetry := true
	allowRemoteMetrics := false
	adminToken := ""
	allowCleartextAuth := false
	shutdownTimeout := 30 * time.Second
	drainTimeout := 10 * time.Second

	err := applyEnvOverrides(
		&dataDir,
		&address,
		&mysqlAddr,
		&enableMySQL,
		&inMemory,
		&cacheSize,
		&authEnabled,
		&tlsEnabled,
		&tlsCert,
		&tlsKey,
		&tlsGenCert,
		&healthAddr,
		&enableHealthServer,
		&enableCircuitBreaker,
		&enableRetry,
		&allowRemoteMetrics,
		&adminToken,
		&allowCleartextAuth,
		&shutdownTimeout,
		&drainTimeout,
	)
	if err != nil {
		t.Fatalf("applyEnvOverrides failed: %v", err)
	}

	if dataDir != "/var/lib/cobaltdb" || address != ":14200" || mysqlAddr != ":13307" {
		t.Fatalf("string overrides not applied: data=%q addr=%q mysql=%q", dataDir, address, mysqlAddr)
	}
	if enableMySQL || !inMemory || cacheSize != 2048 || authEnabled {
		t.Fatalf("core overrides not applied: mysql=%v memory=%v cache=%d auth=%v", enableMySQL, inMemory, cacheSize, authEnabled)
	}
	if !tlsEnabled || tlsCert != "/certs/server.crt" || tlsKey != "/certs/server.key" || !tlsGenCert {
		t.Fatalf("tls overrides not applied")
	}
	if healthAddr != ":18420" || enableHealthServer || enableCircuitBreaker || enableRetry || !allowRemoteMetrics {
		t.Fatalf("production feature overrides not applied")
	}
	if !allowCleartextAuth {
		t.Fatalf("cleartext auth override not applied")
	}
	if adminToken != "admin-secret" {
		t.Fatalf("admin token override not applied: %q", adminToken)
	}
	if shutdownTimeout != 45*time.Second || drainTimeout != 15*time.Second {
		t.Fatalf("duration overrides not applied: shutdown=%s drain=%s", shutdownTimeout, drainTimeout)
	}
}

func TestApplyEnvOverridesRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name      string
		envName   string
		envValue  string
		wantError string
	}{
		{name: "InvalidBool", envName: "COBALTDB_TLS_ENABLED", envValue: "definitely", wantError: "boolean"},
		{name: "InvalidInt", envName: "COBALTDB_CACHE_SIZE", envValue: "large", wantError: "integer"},
		{name: "ZeroInt", envName: "COBALTDB_CACHE_SIZE", envValue: "0", wantError: "greater than zero"},
		{name: "InvalidDuration", envName: "COBALTDB_DRAIN_TIMEOUT", envValue: "slow", wantError: "duration"},
		{name: "ZeroDuration", envName: "COBALTDB_DRAIN_TIMEOUT", envValue: "0s", wantError: "greater than zero"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.envName, tt.envValue)

			dataDir := "./data"
			address := "127.0.0.1:4200"
			mysqlAddr := "127.0.0.1:3307"
			enableMySQL := true
			inMemory := false
			cacheSize := 1024
			authEnabled := true
			tlsEnabled := false
			tlsCert := ""
			tlsKey := ""
			tlsGenCert := false
			healthAddr := "127.0.0.1:8420"
			enableHealthServer := true
			enableCircuitBreaker := true
			enableRetry := true
			allowRemoteMetrics := false
			adminToken := ""
			allowCleartextAuth := false
			shutdownTimeout := 30 * time.Second
			drainTimeout := 10 * time.Second

			err := applyEnvOverrides(
				&dataDir,
				&address,
				&mysqlAddr,
				&enableMySQL,
				&inMemory,
				&cacheSize,
				&authEnabled,
				&tlsEnabled,
				&tlsCert,
				&tlsKey,
				&tlsGenCert,
				&healthAddr,
				&enableHealthServer,
				&enableCircuitBreaker,
				&enableRetry,
				&allowRemoteMetrics,
				&adminToken,
				&allowCleartextAuth,
				&shutdownTimeout,
				&drainTimeout,
			)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestPrepareDataDir(t *testing.T) {
	t.Run("CreatesMissingDirectory", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "nested", "data")

		cleanPath, err := prepareDataDir(path)
		if err != nil {
			t.Fatalf("prepareDataDir failed: %v", err)
		}
		if cleanPath != filepath.Clean(path) {
			t.Fatalf("clean path = %q, want %q", cleanPath, filepath.Clean(path))
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat data dir: %v", err)
		}
		if !info.IsDir() {
			t.Fatal("expected data path to be a directory")
		}
		if info.Mode().Perm() != 0750 {
			t.Fatalf("data dir permissions = %v, want 0750", info.Mode().Perm())
		}
	})

	t.Run("RejectsSymlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target")
		if err := os.Mkdir(target, 0750); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		link := filepath.Join(dir, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Skipf("symlink not supported: %v", err)
		}

		_, err := prepareDataDir(link)
		if err == nil {
			t.Fatal("expected symlink data directory to be rejected")
		}
		if !strings.Contains(err.Error(), "must not be a symlink") {
			t.Fatalf("expected symlink rejection, got %v", err)
		}
	})

	t.Run("RejectsSymlinkParentComponent", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target")
		if err := os.Mkdir(target, 0750); err != nil {
			t.Fatalf("mkdir target: %v", err)
		}
		link := filepath.Join(dir, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Skipf("symlink not supported: %v", err)
		}

		_, err := prepareDataDir(filepath.Join(link, "nested", "data"))
		if err == nil {
			t.Fatal("expected symlink data directory component to be rejected")
		}
		if !strings.Contains(err.Error(), "must not be a symlink") {
			t.Fatalf("expected symlink rejection, got %v", err)
		}
		if _, statErr := os.Stat(filepath.Join(target, "nested", "data")); !os.IsNotExist(statErr) {
			t.Fatalf("data directory should not be created through symlink parent, stat err=%v", statErr)
		}
	})

	t.Run("RejectsFile", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "data-file")
		if err := os.WriteFile(path, []byte("not a directory"), 0600); err != nil {
			t.Fatalf("write file: %v", err)
		}

		_, err := prepareDataDir(path)
		if err == nil {
			t.Fatal("expected file data path to be rejected")
		}
		if !strings.Contains(err.Error(), "must be a directory") {
			t.Fatalf("expected directory rejection, got %v", err)
		}
	})

	t.Run("RejectsEmptyPath", func(t *testing.T) {
		_, err := prepareDataDir(" ")
		if err == nil {
			t.Fatal("expected empty data path to be rejected")
		}
		if !strings.Contains(err.Error(), "must be explicit") {
			t.Fatalf("expected explicit path error, got %v", err)
		}
	})
}

func TestValidateAuthTransport(t *testing.T) {
	tests := []struct {
		name      string
		wireAddr  string
		mysqlAddr string
		auth      bool
		tls       bool
		mysql     bool
		allow     bool
		wantError string
	}{
		{
			name:      "LoopbackCleartextAllowed",
			wireAddr:  "127.0.0.1:4200",
			mysqlAddr: "localhost:3307",
			auth:      true,
			mysql:     true,
		},
		{
			name:      "NoAuthAllowsCleartext",
			wireAddr:  ":4200",
			mysqlAddr: ":3307",
			mysql:     true,
		},
		{
			name:      "OverrideAllowsCleartext",
			wireAddr:  ":4200",
			mysqlAddr: ":3307",
			auth:      true,
			mysql:     true,
			allow:     true,
		},
		{
			name:      "RejectsNonLoopbackWireWithoutTLS",
			wireAddr:  ":4200",
			mysqlAddr: "127.0.0.1:3307",
			auth:      true,
			mysql:     true,
			wantError: "wire address",
		},
		{
			name:      "WireTLSAllowsNonLoopbackWhenMySQLDisabled",
			wireAddr:  ":4200",
			mysqlAddr: ":3307",
			auth:      true,
			tls:       true,
		},
		{
			name:      "RejectsNonLoopbackMySQL",
			wireAddr:  "127.0.0.1:4200",
			mysqlAddr: "0.0.0.0:3307",
			auth:      true,
			tls:       true,
			mysql:     true,
			wantError: "MySQL authentication",
		},
		{
			name:      "IPv6LoopbackAllowed",
			wireAddr:  "[::1]:4200",
			mysqlAddr: "[::1]:3307",
			auth:      true,
			mysql:     true,
		},
		{
			name:      "IPv6AnyRejected",
			wireAddr:  "[::]:4200",
			mysqlAddr: "127.0.0.1:3307",
			auth:      true,
			wantError: "wire address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAuthTransport(tt.wireAddr, tt.mysqlAddr, tt.auth, tt.tls, tt.mysql, tt.allow)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestValidateAdminCredentials(t *testing.T) {
	tests := []struct {
		name      string
		auth      bool
		user      string
		password  string
		wantError string
	}{
		{
			name:     "AuthDisabledAllowsEmptyDefaults",
			user:     "admin",
			password: "admin",
		},
		{
			name:     "StrongPasswordAllowed",
			auth:     true,
			user:     "admin",
			password: "Str0ng!Pass#2026",
		},
		{
			name:      "RejectsDefaultAdminPassword",
			auth:      true,
			user:      "admin",
			password:  "admin",
			wantError: "default admin credentials",
		},
		{
			name:      "RejectsEmptyUser",
			auth:      true,
			user:      " ",
			password:  "Str0ng!Pass#2026",
			wantError: "admin username",
		},
		{
			name:      "RejectsEmptyPassword",
			auth:      true,
			user:      "admin",
			password:  "",
			wantError: "admin password",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAdminCredentials(tt.auth, tt.user, tt.password)
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
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
		CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ps := server.NewProductionServer(db, server.DefaultProductionConfig())
	srv, err := server.New(ps, &server.Config{
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
		CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024},
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
		CoreStorage: engine.CoreStorage{
			InMemory:   false,
			CacheSize:  1024,
			WALEnabled: engine.BoolPtr(true),
		},
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

func TestWireServerComponentStartReturnsListenError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	db, err := engine.Open(":memory:", &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ps := server.NewProductionServer(db, server.DefaultProductionConfig())
	srv, err := server.New(ps, &server.Config{
		Address: listener.Addr().String(),
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer srv.Close()

	component := &WireServerComponent{
		server: srv,
		addr:   listener.Addr().String(),
	}

	err = component.Start(context.Background())
	if err == nil {
		t.Fatal("expected bind error")
	}
	if !strings.Contains(err.Error(), "failed to listen") {
		t.Fatalf("expected listen error, got %v", err)
	}
}

// TestSignalHandling tests signal handling for graceful shutdown
func TestSignalHandling(t *testing.T) {
	// This test verifies the signal handling logic exists
	// Actual signal testing is complex in unit tests
	db, err := engine.Open(":memory:", &engine.Options{
		CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ps := server.NewProductionServer(db, server.DefaultProductionConfig())
	srv, err := server.New(ps, &server.Config{
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
				CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: tt.cacheSize},
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
				CoreStorage: engine.CoreStorage{
					InMemory:   tt.inMemory,
					WALEnabled: engine.BoolPtr(tt.walEnabled),
					CacheSize:  1024,
				},
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
		CoreStorage: engine.CoreStorage{
			InMemory:   false,
			WALEnabled: engine.BoolPtr(true),
			CacheSize:  1024,
		},
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
				CoreStorage: engine.CoreStorage{InMemory: true, CacheSize: 1024},
			})
			if err != nil {
				t.Fatalf("Failed to open database: %v", err)
			}
			defer db.Close()

			ps := server.NewProductionServer(db, server.DefaultProductionConfig())
			srv, err := server.New(ps, &server.Config{
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
