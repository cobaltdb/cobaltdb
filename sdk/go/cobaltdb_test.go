package cobaltdb

import (
	"database/sql"
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		wantErr bool
	}{
		{
			name:    "basic memory",
			dsn:     "cobaltdb://:memory:",
			wantErr: false,
		},
		{
			name:    "with host and port",
			dsn:     "cobaltdb://localhost:15200/testdb",
			wantErr: false,
		},
		{
			name:    "with auth",
			dsn:     "cobaltdb://user:pass@localhost:15200/testdb",
			wantErr: false,
		},
		{
			name:    "with options",
			dsn:     "cobaltdb://localhost:15200/testdb?cache_size=1024&wal_enabled=true",
			wantErr: false,
		},
		{
			name:    "empty dsn uses defaults",
			dsn:     "",
			wantErr: false,
		},
		{
			name:    "non url format parses as key value",
			dsn:     "host=localhost port=15200",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseDSN(tt.dsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && cfg == nil {
				t.Error("ParseDSN() returned nil config without error")
			}
		})
	}
}

func TestConfigDSN(t *testing.T) {
	cfg := &Config{
		Host:       "localhost",
		Port:       15200,
		Database:   "testdb",
		Username:   "admin",
		Password:   "secret",
		CacheSize:  1024,
		WALEnabled: true,
	}

	dsn := cfg.FormatDSN()
	if dsn == "" {
		t.Error("DSN() returned empty string")
	}

	// Parse it back
	cfg2, err := ParseDSN(dsn)
	if err != nil {
		t.Errorf("Failed to parse generated DSN: %v", err)
	}

	if cfg2.Host != cfg.Host {
		t.Errorf("Host mismatch: got %s, want %s", cfg2.Host, cfg.Host)
	}
	if cfg2.Port != cfg.Port {
		t.Errorf("Port mismatch: got %d, want %d", cfg2.Port, cfg.Port)
	}
}

func TestDriverOpen(t *testing.T) {
	driver := &Driver{}

	// Test in-memory database
	conn, err := driver.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open in-memory database: %v", err)
	}
	defer conn.Close()
}

func TestSQLDriver(t *testing.T) {
	// Register and open via database/sql
	db, err := sql.Open("cobaltdb", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	// Test Ping
	if err := db.Ping(); err != nil {
		t.Errorf("db.Ping() failed: %v", err)
	}
}

func TestOpen(t *testing.T) {
	cfg := &Config{
		Host:       "",
		Port:       0,
		Database:   ":memory:",
		CacheSize:  1024,
		WALEnabled: false,
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Error("Open() returned nil db")
	}
}

func TestConnPrepare(t *testing.T) {
	driver := &Driver{}
	conn, err := driver.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer conn.Close()

	// Test prepare
	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Errorf("Prepare() failed: %v", err)
	}
	if stmt != nil {
		stmt.Close()
	}
}

func TestConnBegin(t *testing.T) {
	driver := &Driver{}
	conn, err := driver.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer conn.Close()

	// Test begin transaction
	tx, err := conn.Begin()
	if err != nil {
		t.Errorf("Begin() failed: %v", err)
	}
	if tx != nil {
		tx.Rollback()
	}
}

func TestConfigWithTimeout(t *testing.T) {
	dsn := "host=localhost port=15200 database=testdb connect_timeout=5s query_timeout=10s"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN failed: %v", err)
	}

	if cfg.ConnectTimeout != 5*time.Second {
		t.Errorf("ConnectTimeout mismatch: got %v, want %v", cfg.ConnectTimeout, 5*time.Second)
	}
	if cfg.QueryTimeout != 10*time.Second {
		t.Errorf("QueryTimeout mismatch: got %v, want %v", cfg.QueryTimeout, 10*time.Second)
	}
}

func TestConfigWithSSL(t *testing.T) {
	cfg := &Config{
		Host:        "localhost",
		Port:        15200,
		Database:    "testdb",
		SSLMode:     "verify-full",
		SSLCert:     "/path/to/cert.pem",
		SSLKey:      "/path/to/key.pem",
		SSLRootCert: "/path/to/ca.pem",
	}

	dsn := cfg.FormatDSN()
	cfg2, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN failed: %v", err)
	}

	if cfg2.SSLMode != cfg.SSLMode {
		t.Errorf("SSLMode mismatch: got %s, want %s", cfg2.SSLMode, cfg.SSLMode)
	}
}

func TestConnectorConnect(t *testing.T) {
	cfg := &Config{
		Database:  ":memory:",
		CacheSize: 1024,
	}

	connector := &connector{cfg: cfg, driver: &Driver{}}
	conn, err := connector.Connect(nil)
	if err != nil {
		t.Fatalf("connector.Connect() failed: %v", err)
	}
	defer conn.Close()
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg == nil {
		t.Fatal("DefaultConfig() returned nil")
	}
	if cfg.Host != "localhost" {
		t.Errorf("Expected Host=localhost, got %s", cfg.Host)
	}
	if cfg.Port != 4200 {
		t.Errorf("Expected Port=4200, got %d", cfg.Port)
	}
	if cfg.CacheSize != 1024 {
		t.Errorf("Expected CacheSize=1024, got %d", cfg.CacheSize)
	}
}

func TestOpenWithNilConfig(t *testing.T) {
	db, err := Open(nil)
	if err != nil {
		t.Fatalf("Open(nil) failed: %v", err)
	}
	defer db.Close()
	if db == nil {
		t.Error("Open(nil) returned nil db")
	}
}

func TestDBPing(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	if err := db.Ping(nil); err != nil {
		t.Errorf("Ping() failed: %v", err)
	}
}

func TestDBStats(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if stats.OpenConnections != 1 {
		t.Errorf("Expected OpenConnections=1, got %d", stats.OpenConnections)
	}
}

func TestDBClose(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Double close should not error
	if err := db.Close(); err != nil {
		t.Errorf("Double Close() failed: %v", err)
	}
}

func TestConnectorDriver(t *testing.T) {
	driver := &Driver{}
	cfg := &Config{Database: ":memory:"}
	connector := &connector{cfg: cfg, driver: driver}

	if d := connector.Driver(); d != driver {
		t.Error("Driver() returned wrong driver")
	}
}

func TestParseDSNWithAllOptions(t *testing.T) {
	dsn := "host=testhost port=1234 database=testdb user=testuser password=testpass sslmode=require sslcert=/cert.pem sslkey=/key.pem sslrootcert=/ca.pem connect_timeout=10s query_timeout=20s max_conns=50 application_name=testapp"
	cfg, err := ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN failed: %v", err)
	}

	if cfg.Host != "testhost" {
		t.Errorf("Host mismatch: got %s, want testhost", cfg.Host)
	}
	if cfg.Port != 1234 {
		t.Errorf("Port mismatch: got %d, want 1234", cfg.Port)
	}
	if cfg.Database != "testdb" {
		t.Errorf("Database mismatch: got %s, want testdb", cfg.Database)
	}
	if cfg.Username != "testuser" {
		t.Errorf("Username mismatch: got %s, want testuser", cfg.Username)
	}
	if cfg.Password != "testpass" {
		t.Errorf("Password mismatch: got %s, want testpass", cfg.Password)
	}
	if cfg.SSLMode != "require" {
		t.Errorf("SSLMode mismatch: got %s, want require", cfg.SSLMode)
	}
	if cfg.MaxConnections != 50 {
		t.Errorf("MaxConnections mismatch: got %d, want 50", cfg.MaxConnections)
	}
	if cfg.ApplicationName != "testapp" {
		t.Errorf("ApplicationName mismatch: got %s, want testapp", cfg.ApplicationName)
	}
}

// SDK Coverage Tests

func TestDriverOpenConnectorAndConnect(t *testing.T) {
	drv := &Driver{}

	// OpenConnector with DSN string
	connector, err := drv.OpenConnector(":memory:")
	if err != nil {
		t.Fatalf("OpenConnector() failed: %v", err)
	}
	if connector == nil {
		t.Fatal("OpenConnector() returned nil")
	}

	// Test Driver() returns the correct driver
	if d := connector.Driver(); d != drv {
		t.Error("Driver() returned wrong driver")
	}
}

func TestConnPrepareAndClose(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Prepare statement
	stmt, err := c.Prepare("CREATE TABLE test (id INTEGER)")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}

	// Close should not error
	if err := stmt.Close(); err != nil {
		t.Errorf("Stmt.Close() failed: %v", err)
	}
}

func TestStmtNumInput(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	s, err := c.Prepare("SELECT * FROM test WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}
	defer s.Close()

	// NumInput should return -1
	n := s.(*stmt).NumInput()
	if n != -1 {
		t.Errorf("NumInput() = %d, want -1", n)
	}
}

func TestTransactionDoubleCommit(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// First commit should work
	if err := tx.Commit(); err != nil {
		t.Errorf("First Commit() failed: %v", err)
	}

	// Second commit should error
	if err := tx.Commit(); err == nil {
		t.Error("Second Commit() should have failed")
	}
}

func TestTransactionDoubleRollback(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Begin transaction
	tx, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// First rollback should work
	if err := tx.Rollback(); err != nil {
		t.Errorf("First Rollback() failed: %v", err)
	}

	// Second rollback should error
	if err := tx.Rollback(); err == nil {
		t.Error("Second Rollback() should have failed")
	}
}

func TestIsolationLevelStrings(t *testing.T) {
	// Test non-default levels have valid strings
	tests := []struct {
		level IsolationLevel
		want  string
	}{
		{LevelReadUncommitted, "READ UNCOMMITTED"},
		{LevelReadCommitted, "READ COMMITTED"},
		{LevelWriteCommitted, "WRITE COMMITTED"},
		{LevelRepeatableRead, "REPEATABLE READ"},
		{LevelSnapshot, "SNAPSHOT"},
		{LevelSerializable, "SERIALIZABLE"},
		{LevelLinearizable, "LINEARIZABLE"},
	}

	for _, tt := range tests {
		if got := tt.level.String(); got != tt.want {
			t.Errorf("%d.String() = %q, want %q", tt.level, got, tt.want)
		}
	}
}
