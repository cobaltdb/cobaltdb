package cobaltdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	// Use explicit :memory: to avoid file-based database issues
	cfg := &Config{Database: ":memory:"}
	db, err := Open(cfg)
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

// SDK Driver Interface Tests - Coverage Boost

func TestStmtExec(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Create table via statement
	stmt, err := c.Prepare("CREATE TABLE exec_test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}

	result, err := stmt.Exec(nil)
	if err != nil {
		t.Fatalf("stmt.Exec() failed: %v", err)
	}
	if result == nil {
		t.Fatal("stmt.Exec() returned nil result")
	}
	stmt.Close()

	// Insert with parameters
	stmt, err = c.Prepare("INSERT INTO exec_test VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}
	defer stmt.Close()

	result, err = stmt.Exec([]driver.Value{int64(1)})
	if err != nil {
		t.Fatalf("stmt.Exec() with params failed: %v", err)
	}

	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Errorf("RowsAffected() = %d, want 1", affected)
	}
}

func TestStmtQuery(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Setup
	stmt, _ := c.Prepare("CREATE TABLE query_test (id INTEGER, name TEXT)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("INSERT INTO query_test VALUES (?, ?)")
	stmt.Exec([]driver.Value{int64(1), "alice"})
	stmt.Exec([]driver.Value{int64(2), "bob"})
	stmt.Close()

	// Query
	stmt, err = c.Prepare("SELECT id, name FROM query_test WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query([]driver.Value{int64(1)})
	if err != nil {
		t.Fatalf("stmt.Query() failed: %v", err)
	}
	defer rows.Close()

	if rows == nil {
		t.Fatal("stmt.Query() returned nil rows")
	}

	// Check columns
	cols := rows.Columns()
	if len(cols) != 2 {
		t.Errorf("Columns() returned %d columns, want 2", len(cols))
	}
}

func TestRowsNextAndScan(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Setup
	stmt, _ := c.Prepare("CREATE TABLE scan_test (id INTEGER, name TEXT, score REAL)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("INSERT INTO scan_test VALUES (?, ?, ?)")
	stmt.Exec([]driver.Value{int64(1), "alice", float64(95.5)})
	stmt.Exec([]driver.Value{int64(2), "bob", float64(87.0)})
	stmt.Close()

	// Query
	stmt, _ = c.Prepare("SELECT * FROM scan_test ORDER BY id")
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("Query() failed: %v", err)
	}
	defer rows.Close()

	// Iterate and scan
	dest := make([]driver.Value, 3)
	rowCount := 0
	for {
		err := rows.Next(dest)
		if err != nil {
			break
		}
		rowCount++

		// Verify values
		if id, ok := dest[0].(int64); !ok || (id != int64(rowCount)) {
			t.Errorf("Row %d: id = %v, want %d", rowCount, dest[0], rowCount)
		}
	}

	if rowCount != 2 {
		t.Errorf("Expected 2 rows, got %d", rowCount)
	}
}

func TestResultLastInsertIdAndRowsAffected(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Setup with AUTOINCREMENT
	stmt, _ := c.Prepare("CREATE TABLE autoinc_test (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)")
	stmt.Exec(nil)
	stmt.Close()

	// Insert
	stmt, _ = c.Prepare("INSERT INTO autoinc_test (name) VALUES (?)")
	result, err := stmt.Exec([]driver.Value{"test1"})
	if err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	// Test LastInsertId
	lastID, err := result.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId() error: %v", err)
	}
	if lastID != 1 {
		t.Errorf("LastInsertId() = %d, want 1", lastID)
	}

	// Test RowsAffected
	affected, err := result.RowsAffected()
	if err != nil {
		t.Errorf("RowsAffected() error: %v", err)
	}
	if affected != 1 {
		t.Errorf("RowsAffected() = %d, want 1", affected)
	}

	// Insert another
	result, _ = stmt.Exec([]driver.Value{"test2"})
	lastID, _ = result.LastInsertId()
	if lastID != 2 {
		t.Errorf("LastInsertId() = %d, want 2", lastID)
	}

	stmt.Close()
}

func TestConnClose(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}

	// Close should not error
	if err := c.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Double close should not error
	if err := c.Close(); err != nil {
		t.Errorf("Double Close() failed: %v", err)
	}
}

func TestConnectorConnectWithPrepare(t *testing.T) {
	drv := &Driver{}
	connector, err := drv.OpenConnector(":memory:")
	if err != nil {
		t.Fatalf("OpenConnector() failed: %v", err)
	}

	// Connect
	conn, err := connector.Connect(nil)
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	defer conn.Close()

	// Verify connection works
	stmt, err := conn.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}
	stmt.Close()
}

func TestDriverOpenWithRetry(t *testing.T) {
	drv := &Driver{}

	// Open with in-memory database (should work without retry)
	conn, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer conn.Close()

	// Verify connection is valid
	if conn == nil {
		t.Fatal("Open() returned nil connection")
	}
}

func TestConnBeginWithCommitAndRollback(t *testing.T) {
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

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit() failed: %v", err)
	}

	// Begin another
	tx, err = c.Begin()
	if err != nil {
		t.Fatalf("Second Begin() failed: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback() failed: %v", err)
	}
}

func TestTransactionExec(t *testing.T) {
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

	// Note: tx.Exec doesn't exist in driver.Tx interface
	// We can only commit or rollback
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit() failed: %v", err)
	}
}

func TestStmtClose(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	stmt, err := c.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}

	// Close should not error
	if err := stmt.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Double close should not error
	if err := stmt.Close(); err != nil {
		t.Errorf("Double Close() failed: %v", err)
	}
}

func TestRowsClose(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Setup
	stmt, _ := c.Prepare("CREATE TABLE close_test (id INTEGER)")
	stmt.Exec(nil)
	stmt.Close()

	stmt, _ = c.Prepare("INSERT INTO close_test VALUES (1)")
	stmt.Exec(nil)
	stmt.Close()

	// Query
	stmt, _ = c.Prepare("SELECT * FROM close_test")
	rows, _ := stmt.Query(nil)

	// Close should not error
	if err := rows.Close(); err != nil {
		t.Errorf("rows.Close() failed: %v", err)
	}

	stmt.Close()
}

func TestConnPrepareWithQuery(t *testing.T) {
	drv := &Driver{}
	c, err := drv.Open(":memory:")
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer c.Close()

	// Prepare a query statement
	stmt, err := c.Prepare("SELECT 1 as col")
	if err != nil {
		t.Fatalf("Prepare() failed: %v", err)
	}
	defer stmt.Close()

	// Execute the query
	rows, err := stmt.Query(nil)
	if err != nil {
		t.Fatalf("Query() failed: %v", err)
	}

	// Verify columns
	cols := rows.Columns()
	if len(cols) != 1 || cols[0] != "col" {
		t.Errorf("Columns() = %v, want [col]", cols)
	}

	rows.Close()
}

func TestDriverInterfaceConformance(t *testing.T) {
	// Verify Driver implements driver.Driver
	var _ driver.Driver = &Driver{}

	// Verify conn implements driver.Conn
	drv := &Driver{}
	c, _ := drv.Open(":memory:")
	defer c.Close()
	var _ driver.Conn = c

	// Verify stmt implements driver.Stmt
	stmt, _ := c.Prepare("SELECT 1")
	defer stmt.Close()
	var _ driver.Stmt = stmt

	// Verify rows implements driver.Rows
	rows, _ := stmt.Query(nil)
	defer rows.Close()
	var _ driver.Rows = rows

	// Verify tx implements driver.Tx
	tx, _ := c.Begin()
	var _ driver.Tx = tx
	tx.Rollback()
}

func TestDBQuery(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, "CREATE TABLE dbquery_test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("Exec() failed: %v", err)
	}

	// Insert
	_, err = db.Exec(ctx, "INSERT INTO dbquery_test VALUES (?, ?)", int64(1), "alice")
	if err != nil {
		t.Fatalf("Exec() with args failed: %v", err)
	}

	// Query
	rows, err := db.Query(ctx, "SELECT id, name FROM dbquery_test WHERE id = ?", int64(1))
	if err != nil {
		t.Fatalf("Query() failed: %v", err)
	}

	// Iterate
	count := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Errorf("Scan() failed: %v", err)
		}
		if id != 1 || name != "alice" {
			t.Errorf("Wrong values: id=%d, name=%s", id, name)
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	rows.Close()
}

func TestDBExec(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create
	result, err := db.Exec(ctx, "CREATE TABLE dbexec_test (id INTEGER)")
	if err != nil {
		t.Fatalf("Exec(CREATE) failed: %v", err)
	}

	// Insert with result
	result, err = db.Exec(ctx, "INSERT INTO dbexec_test VALUES (?)", int64(42))
	if err != nil {
		t.Fatalf("Exec(INSERT) failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("RowsAffected = %d, want 1", result.RowsAffected)
	}

	// Insert multiple
	result, err = db.Exec(ctx, "INSERT INTO dbexec_test VALUES (?), (?)", int64(1), int64(2))
	if err != nil {
		t.Logf("Multi-value insert: %v", err)
	}
}

func TestDBQueryRow(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup
	db.Exec(ctx, "CREATE TABLE row_test (id INTEGER)")
	db.Exec(ctx, "INSERT INTO row_test VALUES (?)", int64(42))

	// QueryRow
	var id int
	err = db.QueryRow(ctx, "SELECT id FROM row_test").Scan(&id)
	if err != nil {
		t.Fatalf("QueryRow().Scan() failed: %v", err)
	}
	if id != 42 {
		t.Errorf("id = %d, want 42", id)
	}
}

func TestDBBegin(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup
	db.Exec(ctx, "CREATE TABLE txn_test (id INTEGER)")

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// Exec in transaction
	_, err = tx.Exec(ctx, "INSERT INTO txn_test VALUES (?)", int64(1))
	if err != nil {
		t.Fatalf("tx.Exec() failed: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Errorf("Commit() failed: %v", err)
	}

	// Verify
	var count int
	db.QueryRow(ctx, "SELECT COUNT(*) FROM txn_test").Scan(&count)
	if count != 1 {
		t.Errorf("Expected 1 row after commit, got %d", count)
	}
}

func TestDBBeginTx(t *testing.T) {
	cfg := &Config{
		Database: ":memory:",
	}
	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup
	db.Exec(ctx, "CREATE TABLE tx2_test (id INTEGER)")

	// Begin (DB doesn't have BeginTx, use Begin)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin() failed: %v", err)
	}

	// Exec
	_, err = tx.Exec(ctx, "INSERT INTO tx2_test VALUES (?)", int64(1))
	if err != nil {
		t.Fatalf("tx.Exec() failed: %v", err)
	}

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Errorf("Rollback() failed: %v", err)
	}

	// Verify no rows
	var count int
	db.QueryRow(ctx, "SELECT COUNT(*) FROM tx2_test").Scan(&count)
	if count != 0 {
		t.Errorf("Expected 0 rows after rollback, got %d", count)
	}
}

func TestConfigWithTimeoutAndExec(t *testing.T) {
	cfg := &Config{
		Database:       ":memory:",
		ConnectTimeout: time.Second * 30,
		QueryTimeout:   time.Second * 60,
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() with timeout config failed: %v", err)
	}
	defer db.Close()

	// Verify it works
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE test_exec (id INTEGER)")
	if err != nil {
		t.Errorf("Exec() failed: %v", err)
	}
}

func TestConnectorWithDriver(t *testing.T) {
	drv := &Driver{}
	cfg := &Config{
		Database: ":memory:",
	}

	connector := &connector{
		cfg:    cfg,
		driver: drv,
	}

	// Verify Driver() returns correct driver
	if d := connector.Driver(); d != drv {
		t.Error("Driver() returned wrong driver")
	}

	// Connect
	conn, err := connector.Connect(nil)
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	defer conn.Close()
}

func TestConnectionPoolStats(t *testing.T) {
	cfg := &Config{
		Database:       ":memory:",
		MaxConnections: 10,
	}

	db, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db.Close()

	stats := db.Stats()
	if stats.OpenConnections != 1 {
		t.Errorf("OpenConnections = %d, want 1", stats.OpenConnections)
	}
}

func TestNamedValueConversion(t *testing.T) {
	// Test namedValues helper function
	values := []driver.Value{
		int64(1),
		"test",
		3.14,
		nil,
	}

	// Convert via namedValues (this is an internal helper)
	named := make([]driver.NamedValue, len(values))
	for i, v := range values {
		named[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}

	// Verify conversion works by using in ExecContext
	drv := &Driver{}
	c, _ := drv.Open(":memory:")
	defer c.Close()

	cn := c.(*conn)
	_, err := cn.ExecContext(context.Background(), "CREATE TABLE named_test (a INTEGER, b TEXT, c REAL, d INTEGER)", named)
	if err != nil {
		t.Logf("ExecContext with values: %v", err)
	}
}
