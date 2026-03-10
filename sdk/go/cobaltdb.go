// Package cobaltdb provides a native Go SDK for CobaltDB
package cobaltdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func init() {
	sql.Register("cobaltdb", &Driver{})
}

// Driver implements the database/sql/driver interface
type Driver struct{}

// Open opens a new database connection
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	db, err := Open(cfg)
	if err != nil {
		return nil, err
	}

	return &conn{db: db, cfg: cfg}, nil
}

// OpenConnector implements driver.DriverContext
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{cfg: cfg, driver: d}, nil
}

// connector implements driver.Connector
type connector struct {
	cfg    *Config
	driver *Driver
	mu     sync.Mutex
	shared *DB
	refs   int
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.shared == nil {
		db, err := Open(c.cfg)
		if err != nil {
			return nil, err
		}
		c.shared = db
	}

	c.refs++
	return &conn{db: c.shared, cfg: c.cfg, connector: c}, nil
}

func (c *connector) Driver() driver.Driver {
	return c.driver
}

// Config holds database configuration
type Config struct {
	Host            string
	Port            int
	Database        string
	Username        string
	Password        string
	SSLMode         string
	SSLCert         string
	SSLKey          string
	SSLRootCert     string
	ConnectTimeout  time.Duration
	QueryTimeout    time.Duration
	MaxConnections  int
	MaxIdleTime     time.Duration
	MaxLifetime     time.Duration
	ApplicationName string
	// Engine-specific options
	CacheSize  int
	WALEnabled bool
	SyncMode   string
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Host:           "localhost",
		Port:           4200,
		Database:       "cobaltdb",
		SSLMode:        "prefer",
		ConnectTimeout: 30 * time.Second,
		QueryTimeout:   60 * time.Second,
		MaxConnections: 10,
		MaxIdleTime:    30 * time.Minute,
		MaxLifetime:    1 * time.Hour,
		CacheSize:      1024,
		WALEnabled:     true,
		SyncMode:       "normal",
	}
}

// ParseDSN parses a connection string
func ParseDSN(dsn string) (*Config, error) {
	cfg := DefaultConfig()

	// Handle key=value format
	if strings.Contains(dsn, "=") {
		pairs := strings.FieldsFunc(dsn, func(r rune) bool {
			return r == ' ' || r == ';'
		})
		for _, pair := range pairs {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				continue
			}
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])

			switch strings.ToLower(key) {
			case "host", "hostname":
				cfg.Host = value
			case "port":
				p, err := strconv.Atoi(value)
				if err != nil {
					return nil, fmt.Errorf("invalid port: %v", err)
				}
				cfg.Port = p
			case "database", "dbname":
				cfg.Database = value
			case "user", "username":
				cfg.Username = value
			case "password", "pass":
				cfg.Password = value
			case "sslmode":
				cfg.SSLMode = value
			case "sslcert":
				cfg.SSLCert = value
			case "sslkey":
				cfg.SSLKey = value
			case "sslrootcert":
				cfg.SSLRootCert = value
			case "connect_timeout":
				d, err := time.ParseDuration(value)
				if err != nil {
					return nil, fmt.Errorf("invalid connect_timeout: %v", err)
				}
				cfg.ConnectTimeout = d
			case "query_timeout":
				d, err := time.ParseDuration(value)
				if err != nil {
					return nil, fmt.Errorf("invalid query_timeout: %v", err)
				}
				cfg.QueryTimeout = d
			case "max_conns":
				n, err := strconv.Atoi(value)
				if err != nil {
					return nil, fmt.Errorf("invalid max_conns: %v", err)
				}
				cfg.MaxConnections = n
			case "application_name":
				cfg.ApplicationName = value
			}
		}
	}

	// Handle URL format
	if strings.HasPrefix(dsn, "cobaltdb://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, fmt.Errorf("invalid DSN: %v", err)
		}
		cfg.Host = u.Hostname()
		if u.Port() != "" {
			cfg.Port, _ = strconv.Atoi(u.Port())
		}
		cfg.Database = strings.TrimPrefix(u.Path, "/")
		if u.User != nil {
			cfg.Username = u.User.Username()
			cfg.Password, _ = u.User.Password()
		}
		// Parse query parameters
		q := u.Query()
		if v := q.Get("sslmode"); v != "" {
			cfg.SSLMode = v
		}
	}

	return cfg, nil
}

// FormatDSN formats a Config into a DSN string
func (cfg *Config) FormatDSN() string {
	parts := []string{
		fmt.Sprintf("host=%s", cfg.Host),
		fmt.Sprintf("port=%d", cfg.Port),
		fmt.Sprintf("database=%s", cfg.Database),
	}
	if cfg.Username != "" {
		parts = append(parts, fmt.Sprintf("user=%s", cfg.Username))
	}
	if cfg.Password != "" {
		parts = append(parts, fmt.Sprintf("password=%s", cfg.Password))
	}
	if cfg.SSLMode != "" {
		parts = append(parts, fmt.Sprintf("sslmode=%s", cfg.SSLMode))
	}
	return strings.Join(parts, " ")
}

// DB represents a database connection
type DB struct {
	*engine.DB
	cfg    *Config
	mu     sync.RWMutex
	closed bool
}

// Open opens a database connection
func Open(cfg *Config) (*DB, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	opts := &engine.Options{
		CacheSize:         cfg.CacheSize,
		WALEnabled:        cfg.WALEnabled,
		MaxConnections:    cfg.MaxConnections,
		ConnectionTimeout: cfg.ConnectTimeout,
		QueryTimeout:      cfg.QueryTimeout,
	}

	// Set sync mode
	switch cfg.SyncMode {
	case "off":
		opts.SyncMode = engine.SyncOff
	case "full":
		opts.SyncMode = engine.SyncFull
	default:
		opts.SyncMode = engine.SyncNormal
	}

	db, err := engine.Open(cfg.Database, opts)
	if err != nil {
		return nil, err
	}

	return &DB{DB: db, cfg: cfg}, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true
	return db.DB.Close()
}

// Ping checks if the database is accessible
func (db *DB) Ping(ctx context.Context) error {
	_, err := db.Exec(ctx, "SELECT 1")
	return err
}

// Stats returns database statistics
func (db *DB) Stats() Stats {
	return Stats{
		OpenConnections: 1,
		InUse:           0,
		Idle:            0,
	}
}

// Stats holds database statistics
type Stats struct {
	OpenConnections int
	InUse           int
	Idle            int
	WaitCount       int64
	WaitDuration    time.Duration
}

// conn implements driver.Conn
type conn struct {
	db        *DB
	cfg       *Config
	connector *connector
	mu        sync.Mutex
	closed    bool
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return &stmt{conn: c, query: query}, nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	if c.connector == nil {
		return nil
	}

	c.connector.mu.Lock()
	defer c.connector.mu.Unlock()
	if c.connector.refs > 0 {
		c.connector.refs--
	}
	if c.connector.refs == 0 && c.connector.shared != nil {
		err := c.connector.shared.Close()
		c.connector.shared = nil
		return err
	}

	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	_, err := c.db.Exec(ctx, "BEGIN")
	if err != nil {
		return nil, err
	}
	return &tx{conn: c}, nil
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Convert NamedValue to interface{}
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}

	result, err := c.db.Exec(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	return &execResult{rowsAffected: result.RowsAffected, lastID: result.LastInsertID}, nil
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Convert NamedValue to interface{}
	values := make([]interface{}, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}

	rows, err := c.db.Query(ctx, query, values...)
	if err != nil {
		return nil, err
	}

	return &driverRows{rows: rows}, nil
}

// stmt implements driver.Stmt
type stmt struct {
	conn  *conn
	query string
}

func (s *stmt) Close() error {
	return nil
}

func (s *stmt) NumInput() int {
	return -1 // Unknown
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedValues(args))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedValues(args))
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

// tx implements driver.Tx
type tx struct {
	conn *conn
	done bool
}

func (t *tx) Commit() error {
	if t.done {
		return errors.New("transaction already completed")
	}
	t.done = true
	_, err := t.conn.db.Exec(context.Background(), "COMMIT")
	return err
}

func (t *tx) Rollback() error {
	if t.done {
		return errors.New("transaction already completed")
	}
	t.done = true
	_, err := t.conn.db.Exec(context.Background(), "ROLLBACK")
	return err
}

// execResult implements driver.Result
type execResult struct {
	rowsAffected int64
	lastID       int64
}

func (r *execResult) LastInsertId() (int64, error) {
	return r.lastID, nil
}

func (r *execResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

// driverRows implements driver.Rows
type driverRows struct {
	rows *engine.Rows
	cols []string
	done bool
}

func (r *driverRows) Columns() []string {
	if r.cols == nil {
		r.cols = r.rows.Columns()
	}
	return r.cols
}

func (r *driverRows) Close() error {
	if r.done {
		return nil
	}
	r.done = true
	return r.rows.Close()
}

func (r *driverRows) Next(dest []driver.Value) error {
	if !r.rows.Next() {
		return io.EOF
	}

	// Allocate slice to scan into
	values := make([]interface{}, len(r.Columns()))
	for i := range values {
		values[i] = new(interface{})
	}

	// Convert to []interface{} for Scan
	scanDest := make([]interface{}, len(values))
	copy(scanDest, values)

	if err := r.rows.Scan(scanDest...); err != nil {
		return err
	}

	// Extract values from pointers
	for i, v := range values {
		if ptr, ok := v.(*interface{}); ok {
			dest[i] = driver.Value(*ptr)
		} else {
			dest[i] = driver.Value(v)
		}
	}

	return nil
}

// ColumnTypeDatabaseTypeName returns the database type name
func (r *driverRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.Columns()) {
		return ""
	}
	return "TEXT" // Simplified
}

// Helper functions

func namedValues(args []driver.Value) []driver.NamedValue {
	result := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		result[i] = driver.NamedValue{Ordinal: i + 1, Value: arg}
	}
	return result
}

// Row is a helper for scanning single rows
type Row struct {
	err error
}

func (r *Row) Scan(dest ...interface{}) error {
	return r.err
}

// NullString represents a nullable string
type NullString struct {
	String string
	Valid  bool
}

// Scan implements the Scanner interface
func (ns *NullString) Scan(value interface{}) error {
	if value == nil {
		ns.String, ns.Valid = "", false
		return nil
	}
	ns.Valid = true
	switch v := value.(type) {
	case string:
		ns.String = v
	case []byte:
		ns.String = string(v)
	default:
		ns.String = fmt.Sprintf("%v", v)
	}
	return nil
}

// NullInt64 represents a nullable int64
type NullInt64 struct {
	Int64 int64
	Valid bool
}

// Scan implements the Scanner interface
func (ni *NullInt64) Scan(value interface{}) error {
	if value == nil {
		ni.Int64, ni.Valid = 0, false
		return nil
	}
	ni.Valid = true
	switch v := value.(type) {
	case int64:
		ni.Int64 = v
	case int:
		ni.Int64 = int64(v)
	case float64:
		ni.Int64 = int64(v)
	case string:
		var err error
		ni.Int64, err = strconv.ParseInt(v, 10, 64)
		return err
	}
	return nil
}

// Value implements the driver Valuer interface
func (ni NullInt64) Value() (driver.Value, error) {
	if !ni.Valid {
		return nil, nil
	}
	return ni.Int64, nil
}

// NullTime represents a nullable time
type NullTime struct {
	Time  time.Time
	Valid bool
}

// Scan implements the Scanner interface
func (nt *NullTime) Scan(value interface{}) error {
	if value == nil {
		nt.Time, nt.Valid = time.Time{}, false
		return nil
	}
	nt.Valid = true
	switch v := value.(type) {
	case time.Time:
		nt.Time = v
	case string:
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return err
		}
		nt.Time = t
	}
	return nil
}

// JSON represents a JSON value
type JSON struct {
	Data  interface{}
	Valid bool
}

// Scan implements the Scanner interface
func (j *JSON) Scan(value interface{}) error {
	if value == nil {
		j.Data, j.Valid = nil, false
		return nil
	}

	var data []byte
	switch v := value.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return fmt.Errorf("cannot scan %T into JSON", value)
	}

	j.Valid = true
	return json.Unmarshal(data, &j.Data)
}

// Value implements the driver Valuer interface
func (j JSON) Value() (driver.Value, error) {
	if !j.Valid {
		return nil, nil
	}
	return json.Marshal(j.Data)
}

// Connection pool management

// Pool represents a connection pool
type Pool struct {
	mu sync.Mutex
}

// PooledConn represents a pooled connection
type PooledConn struct {
	db       *DB
	pool     *Pool
	lastUsed time.Time
	inUse    bool
}

// Conn returns the underlying database connection
func (pc *PooledConn) Conn() *DB {
	return pc.db
}

// Release returns the connection to the pool
func (pc *PooledConn) Release() {
	pc.pool.mu.Lock()
	defer pc.pool.mu.Unlock()
	pc.inUse = false
	pc.lastUsed = time.Now()
}

// Helper type for transaction isolation levels
type IsolationLevel int

const (
	LevelDefault IsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)

// String returns the isolation level as a string
func (i IsolationLevel) String() string {
	switch i {
	case LevelReadUncommitted:
		return "READ UNCOMMITTED"
	case LevelReadCommitted:
		return "READ COMMITTED"
	case LevelWriteCommitted:
		return "WRITE COMMITTED"
	case LevelRepeatableRead:
		return "REPEATABLE READ"
	case LevelSnapshot:
		return "SNAPSHOT"
	case LevelSerializable:
		return "SERIALIZABLE"
	case LevelLinearizable:
		return "LINEARIZABLE"
	default:
		return ""
	}
}

// ErrConnBusy is returned when the connection pool is exhausted
var ErrConnBusy = errors.New("connection pool exhausted")

// ErrConnClosed is returned when operating on a closed connection
var ErrConnClosed = errors.New("connection is closed")

// ErrTxDone is returned when operating on a completed transaction
var ErrTxDone = errors.New("transaction already completed")

// Logger interface for custom logging
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}
