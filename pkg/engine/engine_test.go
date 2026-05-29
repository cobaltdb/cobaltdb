package engine

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/audit"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestOpenMemory(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("Database is nil")
	}
}

func TestOpenNormalizesOptionsWithoutMutation(t *testing.T) {
	opts := &Options{CoreStorage: CoreStorage{InMemory: true}}
	db, err := Open(":memory:", opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	defaults := DefaultOptions()
	if db.options.CoreStorage.CacheSize != defaults.CoreStorage.CacheSize {
		t.Fatalf("CacheSize = %d, want %d", db.options.CoreStorage.CacheSize, defaults.CoreStorage.CacheSize)
	}
	if db.options.CoreStorage.PageSize != defaults.CoreStorage.PageSize {
		t.Fatalf("PageSize = %d, want %d", db.options.CoreStorage.PageSize, defaults.CoreStorage.PageSize)
	}
	if db.options.CoreStorage.WALEnabled == nil || *db.options.CoreStorage.WALEnabled != *defaults.CoreStorage.WALEnabled {
		t.Fatal("WALEnabled was not defaulted")
	}
	if opts.CoreStorage.CacheSize != 0 || opts.CoreStorage.PageSize != 0 || opts.CoreStorage.WALEnabled != nil || opts.CoreStorage.Logger != nil {
		t.Fatal("Open should not mutate caller options")
	}
}

func TestOpenCopiesMutableNestedOptions(t *testing.T) {
	walEnabled := false
	auditEvents := []audit.EventType{audit.EventQuery}
	sensitiveFields := []string{"token"}
	auditKey := []byte("0123456789abcdef0123456789abcdef")
	compressionConfig := &storage.CompressionConfig{
		Enabled:   true,
		Algorithm: storage.CompressionAlgorithmLZ4,
		Level:     storage.CompressionLevelFast,
		MinRatio:  0.75,
	}
	opts := &Options{
		CoreStorage: CoreStorage{
			InMemory:   true,
			WALEnabled: &walEnabled,
		},
		Security: Security{
			AuditConfig: &audit.Config{
				Events:          auditEvents,
				SensitiveFields: sensitiveFields,
				EncryptionKey:   auditKey,
			},
		},
		PageCompression: PageCompressionConfig{
			Config: compressionConfig,
		},
	}

	db, err := Open(":memory:", opts)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	walEnabled = true
	auditEvents[0] = audit.EventDDL
	sensitiveFields[0] = "password"
	auditKey[0] = 'x'
	compressionConfig.Level = storage.CompressionLevelBest
	compressionConfig.MinRatio = 0.25

	if db.options.CoreStorage.WALEnabled == opts.CoreStorage.WALEnabled || *db.options.CoreStorage.WALEnabled {
		t.Fatal("WALEnabled should be copied from caller options")
	}
	if db.options.Security.AuditConfig == opts.Security.AuditConfig {
		t.Fatal("AuditConfig should be copied from caller options")
	}
	if db.options.Security.AuditConfig.Events[0] != audit.EventQuery {
		t.Fatalf("AuditConfig.Events aliased caller slice: %v", db.options.Security.AuditConfig.Events[0])
	}
	if db.options.Security.AuditConfig.SensitiveFields[0] != "token" {
		t.Fatalf("AuditConfig.SensitiveFields aliased caller slice: %q", db.options.Security.AuditConfig.SensitiveFields[0])
	}
	if db.options.Security.AuditConfig.EncryptionKey[0] != '0' {
		t.Fatal("AuditConfig.EncryptionKey aliased caller slice")
	}
	if db.options.PageCompression.Config == opts.PageCompression.Config {
		t.Fatal("CompressionConfig should be copied from caller options")
	}
	if db.options.PageCompression.Config.Level != storage.CompressionLevelFast || db.options.PageCompression.Config.MinRatio != 0.75 {
		t.Fatalf("CompressionConfig aliased caller config: %+v", db.options.PageCompression.Config)
	}
}

func TestCreateTable(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024},
	})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	result, err := db.Exec(ctx, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}

	// Try to create same table again (should fail)
	_, err = db.Exec(ctx, `CREATE TABLE users (id INTEGER)`)
	if err == nil {
		t.Error("Expected error when creating duplicate table")
	}
}

func TestInsertAndSelect(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE users (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert data
	result, err := db.Exec(ctx, `INSERT INTO users (id, name) VALUES (?, ?)`, 1, "Alice")
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	// Select data
	rows, err := db.Query(ctx, `SELECT id, name FROM users`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	columns := rows.Columns()
	if len(columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(columns))
	}

	count := 0
	for rows.Next() {
		var id interface{}
		var name interface{}
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Failed to scan: %v", err)
		}
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}
}

func TestTransaction(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE items (id INTEGER, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Begin transaction
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Insert in transaction
	_, err = tx.Exec(ctx, `INSERT INTO items (id, name) VALUES (?, ?)`, 1, "Item1")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to insert in transaction: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data
	rows, err := db.Query(ctx, `SELECT id FROM items`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("Expected 1 row after transaction, got %d", count)
	}
}

func TestMultipleInserts(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create table
	_, err = db.Exec(ctx, `CREATE TABLE test (id INTEGER, value TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert multiple rows
	for i := 0; i < 10; i++ {
		_, err = db.Exec(ctx, `INSERT INTO test (id, value) VALUES (?, ?)`, i, "value")
		if err != nil {
			t.Fatalf("Failed to insert row %d: %v", i, err)
		}
	}

	// Count rows
	rows, err := db.Query(ctx, `SELECT id FROM test`)
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}

	if count != 10 {
		t.Errorf("Expected 10 rows, got %d", count)
	}
}

func TestQueryRow(t *testing.T) {
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true, CacheSize: 1024}})
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Create and insert
	db.Exec(ctx, `CREATE TABLE single (id INTEGER, name TEXT)`)
	db.Exec(ctx, `INSERT INTO single (id, name) VALUES (?, ?)`, 1, "Test")

	// Query single row
	row := db.QueryRow(ctx, `SELECT id, name FROM single`)
	if row == nil {
		t.Fatal("QueryRow returned nil")
	}

	var id, name interface{}
	if err := row.Scan(&id, &name); err != nil {
		t.Fatalf("Failed to scan row: %v", err)
	}
}
