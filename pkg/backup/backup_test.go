package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Mock database interface for testing
type mockDB struct {
	tables  map[string][]map[string]interface{}
	schemas map[string]string
	execSQL []string
}

func newMockDB() *mockDB {
	return &mockDB{
		tables: make(map[string][]map[string]interface{}),
		schemas: map[string]string{
			"users": "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
			"posts": "CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title TEXT)",
		},
		execSQL: make([]string, 0),
	}
}

func (m *mockDB) Query(ctx context.Context, sql string, args ...interface{}) (RowsInterface, error) {
	// Extract table name from query
	var tableName string
	if strings.Contains(sql, "FROM users") {
		tableName = "users"
	} else if strings.Contains(sql, "FROM posts") {
		tableName = "posts"
	}

	rows, exists := m.tables[tableName]
	if !exists {
		return &mockRows{data: nil, columns: nil}, nil
	}

	// Determine columns from first row
	var columns []string
	if len(rows) > 0 {
		for col := range rows[0] {
			columns = append(columns, col)
		}
	}

	return &mockRows{data: rows, columns: columns, index: -1}, nil
}

func (m *mockDB) Exec(ctx context.Context, sql string, args ...interface{}) (ResultInterface, error) {
	m.execSQL = append(m.execSQL, sql)
	return &mockResult{}, nil
}

func (m *mockDB) GetTables() ([]string, error) {
	tables := make([]string, 0, len(m.schemas))
	for table := range m.schemas {
		tables = append(tables, table)
	}
	return tables, nil
}

func (m *mockDB) GetTableSchema(table string) (string, error) {
	schema, exists := m.schemas[table]
	if !exists {
		return "", fmt.Errorf("table not found: %s", table)
	}
	return schema, nil
}

type mockRows struct {
	data    []map[string]interface{}
	columns []string
	index   int
}

func (r *mockRows) Next() bool {
	r.index++
	return r.index < len(r.data)
}

func (r *mockRows) Scan(dest ...interface{}) error {
	if r.index < 0 || r.index >= len(r.data) {
		return fmt.Errorf("no current row")
	}
	row := r.data[r.index]
	for i, col := range r.columns {
		if i < len(dest) {
			*(dest[i].(*interface{})) = row[col]
		}
	}
	return nil
}

func (r *mockRows) Close() error {
	return nil
}

func (r *mockRows) Columns() ([]string, error) {
	return r.columns, nil
}

type mockResult struct{}

func (r *mockResult) RowsAffected() (int64, error) {
	return 1, nil
}

func setupTestManager(t *testing.T) (*Manager, *mockDB, string, func()) {
	tmpDir := t.TempDir()
	db := newMockDB()

	// Add some test data
	db.tables["users"] = []map[string]interface{}{
		{"id": 1, "name": "Alice"},
		{"id": 2, "name": "Bob"},
	}
	db.tables["posts"] = []map[string]interface{}{
		{"id": 1, "user_id": 1, "title": "First Post"},
		{"id": 2, "user_id": 1, "title": "Second Post"},
		{"id": 3, "user_id": 2, "title": "Bob's Post"},
	}

	config := &Config{
		DefaultDir:      tmpDir,
		CompressionType: "none",
		MaxBackups:      5,
	}

	manager := NewManager(db, config)

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}

	return manager, db, tmpDir, cleanup
}

func TestCreateBackup(t *testing.T) {
	manager, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create backup of all tables
	metadata, err := manager.CreateBackup(ctx, nil)
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Verify metadata
	if metadata.Version != "1.0" {
		t.Errorf("Expected version 1.0, got %s", metadata.Version)
	}

	if len(metadata.Tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(metadata.Tables))
	}

	if metadata.TableCounts["users"] != 2 {
		t.Errorf("Expected 2 users, got %d", metadata.TableCounts["users"])
	}

	if metadata.TableCounts["posts"] != 3 {
		t.Errorf("Expected 3 posts, got %d", metadata.TableCounts["posts"])
	}

	// Verify backup file exists
	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s.sql", metadata.CreatedAt.Format("20060102_150405")))
	}
	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		t.Errorf("Backup file not found: %s", backupFile)
	}

	// Verify metadata file exists
	metaFile := backupFile + ".meta"
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		t.Errorf("Metadata file not found: %s", metaFile)
	}
}

func TestCreateBackupSpecificTables(t *testing.T) {
	manager, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create backup of specific table
	metadata, err := manager.CreateBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	if len(metadata.Tables) != 1 {
		t.Errorf("Expected 1 table, got %d", len(metadata.Tables))
	}

	if metadata.Tables[0] != "users" {
		t.Errorf("Expected 'users' table, got %s", metadata.Tables[0])
	}

	// Verify backup file content
	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s.sql", metadata.CreatedAt.Format("20060102_150405")))
	}
	content, err := os.ReadFile(backupFile)
	if err != nil {
		t.Fatalf("Failed to read backup file: %v", err)
	}

	contentStr := string(content)
	if !strings.Contains(contentStr, "CREATE TABLE users") {
		t.Error("Backup should contain users table schema")
	}
	if strings.Contains(contentStr, "CREATE TABLE posts") {
		t.Error("Backup should not contain posts table schema")
	}
}

func TestListBackups(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple backups
	for i := 0; i < 3; i++ {
		_, err := manager.CreateBackup(ctx, nil)
		if err != nil {
			t.Fatalf("Failed to create backup %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// List backups
	backups, err := manager.ListBackups()
	if err != nil {
		t.Fatalf("Failed to list backups: %v", err)
	}

	if len(backups) != 3 {
		t.Errorf("Expected 3 backups, got %d", len(backups))
	}
}

func TestRestore(t *testing.T) {
	manager, db, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create backup
	metadata, err := manager.CreateBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Clear execSQL log
	db.execSQL = make([]string, 0)

	// Restore from backup
	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s.sql", metadata.CreatedAt.Format("20060102_150405")))
	}
	err = manager.Restore(ctx, backupFile)
	if err != nil {
		t.Fatalf("Failed to restore backup: %v", err)
	}

	// Verify SQL statements were executed
	if len(db.execSQL) == 0 {
		t.Error("Expected SQL statements to be executed during restore")
	}

	// Check for CREATE TABLE statement
	foundCreate := false
	for _, sql := range db.execSQL {
		if strings.Contains(sql, "CREATE TABLE users") {
			foundCreate = true
			break
		}
	}
	if !foundCreate {
		t.Error("Expected CREATE TABLE statement in restore")
	}
}

func TestVerifyBackup(t *testing.T) {
	manager, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create backup
	metadata, err := manager.CreateBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	// Verify backup
	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s.sql", metadata.CreatedAt.Format("20060102_150405")))
	}
	err = manager.VerifyBackup(backupFile)
	if err != nil {
		t.Errorf("Backup verification failed: %v", err)
	}
}

func TestVerifyBackupNotFound(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	err := manager.VerifyBackup("/nonexistent/backup.sql")
	if err == nil {
		t.Error("Expected error for non-existent backup")
	}
}

func TestCleanupOldBackups(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create more backups than MaxBackups
	for i := 0; i < 7; i++ {
		_, err := manager.CreateBackup(ctx, []string{"users"})
		if err != nil {
			t.Fatalf("Failed to create backup %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// List backups - should have MaxBackups (5) remaining
	backups, err := manager.ListBackups()
	if err != nil {
		t.Fatalf("Failed to list backups: %v", err)
	}

	if len(backups) != 5 {
		t.Errorf("Expected 5 backups after cleanup, got %d", len(backups))
	}
}

func TestCreateCompressedBackup(t *testing.T) {
	manager, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create compressed backup
	metadata, err := manager.CreateCompressedBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create compressed backup: %v", err)
	}

	if metadata.Compression != "gzip" {
		t.Errorf("Expected compression 'gzip', got %s", metadata.Compression)
	}

	// Verify compressed file exists
	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s_%06d.sql.gz", metadata.CreatedAt.Format("20060102_150405"), metadata.CreatedAt.UnixNano()%1000000))
	}
	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		t.Errorf("Compressed backup file not found: %s", backupFile)
	}
}

func TestRestoreCompressed(t *testing.T) {
	manager, db, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create compressed backup
	metadata, err := manager.CreateCompressedBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create compressed backup: %v", err)
	}

	// Clear execSQL log
	db.execSQL = make([]string, 0)

	// Restore from compressed backup
	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s_%06d.sql.gz", metadata.CreatedAt.Format("20060102_150405"), metadata.CreatedAt.UnixNano()%1000000))
	}
	err = manager.RestoreCompressed(ctx, backupFile)
	if err != nil {
		t.Fatalf("Failed to restore compressed backup: %v", err)
	}

	// Verify SQL statements were executed
	if len(db.execSQL) == 0 {
		t.Error("Expected SQL statements to be executed during restore")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.DefaultDir != "./backups" {
		t.Errorf("Expected default dir './backups', got %s", config.DefaultDir)
	}

	if config.CompressionType != "gzip" {
		t.Errorf("Expected compression 'gzip', got %s", config.CompressionType)
	}

	if config.MaxBackups != 10 {
		t.Errorf("Expected max backups 10, got %d", config.MaxBackups)
	}
}

func TestNewManager(t *testing.T) {
	db := newMockDB()
	config := DefaultConfig()

	manager := NewManager(db, config)
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}

	if manager.db != db {
		t.Error("Manager should store database interface")
	}

	if manager.config != config {
		t.Error("Manager should store config")
	}
}

func TestNewManagerNilConfig(t *testing.T) {
	db := newMockDB()

	manager := NewManager(db, nil)
	if manager == nil {
		t.Fatal("Expected non-nil manager")
	}

	if manager.config == nil {
		t.Fatal("Expected default config to be set")
	}

	if manager.config.CompressionType != "gzip" {
		t.Error("Expected default compression type")
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{true, "TRUE"},
		{false, "FALSE"},
		{42, "42"},
		{3.14, "3.14"},
		{[]byte{0x01, 0x02, 0x03}, "X'010203'"},
	}

	for _, test := range tests {
		result := formatValue(test.input)
		if result != test.expected {
			t.Errorf("formatValue(%v) = %s, expected %s", test.input, result, test.expected)
		}
	}
}

func TestFormatValueQuoteEscaping(t *testing.T) {
	// Test single quote escaping
	input := "O'Brien's"
	expected := "'O''Brien''s'"
	result := formatValue(input)
	if result != expected {
		t.Errorf("formatValue(%q) = %s, expected %s", input, result, expected)
	}
}

func TestCreateIncrementalBackup(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to create incremental backup (not implemented yet)
	_, err := manager.CreateIncrementalBackup(ctx, time.Now(), []string{"users"})
	if err == nil {
		t.Error("Expected error for unimplemented incremental backup")
	}

	// Error message should indicate not implemented
	if err != nil && !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("Expected 'not yet implemented' error, got: %v", err)
	}
}

func TestCalculateTableChecksum(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a backup first
	metadata, err := manager.CreateBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	backupFile := metadata.Filename
	if backupFile == "" {
		t.Fatal("Backup filename is empty")
	}

	// Open backup file
	file, err := os.Open(backupFile)
	if err != nil {
		t.Fatalf("Failed to open backup file: %v", err)
	}
	defer file.Close()

	// Calculate checksum for users table
	checksum, err := manager.calculateTableChecksum(file, "users")
	if err != nil {
		t.Errorf("Failed to calculate checksum: %v", err)
	}

	if checksum == "" {
		t.Error("Expected non-empty checksum")
	}

	// Verify checksum is 8 hex characters
	if len(checksum) != 8 {
		t.Errorf("Expected 8 character checksum, got %d: %s", len(checksum), checksum)
	}
}

func TestCalculateTableChecksumNonExistent(t *testing.T) {
	manager, _, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a backup
	metadata, err := manager.CreateBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	backupFile := metadata.Filename
	if backupFile == "" {
		t.Fatal("Backup filename is empty")
	}

	// Open backup file
	file, err := os.Open(backupFile)
	if err != nil {
		t.Fatalf("Failed to open backup file: %v", err)
	}
	defer file.Close()

	// Calculate checksum for non-existent table
	checksum, err := manager.calculateTableChecksum(file, "nonexistent")
	if err != nil {
		t.Errorf("Failed to calculate checksum: %v", err)
	}

	// Checksum should be valid even for non-existent table (just empty)
	if checksum == "" {
		t.Error("Expected checksum even for non-existent table")
	}
}

func TestVerifyBackupWithMissingMetadata(t *testing.T) {
	manager, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create backup
	metadata, err := manager.CreateBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s.sql", metadata.CreatedAt.Format("20060102_150405")))
	}

	// Delete metadata file
	metaFile := backupFile + ".meta"
	os.Remove(metaFile)

	// Verify should fail
	err = manager.VerifyBackup(backupFile)
	if err == nil {
		t.Error("Expected error when metadata is missing")
	}
}

func TestVerifyBackupWithCorruptMetadata(t *testing.T) {
	manager, _, tmpDir, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create backup
	metadata, err := manager.CreateBackup(ctx, []string{"users"})
	if err != nil {
		t.Fatalf("Failed to create backup: %v", err)
	}

	backupFile := metadata.Filename
	if backupFile == "" {
		backupFile = filepath.Join(tmpDir, fmt.Sprintf("backup_%s.sql", metadata.CreatedAt.Format("20060102_150405")))
	}

	// Corrupt metadata file
	metaFile := backupFile + ".meta"
	err = os.WriteFile(metaFile, []byte("invalid json"), 0644)
	if err != nil {
		t.Fatalf("Failed to corrupt metadata: %v", err)
	}

	// Verify should fail
	err = manager.VerifyBackup(backupFile)
	if err == nil {
		t.Error("Expected error when metadata is corrupt")
	}
}
