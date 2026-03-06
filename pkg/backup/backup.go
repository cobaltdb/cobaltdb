package backup

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Backup represents a database backup
type Backup struct {
	Version     string    `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	Database    string    `json:"database"`
	Tables      []string  `json:"tables"`
	Compression string    `json:"compression"`
	Encrypted   bool      `json:"encrypted"`
}

// BackupMetadata contains backup metadata
type BackupMetadata struct {
	Backup
	TableCounts map[string]int64  `json:"table_counts"`
	Checksums   map[string]string `json:"checksums"`
	Filename    string            `json:"filename"` // Full path to backup file
}

// Manager handles backup and restore operations
type Manager struct {
	db     DatabaseInterface
	config *Config
}

// Config contains backup configuration
type Config struct {
	DefaultDir      string
	CompressionType string // "none", "gzip", "zstd"
	EncryptionKey   string // If empty, no encryption
	MaxBackups      int    // Maximum number of backups to keep
}

// DatabaseInterface provides database access
type DatabaseInterface interface {
	Query(ctx context.Context, sql string, args ...interface{}) (RowsInterface, error)
	Exec(ctx context.Context, sql string, args ...interface{}) (ResultInterface, error)
	GetTables() ([]string, error)
	GetTableSchema(table string) (string, error)
}

// RowsInterface provides row access
type RowsInterface interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Columns() ([]string, error)
}

// ResultInterface provides result access
type ResultInterface interface {
	RowsAffected() (int64, error)
}

// DefaultConfig returns default backup configuration
func DefaultConfig() *Config {
	return &Config{
		DefaultDir:      "./backups",
		CompressionType: "gzip",
		MaxBackups:      10,
	}
}

// NewManager creates a new backup manager
func NewManager(db DatabaseInterface, config *Config) *Manager {
	if config == nil {
		config = DefaultConfig()
	}
	return &Manager{
		db:     db,
		config: config,
	}
}

// CreateBackup creates a full database backup
func (m *Manager) CreateBackup(ctx context.Context, tables []string) (*BackupMetadata, error) {
	// Ensure backup directory exists
	if err := os.MkdirAll(m.config.DefaultDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Get tables to backup
	if len(tables) == 0 {
		allTables, err := m.db.GetTables()
		if err != nil {
			return nil, fmt.Errorf("failed to get tables: %w", err)
		}
		tables = allTables
	}

	// Create backup metadata
	metadata := &BackupMetadata{
		Backup: Backup{
			Version:     "1.0",
			CreatedAt:   time.Now(),
			Database:    "cobaltdb",
			Tables:      tables,
			Compression: m.config.CompressionType,
			Encrypted:   m.config.EncryptionKey != "",
		},
		TableCounts: make(map[string]int64),
		Checksums:   make(map[string]string),
	}

	// Generate backup filename with nanoseconds for uniqueness
	timestamp := time.Now().Format("20060102_150405")
	nanos := time.Now().UnixNano() % 1000000
	backupFile := filepath.Join(m.config.DefaultDir, fmt.Sprintf("backup_%s_%06d.sql", timestamp, nanos))

	// Create backup file
	file, err := os.Create(backupFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup file: %w", err)
	}
	defer file.Close()

	// Write backup header
	if err := m.writeHeader(file, metadata); err != nil {
		return nil, err
	}

	// Backup each table
	for _, table := range tables {
		count, err := m.backupTable(ctx, file, table)
		if err != nil {
			return nil, fmt.Errorf("failed to backup table %s: %w", table, err)
		}
		metadata.TableCounts[table] = count
	}

	// Store filename in metadata
	metadata.Filename = backupFile

	// Write metadata
	if err := m.writeMetadata(backupFile, metadata); err != nil {
		return nil, err
	}

	// Cleanup old backups
	if err := m.cleanupOldBackups(); err != nil {
		// Log but don't fail
		fmt.Printf("Warning: failed to cleanup old backups: %v\n", err)
	}

	return metadata, nil
}

// writeHeader writes backup header
func (m *Manager) writeHeader(w io.Writer, metadata *BackupMetadata) error {
	header := fmt.Sprintf("-- CobaltDB Backup\n")
	header += fmt.Sprintf("-- Version: %s\n", metadata.Version)
	header += fmt.Sprintf("-- Created: %s\n", metadata.CreatedAt.Format(time.RFC3339))
	header += fmt.Sprintf("-- Database: %s\n", metadata.Database)
	header += fmt.Sprintf("-- Tables: %v\n", metadata.Tables)
	header += fmt.Sprintf("-- Compression: %s\n", metadata.Compression)
	header += fmt.Sprintf("-- Encrypted: %v\n", metadata.Encrypted)
	header += "--\n\n"

	_, err := w.Write([]byte(header))
	return err
}

// backupTable backs up a single table
func (m *Manager) backupTable(ctx context.Context, w io.Writer, table string) (int64, error) {
	// Get table schema
	schema, err := m.db.GetTableSchema(table)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema: %w", err)
	}

	// Write CREATE TABLE statement
	if _, err := w.Write([]byte(fmt.Sprintf("\n-- Table: %s\n", table))); err != nil {
		return 0, err
	}
	if _, err := w.Write([]byte(schema + ";\n\n")); err != nil {
		return 0, err
	}

	// Query all data
	rows, err := m.db.Query(ctx, fmt.Sprintf("SELECT * FROM %s", table))
	if err != nil {
		return 0, fmt.Errorf("failed to query table: %w", err)
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("failed to get columns: %w", err)
	}

	// Write INSERT statements
	var count int64
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return count, fmt.Errorf("failed to scan row: %w", err)
		}

		insert := m.formatInsert(table, columns, values)
		if _, err := w.Write([]byte(insert + "\n")); err != nil {
			return count, err
		}
		count++
	}

	return count, rows.Close()
}

// quoteIdentifier safely quotes a SQL identifier to prevent injection
func quoteIdentifier(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

// formatInsert formats an INSERT statement
func (m *Manager) formatInsert(table string, columns []string, values []interface{}) string {
	// Build INSERT statement
	sql := fmt.Sprintf("INSERT INTO %s (", quoteIdentifier(table))
	for i, col := range columns {
		if i > 0 {
			sql += ", "
		}
		sql += quoteIdentifier(col)
	}
	sql += ") VALUES ("

	for i, val := range values {
		if i > 0 {
			sql += ", "
		}
		sql += formatValue(val)
	}
	sql += ");"

	return sql
}

// formatValue formats a value for SQL
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		// Escape single quotes
		escaped := ""
		for _, c := range val {
			if c == '\'' {
				escaped += "''"
			} else {
				escaped += string(c)
			}
		}
		return fmt.Sprintf("'%s'", escaped)
	case []byte:
		return fmt.Sprintf("X'%x'", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// writeMetadata writes backup metadata to a separate file
func (m *Manager) writeMetadata(backupFile string, metadata *BackupMetadata) error {
	metaFile := backupFile + ".meta"
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	return os.WriteFile(metaFile, data, 0600)
}

// Restore restores a database from backup
func (m *Manager) Restore(ctx context.Context, backupFile string) error {
	// Read metadata
	metaFile := backupFile + ".meta"
	metaData, err := os.ReadFile(metaFile)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata BackupMetadata
	if err := json.Unmarshal(metaData, &metadata); err != nil {
		return fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Open backup file
	file, err := os.Open(backupFile)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// Parse and execute SQL statements
	if err := m.restoreFromSQL(ctx, file); err != nil {
		return fmt.Errorf("failed to restore from SQL: %w", err)
	}

	return nil
}

// restoreFromSQL parses and executes SQL statements from a backup file
func (m *Manager) restoreFromSQL(ctx context.Context, r io.Reader) error {
	scanner := bufio.NewScanner(r)
	var currentStatement strings.Builder

	for scanner.Scan() {
		line := scanner.Text()

		// Skip comments and empty lines
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}

		currentStatement.WriteString(line)
		currentStatement.WriteString("\n")

		// If line ends with semicolon, execute the statement
		if strings.HasSuffix(trimmed, ";") {
			sql := strings.TrimSpace(currentStatement.String())
			if sql != "" {
				if _, err := m.db.Exec(ctx, sql); err != nil {
					return fmt.Errorf("failed to execute statement: %w\nSQL: %s", err, sql)
				}
			}
			currentStatement.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	// Execute any remaining statement
	if sql := strings.TrimSpace(currentStatement.String()); sql != "" {
		if _, err := m.db.Exec(ctx, sql); err != nil {
			return fmt.Errorf("failed to execute final statement: %w\nSQL: %s", err, sql)
		}
	}

	return nil
}

// ListBackups lists available backups
func (m *Manager) ListBackups() ([]BackupMetadata, error) {
	entries, err := os.ReadDir(m.config.DefaultDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []BackupMetadata{}, nil
		}
		return nil, fmt.Errorf("failed to read backup directory: %w", err)
	}

	var backups []BackupMetadata
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".meta" {
			metaFile := filepath.Join(m.config.DefaultDir, entry.Name())
			data, err := os.ReadFile(metaFile)
			if err != nil {
				continue
			}

			var metadata BackupMetadata
			if err := json.Unmarshal(data, &metadata); err != nil {
				continue
			}
			backups = append(backups, metadata)
		}
	}

	return backups, nil
}

// cleanupOldBackups removes old backups beyond MaxBackups
func (m *Manager) cleanupOldBackups() error {
	if m.config.MaxBackups <= 0 {
		return nil
	}

	backups, err := m.ListBackups()
	if err != nil {
		return err
	}

	if len(backups) <= m.config.MaxBackups {
		return nil
	}

	// Sort by creation time (oldest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].CreatedAt.Before(backups[j].CreatedAt)
	})

	// Remove oldest backups
	toRemove := len(backups) - m.config.MaxBackups
	for i := 0; i < toRemove; i++ {
		backupFile := backups[i].Filename
		if backupFile == "" {
			// Fallback to timestamp-based naming for older backups
			backupFile = filepath.Join(m.config.DefaultDir, fmt.Sprintf("backup_%s.sql", backups[i].CreatedAt.Format("20060102_150405")))
		}
		metaFile := backupFile + ".meta"

		// Remove backup file
		if err := os.Remove(backupFile); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: failed to remove old backup file %s: %v\n", backupFile, err)
		}

		// Remove metadata file
		if err := os.Remove(metaFile); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Warning: failed to remove old metadata file %s: %v\n", metaFile, err)
		}
	}

	return nil
}

// CreateIncrementalBackup creates an incremental backup
func (m *Manager) CreateIncrementalBackup(ctx context.Context, since time.Time, tables []string) (*BackupMetadata, error) {
	// TODO: Implement incremental backup based on WAL or timestamps
	return nil, fmt.Errorf("incremental backup not yet implemented")
}

// VerifyBackup verifies a backup file
func (m *Manager) VerifyBackup(backupFile string) error {
	// Check if backup file exists
	if _, err := os.Stat(backupFile); err != nil {
		return fmt.Errorf("backup file not found: %w", err)
	}

	// Check if metadata exists
	metaFile := backupFile + ".meta"
	if _, err := os.Stat(metaFile); err != nil {
		return fmt.Errorf("metadata file not found: %w", err)
	}

	// Read and validate metadata
	data, err := os.ReadFile(metaFile)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata BackupMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return fmt.Errorf("invalid metadata: %w", err)
	}

	// Verify checksums for each table
	for table, expectedChecksum := range metadata.Checksums {
		// Open backup file and calculate checksum
		file, err := os.Open(backupFile)
		if err != nil {
			return fmt.Errorf("failed to open backup for checksum verification: %w", err)
		}

		calculatedChecksum, err := m.calculateTableChecksum(file, table)
		file.Close()

		if err != nil {
			return fmt.Errorf("failed to calculate checksum for table %s: %w", table, err)
		}

		if calculatedChecksum != expectedChecksum {
			return fmt.Errorf("checksum mismatch for table %s: expected %s, got %s", table, expectedChecksum, calculatedChecksum)
		}
	}

	return nil
}

// calculateTableChecksum calculates a CRC32 checksum for a table's data in the backup
func (m *Manager) calculateTableChecksum(r io.Reader, tableName string) (string, error) {
	scanner := bufio.NewScanner(r)
	inTargetTable := false
	hash := crc32.NewIEEE()

	for scanner.Scan() {
		line := scanner.Text()

		// Check for table header
		if strings.HasPrefix(line, "-- Table: ") {
			table := strings.TrimPrefix(line, "-- Table: ")
			inTargetTable = (table == tableName)
			continue
		}

		// If we're in the target table and it's an INSERT statement, hash it
		if inTargetTable && strings.HasPrefix(strings.TrimSpace(line), "INSERT INTO") {
			hash.Write([]byte(line))
			hash.Write([]byte("\n"))
		}
	}

	if err := scanner.Err(); err != nil {
		return "", err
	}

	return fmt.Sprintf("%08x", hash.Sum32()), nil
}

// CreateCompressedBackup creates a compressed backup using gzip
func (m *Manager) CreateCompressedBackup(ctx context.Context, tables []string) (*BackupMetadata, error) {
	// Create regular backup first
	metadata, err := m.CreateBackup(ctx, tables)
	if err != nil {
		return nil, err
	}

	// Get backup filename from metadata
	backupFile := metadata.Filename
	if backupFile == "" {
		// Fallback to timestamp-based naming
		timestamp := metadata.CreatedAt.Format("20060102_150405")
		backupFile = filepath.Join(m.config.DefaultDir, fmt.Sprintf("backup_%s.sql", timestamp))
	}
	compressedFile := backupFile + ".gz"

	// Open backup file for reading
	inFile, err := os.Open(backupFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open backup file for compression: %w", err)
	}
	defer inFile.Close()

	// Create compressed file
	outFile, err := os.Create(compressedFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressed file: %w", err)
	}
	defer outFile.Close()

	// Create gzip writer
	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()

	// Copy and compress
	if _, err := io.Copy(gzWriter, inFile); err != nil {
		return nil, fmt.Errorf("failed to compress backup: %w", err)
	}

	// Close gzip writer to flush data
	if err := gzWriter.Close(); err != nil {
		return nil, fmt.Errorf("failed to finalize compression: %w", err)
	}

	// Remove original uncompressed file
	inFile.Close()
	if err := os.Remove(backupFile); err != nil {
		fmt.Printf("Warning: failed to remove uncompressed backup: %v\n", err)
	}

	// Update metadata
	metadata.Compression = "gzip"
	metadata.Filename = compressedFile
	metaFile := backupFile + ".meta"
	if err := m.writeMetadata(compressedFile, metadata); err != nil {
		return nil, err
	}

	// Remove old metadata file
	os.Remove(metaFile)

	return metadata, nil
}

// RestoreCompressed restores from a compressed backup
func (m *Manager) RestoreCompressed(ctx context.Context, compressedFile string) error {
	// Open compressed file
	inFile, err := os.Open(compressedFile)
	if err != nil {
		return fmt.Errorf("failed to open compressed file: %w", err)
	}
	defer inFile.Close()

	// Create gzip reader
	gzReader, err := gzip.NewReader(inFile)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Restore from decompressed stream
	return m.restoreFromSQL(ctx, gzReader)
}
