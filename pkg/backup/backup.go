// Package backup provides hot backup capabilities for CobaltDB
package backup

import (
	"compress/gzip"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Type defines the backup type
type Type int

const (
	// TypeFull creates a complete backup
	TypeFull Type = iota
	// TypeIncremental only backs up changes since last backup
	TypeIncremental
	// TypeDifferential backs up changes since last full backup
	TypeDifferential
)

// Config holds backup configuration
type Config struct {
	// Backup directory
	BackupDir string
	// Compression level (1-9, 0 = no compression)
	CompressionLevel int
	// Max backups to keep (0 = unlimited)
	MaxBackups int
	// Backup retention period
	RetentionPeriod time.Duration
	// Include WAL files
	IncludeWAL bool
	// Verify backup after creation
	Verify bool
	// Encrypt backups
	Encrypt bool
	// Encryption key file
	KeyFile string
}

// DefaultConfig returns default backup configuration
func DefaultConfig() *Config {
	return &Config{
		BackupDir:        "./backups",
		CompressionLevel: 6,
		MaxBackups:       10,
		RetentionPeriod:  7 * 24 * time.Hour,
		IncludeWAL:       true,
		Verify:           true,
		Encrypt:          false,
	}
}

// Backup represents a database backup
type Backup struct {
	ID          string
	Type        Type
	StartedAt   time.Time
	CompletedAt time.Time
	Size        int64
	Checksum    uint32
	Source      string
	Destination string
	Incremental bool
	ParentID    string // For incremental/differential backups
	WALFiles    []string
}

// Metadata stores backup metadata
type Metadata struct {
	Backups []*Backup
	mu      sync.RWMutex
}

// Manager handles backup operations
type Manager struct {
	config   *Config
	metadata *Metadata
	// Callbacks
	OnProgress func(percent int)
	OnComplete func(backup *Backup, err error)
	OnVerify   func(backup *Backup, valid bool)
	// State
	activeBackup bool
	mu           sync.Mutex
	// Database interface
	db Database
}

// Database interface for backup operations
type Database interface {
	// GetDatabasePath returns the database file path
	GetDatabasePath() string
	// GetWALPath returns the WAL directory path
	GetWALPath() string
	// Checkpoint performs a database checkpoint
	Checkpoint() error
	// BeginHotBackup starts a hot backup (prevents checkpointing)
	BeginHotBackup() error
	// EndHotBackup ends a hot backup
	EndHotBackup() error
	// GetCurrentLSN returns the current log sequence number
	GetCurrentLSN() uint64
}

// NewManager creates a new backup manager
func NewManager(config *Config, db Database) *Manager {
	return &Manager{
		config:   config,
		metadata: &Metadata{Backups: make([]*Backup, 0)},
		db:       db,
	}
}

// CreateBackup creates a new backup
func (m *Manager) CreateBackup(ctx context.Context, backupType Type) (*Backup, error) {
	m.mu.Lock()
	if m.activeBackup {
		m.mu.Unlock()
		return nil, fmt.Errorf("backup already in progress")
	}
	m.activeBackup = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.activeBackup = false
		m.mu.Unlock()
	}()

	// Create backup directory if not exists
	if err := os.MkdirAll(m.config.BackupDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Generate backup ID
	backupID := generateBackupID()

	backup := &Backup{
		ID:          backupID,
		Type:        backupType,
		StartedAt:   time.Now(),
		Source:      m.db.GetDatabasePath(),
		Incremental: backupType != TypeFull,
	}

	// Start hot backup
	if err := m.db.BeginHotBackup(); err != nil {
		return nil, fmt.Errorf("failed to begin hot backup: %w", err)
	}
	defer m.db.EndHotBackup()

	// Perform checkpoint to minimize WAL
	if err := m.db.Checkpoint(); err != nil {
		return nil, fmt.Errorf("failed to checkpoint: %w", err)
	}

	// Create backup file
	backupFile := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s.db", backupID))
	if m.config.CompressionLevel > 0 {
		backupFile += ".gz"
	}
	backup.Destination = backupFile

	// Copy database file
	if err := m.copyDatabase(ctx, backup); err != nil {
		return nil, fmt.Errorf("failed to copy database: %w", err)
	}

	// Copy WAL files if needed
	if m.config.IncludeWAL {
		if err := m.copyWALFiles(ctx, backup); err != nil {
			return nil, fmt.Errorf("failed to copy WAL files: %w", err)
		}
	}

	backup.CompletedAt = time.Now()

	// Verify backup
	if m.config.Verify {
		if err := m.verifyBackup(backup); err != nil {
			return nil, fmt.Errorf("backup verification failed: %w", err)
		}
	}

	// Save metadata
	m.metadata.mu.Lock()
	m.metadata.Backups = append(m.metadata.Backups, backup)
	m.metadata.mu.Unlock()

	// Cleanup old backups
	if err := m.cleanupOldBackups(); err != nil {
		// Log error but don't fail
		fmt.Printf("Warning: failed to cleanup old backups: %v\n", err)
	}

	// Trigger callback
	if m.OnComplete != nil {
		m.OnComplete(backup, nil)
	}

	return backup, nil
}

// copyDatabase copies the database file to backup location
func (m *Manager) copyDatabase(ctx context.Context, backup *Backup) error {
	srcPath := m.db.GetDatabasePath()
	dstPath := backup.Destination

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer srcFile.Close()

	// Get file info
	srcInfo, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	// Create destination file
	dstFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer dstFile.Close()

	var writer io.Writer = dstFile
	var compressor *gzip.Writer

	// Add compression if enabled
	if m.config.CompressionLevel > 0 {
		compressor, err = gzip.NewWriterLevel(dstFile, m.config.CompressionLevel)
		if err != nil {
			return fmt.Errorf("failed to create compressor: %w", err)
		}
		writer = compressor
	}

	// Copy with progress tracking
	bufSize := 64 * 1024 // 64KB buffer
	buf := make([]byte, bufSize)
	written := int64(0)
	crc := crc32.NewIEEE()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := srcFile.Read(buf)
		if n > 0 {
			if _, werr := writer.Write(buf[:n]); werr != nil {
				return fmt.Errorf("failed to write backup data: %w", werr)
			}
			crc.Write(buf[:n])
			written += int64(n)

			// Report progress
			if m.OnProgress != nil && srcInfo.Size() > 0 {
				percent := int((written * 100) / srcInfo.Size())
				m.OnProgress(percent)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read source file: %w", err)
		}
	}

	// Close compressor
	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return fmt.Errorf("failed to finalize compression: %w", err)
		}
	}

	backup.Size = written
	backup.Checksum = crc.Sum32()

	return nil
}

// copyWALFiles copies WAL files to backup
func (m *Manager) copyWALFiles(ctx context.Context, backup *Backup) error {
	walDir := m.db.GetWALPath()
	if walDir == "" {
		return nil
	}

	entries, err := os.ReadDir(walDir)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	walBackupDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", backup.ID))
	if err := os.MkdirAll(walBackupDir, 0755); err != nil {
		return fmt.Errorf("failed to create WAL backup directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		srcPath := filepath.Join(walDir, entry.Name())
		dstPath := filepath.Join(walBackupDir, entry.Name())

		if err := copyFile(srcPath, dstPath); err != nil {
			return fmt.Errorf("failed to copy WAL file %s: %w", entry.Name(), err)
		}

		backup.WALFiles = append(backup.WALFiles, entry.Name())
	}

	return nil
}

// verifyBackup verifies the integrity of a backup
func (m *Manager) verifyBackup(backup *Backup) error {
	file, err := os.Open(backup.Destination)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file

	// Handle compressed backups
	if filepath.Ext(backup.Destination) == ".gz" {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Calculate checksum
	crc := crc32.NewIEEE()
	if _, err := io.Copy(crc, reader); err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	calculatedChecksum := crc.Sum32()
	valid := calculatedChecksum == backup.Checksum

	if m.OnVerify != nil {
		m.OnVerify(backup, valid)
	}

	if !valid {
		return fmt.Errorf("checksum mismatch: expected %d, got %d", backup.Checksum, calculatedChecksum)
	}

	return nil
}

// Restore restores database from a backup
func (m *Manager) Restore(ctx context.Context, backupID string, targetPath string) error {
	// Find backup
	m.metadata.mu.RLock()
	var backup *Backup
	for _, b := range m.metadata.Backups {
		if b.ID == backupID {
			backup = b
			break
		}
	}
	m.metadata.mu.RUnlock()

	if backup == nil {
		return fmt.Errorf("backup not found: %s", backupID)
	}

	// Check if backup file exists
	if _, err := os.Stat(backup.Destination); err != nil {
		return fmt.Errorf("backup file not found: %w", err)
	}

	// Create target directory if needed
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Open backup file
	backupFile, err := os.Open(backup.Destination)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer backupFile.Close()

	// Create target file
	targetFile, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	defer targetFile.Close()

	var reader io.Reader = backupFile

	// Handle decompression
	if filepath.Ext(backup.Destination) == ".gz" {
		gzReader, err := gzip.NewReader(backupFile)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Copy with progress
	buf := make([]byte, 64*1024)
	written := int64(0)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := reader.Read(buf)
		if n > 0 {
			if _, werr := targetFile.Write(buf[:n]); werr != nil {
				return fmt.Errorf("failed to write target file: %w", werr)
			}
			written += int64(n)

			if m.OnProgress != nil && backup.Size > 0 {
				percent := int((written * 100) / backup.Size)
				m.OnProgress(percent)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read backup file: %w", err)
		}
	}

	// Restore WAL files if present
	if len(backup.WALFiles) > 0 {
		walBackupDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", backup.ID))
		targetWALDir := filepath.Join(filepath.Dir(targetPath), "wal")

		if err := os.MkdirAll(targetWALDir, 0755); err != nil {
			return fmt.Errorf("failed to create WAL directory: %w", err)
		}

		for _, walFile := range backup.WALFiles {
			srcPath := filepath.Join(walBackupDir, walFile)
			dstPath := filepath.Join(targetWALDir, walFile)

			if err := copyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("failed to restore WAL file %s: %w", walFile, err)
			}
		}
	}

	return nil
}

// ListBackups returns all backups
func (m *Manager) ListBackups() []*Backup {
	m.metadata.mu.RLock()
	defer m.metadata.mu.RUnlock()

	result := make([]*Backup, len(m.metadata.Backups))
	copy(result, m.metadata.Backups)
	return result
}

// GetBackup returns a specific backup by ID
func (m *Manager) GetBackup(backupID string) *Backup {
	m.metadata.mu.RLock()
	defer m.metadata.mu.RUnlock()

	for _, b := range m.metadata.Backups {
		if b.ID == backupID {
			return b
		}
	}
	return nil
}

// DeleteBackup deletes a backup
func (m *Manager) DeleteBackup(backupID string) error {
	m.metadata.mu.Lock()
	defer m.metadata.mu.Unlock()

	for i, b := range m.metadata.Backups {
		if b.ID == backupID {
			// Remove backup file
			if err := os.Remove(b.Destination); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove backup file: %w", err)
			}

			// Remove WAL files
			if len(b.WALFiles) > 0 {
				walDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", backupID))
				os.RemoveAll(walDir)
			}

			// Remove from metadata
			m.metadata.Backups = append(m.metadata.Backups[:i], m.metadata.Backups[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("backup not found: %s", backupID)
}

// cleanupOldBackups removes old backups based on retention policy
func (m *Manager) cleanupOldBackups() error {
	if m.config.MaxBackups == 0 && m.config.RetentionPeriod == 0 {
		return nil
	}

	m.metadata.mu.Lock()
	defer m.metadata.mu.Unlock()

	if len(m.metadata.Backups) == 0 {
		return nil
	}

	// Sort backups by time (newest first)
	sorted := make([]*Backup, len(m.metadata.Backups))
	copy(sorted, m.metadata.Backups)
	for i := 0; i < len(sorted)-1; i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i].CompletedAt.Before(sorted[j].CompletedAt) {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	now := time.Now()
	var toDelete []*Backup

	// Check max backups
	if m.config.MaxBackups > 0 && len(sorted) > m.config.MaxBackups {
		toDelete = append(toDelete, sorted[m.config.MaxBackups:]...)
	}

	// Check retention period
	if m.config.RetentionPeriod > 0 {
		for _, b := range sorted {
			if now.Sub(b.CompletedAt) > m.config.RetentionPeriod {
				// Check if already marked for deletion
				alreadyMarked := false
				for _, d := range toDelete {
					if d.ID == b.ID {
						alreadyMarked = true
						break
					}
				}
				if !alreadyMarked {
					toDelete = append(toDelete, b)
				}
			}
		}
	}

	// Delete marked backups
	for _, b := range toDelete {
		os.Remove(b.Destination)
		if len(b.WALFiles) > 0 {
			walDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", b.ID))
			os.RemoveAll(walDir)
		}

		// Remove from metadata
		for i, meta := range m.metadata.Backups {
			if meta.ID == b.ID {
				m.metadata.Backups = append(m.metadata.Backups[:i], m.metadata.Backups[i+1:]...)
				break
			}
		}
	}

	return nil
}

// IsBackupInProgress returns true if a backup is currently running
func (m *Manager) IsBackupInProgress() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeBackup
}

// Helper functions

func generateBackupID() string {
	return fmt.Sprintf("backup_%d", time.Now().UnixNano())
}

func copyFile(srcPath, dstPath string) error {
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
