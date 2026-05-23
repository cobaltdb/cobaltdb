// Package backup provides hot backup capabilities for CobaltDB
package backup

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
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

const (
	metadataFileName = "metadata.json"
	backupDirPerm    = 0750
	backupFilePerm   = 0600
)

const (
	deltaMagic     = "CBDBDELTA1\n"
	deltaChunkSize = 64 * 1024
)

type deltaHeader struct {
	ChunkSize  int   `json:"chunk_size"`
	TargetSize int64 `json:"target_size"`
}

type payloadWriter struct {
	writer  io.Writer
	crc     hash.Hash32
	written int64
}

func (w *payloadWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	if n > 0 {
		_, _ = w.crc.Write(p[:n])
		w.written += int64(n)
	}
	return n, err
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
	// WALPathIsFile is true when the source database exposes WAL as a single
	// file instead of a directory of segment files.
	WALPathIsFile bool
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
	loadErr  error
	// lastCleanupErr stores non-fatal retention cleanup failures after a
	// successful backup, so callers can alert without treating the backup as lost.
	lastCleanupErr error
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
	if config == nil {
		config = DefaultConfig()
	}

	m := &Manager{
		config:   config,
		metadata: &Metadata{Backups: make([]*Backup, 0)},
		db:       db,
	}
	m.loadErr = m.loadMetadata()
	return m
}

// MetadataError returns any metadata load error encountered during manager creation.
func (m *Manager) MetadataError() error {
	return m.loadErr
}

// LastCleanupError returns the most recent non-fatal retention cleanup error.
func (m *Manager) LastCleanupError() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.lastCleanupErr
}

func (m *Manager) setLastCleanupError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.lastCleanupErr = err
}

func (m *Manager) ensureMetadataLoaded() error {
	if m.loadErr != nil {
		return fmt.Errorf("backup metadata unavailable: %w", m.loadErr)
	}
	return nil
}

// CreateBackup creates a new backup
func (m *Manager) CreateBackup(ctx context.Context, backupType Type) (backup *Backup, err error) {
	var callbackBackup *Backup
	defer func() {
		if m.OnComplete != nil && (callbackBackup != nil || err != nil) {
			m.OnComplete(callbackBackup, err)
		}
	}()

	if err := m.ensureMetadataLoaded(); err != nil {
		return nil, err
	}

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
	if err := os.MkdirAll(m.config.BackupDir, backupDirPerm); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Generate backup ID
	backupID := generateBackupID()

	backup = &Backup{
		ID:          backupID,
		Type:        backupType,
		StartedAt:   time.Now(),
		Source:      m.db.GetDatabasePath(),
		Incremental: backupType != TypeFull,
	}
	callbackBackup = backup
	if backup.Incremental {
		backup.ParentID = m.findParentBackupID(backupType)
	}

	// Start hot backup
	if err := m.db.BeginHotBackup(); err != nil {
		return nil, fmt.Errorf("failed to begin hot backup: %w", err)
	}
	defer func() {
		if endErr := m.db.EndHotBackup(); endErr != nil && err == nil {
			err = fmt.Errorf("failed to end hot backup: %w", endErr)
		}
	}()

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
	if err := m.saveMetadataLocked(); err != nil {
		m.metadata.mu.Unlock()
		return nil, fmt.Errorf("failed to save backup metadata: %w", err)
	}
	m.metadata.mu.Unlock()

	// Cleanup old backups
	if err := m.cleanupOldBackups(); err != nil {
		m.setLastCleanupError(err)
	} else {
		m.setLastCleanupError(nil)
	}

	return backup, nil
}

// copyDatabase copies the database file to backup location
func (m *Manager) copyDatabase(ctx context.Context, backup *Backup) error {
	if backup.Incremental && backup.ParentID != "" {
		return m.copyDatabaseDelta(ctx, backup)
	}

	srcPath := m.db.GetDatabasePath()
	dstPath := backup.Destination

	srcPath, err := cleanBackupFilePath(srcPath)
	if err != nil {
		return fmt.Errorf("invalid source database path: %w", err)
	}
	srcFile, err := os.Open(srcPath) // #nosec G304 - source path comes from the configured database and is cleaned before use.
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
	dstFile, err := createSecureFile(dstPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	dstClosed := false
	defer func() {
		if !dstClosed {
			_ = dstFile.Close()
		}
	}()

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
	if err := dstFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync backup file: %w", err)
	}
	if err := dstFile.Close(); err != nil {
		return fmt.Errorf("failed to close backup file: %w", err)
	}
	dstClosed = true
	if err := syncParentDir(dstPath); err != nil {
		return fmt.Errorf("failed to sync backup directory: %w", err)
	}

	backup.Size = written
	backup.Checksum = crc.Sum32()

	return nil
}

func (m *Manager) copyDatabaseDelta(ctx context.Context, backup *Backup) error {
	parent := m.GetBackup(backup.ParentID)
	if parent == nil {
		return fmt.Errorf("parent backup not found: %s", backup.ParentID)
	}

	tmpParent, err := os.CreateTemp(m.config.BackupDir, "parent-*.db")
	if err != nil {
		return fmt.Errorf("failed to create parent restore file: %w", err)
	}
	parentPath := tmpParent.Name()
	if err := tmpParent.Close(); err != nil {
		_ = os.Remove(parentPath)
		return fmt.Errorf("failed to close parent restore file: %w", err)
	}
	defer os.Remove(parentPath)

	if err := m.materializeBackup(ctx, parent, parentPath); err != nil {
		return fmt.Errorf("failed to materialize parent backup: %w", err)
	}

	srcPath, err := cleanBackupFilePath(m.db.GetDatabasePath())
	if err != nil {
		return fmt.Errorf("invalid source database path: %w", err)
	}
	srcFile, err := os.Open(srcPath) // #nosec G304 - source path comes from the configured database and is cleaned before use.
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	parentPath, err = cleanBackupFilePath(parentPath)
	if err != nil {
		return fmt.Errorf("invalid parent database path: %w", err)
	}
	parentFile, err := os.Open(parentPath) // #nosec G304 - parent path is returned by os.CreateTemp and cleaned before use.
	if err != nil {
		return fmt.Errorf("failed to open parent database: %w", err)
	}
	defer parentFile.Close()

	dstFile, err := createSecureFile(backup.Destination)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	dstClosed := false
	defer func() {
		if !dstClosed {
			_ = dstFile.Close()
		}
	}()

	var writer io.Writer = dstFile
	var compressor *gzip.Writer
	if m.config.CompressionLevel > 0 {
		compressor, err = gzip.NewWriterLevel(dstFile, m.config.CompressionLevel)
		if err != nil {
			return fmt.Errorf("failed to create compressor: %w", err)
		}
		writer = compressor
	}

	crc := crc32.NewIEEE()
	payload := &payloadWriter{writer: writer, crc: crc}
	if _, err := payload.Write([]byte(deltaMagic)); err != nil {
		return fmt.Errorf("failed to write delta header: %w", err)
	}
	header := deltaHeader{ChunkSize: deltaChunkSize, TargetSize: srcInfo.Size()}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("failed to encode delta header: %w", err)
	}
	if _, err := payload.Write(append(headerBytes, '\n')); err != nil {
		return fmt.Errorf("failed to write delta header: %w", err)
	}

	srcBuf := make([]byte, deltaChunkSize)
	parentBuf := make([]byte, deltaChunkSize)
	var offset int64
	var copied int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, readErr := srcFile.Read(srcBuf)
		if n > 0 {
			parentN, parentErr := parentFile.ReadAt(parentBuf[:n], offset)
			if parentErr != nil && parentErr != io.EOF {
				return fmt.Errorf("failed to read parent database: %w", parentErr)
			}
			if parentN != n || !bytes.Equal(srcBuf[:n], parentBuf[:n]) {
				if err := writeDeltaRecord(payload, uint64(offset), srcBuf[:n]); err != nil {
					return err
				}
			}

			offset += int64(n)
			copied += int64(n)
			if m.OnProgress != nil && srcInfo.Size() > 0 {
				percent := int((copied * 100) / srcInfo.Size())
				m.OnProgress(percent)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("failed to read source file: %w", readErr)
		}
	}

	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return fmt.Errorf("failed to finalize compression: %w", err)
		}
	}
	if err := dstFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync backup file: %w", err)
	}
	if err := dstFile.Close(); err != nil {
		return fmt.Errorf("failed to close backup file: %w", err)
	}
	dstClosed = true
	if err := syncParentDir(backup.Destination); err != nil {
		return fmt.Errorf("failed to sync backup directory: %w", err)
	}

	backup.Size = payload.written
	backup.Checksum = crc.Sum32()

	return nil
}

func writeDeltaRecord(writer io.Writer, offset uint64, data []byte) error {
	if err := binary.Write(writer, binary.LittleEndian, offset); err != nil {
		return fmt.Errorf("failed to write delta offset: %w", err)
	}
	if len(data) > 1<<32-1 {
		return fmt.Errorf("delta record too large: %d bytes", len(data))
	}
	length := uint32(len(data)) // #nosec G115 - range checked above.
	if err := binary.Write(writer, binary.LittleEndian, length); err != nil {
		return fmt.Errorf("failed to write delta length: %w", err)
	}
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("failed to write delta data: %w", err)
	}
	return nil
}

// copyWALFiles copies WAL files to backup
func (m *Manager) copyWALFiles(ctx context.Context, backup *Backup) error {
	walPath := m.db.GetWALPath()
	if walPath == "" {
		return nil
	}

	walInfo, err := os.Stat(walPath)
	if err != nil {
		return fmt.Errorf("failed to stat WAL path: %w", err)
	}

	walBackupDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", backup.ID))
	if err := os.MkdirAll(walBackupDir, backupDirPerm); err != nil {
		return fmt.Errorf("failed to create WAL backup directory: %w", err)
	}

	if !walInfo.IsDir() {
		const walFileName = "wal"
		if err := copyFile(walPath, filepath.Join(walBackupDir, walFileName)); err != nil {
			return fmt.Errorf("failed to copy WAL file: %w", err)
		}
		backup.WALFiles = append(backup.WALFiles, walFileName)
		backup.WALPathIsFile = true
		return nil
	}

	entries, err := os.ReadDir(walPath)
	if err != nil {
		return fmt.Errorf("failed to read WAL directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		srcPath := filepath.Join(walPath, entry.Name())
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
	if backup.Size < 0 || backup.Size == math.MaxInt64 {
		return fmt.Errorf("invalid backup size: %d", backup.Size)
	}
	crc := crc32.NewIEEE()
	limitedReader := &io.LimitedReader{R: reader, N: backup.Size + 1}
	readSize, err := io.Copy(crc, limitedReader)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	calculatedChecksum := crc.Sum32()
	valid := readSize == backup.Size && calculatedChecksum == backup.Checksum

	if m.OnVerify != nil {
		m.OnVerify(backup, valid)
	}

	if readSize != backup.Size {
		return fmt.Errorf("backup size mismatch: expected %d, got %d", backup.Size, readSize)
	}
	if !valid {
		return fmt.Errorf("checksum mismatch: expected %d, got %d", backup.Checksum, calculatedChecksum)
	}

	return nil
}

// Restore restores database from a backup
func (m *Manager) Restore(ctx context.Context, backupID string, targetPath string) error {
	if err := m.ensureMetadataLoaded(); err != nil {
		return err
	}
	targetPath, err := cleanBackupFilePath(targetPath)
	if err != nil {
		return fmt.Errorf("invalid restore target path: %w", err)
	}

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

	chain, err := m.buildRestoreChain(backup)
	if err != nil {
		return err
	}

	// Create target directory if needed
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, backupDirPerm); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	tmpTarget, err := os.CreateTemp(targetDir, ".restore-*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary restore file: %w", err)
	}
	tmpTargetPath := tmpTarget.Name()
	if err := tmpTarget.Close(); err != nil {
		_ = os.Remove(tmpTargetPath)
		return fmt.Errorf("failed to close temporary restore file: %w", err)
	}
	defer func() {
		if tmpTargetPath != "" {
			_ = os.Remove(tmpTargetPath)
		}
	}()

	for _, chainBackup := range chain {
		if err := m.restoreBackupPayload(ctx, chainBackup, tmpTargetPath); err != nil {
			return err
		}
	}

	if err := os.Rename(tmpTargetPath, targetPath); err != nil {
		return fmt.Errorf("failed to replace restored database: %w", err)
	}
	tmpTargetPath = ""
	if err := syncParentDir(targetPath); err != nil {
		return fmt.Errorf("failed to sync restore directory: %w", err)
	}

	// Restore WAL files if present
	if len(backup.WALFiles) > 0 {
		walBackupDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", backup.ID))
		targetWALPath := targetPath + ".wal"

		if backup.WALPathIsFile {
			if err := copyFile(filepath.Join(walBackupDir, backup.WALFiles[0]), targetWALPath); err != nil {
				return fmt.Errorf("failed to restore WAL file: %w", err)
			}
			return nil
		}

		// Directory WALs are restored at <db-path>.wal for compatibility with
		// segmented WAL implementations and existing backup tests.
		tmpWALPath, err := os.MkdirTemp(filepath.Dir(targetWALPath), ".restore-wal-*")
		if err != nil {
			return fmt.Errorf("failed to create temporary WAL restore directory: %w", err)
		}
		defer func() {
			if tmpWALPath != "" {
				_ = os.RemoveAll(tmpWALPath)
			}
		}()

		for _, walFile := range backup.WALFiles {
			srcPath := filepath.Join(walBackupDir, walFile)
			dstPath := filepath.Join(tmpWALPath, walFile)

			if err := copyFile(srcPath, dstPath); err != nil {
				return fmt.Errorf("failed to restore WAL file %s: %w", walFile, err)
			}
		}
		if err := syncParentDir(filepath.Join(tmpWALPath, "wal")); err != nil {
			return fmt.Errorf("failed to sync temporary WAL restore directory: %w", err)
		}
		if err := os.RemoveAll(targetWALPath); err != nil {
			return fmt.Errorf("failed to remove existing WAL directory: %w", err)
		}
		if err := os.Rename(tmpWALPath, targetWALPath); err != nil {
			return fmt.Errorf("failed to replace restored WAL directory: %w", err)
		}
		tmpWALPath = ""
		if err := syncParentDir(targetWALPath); err != nil {
			return fmt.Errorf("failed to sync restored WAL directory parent: %w", err)
		}
	}

	return nil
}

func (m *Manager) materializeBackup(ctx context.Context, backup *Backup, targetPath string) error {
	chain, err := m.buildRestoreChain(backup)
	if err != nil {
		return err
	}

	for _, chainBackup := range chain {
		if err := m.restoreBackupPayload(ctx, chainBackup, targetPath); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) restoreBackupPayload(ctx context.Context, backup *Backup, targetPath string) error {
	if _, err := os.Stat(backup.Destination); err != nil {
		return fmt.Errorf("backup file not found: %w", err)
	}

	reader, err := m.openBackupReader(backup)
	if err != nil {
		return err
	}
	defer reader.Close()

	prefix := make([]byte, len(deltaMagic))
	n, err := io.ReadFull(reader, prefix)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return fmt.Errorf("failed to read backup file: %w", err)
	}
	if backup.Incremental && backup.ParentID != "" && string(prefix[:n]) == deltaMagic {
		return m.applyDeltaPayload(ctx, reader, targetPath)
	}

	targetFile, err := createSecureFile(targetPath)
	if err != nil {
		return fmt.Errorf("failed to create target file: %w", err)
	}
	defer targetFile.Close()

	if n > 0 {
		if _, err := targetFile.Write(prefix[:n]); err != nil {
			return fmt.Errorf("failed to write target file: %w", err)
		}
	}

	buf := make([]byte, 64*1024)
	written := int64(n)
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

	if err := targetFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync restored file: %w", err)
	}
	return nil
}

func (m *Manager) openBackupReader(backup *Backup) (io.ReadCloser, error) {
	file, err := os.Open(backup.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to open backup file: %w", err)
	}

	if filepath.Ext(backup.Destination) != ".gz" {
		return file, nil
	}

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	return &compoundReadCloser{Reader: gzReader, closers: []io.Closer{gzReader, file}}, nil
}

type compoundReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (r *compoundReadCloser) Close() error {
	var closeErr error
	for _, closer := range r.closers {
		if err := closer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func (m *Manager) applyDeltaPayload(ctx context.Context, reader io.Reader, targetPath string) error {
	buffered := bufio.NewReader(reader)
	headerLine, err := buffered.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("failed to read delta header: %w", err)
	}

	var header deltaHeader
	if err := json.Unmarshal(bytes.TrimSpace(headerLine), &header); err != nil {
		return fmt.Errorf("failed to decode delta header: %w", err)
	}
	if header.ChunkSize <= 0 || header.TargetSize < 0 {
		return fmt.Errorf("invalid delta header")
	}

	targetPath, err = cleanBackupFilePath(targetPath)
	if err != nil {
		return fmt.Errorf("invalid target file path: %w", err)
	}
	targetFile, err := os.OpenFile(targetPath, os.O_RDWR, 0600) // #nosec G304 - restore target path is an explicit API argument and is cleaned before use.
	if err != nil {
		return fmt.Errorf("failed to open target file for delta restore: %w", err)
	}
	defer targetFile.Close()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var offset uint64
		err := binary.Read(buffered, binary.LittleEndian, &offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read delta offset: %w", err)
		}

		var length uint32
		if err := binary.Read(buffered, binary.LittleEndian, &length); err != nil {
			return fmt.Errorf("failed to read delta length: %w", err)
		}
		if int(length) > header.ChunkSize {
			return fmt.Errorf("delta record length %d exceeds chunk size %d", length, header.ChunkSize)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(buffered, data); err != nil {
			return fmt.Errorf("failed to read delta data: %w", err)
		}
		if _, err := targetFile.WriteAt(data, int64(offset)); err != nil {
			return fmt.Errorf("failed to apply delta data: %w", err)
		}
	}

	if err := targetFile.Truncate(header.TargetSize); err != nil {
		return fmt.Errorf("failed to truncate restored file: %w", err)
	}
	if err := targetFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync restored file: %w", err)
	}

	return nil
}

func (m *Manager) findParentBackupID(backupType Type) string {
	m.metadata.mu.RLock()
	defer m.metadata.mu.RUnlock()

	var parent *Backup
	for _, backup := range m.metadata.Backups {
		if backup.CompletedAt.IsZero() {
			continue
		}
		if backupType == TypeDifferential && backup.Type != TypeFull {
			continue
		}
		if parent == nil || backup.CompletedAt.After(parent.CompletedAt) {
			parent = backup
		}
	}

	if parent == nil {
		return ""
	}
	return parent.ID
}

func (m *Manager) buildRestoreChain(backup *Backup) ([]*Backup, error) {
	if backup == nil {
		return nil, fmt.Errorf("backup not found")
	}
	if backup.ParentID == "" {
		return []*Backup{backup}, nil
	}

	m.metadata.mu.RLock()
	backupsByID := make(map[string]*Backup, len(m.metadata.Backups))
	for _, candidate := range m.metadata.Backups {
		backupsByID[candidate.ID] = candidate
	}
	m.metadata.mu.RUnlock()

	seen := map[string]bool{backup.ID: true}
	current := backup
	chain := []*Backup{backup}
	for current.ParentID != "" {
		parent, ok := backupsByID[current.ParentID]
		if !ok {
			return nil, fmt.Errorf("backup %s parent not found: %s", current.ID, current.ParentID)
		}
		if seen[parent.ID] {
			return nil, fmt.Errorf("backup chain contains a cycle at %s", parent.ID)
		}
		if _, err := os.Stat(parent.Destination); err != nil {
			return nil, fmt.Errorf("backup %s parent file not found: %w", current.ID, err)
		}

		switch current.Type {
		case TypeIncremental:
			if parent.Type != TypeFull && parent.Type != TypeIncremental {
				return nil, fmt.Errorf("incremental backup %s has invalid parent type %v", current.ID, parent.Type)
			}
		case TypeDifferential:
			if parent.Type != TypeFull {
				return nil, fmt.Errorf("differential backup %s requires full parent, got %v", current.ID, parent.Type)
			}
		case TypeFull:
			return nil, fmt.Errorf("full backup %s cannot have a parent backup", current.ID)
		default:
			return nil, fmt.Errorf("unknown backup type %v for backup %s", current.Type, current.ID)
		}

		seen[parent.ID] = true
		chain = append(chain, parent)
		current = parent
	}

	for left, right := 0, len(chain)-1; left < right; left, right = left+1, right-1 {
		chain[left], chain[right] = chain[right], chain[left]
	}

	return chain, nil
}

func (m *Manager) metadataPath() string {
	return filepath.Join(m.config.BackupDir, metadataFileName)
}

func (m *Manager) loadMetadata() error {
	file, err := os.Open(m.metadataPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open backup metadata: %w", err)
	}
	defer file.Close()

	var metadata Metadata
	if err := json.NewDecoder(file).Decode(&metadata); err != nil {
		return fmt.Errorf("failed to decode backup metadata: %w", err)
	}
	if metadata.Backups == nil {
		metadata.Backups = make([]*Backup, 0)
	}

	m.metadata.mu.Lock()
	m.metadata.Backups = metadata.Backups
	m.metadata.mu.Unlock()

	return nil
}

func (m *Manager) saveMetadataLocked() error {
	if _, err := os.Stat(m.config.BackupDir); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to stat backup directory: %w", err)
	}

	path := m.metadataPath()
	tmpPath := path + ".tmp"

	file, err := createSecureFile(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create backup metadata file: %w", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(m.metadata); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to encode backup metadata: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to sync backup metadata: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to close backup metadata: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to replace backup metadata: %w", err)
	}
	if err := syncParentDir(path); err != nil {
		return fmt.Errorf("failed to sync backup metadata directory: %w", err)
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
	if err := m.ensureMetadataLoaded(); err != nil {
		return err
	}

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
				if err := os.RemoveAll(walDir); err != nil {
					return fmt.Errorf("failed to remove backup WAL directory: %w", err)
				}
			}

			// Remove from metadata
			m.metadata.Backups = append(m.metadata.Backups[:i], m.metadata.Backups[i+1:]...)
			if err := m.saveMetadataLocked(); err != nil {
				return fmt.Errorf("failed to save backup metadata: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("backup not found: %s", backupID)
}

// cleanupOldBackups removes old backups based on retention policy
func (m *Manager) cleanupOldBackups() error {
	if err := m.ensureMetadataLoaded(); err != nil {
		return err
	}

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
	changed := false
	for _, b := range toDelete {
		if b.Destination != "" {
			if err := os.Remove(b.Destination); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("failed to remove old backup file %s: %w", b.Destination, err)
			}
		}
		if len(b.WALFiles) > 0 {
			walDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", b.ID))
			if err := os.RemoveAll(walDir); err != nil {
				return fmt.Errorf("failed to remove old backup WAL directory %s: %w", walDir, err)
			}
		}

		// Remove from metadata
		for i, meta := range m.metadata.Backups {
			if meta.ID == b.ID {
				m.metadata.Backups = append(m.metadata.Backups[:i], m.metadata.Backups[i+1:]...)
				changed = true
				break
			}
		}
	}

	if changed {
		if err := m.saveMetadataLocked(); err != nil {
			return fmt.Errorf("failed to save backup metadata: %w", err)
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
	srcPath, err := cleanBackupFilePath(srcPath)
	if err != nil {
		return err
	}
	srcFile, err := os.Open(srcPath) // #nosec G304 - caller supplies a file path that is cleaned before use.
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := createSecureFile(dstPath)
	if err != nil {
		return err
	}

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		_ = dstFile.Close()
		return err
	}
	if err := dstFile.Sync(); err != nil {
		_ = dstFile.Close()
		return err
	}
	if err := dstFile.Close(); err != nil {
		return err
	}
	return nil
}

func createSecureFile(path string) (*os.File, error) {
	path, err := cleanBackupFilePath(path)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, backupFilePerm) // #nosec G304 - path is cleaned and created with restrictive permissions.
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(backupFilePerm); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func cleanBackupFilePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("path cannot be empty")
	}
	return filepath.Clean(path), nil
}

func syncParentDir(path string) error {
	dir := filepath.Dir(path)
	// #nosec G304 -- caller path is already validated by the backup path helpers.
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
