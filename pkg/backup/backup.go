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

const metadataFileName = "metadata.json"

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
	m := &Manager{
		config:   config,
		metadata: &Metadata{Backups: make([]*Backup, 0)},
		db:       db,
	}
	_ = m.loadMetadata()
	return m
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
	if backup.Incremental {
		backup.ParentID = m.findParentBackupID(backupType)
	}

	// Start hot backup
	if err := m.db.BeginHotBackup(); err != nil {
		return nil, fmt.Errorf("failed to begin hot backup: %w", err)
	}
	defer func() {
		_ = m.db.EndHotBackup()
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
	if backup.Incremental && backup.ParentID != "" {
		return m.copyDatabaseDelta(ctx, backup)
	}

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
		defer compressor.Close()
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

	srcFile, err := os.Open(m.db.GetDatabasePath())
	if err != nil {
		return fmt.Errorf("failed to open source database: %w", err)
	}
	defer srcFile.Close()

	srcInfo, err := srcFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat source file: %w", err)
	}

	parentFile, err := os.Open(parentPath)
	if err != nil {
		return fmt.Errorf("failed to open parent database: %w", err)
	}
	defer parentFile.Close()

	dstFile, err := os.Create(backup.Destination)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer dstFile.Close()

	var writer io.Writer = dstFile
	var compressor *gzip.Writer
	if m.config.CompressionLevel > 0 {
		compressor, err = gzip.NewWriterLevel(dstFile, m.config.CompressionLevel)
		if err != nil {
			return fmt.Errorf("failed to create compressor: %w", err)
		}
		defer compressor.Close()
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

	backup.Size = payload.written
	backup.Checksum = crc.Sum32()

	return nil
}

func writeDeltaRecord(writer io.Writer, offset uint64, data []byte) error {
	if err := binary.Write(writer, binary.LittleEndian, offset); err != nil {
		return fmt.Errorf("failed to write delta offset: %w", err)
	}
	length := uint32(len(data))
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
		// Limit reader to prevent zip bomb attacks (100MB max decompressed size)
		const maxDecompressedSize = 100 * 1024 * 1024
		limitedReader := io.LimitReader(file, maxDecompressedSize)
		gzReader, err := gzip.NewReader(limitedReader)
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

	chain, err := m.buildRestoreChain(backup)
	if err != nil {
		return err
	}

	// Create target directory if needed
	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	for _, chainBackup := range chain {
		if err := m.restoreBackupPayload(ctx, chainBackup, targetPath); err != nil {
			return err
		}
	}

	// Restore WAL files if present
	if len(backup.WALFiles) > 0 {
		walBackupDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", backup.ID))
		// WAL directory is expected at <db-path>.wal (same convention as engine.GetWALPath)
		targetWALDir := targetPath + ".wal"

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

	targetFile, err := os.Create(targetPath)
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

	targetFile, err := os.OpenFile(targetPath, os.O_RDWR, 0644)
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

	file, err := os.Create(tmpPath)
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
		os.Remove(b.Destination)
		if len(b.WALFiles) > 0 {
			walDir := filepath.Join(m.config.BackupDir, fmt.Sprintf("%s_wal", b.ID))
			os.RemoveAll(walDir)
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

	if _, err = io.Copy(dstFile, srcFile); err != nil {
		return err
	}
	return dstFile.Sync()
}
