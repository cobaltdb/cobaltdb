package storage

import (
	"bufio"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	ErrWALCorrupted = errors.New("WAL is corrupted")
	ErrWALClosed    = errors.New("WAL is closed")
)

// WALRecordType represents the type of a WAL record
type WALRecordType uint8

const (
	WALInsert     WALRecordType = 0x01
	WALUpdate     WALRecordType = 0x02
	WALDelete     WALRecordType = 0x03
	WALCommit     WALRecordType = 0x04
	WALRollback   WALRecordType = 0x05
	WALCheckpoint WALRecordType = 0x06
)

// WALRecord represents a single write-ahead log record
type WALRecord struct {
	LSN    uint64
	TxnID  uint64
	Type   WALRecordType
	PageID uint32
	Offset uint16
	Data   []byte
}

// WAL (Write-Ahead Log) provides durability and crash recovery
type WAL struct {
	file       *os.File
	mu         sync.Mutex
	bufWriter  *bufio.Writer
	lsn        uint64 // Log Sequence Number (monotonic)
	checkpoint uint64 // last checkpoint LSN
	path       string
	cipher     cipher.AEAD // optional encryption cipher for WAL data
}

// SetEncryptionCipher sets an AEAD cipher for encrypting WAL record data.
// When set, WAL record Data fields are encrypted before writing and decrypted on read.
// The cipher must use the same key as the main storage encryption.
func (w *WAL) SetEncryptionCipher(c cipher.AEAD) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cipher = c
}

// encryptData encrypts WAL record data with optional header as AAD
func (w *WAL) encryptData(plaintext []byte, headerAAD []byte) ([]byte, error) {
	if w.cipher == nil || len(plaintext) == 0 {
		return plaintext, nil
	}
	nonce := make([]byte, w.cipher.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("WAL encrypt: failed to generate nonce: %w", err)
	}
	// Use header as Authenticated Associated Data - protects header integrity
	return w.cipher.Seal(nonce, nonce, plaintext, headerAAD), nil
}

// decryptData decrypts WAL record data with optional header as AAD
func (w *WAL) decryptData(ciphertext []byte, headerAAD []byte) ([]byte, error) {
	if w.cipher == nil || len(ciphertext) == 0 {
		return ciphertext, nil
	}
	nonceSize := w.cipher.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("WAL decrypt: ciphertext too short")
	}
	nonce := ciphertext[:nonceSize]
	data := ciphertext[nonceSize:]
	return w.cipher.Open(nil, nonce, data, headerAAD)
}

// OpenWAL opens or creates a WAL file
func OpenWAL(path string) (*WAL, error) {
	// #nosec G304 -- Path is provided by trusted application configuration.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		file:      file,
		bufWriter: bufio.NewWriter(file),
		path:      path,
	}

	// Read existing records to find current LSN
	if err := wal.readLSN(); err != nil {
		_ = file.Close()
		return nil, err
	}

	return wal, nil
}

// readLSN scans the WAL file to find the last LSN
func (w *WAL) readLSN() error {
	stat, err := w.file.Stat()
	if err != nil {
		return err
	}

	if stat.Size() == 0 {
		w.lsn = 0
		w.checkpoint = 0
		return nil
	}

	// Seek to beginning and read all records
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	var lastLSN uint64
	var headerBuf [25]byte // reusable header buffer across readRecord calls

	for {
		record, err := w.readRecord(reader, headerBuf[:])
		if err != nil {
			if errors.Is(err, ErrWALCorrupted) {
				return fmt.Errorf("WAL recovery failed at LSN %d: %w", lastLSN, ErrWALCorrupted)
			}
			break // End of file (io.EOF or io.ErrUnexpectedEOF from partial trailing record)
		}
		lastLSN = record.LSN
		if record.Type == WALCheckpoint {
			w.checkpoint = record.LSN
		}
	}

	w.lsn = lastLSN

	// Seek to end for appending
	if _, err := w.file.Seek(0, 2); err != nil {
		return err
	}

	return nil
}

// readRecord reads a single WAL record from the reader.
// header must be a 25-byte slice that is reused across calls to avoid per-record allocation.
func (w *WAL) readRecord(reader *bufio.Reader, header []byte) (*WALRecord, error) {
	// Read header: [LSN:8][TxnID:8][Type:1][PageID:4][Offset:2][Length:2]
	if _, err := io.ReadFull(reader, header[:25]); err != nil {
		return nil, err
	}

	record := &WALRecord{
		LSN:    binary.LittleEndian.Uint64(header[0:8]),
		TxnID:  binary.LittleEndian.Uint64(header[8:16]),
		Type:   WALRecordType(header[16]),
		PageID: binary.LittleEndian.Uint32(header[17:21]),
		Offset: binary.LittleEndian.Uint16(header[21:23]),
	}

	dataLen := binary.LittleEndian.Uint16(header[23:25])
	if dataLen > 0 {
		record.Data = make([]byte, dataLen)
		if _, err := io.ReadFull(reader, record.Data); err != nil {
			return nil, err
		}
	}

	// Read and verify CRC (direct read avoids binary.Read reflection)
	var crcBuf [4]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		return nil, err
	}
	storedCRC := binary.LittleEndian.Uint32(crcBuf[:])

	// Calculate CRC from header bytes + data directly (avoids re-encode allocation)
	crcHash := crc32.NewIEEE()
	if _, err := crcHash.Write(header[:25]); err != nil {
		return nil, err
	}
	if len(record.Data) > 0 {
		if _, err := crcHash.Write(record.Data); err != nil {
			return nil, err
		}
	}
	calculatedCRC := crcHash.Sum32()

	if storedCRC != calculatedCRC {
		return nil, ErrWALCorrupted
	}

	// Decrypt record data if cipher is configured
	if w.cipher != nil && len(record.Data) > 0 {
		// Use header bytes as AAD to verify header integrity
		decrypted, err := w.decryptData(record.Data, header[:25])
		if err != nil {
			return nil, fmt.Errorf("WAL record decryption failed at LSN %d: %w", record.LSN, err)
		}
		record.Data = decrypted
	}

	return record, nil
}

// Append adds a record to the WAL
func (w *WAL) Append(record *WALRecord) error {
	if len(record.Data) > 65535 {
		return fmt.Errorf("WAL record data size (%d bytes) exceeds maximum (65535 bytes)", len(record.Data))
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.appendInternal(record, true)
}

// AppendWithoutSync adds a record without syncing (for group commit)
func (w *WAL) AppendWithoutSync(record *WALRecord) error {
	if len(record.Data) > 65535 {
		return fmt.Errorf("WAL record data size (%d bytes) exceeds maximum (65535 bytes)", len(record.Data))
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.appendInternal(record, false)
}

// appendInternal is the internal append implementation
func (w *WAL) appendInternal(record *WALRecord, sync bool) error {
	if w.file == nil {
		return ErrWALClosed
	}

	newLSN := w.lsn + 1
	record.LSN = newLSN

	// Encrypt record data if cipher is configured
	originalData := record.Data
	if w.cipher != nil && len(record.Data) > 0 {
		// Build header bytes as AAD (LSN+TxnID+Type+PageID+Offset)
		headerAAD := make([]byte, 25)
		binary.LittleEndian.PutUint64(headerAAD[0:8], record.LSN)
		binary.LittleEndian.PutUint64(headerAAD[8:16], record.TxnID)
		headerAAD[16] = byte(record.Type)
		binary.LittleEndian.PutUint32(headerAAD[17:21], record.PageID)
		binary.LittleEndian.PutUint16(headerAAD[21:23], record.Offset)
		binary.LittleEndian.PutUint16(headerAAD[23:25], uint16(len(record.Data)))

		encrypted, err := w.encryptData(record.Data, headerAAD)
		if err != nil {
			return err
		}
		record.Data = encrypted
	}

	// Encode record
	buf := w.encodeRecord(record)

	// Restore original data to avoid modifying caller's record
	record.Data = originalData

	// Calculate and write CRC
	crc := crc32.ChecksumIEEE(buf)

	// Write record
	if _, err := w.bufWriter.Write(buf); err != nil {
		return err
	}

	// Write CRC (direct encoding avoids binary.Write reflection)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.bufWriter.Write(crcBuf[:]); err != nil {
		return err
	}

	// Only update LSN after successful write
	w.lsn = newLSN

	// Sync if requested (for commit records or explicit sync)
	if sync {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}

// Sync flushes the buffer and syncs to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrWALClosed
	}

	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// encodeRecord encodes a WAL record to bytes (without CRC)
// Format: [LSN:8][TxnID:8][Type:1][PageID:4][Offset:2][Length:2][Data:N]
func (w *WAL) encodeRecord(record *WALRecord) []byte {
	dataLen := len(record.Data)
	buf := make([]byte, 25+dataLen)

	binary.LittleEndian.PutUint64(buf[0:8], record.LSN)
	binary.LittleEndian.PutUint64(buf[8:16], record.TxnID)
	buf[16] = byte(record.Type)
	binary.LittleEndian.PutUint32(buf[17:21], record.PageID)
	binary.LittleEndian.PutUint16(buf[21:23], record.Offset)
	binary.LittleEndian.PutUint16(buf[23:25], uint16(dataLen))
	copy(buf[25:], record.Data)

	return buf
}

// Checkpoint flushes dirty pages to main DB file and truncates WAL
func (w *WAL) Checkpoint(bp *BufferPool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrWALClosed
	}

	// 1. Flush all dirty pages from buffer pool
	if err := bp.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush pages: %w", err)
	}

	// 2. Write checkpoint record
	checkpointRecord := &WALRecord{
		TxnID: 0,
		Type:  WALCheckpoint,
	}
	newLSN := w.lsn + 1
	checkpointRecord.LSN = newLSN

	buf := w.encodeRecord(checkpointRecord)
	crc := crc32.ChecksumIEEE(buf)

	if _, err := w.bufWriter.Write(buf); err != nil {
		return err
	}
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.bufWriter.Write(crcBuf[:]); err != nil {
		return err
	}

	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	// Update LSN and checkpoint after successful write
	w.lsn = newLSN
	w.checkpoint = newLSN

	// 3. Truncate WAL file
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.bufWriter = bufio.NewWriter(w.file)

	return nil
}

// Recover replays WAL records after a crash
func (w *WAL) Recover(bp *BufferPool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrWALClosed
	}

	// Seek to beginning
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	reader := bufio.NewReader(w.file)
	var committedTxns = make(map[uint64]bool)
	var pendingTxns = make(map[uint64][]*WALRecord)
	var headerBuf [25]byte // reusable header buffer across readRecord calls

	// Read all records
	for {
		record, err := w.readRecord(reader, headerBuf[:])
		if err != nil {
			break // End of file
		}

		switch record.Type {
		case WALCommit:
			committedTxns[record.TxnID] = true
			// Apply pending records for this transaction
			if records, ok := pendingTxns[record.TxnID]; ok {
				for _, r := range records {
					if err := w.applyRecord(bp, r); err != nil {
						return err
					}
				}
				delete(pendingTxns, record.TxnID)
			}

		case WALRollback:
			delete(pendingTxns, record.TxnID)

		case WALInsert, WALUpdate, WALDelete:
			if committedTxns[record.TxnID] {
				// Transaction already committed, apply immediately
				if err := w.applyRecord(bp, record); err != nil {
					return err
				}
			} else {
				// Buffer for later
				pendingTxns[record.TxnID] = append(pendingTxns[record.TxnID], record)
			}
		}
	}

	// Flush recovered pages
	return bp.FlushAll()
}

// applyRecord applies a WAL record to the database
func (w *WAL) applyRecord(bp *BufferPool, record *WALRecord) error {
	page, err := bp.GetPage(record.PageID)
	if err != nil {
		return err
	}
	defer bp.Unpin(page)

	// Apply the change
	if len(record.Data) > 0 {
		offset := int(record.Offset)
		end := offset + len(record.Data)
		if offset < 0 || end > len(page.data) {
			return ErrWALCorrupted
		}
		copy(page.data[offset:end], record.Data)
	}

	page.SetDirty(true)
	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	if err := w.bufWriter.Flush(); err != nil {
		return err
	}

	err := w.file.Close()
	w.file = nil
	return err
}

// LSN returns the current log sequence number
func (w *WAL) LSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lsn
}

// CheckpointLSN returns the last checkpoint LSN
func (w *WAL) CheckpointLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.checkpoint
}
