package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	file      *os.File
	mu        sync.Mutex
	bufWriter *bufio.Writer
	lsn       uint64 // Log Sequence Number (monotonic)
	checkpoint  uint64 // last checkpoint LSN
	path      string
}

// OpenWAL opens or creates a WAL file
func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
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
		file.Close()
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

	for {
		record, err := w.readRecord(reader)
		if err != nil {
			break // End of file or corruption
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

// readRecord reads a single WAL record from the reader
func (w *WAL) readRecord(reader *bufio.Reader) (*WALRecord, error) {
	// Read header: [TxnID:8][Type:1][PageID:4][Offset:2][Length:2]
	header := make([]byte, 17)
	if _, err := reader.Read(header); err != nil {
		return nil, err
	}

	record := &WALRecord{
		TxnID:  binary.LittleEndian.Uint64(header[0:8]),
		Type:   WALRecordType(header[8]),
		PageID: binary.LittleEndian.Uint32(header[9:13]),
		Offset: binary.LittleEndian.Uint16(header[13:15]),
	}

	dataLen := binary.LittleEndian.Uint16(header[15:17])
	if dataLen > 0 {
		record.Data = make([]byte, dataLen)
		if _, err := reader.Read(record.Data); err != nil {
			return nil, err
		}
	}

	// Read and verify CRC
	var storedCRC uint32
	if err := binary.Read(reader, binary.LittleEndian, &storedCRC); err != nil {
		return nil, err
	}

	// Calculate CRC
	buf := w.encodeRecord(record)
	calculatedCRC := crc32.ChecksumIEEE(buf)

	if storedCRC != calculatedCRC {
		return nil, ErrWALCorrupted
	}

	return record, nil
}

// Append adds a record to the WAL
func (w *WAL) Append(record *WALRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrWALClosed
	}

	w.lsn++
	record.LSN = w.lsn

	// Encode record
	buf := w.encodeRecord(record)

	// Calculate and write CRC
	crc := crc32.ChecksumIEEE(buf)

	// Write record
	if _, err := w.bufWriter.Write(buf); err != nil {
		return err
	}

	// Write CRC
	if err := binary.Write(w.bufWriter, binary.LittleEndian, crc); err != nil {
		return err
	}

	// Commit records must be flushed immediately
	if record.Type == WALCommit {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}

// encodeRecord encodes a WAL record to bytes (without CRC)
func (w *WAL) encodeRecord(record *WALRecord) []byte {
	dataLen := len(record.Data)
	buf := make([]byte, 17+dataLen)

	binary.LittleEndian.PutUint64(buf[0:8], record.TxnID)
	buf[8] = byte(record.Type)
	binary.LittleEndian.PutUint32(buf[9:13], record.PageID)
	binary.LittleEndian.PutUint16(buf[13:15], record.Offset)
	binary.LittleEndian.PutUint16(buf[15:17], uint16(dataLen))
	copy(buf[17:], record.Data)

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
	w.lsn++
	checkpointRecord.LSN = w.lsn

	buf := w.encodeRecord(checkpointRecord)
	crc := crc32.ChecksumIEEE(buf)

	if _, err := w.bufWriter.Write(buf); err != nil {
		return err
	}
	if err := binary.Write(w.bufWriter, binary.LittleEndian, crc); err != nil {
		return err
	}

	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	// 3. Truncate WAL file
	w.checkpoint = w.lsn
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

	// Read all records
	for {
		record, err := w.readRecord(reader)
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
	if record.Data != nil && len(record.Data) > 0 {
		offset := record.Offset
		copy(page.data[offset:], record.Data)
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
