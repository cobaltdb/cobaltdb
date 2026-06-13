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
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrWALCorrupted             = errors.New("WAL is corrupted")
	ErrWALClosed                = errors.New("WAL is closed")
	ErrInvalidWALRecord         = errors.New("invalid WAL record")
	ErrInvalidWALRecoveryTarget = errors.New("invalid WAL recovery target")
)

// walBatchBufPool provides 2 KB reusable buffers for WAL AppendBatch fast path.
// This avoids a per-call heap escape from local arrays passed to bufio.Writer.Write.
var walBatchBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, walBatchBufferSize)
		return &b
	},
}

// WALRecordType represents the type of a WAL record
type WALRecordType uint8

const (
	WALInsert       WALRecordType = 0x01
	WALUpdate       WALRecordType = 0x02
	WALDelete       WALRecordType = 0x03
	WALCommit       WALRecordType = 0x04
	WALRollback     WALRecordType = 0x05
	WALCheckpoint   WALRecordType = 0x06
	WALUpdateCommit WALRecordType = 0x07
)

// WAL on-disk format constants.
//
//	[LSN:8][TxnID:8][Type:1][PageID:4][Offset:2][Length:2][Data:N][CRC:4]
const (
	walHeaderSize        = 25        // bytes preceding the variable-length data payload
	walMaxRecordDataSize = 1<<16 - 1 // payload length is encoded as uint16
	walBatchBufferSize   = 2048
	// Recovery buffers uncommitted transaction records until a matching commit is
	// seen. These limits keep a corrupt WAL from turning recovery into unbounded
	// heap growth before the file is rejected.
	walMaxRecoveryPendingRecords = 100000
	walMaxRecoveryPendingBytes   = 256 << 20 // 256 MiB
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

// WALReplayOp represents a logical operation recovered from WAL that must be
// replayed through the catalog/B-tree layer rather than applied directly to
// raw buffer-pool pages.
type WALReplayOp struct {
	TxnID uint64
	Type  WALRecordType
	Data  []byte
}

type walRecoveryBufferTracker struct {
	records int
	bytes   int64
}

func (t *walRecoveryBufferTracker) add(record *WALRecord) error {
	if record == nil {
		return ErrInvalidWALRecord
	}
	if t.records >= walMaxRecoveryPendingRecords {
		return fmt.Errorf("%w: pending WAL recovery record count exceeds maximum %d", ErrWALCorrupted, walMaxRecoveryPendingRecords)
	}
	recordBytes := int64(len(record.Data))
	if recordBytes > walMaxRecoveryPendingBytes-t.bytes {
		return fmt.Errorf("%w: pending WAL recovery data exceeds maximum %d bytes", ErrWALCorrupted, walMaxRecoveryPendingBytes)
	}
	t.records++
	t.bytes += recordBytes
	return nil
}

func (t *walRecoveryBufferTracker) remove(records []*WALRecord) {
	for _, record := range records {
		if record == nil {
			continue
		}
		if t.records > 0 {
			t.records--
		}
		t.bytes -= int64(len(record.Data))
		if t.bytes < 0 {
			t.bytes = 0
		}
	}
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

	// Recovered logical operations (PageID/Offset == 0) that the engine must
	// replay through the catalog after catalog initialization.
	replayOps []WALReplayOp

	// Reusable buffer for appendInternal to avoid per-record stack-array
	// escapes caused by bufio.Writer.Write. Protected by mu.
	appendBuf [walHeaderSize + 4]byte

	// Group commit fields
	groupCommitEnabled bool
	groupCommitMu      sync.Mutex
	pendingSyncs       []chan error // each caller receives the flush/sync error (nil on success)
	batchSize          int
	syncInterval       time.Duration
	stopGC             chan struct{}
}

var walOpenFile = os.OpenFile

func writeWALFull(writer io.Writer, data []byte) error {
	n, err := writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("%w: wrote %d of %d WAL bytes", io.ErrShortWrite, n, len(data))
	}
	return nil
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
	return encryptDataWithCipher(w.cipher, plaintext, headerAAD)
}

func encryptDataWithCipher(c cipher.AEAD, plaintext []byte, headerAAD []byte) ([]byte, error) {
	if c == nil || len(plaintext) == 0 {
		return plaintext, nil
	}
	nonce := make([]byte, c.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("WAL encrypt: failed to generate nonce: %w", err)
	}
	// Use header as Authenticated Associated Data - protects header integrity
	return c.Seal(nonce, nonce, plaintext, headerAAD), nil
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
	cleanPath := filepath.Clean(path)
	if err := rejectStoragePathSymlinkComponents(filepath.Dir(cleanPath), "WAL directory"); err != nil {
		return nil, err
	}
	info, statErr := os.Lstat(cleanPath)
	preexisting := statErr == nil
	if statErr != nil && !os.IsNotExist(statErr) {
		return nil, fmt.Errorf("failed to stat WAL file: %w", statErr)
	}
	if preexisting {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("WAL file must not be a symlink: %s", cleanPath)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("WAL file must be a regular file: %s", cleanPath)
		}
	}

	flags := os.O_RDWR
	if !preexisting {
		flags |= os.O_CREATE | os.O_EXCL
	}

	// #nosec G304 -- Path is provided by trusted application configuration and validated before use.
	file, err := walOpenFile(cleanPath, flags, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		if !preexisting {
			_ = os.Remove(cleanPath)
		}
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}
	if !openedInfo.Mode().IsRegular() {
		_ = file.Close()
		if !preexisting {
			_ = os.Remove(cleanPath)
		}
		return nil, fmt.Errorf("WAL file must be a regular file: %s", cleanPath)
	}
	if preexisting && !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("WAL file changed while opening: %s", cleanPath)
	}
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		if !preexisting {
			_ = os.Remove(cleanPath)
		}
		return nil, fmt.Errorf("failed to set WAL file permissions: %w", err)
	}
	if !preexisting {
		if err := syncDir(filepath.Dir(cleanPath)); err != nil {
			_ = file.Close()
			_ = os.Remove(cleanPath)
			return nil, fmt.Errorf("failed to sync WAL directory: %w", err)
		}
	}

	// 64 KiB write buffer cuts write() syscalls by ~16x vs the default 4 KiB,
	// since each small WAL record (~60-130 B) would otherwise trigger a flush.
	wal := &WAL{
		file:      file,
		bufWriter: bufio.NewWriterSize(file, 1024*1024),
		path:      cleanPath,
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
	var offset int64
	var headerBuf [walHeaderSize]byte // reusable header buffer across readRecord calls

	for {
		record, recordSize, err := w.readRecord(reader, headerBuf[:])
		if err != nil {
			if errors.Is(err, ErrWALCorrupted) {
				return fmt.Errorf("WAL recovery failed at LSN %d: %w", lastLSN, ErrWALCorrupted)
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if stat.Size() > offset {
					if err := w.file.Truncate(offset); err != nil {
						return fmt.Errorf("failed to truncate partial WAL tail: %w", err)
					}
					if err := w.file.Sync(); err != nil {
						return fmt.Errorf("failed to sync truncated WAL tail: %w", err)
					}
				}
				break
			}
			return fmt.Errorf("WAL recovery failed at offset %d: %w", offset, err)
		}
		offset += recordSize
		if !isKnownWALRecordType(record.Type) {
			return fmt.Errorf("%w: unknown WAL record type 0x%02x at offset %d", ErrInvalidWALRecord, uint8(record.Type), offset-recordSize)
		}
		if record.LSN == 0 {
			return fmt.Errorf("%w: zero WAL LSN at offset %d", ErrWALCorrupted, offset-recordSize)
		}
		if lastLSN != 0 && record.LSN != lastLSN+1 {
			return fmt.Errorf("%w: non-monotonic WAL LSN %d after %d at offset %d", ErrWALCorrupted, record.LSN, lastLSN, offset-recordSize)
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
// header must be a walHeaderSize slice that is reused across calls to avoid per-record allocation.
func (w *WAL) readRecord(reader *bufio.Reader, header []byte) (*WALRecord, int64, error) {
	// Read header: [LSN:8][TxnID:8][Type:1][PageID:4][Offset:2][Length:2]
	if _, err := io.ReadFull(reader, header[:walHeaderSize]); err != nil {
		return nil, 0, err
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
			return nil, 0, err
		}
	}
	recordSize := int64(walHeaderSize + int(dataLen) + 4)

	// Read and verify CRC (direct read avoids binary.Read reflection)
	var crcBuf [4]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		return nil, 0, err
	}
	storedCRC := binary.LittleEndian.Uint32(crcBuf[:])

	// Calculate CRC from header bytes + data directly (avoids re-encode allocation)
	crcHash := crc32.NewIEEE()
	if _, err := crcHash.Write(header[:walHeaderSize]); err != nil {
		return nil, 0, err
	}
	if len(record.Data) > 0 {
		if _, err := crcHash.Write(record.Data); err != nil {
			return nil, 0, err
		}
	}
	calculatedCRC := crcHash.Sum32()

	if storedCRC != calculatedCRC {
		return nil, 0, ErrWALCorrupted
	}

	// Decrypt record data if cipher is configured
	if w.cipher != nil && len(record.Data) > 0 {
		// Use header bytes as AAD, with the LSN zeroed to match the encrypt side
		// (the on-disk LSN is patched after encryption in the group-commit path).
		var aad [walHeaderSize]byte
		copy(aad[:], header[:walHeaderSize])
		zeroWALHeaderLSN(aad[:])
		decrypted, err := w.decryptData(record.Data, aad[:])
		if err != nil {
			return nil, 0, fmt.Errorf("WAL record decryption failed at LSN %d: %w", record.LSN, err)
		}
		record.Data = decrypted
	}

	return record, recordSize, nil
}

// Append adds a record to the WAL.
// When group commit is enabled, the record is written without an immediate sync
// and the caller blocks until the next batch sync.
func (w *WAL) Append(record *WALRecord) error {
	if err := validateRecordSize(record); err != nil {
		return err
	}
	w.groupCommitMu.Lock()
	groupCommitEnabled := w.groupCommitEnabled
	w.groupCommitMu.Unlock()
	if groupCommitEnabled {
		return w.groupCommitAppend(record)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendInternal(record, true)
}

// AppendWithoutSync adds a record without syncing (for group commit)
func (w *WAL) AppendWithoutSync(record *WALRecord) error {
	if err := validateRecordSize(record); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.appendInternal(record, false)
}

// AppendBatchWithoutSync appends multiple records under a single lock
// acquisition without syncing. This dramatically reduces mutex contention
// when a transaction produces many WAL records.
func (w *WAL) AppendBatchWithoutSync(records []*WALRecord) error {
	for _, r := range records {
		if err := validateRecordSize(r); err != nil {
			return err
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, r := range records {
		if err := validateEncryptedRecordSize(r, w.cipher); err != nil {
			return err
		}
	}
	for _, r := range records {
		if err := w.appendInternal(r, false); err != nil {
			return err
		}
	}
	return nil
}

// AppendBatch appends multiple records.  It pre-formats the batch into a
// single byte slice outside the lock so the critical section is reduced to
// one bufio.Writer.Write call plus an LSN bump.  This cuts lock hold time
// by ~2-3x for the common two-record transaction compared with calling
// appendInternal repeatedly inside the lock.
func (w *WAL) AppendBatch(records []*WALRecord) error {
	for _, r := range records {
		if err := validateRecordSize(r); err != nil {
			return err
		}
	}
	w.mu.Lock()
	cipherSnapshot := w.cipher
	w.mu.Unlock()

	// Fast path: ≤2 records, no encryption, total size fits in pooled buffer.
	// Uses a sync.Pool buffer to avoid per-call heap escape from local arrays
	// passed to bufio.Writer.Write (which would allocate 2 KB on every call).
	if cipherSnapshot == nil && len(records) <= 2 && batchRecordsFitPooledBuffer(records) {
		bp := walBatchBufPool.Get().(*[]byte)
		batchBuf := *bp
		var lsnOffs [2]int
		var dataLens [2]int
		totalSize := 0
		for i, r := range records {
			dataLen := len(r.Data)
			lsnOffs[i] = totalSize
			dataLens[i] = dataLen
			if err := writeRecordHeader(batchBuf[totalSize:], &WALRecord{
				TxnID:  r.TxnID,
				Type:   r.Type,
				PageID: r.PageID,
				Offset: r.Offset,
			}, dataLen); err != nil {
				walBatchBufPool.Put(bp)
				return err
			}
			copy(batchBuf[totalSize+walHeaderSize:], r.Data)
			crcHash := crc32.ChecksumIEEE(batchBuf[totalSize : totalSize+walHeaderSize])
			if dataLen > 0 {
				crcHash = crc32.Update(crcHash, crc32.IEEETable, r.Data)
			}
			binary.LittleEndian.PutUint32(batchBuf[totalSize+walHeaderSize+dataLen:], crcHash)
			totalSize += walHeaderSize + dataLen + 4
		}
		if totalSize <= len(batchBuf) {
			w.mu.Lock()
			if w.file == nil {
				w.mu.Unlock()
				walBatchBufPool.Put(bp)
				return ErrWALClosed
			}
			lsn := w.lsn
			for i := range records {
				lsn++
				binary.LittleEndian.PutUint64(batchBuf[lsnOffs[i]:], lsn)
				crcOff := lsnOffs[i] + walHeaderSize + dataLens[i]
				crcHash := crc32.ChecksumIEEE(batchBuf[lsnOffs[i] : lsnOffs[i]+walHeaderSize])
				if dataLens[i] > 0 {
					crcHash = crc32.Update(crcHash, crc32.IEEETable, batchBuf[lsnOffs[i]+walHeaderSize:crcOff])
				}
				binary.LittleEndian.PutUint32(batchBuf[crcOff:], crcHash)
			}
			if err := writeWALFull(w.bufWriter, batchBuf[:totalSize]); err != nil {
				w.mu.Unlock()
				walBatchBufPool.Put(bp)
				return err
			}
			w.lsn = lsn
			w.mu.Unlock()
			walBatchBufPool.Put(bp)
			if !w.groupCommitEnabled {
				return w.Sync()
			}
			return nil
		}
		walBatchBufPool.Put(bp)
	}

	formatted, lsnOffsets, err := w.formatBatch(records, cipherSnapshot)
	if err != nil {
		return err
	}

	w.mu.Lock()
	if w.file == nil {
		w.mu.Unlock()
		return ErrWALClosed
	}
	lsn := w.lsn
	for _, off := range lsnOffsets {
		lsn++
		binary.LittleEndian.PutUint64(formatted[off:], lsn)
	}
	for off := 0; off < len(formatted); {
		dataLen := int(binary.LittleEndian.Uint16(formatted[off+23 : off+25]))
		crcOff := off + walHeaderSize + dataLen
		crcHash := crc32.ChecksumIEEE(formatted[off : off+walHeaderSize])
		if dataLen > 0 {
			crcHash = crc32.Update(crcHash, crc32.IEEETable, formatted[off+walHeaderSize:crcOff])
		}
		binary.LittleEndian.PutUint32(formatted[crcOff:], crcHash)
		off = crcOff + 4
	}
	if err := writeWALFull(w.bufWriter, formatted); err != nil {
		w.mu.Unlock()
		return err
	}
	w.lsn = lsn
	w.mu.Unlock()
	if !w.groupCommitEnabled {
		return w.Sync()
	}
	return nil
}

// formatBatch serialises records into a contiguous byte slice.  LSN fields
// are left as zeroes; the caller must patch them before writing.  The second
// return value contains the offsets of each LSN field so they can be patched
// efficiently.
func (w *WAL) formatBatch(records []*WALRecord, c cipher.AEAD) ([]byte, []int, error) {
	totalSize := 0
	encrypted := make([][]byte, len(records))
	for i, r := range records {
		data := r.Data
		if c != nil && len(data) > 0 {
			// The AAD header must match the header stored on disk and used as the
			// AAD on read: use the on-disk (ciphertext) data length, and zero the
			// LSN — in the group-commit path the on-disk LSN is written as 0 and
			// patched under lock *after* encryption, so the LSN cannot be part of
			// the authenticated header. CRC still covers the LSN's integrity.
			var headerAAD [walHeaderSize]byte
			cipherLen, err := encryptedRecordDataLen(len(data), c)
			if err != nil {
				return nil, nil, err
			}
			if err := writeRecordHeader(headerAAD[:], r, cipherLen); err != nil {
				return nil, nil, err
			}
			zeroWALHeaderLSN(headerAAD[:])
			enc, err := encryptDataWithCipher(c, data, headerAAD[:])
			if err != nil {
				return nil, nil, err
			}
			data = enc
			encrypted[i] = data
		}
		totalSize += walHeaderSize + len(data) + 4
	}

	buf := make([]byte, totalSize)
	offset := 0
	lsnOffsets := make([]int, 0, len(records))

	for i, r := range records {
		data := r.Data
		if encrypted[i] != nil {
			data = encrypted[i]
		}

		lsnOffsets = append(lsnOffsets, offset)
		// Header with LSN=0 – patched under lock.
		if err := writeRecordHeader(buf[offset:], &WALRecord{
			LSN:    0,
			TxnID:  r.TxnID,
			Type:   r.Type,
			PageID: r.PageID,
			Offset: r.Offset,
		}, len(data)); err != nil {
			return nil, nil, err
		}
		copy(buf[offset+walHeaderSize:], data)

		crcHash := crc32.ChecksumIEEE(buf[offset : offset+walHeaderSize])
		if len(data) > 0 {
			crcHash = crc32.Update(crcHash, crc32.IEEETable, data)
		}
		binary.LittleEndian.PutUint32(buf[offset+walHeaderSize+len(data):], crcHash)

		offset += walHeaderSize + len(data) + 4
	}

	return buf, lsnOffsets, nil
}

func validateRecordSize(record *WALRecord) error {
	if record == nil {
		return ErrInvalidWALRecord
	}
	if !isKnownWALRecordType(record.Type) {
		return fmt.Errorf("%w: unknown WAL record type 0x%02x", ErrInvalidWALRecord, uint8(record.Type))
	}
	if len(record.Data) > walMaxRecordDataSize {
		return fmt.Errorf("WAL record data size (%d bytes) exceeds maximum (%d bytes)",
			len(record.Data), walMaxRecordDataSize)
	}
	return nil
}

func encryptedRecordDataLen(dataLen int, c cipher.AEAD) (int, error) {
	if dataLen < 0 {
		return 0, fmt.Errorf("WAL record data size (%d bytes) is invalid", dataLen)
	}
	if dataLen > walMaxRecordDataSize {
		return 0, fmt.Errorf("WAL record data size (%d bytes) exceeds maximum (%d bytes)",
			dataLen, walMaxRecordDataSize)
	}
	if c == nil || dataLen == 0 {
		return dataLen, nil
	}
	overhead := c.NonceSize() + c.Overhead()
	if dataLen > walMaxRecordDataSize-overhead {
		return 0, fmt.Errorf("encrypted WAL record data size (%d bytes) exceeds maximum (%d bytes)",
			dataLen+overhead, walMaxRecordDataSize)
	}
	return dataLen + overhead, nil
}

func validateEncryptedRecordSize(record *WALRecord, c cipher.AEAD) error {
	if err := validateRecordSize(record); err != nil {
		return err
	}
	_, err := encryptedRecordDataLen(len(record.Data), c)
	return err
}

func isKnownWALRecordType(recordType WALRecordType) bool {
	switch recordType {
	case WALInsert, WALUpdate, WALDelete, WALCommit, WALRollback, WALCheckpoint, WALUpdateCommit:
		return true
	default:
		return false
	}
}

func batchRecordsFitPooledBuffer(records []*WALRecord) bool {
	totalSize := 0
	for _, r := range records {
		totalSize += walHeaderSize + len(r.Data) + 4
	}
	return totalSize <= walBatchBufferSize
}

// appendInternal is the internal append implementation
func (w *WAL) appendInternal(record *WALRecord, sync bool) error {
	if w.file == nil {
		return ErrWALClosed
	}
	if err := validateEncryptedRecordSize(record, w.cipher); err != nil {
		return err
	}

	originalLSN := record.LSN
	originalData := record.Data
	assignedLSN := false
	defer func() {
		record.Data = originalData
		if !assignedLSN {
			record.LSN = originalLSN
		}
	}()

	newLSN := w.lsn + 1
	record.LSN = newLSN

	// Encrypt record data if cipher is configured
	if w.cipher != nil && len(record.Data) > 0 {
		// Match the read-side AAD: on-disk (ciphertext) data length and a zeroed
		// LSN (see formatBatch and decode for why the LSN is excluded).
		var headerAAD [walHeaderSize]byte
		cipherLen, err := encryptedRecordDataLen(len(record.Data), w.cipher)
		if err != nil {
			return err
		}
		if err := writeRecordHeader(headerAAD[:], record, cipherLen); err != nil {
			return err
		}
		zeroWALHeaderLSN(headerAAD[:])

		encrypted, err := w.encryptData(record.Data, headerAAD[:])
		if err != nil {
			return err
		}
		record.Data = encrypted
	}

	// Write record header and data directly to bufWriter, avoiding the
	// intermediate allocation from encodeRecord. CRC is computed incrementally.
	// Use the reusable appendBuf so the arrays don't escape to heap.
	dataLen := len(record.Data)
	buf := w.appendBuf[:]
	if err := writeRecordHeader(buf[:walHeaderSize], record, dataLen); err != nil {
		return err
	}
	crcHash := crc32.ChecksumIEEE(buf[:walHeaderSize])
	if err := writeWALFull(w.bufWriter, buf[:walHeaderSize]); err != nil {
		return err
	}

	if dataLen > 0 {
		crcHash = crc32.Update(crcHash, crc32.IEEETable, record.Data)
		if err := writeWALFull(w.bufWriter, record.Data); err != nil {
			return err
		}
	}

	// Write CRC (direct encoding avoids binary.Write reflection)
	binary.LittleEndian.PutUint32(buf[walHeaderSize:walHeaderSize+4], crcHash)
	if err := writeWALFull(w.bufWriter, buf[walHeaderSize:walHeaderSize+4]); err != nil {
		return err
	}

	// Only update LSN after successful write
	w.lsn = newLSN
	assignedLSN = true

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
	pending := w.popPendingSyncs()

	w.mu.Lock()
	var syncErr error
	if w.file == nil {
		syncErr = ErrWALClosed
	} else if err := w.bufWriter.Flush(); err != nil {
		syncErr = err
	} else if err := w.file.Sync(); err != nil {
		syncErr = err
	}
	w.mu.Unlock()

	signalPendingSyncs(pending, syncErr)
	return syncErr
}

// EnableGroupCommit turns on group commit batching. Append calls block until
// the next periodic or batch-size-triggered sync. AppendWithoutSync is
// unaffected. Sync() forces an immediate sync of pending records.
//
// interval is the maximum time a caller waits before being synced.
// If interval <= 0, callers never wait for a background sync; they rely on
// explicit Sync() or Close().
//
// batchSize is the number of pending Append calls that trigger an immediate
// sync. If batchSize <= 0, only the ticker (or explicit Sync/close) triggers
// a sync.
func (w *WAL) EnableGroupCommit(batchSize int, interval time.Duration) {
	w.groupCommitMu.Lock()
	defer w.groupCommitMu.Unlock()
	if w.stopGC != nil {
		close(w.stopGC)
		w.stopGC = nil
	}
	w.groupCommitEnabled = true
	w.batchSize = batchSize
	w.syncInterval = interval
	if interval > 0 {
		stop := make(chan struct{})
		w.stopGC = stop
		go w.groupCommitLoop(interval, stop)
	}
}

// DisableGroupCommit turns off group commit and flushes any pending records.
func (w *WAL) DisableGroupCommit() {
	w.groupCommitMu.Lock()
	defer w.groupCommitMu.Unlock()
	if !w.groupCommitEnabled {
		return
	}
	w.groupCommitEnabled = false
	// flushPendingLocked acquires groupCommitMu itself, so release first.
	w.groupCommitMu.Unlock()
	_ = w.flushPendingLocked()
	w.groupCommitMu.Lock()
	if w.stopGC != nil {
		close(w.stopGC)
		w.stopGC = nil
	}
}

// groupCommitAppend writes the record without sync and blocks until the next
// batch sync. Must NOT be called while holding w.mu.
func (w *WAL) groupCommitAppend(record *WALRecord) error {
	w.groupCommitMu.Lock()
	if !w.groupCommitEnabled {
		w.groupCommitMu.Unlock()
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.appendInternal(record, true)
	}

	// Write record without syncing
	w.mu.Lock()
	err := w.appendInternal(record, false)
	w.mu.Unlock()
	if err != nil {
		w.groupCommitMu.Unlock()
		return err
	}

	// SyncOff mode: don't wait for background sync
	if w.syncInterval <= 0 && w.batchSize <= 0 {
		w.groupCommitMu.Unlock()
		return nil
	}

	done := make(chan error, 1)
	w.pendingSyncs = append(w.pendingSyncs, done)

	// Trigger immediate sync if batch is full
	if w.batchSize > 0 && len(w.pendingSyncs) >= w.batchSize {
		w.groupCommitMu.Unlock()
		_ = w.flushPendingLocked()
		if err := <-done; err != nil {
			return fmt.Errorf("group commit failed: %w", err)
		}
		return nil
	}

	w.groupCommitMu.Unlock()

	// Wait for sync result
	if err := <-done; err != nil {
		return fmt.Errorf("group commit failed: %w", err)
	}
	return nil
}

// flushPendingLocked syncs the WAL and signals all pending callers.
// It acquires w.groupCommitMu internally so callers need not hold it
// across the long fsync critical section.
func (w *WAL) flushPendingLocked() error {
	pending := w.popPendingSyncs()

	var flushErr error
	var file *os.File
	w.mu.Lock()
	if w.file != nil {
		if err := w.bufWriter.Flush(); err != nil {
			flushErr = fmt.Errorf("WAL flush: %w", err)
		} else {
			file = w.file
		}
	} else {
		flushErr = ErrWALClosed
	}
	w.mu.Unlock()

	if file != nil && flushErr == nil {
		if err := file.Sync(); err != nil {
			flushErr = fmt.Errorf("WAL sync: %w", err)
		}
	}

	signalPendingSyncs(pending, flushErr)
	return flushErr
}

func (w *WAL) popPendingSyncs() []chan error {
	w.groupCommitMu.Lock()
	pending := append([]chan error(nil), w.pendingSyncs...)
	clear(w.pendingSyncs)
	w.pendingSyncs = w.pendingSyncs[:0]
	w.groupCommitMu.Unlock()
	return pending
}

func signalPendingSyncs(pending []chan error, syncErr error) {
	for _, done := range pending {
		done <- syncErr
	}
}

// groupCommitLoop runs a ticker that periodically syncs pending records.
func (w *WAL) groupCommitLoop(interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = w.flushPendingLocked()
		}
	}
}

// writeRecordHeader serializes the fixed-size WAL record header into dst[:walHeaderSize].
// dataLen carries the length of the (possibly encrypted) payload that will follow the header.
// zeroWALHeaderLSN clears the 8-byte LSN field at the start of a WAL record
// header so it can be used as encryption AAD consistently on both the write and
// read sides (the LSN is patched on disk after encryption in the group-commit
// path, so it must not be authenticated by GCM; CRC still covers it).
func zeroWALHeaderLSN(header []byte) {
	if len(header) >= 8 {
		binary.LittleEndian.PutUint64(header[0:8], 0)
	}
}

func writeRecordHeader(dst []byte, record *WALRecord, dataLen int) error {
	dataLen16, err := checkedUint16(dataLen, "WAL record data length")
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint64(dst[0:8], record.LSN)
	binary.LittleEndian.PutUint64(dst[8:16], record.TxnID)
	dst[16] = byte(record.Type)
	binary.LittleEndian.PutUint32(dst[17:21], record.PageID)
	binary.LittleEndian.PutUint16(dst[21:23], record.Offset)
	binary.LittleEndian.PutUint16(dst[23:25], dataLen16)
	return nil
}

// encodeRecord encodes a WAL record to bytes (without CRC)
// Format: [LSN:8][TxnID:8][Type:1][PageID:4][Offset:2][Length:2][Data:N]
func (w *WAL) encodeRecord(record *WALRecord) ([]byte, error) {
	dataLen := len(record.Data)
	buf := make([]byte, walHeaderSize+dataLen)
	if err := writeRecordHeader(buf, record, dataLen); err != nil {
		return nil, err
	}
	copy(buf[walHeaderSize:], record.Data)
	return buf, nil
}

// Checkpoint flushes dirty pages to main DB file and truncates WAL
func (w *WAL) Checkpoint(bp *BufferPool) error {
	if err := w.flushPendingLocked(); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrWALClosed
	}

	// 1. Flush dirty pages from buffer pool (incremental — skips clean pages
	// and does not hold bp.mu during I/O, reducing contention).
	if err := bp.FlushDirty(); err != nil {
		return fmt.Errorf("failed to flush dirty pages: %w", err)
	}

	// 2. Truncate old WAL contents before writing a durable checkpoint marker.
	// Keeping the marker preserves LSN continuity across restart while still
	// discarding records that are already represented in the main DB file.
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.bufWriter = bufio.NewWriter(w.file)

	// 3. Write checkpoint record
	checkpointRecord := &WALRecord{
		TxnID: 0,
		Type:  WALCheckpoint,
	}
	newLSN := w.lsn + 1
	checkpointRecord.LSN = newLSN

	buf, err := w.encodeRecord(checkpointRecord)
	if err != nil {
		return err
	}
	crc := crc32.ChecksumIEEE(buf)

	if err := writeWALFull(w.bufWriter, buf); err != nil {
		return err
	}
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if err := writeWALFull(w.bufWriter, crcBuf[:]); err != nil {
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

	return nil
}

// Recover replays WAL records after a crash.  Physical records (PageID > 0)
// are applied directly to the buffer pool; logical records (PageID == 0) are
// buffered in w.replayOps for catalog-level replay after catalog init.
func (w *WAL) Recover(bp *BufferPool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrWALClosed
	}
	if bp == nil {
		return ErrInvalidWALRecoveryTarget
	}

	// Seek to beginning
	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}
	w.replayOps = nil

	reader := bufio.NewReader(w.file)
	var committedTxns = make(map[uint64]bool)
	var pendingTxns = make(map[uint64][]*WALRecord)
	var pendingTracker walRecoveryBufferTracker
	var headerBuf [walHeaderSize]byte // reusable header buffer across readRecord calls

	// Read all records
	var lastCheckpointLSN uint64
	for {
		record, _, err := w.readRecord(reader, headerBuf[:])
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			if errors.Is(err, ErrWALCorrupted) {
				return ErrWALCorrupted
			}
			return fmt.Errorf("failed to read WAL record: %w", err)
		}

		// Track the latest checkpoint
		if record.Type == WALCheckpoint {
			lastCheckpointLSN = record.LSN
			continue
		}

		// Skip records before or at the last checkpoint — already applied
		if lastCheckpointLSN > 0 && record.LSN <= lastCheckpointLSN {
			continue
		}

		switch record.Type {
		case WALCommit:
			committedTxns[record.TxnID] = true
			// Apply pending records for this transaction
			if records, ok := pendingTxns[record.TxnID]; ok {
				for _, r := range records {
					if err := w.recoverRecord(bp, r); err != nil {
						return err
					}
				}
				pendingTracker.remove(records)
				delete(pendingTxns, record.TxnID)
			}

		case WALRollback:
			pendingTracker.remove(pendingTxns[record.TxnID])
			delete(pendingTxns, record.TxnID)

		case WALInsert, WALUpdate, WALDelete:
			if committedTxns[record.TxnID] {
				// Transaction already committed, apply immediately
				if err := w.recoverRecord(bp, record); err != nil {
					return err
				}
			} else {
				// Buffer for later
				if err := pendingTracker.add(record); err != nil {
					return err
				}
				pendingTxns[record.TxnID] = append(pendingTxns[record.TxnID], record)
			}

		case WALUpdateCommit:
			committedTxns[record.TxnID] = true
			// Apply pending records for this transaction
			if records, ok := pendingTxns[record.TxnID]; ok {
				for _, r := range records {
					if err := w.recoverRecord(bp, r); err != nil {
						return err
					}
				}
				pendingTracker.remove(records)
				delete(pendingTxns, record.TxnID)
			}
			// Apply the combined update+commit record immediately
			if err := w.recoverRecord(bp, record); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: unknown WAL record type 0x%02x at LSN %d", ErrInvalidWALRecord, uint8(record.Type), record.LSN)
		}
	}

	// Flush recovered pages
	return bp.FlushAll()
}

// recoverRecord dispatches a WAL record to either page-level apply or logical
// replay buffering.
func (w *WAL) recoverRecord(bp *BufferPool, record *WALRecord) error {
	if record.PageID == 0 && record.Offset == 0 {
		// Logical record — buffer for catalog-level replay.
		w.replayOps = append(w.replayOps, WALReplayOp{
			TxnID: record.TxnID,
			Type:  record.Type,
			Data:  append([]byte(nil), record.Data...),
		})
		return nil
	}
	return w.applyRecord(bp, record)
}

// GetReplayOps returns logical WAL operations recovered since the last
// checkpoint.  The engine/catalog must replay these after catalog init.
func (w *WAL) GetReplayOps() []WALReplayOp {
	w.mu.Lock()
	defer w.mu.Unlock()
	ops := make([]WALReplayOp, len(w.replayOps))
	for i, op := range w.replayOps {
		ops[i] = WALReplayOp{
			TxnID: op.TxnID,
			Type:  op.Type,
			Data:  append([]byte(nil), op.Data...),
		}
	}
	return ops
}

// applyRecord applies a physical WAL record to buffer-pool pages.
func (w *WAL) applyRecord(bp *BufferPool, record *WALRecord) error {
	if bp == nil {
		return ErrInvalidWALRecoveryTarget
	}
	if record == nil {
		return ErrInvalidWALRecord
	}
	if record.PageID == 0 && record.Offset == 0 {
		// Logical record — should have been handled by recoverRecord.
		return nil
	}
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
	// Stop group commit and flush pending records first
	w.DisableGroupCommit()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	var errs []error
	if err := w.bufWriter.Flush(); err != nil {
		errs = append(errs, err)
	} else if err := w.file.Sync(); err != nil {
		errs = append(errs, err)
	}

	if err := w.file.Close(); err != nil {
		errs = append(errs, err)
	}
	w.file = nil
	return errors.Join(errs...)
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
