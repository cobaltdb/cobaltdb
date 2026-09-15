package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// The WAL header LSN is the one field excluded from AEAD authentication (it is
// patched after encryption in the group-commit path) and is covered only by a
// forgeable CRC32. Tamper-evidence therefore rests on readLSN's strict +1 LSN
// continuity check at open time: any LSN rewrite that would let recovery
// silently drop records (checkpoint-cutoff inflation, swaps, duplicates)
// breaks the chain and must fail with ErrWALCorrupted. These tests pin that
// guarantee — a future change to the record format or the open-time scan must
// not silently drop it.

// writeTamperedWAL builds a three-record encrypted WAL (checkpoint, logical
// update, commit), closes it, and returns its raw bytes.
func writeTamperedWAL(t *testing.T, dir string) []byte {
	t.Helper()
	walPath := filepath.Join(dir, "tamper.wal")
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	wal.SetEncryptionCipher(makeTestCipher(t))
	if err := wal.Append(&WALRecord{Type: WALCheckpoint}); err != nil {
		t.Fatalf("append checkpoint: %v", err)
	}
	if err := wal.Append(&WALRecord{Type: WALUpdate, TxnID: 1, Data: []byte("update-users-set-x")}); err != nil {
		t.Fatalf("append update: %v", err)
	}
	if err := wal.Append(&WALRecord{Type: WALCommit, TxnID: 1}); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	if err := wal.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	return raw
}

// recordCRCOffset returns the byte offset of the CRC field of the record that
// starts at start in raw, and the record's total size.
func recordCRCOffset(raw []byte, start int) (crcOff, size int, err error) {
	if start+walHeaderSize > len(raw) {
		return 0, 0, errors.New("record start out of range")
	}
	dataLen := int(binary.LittleEndian.Uint16(raw[start+23 : start+25]))
	crcOff = start + walHeaderSize + dataLen
	return crcOff, crcOff + 4 - start, nil
}

// patchLSN rewrites the LSN field of the record at start and fixes its CRC32,
// simulating an attacker who can rewrite the file but cannot break AEAD.
func patchLSN(raw []byte, start int, lsn uint64) {
	binary.LittleEndian.PutUint64(raw[start:start+8], lsn)
	crcOff, _, err := recordCRCOffset(raw, start)
	if err != nil {
		return
	}
	binary.LittleEndian.PutUint32(raw[crcOff:crcOff+4], crc32.ChecksumIEEE(raw[start:crcOff]))
}

// TestOpenWALRejectsCheckpointLSNInflation verifies that inflating the
// checkpoint record's LSN (which would make recovery silently drop every
// subsequent record via the checkpoint cutoff) is rejected at open.
func TestOpenWALRejectsCheckpointLSNInflation(t *testing.T) {
	raw := writeTamperedWAL(t, t.TempDir())
	patchLSN(raw, 0, 0xFFFFFFFFFFFFFFFF)

	walPath := filepath.Join(t.TempDir(), "inflated.wal")
	if err := os.WriteFile(walPath, raw, 0o600); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	wal, err := OpenWAL(walPath)
	if err == nil {
		wal.Close()
		t.Fatal("OpenWAL accepted an inflated checkpoint LSN; recovery would silently drop all subsequent records")
	}
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("OpenWAL error = %v, want ErrWALCorrupted", err)
	}
}

// TestOpenWALRejectsMidFileLSNSwap verifies that swapping the LSNs of two
// records (breaking the strictly increasing sequence) is rejected at open.
func TestOpenWALRejectsMidFileLSNSwap(t *testing.T) {
	raw := writeTamperedWAL(t, t.TempDir())

	// Records: checkpoint(1) at 0, update(2), commit(3). Swap LSNs of the
	// update and commit records.
	off2, _, err := recordCRCOffset(raw, 0)
	if err != nil {
		t.Fatalf("locate record 2: %v", err)
	}
	off2 += 4
	off3, _, err := recordCRCOffset(raw, off2)
	if err != nil {
		t.Fatalf("locate record 3: %v", err)
	}
	off3 += 4

	lsn2 := binary.LittleEndian.Uint64(raw[off2 : off2+8])
	lsn3 := binary.LittleEndian.Uint64(raw[off3 : off3+8])
	patchLSN(raw, off2, lsn3)
	patchLSN(raw, off3, lsn2)

	walPath := filepath.Join(t.TempDir(), "swapped.wal")
	if err := os.WriteFile(walPath, raw, 0o600); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	wal, err := OpenWAL(walPath)
	if err == nil {
		wal.Close()
		t.Fatal("OpenWAL accepted a mid-file LSN swap; record ordering is not tamper-evident")
	}
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("OpenWAL error = %v, want ErrWALCorrupted", err)
	}
}

// TestOpenWALAcceptsSequentialEncryptedWAL is the false-positive guard: a
// legitimate, untampered encrypted WAL must open and recover cleanly.
func TestOpenWALAcceptsSequentialEncryptedWAL(t *testing.T) {
	dir := t.TempDir()
	raw := writeTamperedWAL(t, dir)

	walPath := filepath.Join(dir, "sequential.wal")
	if err := os.WriteFile(walPath, raw, 0o600); err != nil {
		t.Fatalf("write wal: %v", err)
	}
	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL rejected a legitimate WAL: %v", err)
	}
	defer wal.Close()
	wal.SetEncryptionCipher(makeTestCipher(t))
	bp, err := NewBufferPoolWithError(16, NewMemory())
	if err != nil {
		t.Fatalf("buffer pool: %v", err)
	}
	if err := wal.Recover(bp); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if ops := wal.GetReplayOps(); len(ops) != 1 {
		t.Fatalf("replay ops = %d, want 1", len(ops))
	}
}
