package storage

import "testing"

func TestCompressedBackendBufferPools(t *testing.T) {
	cb, err := NewCompressedBackend(NewMemory(), DefaultCompressionConfig())
	if err != nil {
		t.Fatalf("NewCompressedBackend failed: %v", err)
	}

	writeBuf := cb.getWriteBuf()
	if writeBuf == nil || len(*writeBuf) != PageSize {
		t.Fatalf("getWriteBuf fallback length = %d, want %d", len(*writeBuf), PageSize)
	}
	(*writeBuf)[0] = 42
	cb.putWriteBuf(writeBuf)
	reusedWrite := cb.getWriteBuf()
	if reusedWrite == nil || len(*reusedWrite) != PageSize {
		t.Fatalf("getWriteBuf pooled length = %d, want %d", len(*reusedWrite), PageSize)
	}

	readBuf := cb.getReadBuf()
	if readBuf == nil || len(*readBuf) != PageSize {
		t.Fatalf("getReadBuf fallback length = %d, want %d", len(*readBuf), PageSize)
	}
	(*readBuf)[0] = 99
	cb.putReadBuf(readBuf)
	reusedRead := cb.getReadBuf()
	if reusedRead == nil || len(*reusedRead) != PageSize {
		t.Fatalf("getReadBuf pooled length = %d, want %d", len(*reusedRead), PageSize)
	}
}
