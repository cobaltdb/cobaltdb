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
	if reusedWrite == nil || (*reusedWrite)[0] != 42 {
		t.Fatal("expected write buffer to be reused from pool")
	}

	readBuf := cb.getReadBuf()
	if readBuf == nil || len(*readBuf) != PageSize {
		t.Fatalf("getReadBuf fallback length = %d, want %d", len(*readBuf), PageSize)
	}
	(*readBuf)[0] = 99
	cb.putReadBuf(readBuf)
	reusedRead := cb.getReadBuf()
	if reusedRead == nil || (*reusedRead)[0] != 99 {
		t.Fatal("expected read buffer to be reused from pool")
	}
}
