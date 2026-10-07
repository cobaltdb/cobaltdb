package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

type evidenceFullEOFBackend struct {
	Backend
	mode     string
	sentinel error
}

func (b *evidenceFullEOFBackend) ReadAt(p []byte, offset int64) (int, error) {
	n, err := b.Backend.ReadAt(p, offset)
	if err != nil {
		return n, err
	}
	switch b.mode {
	case "full":
		return n, io.EOF
	case "wrapped":
		return n, fmt.Errorf("reader at end: %w", io.EOF)
	case "partial":
		return n - 1, io.EOF
	case "failure":
		return n, b.sentinel
	}
	return n, nil
}
func TestEncryptedBackendAcceptsFullReadEOF(t *testing.T) {
	sentinel := errors.New("injected read failure")
	backend := &evidenceFullEOFBackend{Backend: NewMemory(), sentinel: sentinel}
	eb, err := NewEncryptedBackend(backend, &EncryptionConfig{Enabled: true, Key: []byte("fixed-test-key"), Salt: []byte("fixed-test-salt"), PBKDF2Iters: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer eb.Close()
	want := bytes.Repeat([]byte{7}, PageSize)
	for _, offset := range []int64{0, PageSize} {
		if n, err := eb.WriteAt(want, offset); err != nil || n != len(want) {
			t.Fatal("write", n, err)
		}
	}
	for _, mode := range []string{"normal", "full", "wrapped"} {
		backend.mode = mode
		for _, offset := range []int64{0, PageSize} {
			buf := make([]byte, PageSize)
			n, err := eb.ReadAt(buf, offset)
			fmt.Printf("EXPECTED: full plaintext, nil; ACTUAL: n=%d equal=%v err=%v (%s offset=%d)\n", n, bytes.Equal(buf, want), err, mode, offset)
			if err != nil || n != len(want) || !bytes.Equal(buf, want) {
				t.Fatal("full read", n, err)
			}
		}
	}
	backend.mode = "partial"
	if n, err := eb.ReadAt(make([]byte, PageSize), 0); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatal("partial EOF was swallowed", n, err)
	}
	backend.mode = "failure"
	if n, err := eb.ReadAt(make([]byte, PageSize), 0); n != 0 || !errors.Is(err, sentinel) {
		t.Fatal("non-EOF failure was swallowed", n, err)
	}
	backend.mode = "normal"
	if n, err := eb.ReadAt(make([]byte, PageSize), 2*PageSize); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatal("empty page", n, err)
	}
	if err := eb.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := eb.ReadAt(make([]byte, PageSize), 0); !errors.Is(err, ErrBackendClosed) {
		t.Fatal("closed backend", err)
	}
	fmt.Println("FIX VERIFIED")
}
