package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

type fullReadEOFBackend struct {
	Backend
	err   error
	short bool
}

func (b fullReadEOFBackend) ReadAt(p []byte, off int64) (int, error) {
	n, err := b.Backend.ReadAt(p, off)
	if err != nil {
		return n, err
	}
	if b.short && n > 0 {
		n--
	}
	return n, b.err
}

func TestReadFullAtAcceptsCompleteEOF(t *testing.T) {
	sentinel := errors.New("injected read failure")
	for _, tc := range []struct {
		name      string
		err, want error
		short     bool
	}{
		{"control", nil, nil, false},
		{"complete EOF", io.EOF, nil, false},
		{"wrapped EOF", fmt.Errorf("end: %w", io.EOF), nil, false},
		{"partial EOF", io.EOF, io.EOF, true},
		{"partial nil", nil, io.ErrUnexpectedEOF, true},
		{"complete other error", sentinel, sentinel, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memory := NewMemory()
			defer memory.Close()
			page := NewPage(1, PageTypeLeaf)
			if _, err := memory.WriteAt(page.Data, PageSize); err != nil {
				t.Fatal(err)
			}
			backend := fullReadEOFBackend{Backend: memory, err: tc.err, short: tc.short}
			pool, err := NewBufferPoolWithError(2, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			loaded, err := pool.GetPage(1)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v, want %v", err, tc.want)
			}
			if err == nil {
				defer loaded.Unpin()
				if !bytes.Equal(loaded.Data(), page.Data) {
					t.Fatal("loaded page differs")
				}
			}
		})
	}
}
