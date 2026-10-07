package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type partialSizeDiskFile struct {
	*os.File
	limit int
	err   error
}

func (f *partialSizeDiskFile) WriteAt(data []byte, offset int64) (int, error) {
	if f.limit >= 0 && len(data) > f.limit {
		data = data[:f.limit]
	}
	n, err := f.File.WriteAt(data, offset)
	if err != nil {
		return n, err
	}
	return n, f.err
}

func TestDiskBackendSizeTracksPartialWrites(t *testing.T) {
	failure := errors.New("injected failure")
	for _, tc := range []struct {
		name            string
		initial, offset int64
		limit           int
		data            string
		err, wantErr    error
	}{
		{"control", 0, 10, -1, "data", nil, nil},
		{"partial_error", 0, 10, 2, "data", failure, failure},
		{"short_write", 0, 10, 2, "data", nil, io.ErrShortWrite},
		{"overwrite", 20, 1, 2, "data", failure, failure},
		{"zero_progress", 0, 10, 0, "data", failure, failure},
		{"empty", 0, 0, -1, "", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.Create(filepath.Join(t.TempDir(), "data"))
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := f.Truncate(tc.initial); err != nil {
				t.Fatal(err)
			}
			writer := &partialSizeDiskFile{File: f, limit: tc.limit, err: tc.err}
			backend := &DiskBackend{file: writer, fileSize: tc.initial}
			_, err = backend.WriteAt([]byte(tc.data), tc.offset)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error=%v want=%v", err, tc.wantErr)
			}
			info, err := f.Stat()
			if err != nil {
				t.Fatal(err)
			}
			if backend.Size() != info.Size() {
				t.Fatal("size diverged")
			}
			writer.limit, writer.err = -1, nil
			if _, err := backend.WriteAt([]byte("retry"), 30); err != nil {
				t.Fatal(err)
			}
			if backend.Size() != 35 {
				t.Fatalf("retry size=%d", backend.Size())
			}
		})
	}
}
