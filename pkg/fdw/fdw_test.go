package fdw

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	r.Register("csv", func() ForeignDataWrapper { return &CSVWrapper{} })

	got, ok := r.Get("csv")
	if !ok {
		t.Fatal("expected csv wrapper to be registered")
	}
	if got.Name() != "csv" {
		t.Fatalf("expected name 'csv', got %q", got.Name())
	}

	_, ok = r.Get("missing")
	if ok {
		t.Fatal("expected missing wrapper to not be found")
	}
}

func TestRegistry_GetDoesNotHoldLockDuringFactory(t *testing.T) {
	r := NewRegistry()
	r.Register("recursive", func() ForeignDataWrapper {
		if !r.Has("recursive") {
			t.Fatal("expected recursive wrapper to be visible")
		}
		return &CSVWrapper{}
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		wrapper, ok := r.Get("recursive")
		if !ok {
			t.Error("expected recursive wrapper")
			return
		}
		if wrapper == nil {
			t.Error("expected non-nil wrapper")
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get deadlocked while factory read from registry")
	}
}

func TestRegistry_GetNilFactory(t *testing.T) {
	r := NewRegistry()
	r.Register("nil", nil)

	if wrapper, ok := r.Get("nil"); ok || wrapper != nil {
		t.Fatalf("expected nil factory to be treated as missing, got wrapper=%v ok=%v", wrapper, ok)
	}
}

func TestRegistry_List(t *testing.T) {
	r := NewRegistry()
	r.Register("http", func() ForeignDataWrapper { return &CSVWrapper{} })
	r.Register("csv", func() ForeignDataWrapper { return &CSVWrapper{} })

	names := r.List()
	if want := []string{"csv", "http"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("expected sorted wrappers %v, got %v", want, names)
	}
}

func TestCSVWrapper_Scan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	content := "id,name\n1,alice\n2,bob\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": path}); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer csv.Close()

	rows, err := csv.Scan("test", []string{"id", "name"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] != "1" || rows[0][1] != "alice" {
		t.Fatalf("unexpected row 0: %v", rows[0])
	}
	if rows[1][0] != "2" || rows[1][1] != "bob" {
		t.Fatalf("unexpected row 1: %v", rows[1])
	}
}

func TestCSVWrapper_NoHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	content := "1,2\n3,4\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": path}); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer csv.Close()

	rows, err := csv.Scan("test", []string{"id", "name"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] != "1" || rows[0][1] != "2" {
		t.Fatalf("unexpected row 0: %v", rows[0])
	}
}

func TestCSVWrapper_OpenMissingFileOption(t *testing.T) {
	csv := &CSVWrapper{}
	err := csv.Open(map[string]string{})
	if err == nil {
		t.Fatal("expected error when missing 'file' option")
	}
}

func TestCSVWrapper_ScanEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(path, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": path}); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer csv.Close()

	rows, err := csv.Scan("test", []string{"id", "name"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if rows != nil {
		t.Fatalf("expected nil rows for empty file, got %v", rows)
	}
}

func TestCSVWrapper_CloseNilFile(t *testing.T) {
	csv := &CSVWrapper{}
	if err := csv.Close(); err != nil {
		t.Fatalf("close on nil file should not error: %v", err)
	}
}

func TestCSVWrapper_CloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	if err := os.WriteFile(path, []byte("id\n1\n"), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": path}); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if err := csv.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if err := csv.Close(); err != nil {
		t.Fatalf("second close should not error: %v", err)
	}
	if csv.file != nil {
		t.Fatal("close should clear file handle")
	}
}

func TestCSVWrapper_ReopenClosesPreviousHandle(t *testing.T) {
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "first.csv")
	secondPath := filepath.Join(dir, "second.csv")
	if err := os.WriteFile(firstPath, []byte("id\n1\n"), 0644); err != nil {
		t.Fatalf("failed to write first csv: %v", err)
	}
	if err := os.WriteFile(secondPath, []byte("id\n2\n"), 0644); err != nil {
		t.Fatalf("failed to write second csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": firstPath}); err != nil {
		t.Fatalf("first open failed: %v", err)
	}
	firstHandle := csv.file
	if err := csv.Open(map[string]string{"file": secondPath}); err != nil {
		t.Fatalf("second open failed: %v", err)
	}
	defer csv.Close()

	if _, err := firstHandle.Stat(); err == nil {
		t.Fatal("expected previous file handle to be closed")
	}

	rows, err := csv.Scan("test", []string{"id"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(rows) != 1 || rows[0][0] != "2" {
		t.Fatalf("expected reopened file rows, got %v", rows)
	}
}

func TestCSVWrapper_OpenMissingFile(t *testing.T) {
	csv := &CSVWrapper{}
	err := csv.Open(map[string]string{"file": "/nonexistent/path/file.csv"})
	if err == nil {
		t.Fatal("expected error when file does not exist")
	}
}

func TestCSVWrapper_MaxRowsLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.csv")
	content := "id,name\n1,alice\n2,bob\n3,charlie\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": path, "max_rows": "2"}); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer csv.Close()

	_, err := csv.Scan("test", []string{"id", "name"})
	if err == nil {
		t.Fatal("expected max_rows error")
	}
	if !strings.Contains(err.Error(), "row limit exceeded") {
		t.Fatalf("expected row limit error, got %v", err)
	}
}

func TestCSVWrapper_MaxBytesLimit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.csv")
	if err := os.WriteFile(path, []byte("id,name\n1,alice\n"), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	err := csv.Open(map[string]string{"file": path, "max_bytes": "4"})
	if err == nil {
		t.Fatal("expected max_bytes error")
	}
	if !strings.Contains(err.Error(), "exceeds max_bytes") {
		t.Fatalf("expected max_bytes error, got %v", err)
	}
}

func TestCSVWrapper_MaxBytesLimitAtScan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "growing.csv")
	if err := os.WriteFile(path, []byte("id\n1\n"), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": path, "max_bytes": "8"}); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer csv.Close()

	if err := os.WriteFile(path, []byte("id\n1\n2\n3\n4\n5\n"), 0644); err != nil {
		t.Fatalf("failed to grow csv: %v", err)
	}

	_, err := csv.Scan("test", []string{"id"})
	if err == nil {
		t.Fatal("expected max_bytes error after file grew")
	}
	if !strings.Contains(err.Error(), "exceeds max_bytes") {
		t.Fatalf("expected max_bytes error, got %v", err)
	}
}

func TestCSVWrapper_InvalidLimitOptions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte("id\n1\n"), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	tests := []map[string]string{
		{"file": path, "max_rows": "-1"},
		{"file": path, "max_bytes": "nope"},
	}
	for _, options := range tests {
		csv := &CSVWrapper{}
		if err := csv.Open(options); err == nil {
			t.Fatalf("expected invalid option error for %#v", options)
		}
	}
}

func TestCSVWrapper_ScanNotOpened(t *testing.T) {
	csv := &CSVWrapper{}
	_, err := csv.Scan("test", []string{"id"})
	if err == nil {
		t.Fatal("expected error when scanning without open")
	}
}

func TestRegistry_Has(t *testing.T) {
	r := NewRegistry()
	if r.Has("csv") {
		t.Fatal("expected csv to not be registered")
	}
	r.Register("csv", func() ForeignDataWrapper { return &CSVWrapper{} })
	if !r.Has("csv") {
		t.Fatal("expected csv to be registered")
	}
	if r.Has("missing") {
		t.Fatal("expected missing to not be registered")
	}
}

func TestCSVWrapper_QuotedFieldsAndEmptyCells(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.csv")
	content := "id,name,desc\n1,\"alice, jr\",\"\"\n2,bob,developer\n"
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write csv: %v", err)
	}

	csv := &CSVWrapper{}
	if err := csv.Open(map[string]string{"file": path}); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer csv.Close()

	rows, err := csv.Scan("test", []string{"id", "name", "desc"})
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][1] != "alice, jr" {
		t.Fatalf("unexpected quoted field: %q", rows[0][1])
	}
	if rows[0][2] != "" {
		t.Fatalf("unexpected empty cell: %q", rows[0][2])
	}
}
