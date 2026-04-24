package fdw

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	csv := &CSVWrapper{}
	r.Register("csv", csv)

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

func TestRegistry_List(t *testing.T) {
	r := NewRegistry()
	r.Register("csv", &CSVWrapper{})
	r.Register("http", &CSVWrapper{})

	names := r.List()
	if len(names) != 2 {
		t.Fatalf("expected 2 wrappers, got %d", len(names))
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

func TestCSVWrapper_OpenMissingFile(t *testing.T) {
	csv := &CSVWrapper{}
	err := csv.Open(map[string]string{"file": "/nonexistent/path/file.csv"})
	if err == nil {
		t.Fatal("expected error when file does not exist")
	}
}

func TestCSVWrapper_ScanNotOpened(t *testing.T) {
	csv := &CSVWrapper{}
	_, err := csv.Scan("test", []string{"id"})
	if err == nil {
		t.Fatal("expected error when scanning without open")
	}
}
