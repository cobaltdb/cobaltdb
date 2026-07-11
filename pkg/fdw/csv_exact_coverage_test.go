package fdw

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestCSVHelpersCoverProjectionAndPredicates(t *testing.T) {
	header := []string{"id", "name"}
	if got := csvProjection(header, []string{"missing"}); got != nil {
		t.Fatalf("missing projection = %v, want nil fallback", got)
	}
	if got := csvProjection(header, []string{"name", "id"}); !reflect.DeepEqual(got, []int{1, 0}) {
		t.Fatalf("reordered projection = %v", got)
	}
	if got := csvProjection(header, header); got != nil {
		t.Fatalf("identity projection = %v, want nil", got)
	}
	if got := projectCSVRow([]interface{}{"x"}, []int{0, -1, 2}); !reflect.DeepEqual(got, []interface{}{"x", nil, nil}) {
		t.Fatalf("projected row = %#v", got)
	}
	preds := csvPredicates(header, []Predicate{{Column: "missing", Operator: "=", Value: 1}, {Column: "id", Operator: "=", Value: 1}})
	if len(preds) != 1 || preds[0].index != 0 {
		t.Fatalf("predicates = %+v", preds)
	}
	if !matchesCSVPredicates([]interface{}{}, []csvPredicate{{index: 1, operator: "=", value: 1}}) {
		t.Fatal("out-of-range predicate should defer filtering")
	}

	tests := []struct {
		cell string
		op   string
		want interface{}
		ok   bool
	}{
		{"", "=", nil, true}, {"x", "=", nil, false}, {"x", "!=", nil, true}, {"", "!=", nil, false}, {"x", "?", nil, true},
		{"1.0", "=", 1, true}, {"1", "=", 2, false}, {"x", "=", "x", true},
		{"1", "!=", 2, true}, {"1", "!=", 1, false}, {"x", "!=", "y", true},
		{"1", "<", 2, true}, {"2", ">", 1, true}, {"2", "<=", 2, true}, {"2", ">=", 2, true},
		{"x", "<", "y", true}, {"1", "unknown", 2, true},
	}
	for _, tt := range tests {
		if got := matchesCSVPredicate(tt.cell, csvPredicate{operator: tt.op, value: tt.want}); got != tt.ok {
			t.Errorf("matches(%q %s %v) = %v, want %v", tt.cell, tt.op, tt.want, got, tt.ok)
		}
	}
}

func TestCSVPathAndCursorEdgeCases(t *testing.T) {
	if _, err := cleanCSVPath("  "); err == nil {
		t.Fatal("cleanCSVPath accepted whitespace")
	}
	wrapper := &CSVWrapper{}
	if err := wrapper.Open(map[string]string{"file": " "}); err == nil {
		t.Fatal("Open accepted whitespace path")
	}

	path := filepath.Join(t.TempDir(), "numeric.csv")
	if err := os.WriteFile(path, []byte("1,2\n3,4\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := wrapper.Open(map[string]string{"file": path, "max_rows": "1"}); err != nil {
		t.Fatal(err)
	}
	cursor, err := wrapper.OpenScan("", ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.Next(); err == nil || !strings.Contains(err.Error(), "row limit") {
		t.Fatalf("second pending-row Next error = %v", err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := wrapper.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCSVRegularFilePropagatesPostValidationFailures(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("id\n1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalOpen, originalStat, originalSame := csvOpenFile, csvFileStat, csvSameFile
	t.Cleanup(func() {
		csvOpenFile, csvFileStat, csvSameFile = originalOpen, originalStat, originalSame
	})

	csvOpenFile = func(string) (*os.File, error) { return nil, errors.New("open failure") }
	if _, err := openCSVRegularFile(path, 0); err == nil || !strings.Contains(err.Error(), "open failure") {
		t.Fatalf("open failure = %v", err)
	}
	csvOpenFile = originalOpen
	csvFileStat = func(*os.File) (os.FileInfo, error) { return nil, errors.New("stat failure") }
	if _, err := openCSVRegularFile(path, 0); err == nil || !strings.Contains(err.Error(), "stat failure") {
		t.Fatalf("stat failure = %v", err)
	}
	csvFileStat = func(file *os.File) (os.FileInfo, error) {
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		return modeFileInfo{FileInfo: info, mode: os.ModeDir}, nil
	}
	if _, err := openCSVRegularFile(path, 0); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("opened type failure = %v", err)
	}
	csvFileStat = originalStat
	csvSameFile = func(os.FileInfo, os.FileInfo) bool { return false }
	if _, err := openCSVRegularFile(path, 0); err == nil || !strings.Contains(err.Error(), "changed while opening") {
		t.Fatalf("identity failure = %v", err)
	}
	csvSameFile = func(os.FileInfo, os.FileInfo) bool { return true }
	csvFileStat = func(file *os.File) (os.FileInfo, error) {
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		return sizedFileInfo{FileInfo: info, size: 10}, nil
	}
	if _, err := openCSVRegularFile(path, 5); err == nil || !strings.Contains(err.Error(), "exceeds max_bytes") {
		t.Fatalf("post-open size failure = %v", err)
	}
}

type modeFileInfo struct {
	os.FileInfo
	mode os.FileMode
}

func (m modeFileInfo) Mode() os.FileMode { return m.mode }

type sizedFileInfo struct {
	os.FileInfo
	size int64
}

func (s sizedFileInfo) Size() int64 { return s.size }

func TestCSVWrapperPropagatesReopenAndInitialReadFailures(t *testing.T) {
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid.csv")
	malformed := filepath.Join(dir, "malformed.csv")
	if err := os.WriteFile(valid, []byte("id\n1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(malformed, []byte("\"unterminated\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	closed, err := os.Open(valid)
	if err != nil {
		t.Fatal(err)
	}
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	wrapper := &CSVWrapper{file: closed}
	if err := wrapper.Open(map[string]string{"file": valid}); err == nil {
		t.Fatal("reopen did not propagate closing the stale handle")
	}

	wrapper = &CSVWrapper{}
	if err := wrapper.Open(map[string]string{"file": malformed}); err != nil {
		t.Fatal(err)
	}
	defer wrapper.Close()
	if _, err := wrapper.OpenScan("", ScanOptions{}); err == nil {
		t.Fatal("OpenScan accepted malformed first record")
	}
}

func TestCSVCursorPendingRowHonorsLimit(t *testing.T) {
	cursor := &csvCursor{pending: []interface{}{"row"}, maxRows: 1, returned: 1}
	if _, err := cursor.Next(); err == nil || !strings.Contains(err.Error(), "row limit") {
		t.Fatalf("pending limit error = %v", err)
	}
}

func TestCSVScannerReportsMalformedRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	if err := os.WriteFile(path, []byte("id,name\n1,\"unterminated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wrapper := &CSVWrapper{}
	if err := wrapper.Open(map[string]string{"file": path}); err != nil {
		t.Fatal(err)
	}
	defer wrapper.Close()
	cursor, err := wrapper.OpenScan("", ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer cursor.Close()
	if _, err := cursor.Next(); err == nil || err == io.EOF {
		t.Fatalf("malformed row error = %v", err)
	}
}
