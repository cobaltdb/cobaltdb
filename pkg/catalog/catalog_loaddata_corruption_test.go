package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDataReturnsMismatchedKeyValueCountsError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	dir := t.TempDir()
	tableData := struct {
		Keys   [][]byte `json:"keys"`
		Values [][]byte `json:"values"`
	}{
		Keys:   [][]byte{[]byte("1"), []byte("2")},
		Values: [][]byte{[]byte(`{"id":1,"name":"alice"}`)},
	}
	data, err := json.Marshal(tableData)
	if err != nil {
		t.Fatalf("Marshal table data: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "users.json"), data, 0600); err != nil {
		t.Fatalf("Write table data: %v", err)
	}

	err = c.LoadData(dir)
	if err == nil || !strings.Contains(err.Error(), "mismatched key/value counts") || !strings.Contains(err.Error(), "users") {
		t.Fatalf("expected mismatched key/value count error, got %v", err)
	}
}
