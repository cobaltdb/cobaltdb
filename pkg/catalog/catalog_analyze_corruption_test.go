package catalog

import (
	"strings"
	"testing"
)

func TestAnalyzeReturnsCorruptRowError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE analyze_corrupt (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := c.tableTrees["analyze_corrupt"].Put([]byte("bad-row"), []byte("{not valid json")); err != nil {
		t.Fatalf("put corrupt row: %v", err)
	}

	err := c.Analyze("analyze_corrupt")
	if err == nil || !strings.Contains(err.Error(), "failed to decode row") || !strings.Contains(err.Error(), "analyze_corrupt") {
		t.Fatalf("expected corrupt row analyze error, got %v", err)
	}
	if _, exists := c.stats["analyze_corrupt"]; exists {
		t.Fatal("Analyze should not publish stats after corrupt row error")
	}
}
