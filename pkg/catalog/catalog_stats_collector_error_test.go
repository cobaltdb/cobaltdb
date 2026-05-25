package catalog

import (
	"strings"
	"testing"
)

func TestStatsCollectorReturnsColumnStatsError(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE stats_col_err (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	table := c.tables["stats_col_err"]
	table.Columns = append(table.Columns, ColumnDef{Name: "bad-name", Type: "TEXT"})
	table.buildColumnIndexCache()

	collector := NewStatsCollector(c)
	stats, err := collector.CollectStats("stats_col_err")
	if err == nil || !strings.Contains(err.Error(), "bad-name") {
		t.Fatalf("expected column stats error, stats=%v err=%v", stats, err)
	}
	if _, exists := collector.GetTableStats("stats_col_err"); exists {
		t.Fatal("CollectStats should not publish partial stats after a column error")
	}
}
