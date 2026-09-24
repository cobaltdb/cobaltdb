package engine

import (
	"context"
	"path/filepath"
	"testing"
)

// Partition tree roots were never persisted: getInsertTargetTree creates
// partition btrees lazily in memory (c.tableTrees[name]), PartitionDef carries
// no root page id, and neither Save/Load nor vacuumTreeLocked persists them.
// A file-backed partitioned table therefore lost ALL partitioned rows on
// reopen — the table metadata survived (TableDef is persisted) but COUNT(*)
// dropped to 0.
func TestPartitionedTableSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "part.db")

	db, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	mustExec(t, db, `CREATE TABLE pm (id INTEGER, region INTEGER) PARTITION BY RANGE (region) (PARTITION p0 VALUES LESS THAN (10), PARTITION pmax VALUES LESS THAN (MAXVALUE))`)
	mustExec(t, db, `INSERT INTO pm VALUES (1, 5), (2, 50), (3, 9999)`)

	var n int64
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM pm`).Scan(&n); err != nil {
		t.Fatalf("pre-close count: %v", err)
	}
	if n != 3 {
		t.Fatalf("pre-close count = %d, want 3", n)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(dbPath, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = db2.Close() }()

	if err := db2.QueryRow(ctx, `SELECT COUNT(*) FROM pm`).Scan(&n); err != nil {
		t.Fatalf("post-reopen count: %v", err)
	}
	if n != 3 {
		t.Fatalf("partitioned rows lost on reopen: count = %d, want 3 (partition tree roots are never persisted)", n)
	}

	// The surviving rows must be the right ones, not just the right count.
	rows, err := db2.Query(ctx, `SELECT region FROM pm ORDER BY region`)
	if err != nil {
		t.Fatalf("post-reopen query: %v", err)
	}
	cols := rows.Columns()
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	var regions []int64
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan: %v", err)
		}
		regions = append(regions, toIntPartition(vals[0]))
	}
	_ = rows.Close()
	want := []int64{5, 50, 9999}
	if len(regions) != len(want) {
		t.Fatalf("post-reopen regions = %v, want %v", regions, want)
	}
	for i := range want {
		if regions[i] != want[i] {
			t.Fatalf("post-reopen regions = %v, want %v", regions, want)
		}
	}
}

func toIntPartition(v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	default:
		return -1
	}
}
