package engine

import (
	"context"
	"testing"
)

func TestZZR22NullifContextDivergence(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, `CREATE TABLE dv (v INTEGER)`)
	mustExec(t, db, `INSERT INTO dv VALUES (7)`)
	probe := func(desc, sql string) {
		t.Helper()
		rows, err := db.Query(ctx, sql)
		if err != nil {
			t.Logf("%s: ERROR: %v", desc, err)
			return
		}
		defer rows.Close()
		if rows.Next() {
			var got interface{}
			_ = rows.Scan(&got)
			t.Logf("%s = %T(%v)", desc, got, got)
		}
	}
	probe("SELECT-list NULLIF(v) [1 arg]", "SELECT NULLIF(v) FROM dv")
	probe("aggregate-arg SUM(NULLIF(v)) [1 arg]", "SELECT SUM(NULLIF(v)) FROM dv")
}
