package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/fdw"
)

// failingFDW's Open always fails.
type failingFDW struct{}

func (w *failingFDW) Name() string { return "failing" }

func (w *failingFDW) Open(options map[string]string) error {
	return fmt.Errorf("simulated fdw open failure")
}

func (w *failingFDW) Close() error { return nil }

func (w *failingFDW) Scan(table string, columns []string) ([][]interface{}, error) {
	return nil, fmt.Errorf("unreachable: Open failed")
}

// okFDW serves two fixed rows.
type okFDW struct{}

func (w *okFDW) Name() string { return "ok" }

func (w *okFDW) Open(options map[string]string) error { return nil }

func (w *okFDW) Close() error { return nil }

func (w *okFDW) Scan(table string, columns []string) ([][]interface{}, error) {
	return [][]interface{}{{"a"}, {"b"}}, nil
}

// TestForeignTableAggregatePropagatesFDWErrors pins the contract that a
// statement over a foreign table whose FDW fails reports the failure instead
// of a silently empty result.
//
// Regression: getEffectiveTableData (pkg/catalog/catalog_core.go) discarded
// getTableTreesForScan's error (`trees, _ :=`), so SELECT COUNT(*) over a
// foreign table whose FDW failed returned COUNT=0 with no error, while the
// plain SELECT path propagated the same failure. Both getEffectiveTableData
// callers (aggregate + join) already propagate.
func TestForeignTableAggregatePropagatesFDWErrors(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	db.RegisterFDW("failing", func() fdw.ForeignDataWrapper { return &failingFDW{} })
	db.RegisterFDW("ok", func() fdw.ForeignDataWrapper { return &okFDW{} })

	ctx := context.Background()
	for _, ddl := range []string{
		"CREATE FOREIGN TABLE fbad (v TEXT) WRAPPER 'failing'",
		"CREATE FOREIGN TABLE fok (v TEXT) WRAPPER 'ok'",
	} {
		if _, err := db.Exec(ctx, ddl); err != nil {
			t.Fatalf("exec %q: %v", ddl, err)
		}
	}

	// Control: the plain SELECT path propagates the failure (pre-existing).
	if _, err := db.Query(ctx, "SELECT * FROM fbad"); err == nil {
		t.Fatal("plain SELECT over a failing foreign table returned no error")
	}

	// Control: the aggregate path works over a healthy foreign table.
	rows, err := db.Query(ctx, "SELECT COUNT(*) FROM fok")
	if err != nil {
		t.Fatalf("COUNT(*) over fok: %v", err)
	}
	var okCount int
	if !rows.Next() {
		t.Fatal("COUNT(*) over fok returned no rows")
	}
	if err := rows.Scan(&okCount); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if okCount != 2 {
		t.Fatalf("COUNT(*) over fok = %d, want 2", okCount)
	}

	// Discriminator: the aggregate over the FAILING foreign table must report
	// the failure, not a silent COUNT=0.
	if _, err := db.Query(ctx, "SELECT COUNT(*) FROM fbad"); err == nil {
		t.Fatal("COUNT(*) over a failing foreign table silently returned a result — getEffectiveTableData swallowed the FDW error")
	}

	// Second caller: a JOIN touching the failing foreign table also fails.
	if _, err := db.Query(ctx, "SELECT * FROM fok JOIN fbad ON fok.v = fbad.v"); err == nil {
		t.Fatal("JOIN over a failing foreign table returned no error")
	}
}
