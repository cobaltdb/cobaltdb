package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

// captureBoth redirects stdout and stderr while fn runs.
func captureBoth(t *testing.T, fn func()) (string, string) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	rOut, wOut, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	rErr, wErr, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout, os.Stderr = wOut, wErr
	fn()
	_ = wOut.Close()
	_ = wErr.Close()
	os.Stdout, os.Stderr = oldOut, oldErr
	outB, err := io.ReadAll(rOut)
	if err != nil {
		t.Fatalf("read stdout: %v", err)
	}
	errB, err := io.ReadAll(rErr)
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	return string(outB), string(errB)
}

func importedIDs(t *testing.T, db *engine.DB, table string) []int {
	t.Helper()
	rows, err := db.Query(context.Background(), "SELECT id FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestImportCSVReportsSkippedRows drives the REAL importCSV over a file
// whose data records include one with FEWER and one with MORE fields than the
// header. Those records cannot be inserted, but they must be REPORTED — never
// dropped silently while the import reports success.
func TestImportCSVReportsSkippedRows(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE sk (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	// record 1 = header; record 2 valid; record 3 too few; record 4 too many; record 5 valid.
	csvPath := filepath.Join(t.TempDir(), "sk.csv")
	content := "id,name\n1,alice\n2\n3,bob,extra\n4,dave\n"
	if err := os.WriteFile(csvPath, []byte(content), 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	var importErr error
	stdout, stderr := captureBoth(t, func() {
		importErr = importCSV(db, csvPath, "sk")
	})
	t.Logf("importCSV err=%v\nstdout=%q\nstderr=%q", importErr, stdout, stderr)
	got := importedIDs(t, db, "sk")
	t.Logf("committed ids so far: %v", got)

	// CONTRACT: a record whose field count mismatches the header is REPORTED
	// and skipped — it must not hard-abort the import (leaving an unreported
	// partial commit behind) nor be dropped in silence.
	if importErr != nil {
		t.Fatalf("FAIL: importCSV hard-aborted on the first mismatched record (err=%v),\n"+
			"after having already committed ids %v — a partial import that the\n"+
			"error never reports. The arity branch in importCSV is dead code\n"+
			"because csv.Reader's default FieldsPerRecord enforces the header\n"+
			"count before that branch can run.", importErr, got)
	}

	if len(got) != 2 || got[0] != 1 || got[1] != 4 {
		t.Fatalf("FAIL: well-formed records lost after a mismatched record; imported ids = %v, want [1 4]", got)
	}

	if !strings.Contains(stderr, "3") || !strings.Contains(stderr, "4") {
		t.Fatalf("FAIL: skipped records 3 and 4 were not reported.\nstdout=%q\nstderr=%q", stdout, stderr)
	}
	if !strings.Contains(stderr, "1") || !strings.Contains(stderr, "2") {
		t.Fatalf("FAIL: skip notice does not state actual vs expected field counts.\nstderr=%q", stderr)
	}
}

// TestImportCSVCleanImportNoWarnings is the control: a well-formed file
// must import with NO skip warnings.
func TestImportCSVCleanImportNoWarnings(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE ok (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	csvPath := filepath.Join(t.TempDir(), "ok.csv")
	if err := os.WriteFile(csvPath, []byte("id,name\n1,alice\n2,bob\n"), 0600); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	var importErr error
	_, stderr := captureBoth(t, func() {
		importErr = importCSV(db, csvPath, "ok")
	})
	if importErr != nil {
		t.Fatalf("importCSV: %v", importErr)
	}
	ids := importedIDs(t, db, "ok")
	if len(ids) != 2 {
		t.Fatalf("control: expected 2 imported rows, got %v", ids)
	}
	if strings.Contains(stderr, "skipping") || strings.Contains(stderr, "Skipped") {
		t.Fatalf("control: clean import produced skip warnings: %q", stderr)
	}
	sort.Ints(ids)
}
