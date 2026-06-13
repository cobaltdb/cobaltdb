package catalog

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestValidateRowAgainstConstraintsNotNull verifies that validateRowAgainstConstraints
// rejects rows that violate NOT NULL.
func TestValidateRowAgainstConstraintsNotNull(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE nn (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("nn")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	row := []interface{}{int64(1), nil}
	if err := c.validateRowAgainstConstraints(table, row, nil); err == nil {
		t.Fatal("expected NOT NULL violation, got nil")
	}
}

// TestValidateRowAgainstConstraintsCheckColumn verifies column-level CHECK
// constraint rejection.
func TestValidateRowAgainstConstraintsCheckColumn(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	// CHECK with a literal expression — false → rejection.
	if _, err := c.ExecuteQuery("CREATE TABLE cc1 (id INTEGER PRIMARY KEY, score INTEGER CHECK (score > 0))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("cc1")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	row := []interface{}{int64(1), int64(-5)}
	if err := c.validateRowAgainstConstraints(table, row, nil); err == nil {
		t.Fatal("expected CHECK violation, got nil")
	}

	// Positive value passes.
	row[1] = int64(5)
	if err := c.validateRowAgainstConstraints(table, row, nil); err != nil {
		t.Fatalf("unexpected CHECK error: %v", err)
	}
}

// TestValidateRowAgainstConstraintsCheckTable verifies table-level CHECK.
func TestValidateRowAgainstConstraintsCheckTable(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE cc2 (a INTEGER, b INTEGER, CHECK (a + b > 0))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("cc2")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	// a + b = 0 → fails
	row := []interface{}{int64(1), int64(-1)}
	if err := c.validateRowAgainstConstraints(table, row, nil); err == nil {
		t.Fatal("expected table-level CHECK violation, got nil")
	}

	// a + b = 5 → passes
	row[1] = int64(4)
	if err := c.validateRowAgainstConstraints(table, row, nil); err != nil {
		t.Fatalf("unexpected CHECK error: %v", err)
	}
}

// TestValidateRowAgainstConstraintsFK verifies the FK part of the
// combined validator. The referenced table must exist and contain the
// referenced value; otherwise the constraint fails.
func TestValidateRowAgainstConstraintsFK(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE fk_p (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE fk_c (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_p(id))"); err != nil {
		t.Fatalf("create child: %v", err)
	}
	table, err := c.getTableLocked("fk_c")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	// pid = 999 not in parent → FK violation
	row := []interface{}{int64(1), int64(999)}
	if err := c.validateRowAgainstConstraints(table, row, nil); err == nil {
		t.Fatal("expected FK violation, got nil")
	}

	// Insert parent row, then child with valid pid → no violation.
	if _, err := c.ExecuteQuery("INSERT INTO fk_p VALUES (5)"); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	row[1] = int64(5)
	if err := c.validateRowAgainstConstraints(table, row, nil); err != nil {
		t.Fatalf("unexpected FK error: %v", err)
	}
}

// TestValidateRowNonFKConstraintsSkipsFK verifies that the non-FK variant
// returns no error for a row that would fail FK validation — it only
// checks NOT NULL + CHECK.
func TestValidateRowNonFKConstraintsSkipsFK(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE fk_p2 (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE fk_c2 (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES fk_p2(id))"); err != nil {
		t.Fatalf("create child: %v", err)
	}
	table, err := c.getTableLocked("fk_c2")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	// pid = 999 not in parent — full validator would reject this row;
	// the non-FK variant must accept it.
	row := []interface{}{int64(1), int64(999)}
	if err := c.validateRowNonFKConstraints(table, row, nil); err != nil {
		t.Fatalf("validateRowNonFKConstraints should not check FK, got: %v", err)
	}
}

// TestValidateRowNonFKConstraintsStillChecksNullAndCheck verifies that
// the non-FK variant still enforces NOT NULL and CHECK.
func TestValidateRowNonFKConstraintsStillChecksNullAndCheck(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE nnc (id INTEGER PRIMARY KEY, n INTEGER NOT NULL, s INTEGER CHECK (s >= 0))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("nnc")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	// n is nil → NOT NULL fail
	row := []interface{}{int64(1), nil, int64(5)}
	if err := c.validateRowNonFKConstraints(table, row, nil); err == nil {
		t.Fatal("expected NOT NULL violation in non-FK variant")
	}

	// n set, but s is negative → CHECK fail
	row[1] = int64(7)
	row[2] = int64(-1)
	if err := c.validateRowNonFKConstraints(table, row, nil); err == nil {
		t.Fatal("expected CHECK violation in non-FK variant")
	}

	// All good
	row[2] = int64(10)
	if err := c.validateRowNonFKConstraints(table, row, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestCheckInsertConstraintsWrapperIsDeprecated verifies that
// checkInsertConstraints (now deprecated) still works as a thin wrapper
// over checkRowConstraints.
func TestCheckInsertConstraintsWrapperIsDeprecated(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE dep (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("dep")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	// The wrapper still rejects CHECK violations.
	row := []interface{}{int64(1), int64(0)}
	if err := c.checkInsertConstraints(table, row, nil); err == nil {
		t.Fatal("checkInsertConstraints should still reject CHECK violation")
	}
	_ = query.NumberLiteral{} // keep the import
}
