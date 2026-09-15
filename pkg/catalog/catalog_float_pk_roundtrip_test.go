package catalog

import (
	"testing"
)

// Fractional float primary keys must use the whole/fractional key contract of
// formatFloatKey/serializePK: whole numbers use the zero-padded integer key,
// fractional values use the "F:"+exact-float key. The equality lookup
// (useIndexForExactMatch "__PK__") and the UPDATE key builder previously
// truncated floats through int64, so rows written under "F:" keys were
// invisible to equality lookups and UPDATE to a fractional PK landed under a
// colliding integer key. These tests pin the round-trip.

// TestFloatPKInsertEqualityRoundTrip verifies that a row inserted with a
// fractional float PK is found by exact equality on that value and is not
// reachable via the truncated integer key.
func TestFloatPKInsertEqualityRoundTrip(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)

	if _, err := c.ExecuteQuery("CREATE TABLE float_pk_ins (id DOUBLE PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO float_pk_ins VALUES (1.5, 'a')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if n := len(mustRows(t, c, "SELECT name FROM float_pk_ins WHERE id = 1.5")); n != 1 {
		t.Fatalf("SELECT id=1.5 after INSERT returned %d rows, want 1", n)
	}
	if n := len(mustRows(t, c, "SELECT name FROM float_pk_ins WHERE id = 1")); n != 0 {
		t.Fatalf("SELECT id=1 after INSERT of 1.5 returned %d rows, want 0", n)
	}
}

// TestFloatPKUpdateEqualityRoundTrip verifies that updating a row's float PK
// to a fractional value moves the row to the exact "F:"-tagged key, keeping it
// reachable by equality on the new value and unreachable by the truncated key.
func TestFloatPKUpdateEqualityRoundTrip(t *testing.T) {
	c, _ := createCatalogWithTxnManager(t)

	if _, err := c.ExecuteQuery("CREATE TABLE float_pk_upd (id DOUBLE PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO float_pk_upd VALUES (1.5, 'a')"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := c.ExecuteQuery("UPDATE float_pk_upd SET id = 2.5 WHERE name = 'a'"); err != nil {
		t.Fatalf("update: %v", err)
	}

	if n := len(mustRows(t, c, "SELECT name FROM float_pk_upd WHERE id = 2.5")); n != 1 {
		t.Fatalf("after UPDATE SET id=2.5, SELECT id=2.5 returned %d rows, want 1", n)
	}
	if n := len(mustRows(t, c, "SELECT name FROM float_pk_upd WHERE id = 2")); n != 0 {
		t.Fatalf("SELECT id=2 unexpectedly returned %d rows after UPDATE SET id=2.5", n)
	}
}

func mustRows(t *testing.T, c *Catalog, sql string) [][]interface{} {
	t.Helper()
	r, err := c.ExecuteQuery(sql)
	if err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
	return r.Rows
}
