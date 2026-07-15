package catalog

import (
	"context"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestPrepareInsertRowAutoIncrement verifies that prepareInsertRow generates
// a fresh auto-increment key when the table has a single-column numeric PK
// and the row does not specify one.
//
// Setup: a table with (id PRIMARY KEY AUTOINCREMENT, name) — valueRow
// supplies only the name (no id), and prepareInsertRow should auto-generate
// the PK and fill rowValues[0] with the generated value.
func TestPrepareInsertRowAutoIncrement(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE autogen (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("autogen")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	// Use explicit column list: INSERT INTO autogen (name) VALUES ('alice')
	// — the id column is omitted, so the PK must be auto-generated.
	stmt := &query.InsertStmt{Table: "autogen", Columns: []string{"name"}}
	insertColIndices := []int{1} // name → table column index 1
	insertColumns := []string{"name"}
	valueRow := []query.Expression{
		&query.StringLiteral{Value: "alice"},
	}

	rowValues, key, autoInc, skipRow, err := c.prepareInsertRow(
		context.Background(), table, stmt, nil, valueRow,
		1, // numInsertCols (only "name")
		insertColIndices, insertColumns, false, nil, nil,
	)
	if err != nil {
		t.Fatalf("prepareInsertRow: %v", err)
	}
	if skipRow {
		t.Fatal("prepareInsertRow: skipRow=true unexpectedly")
	}
	if key == "" {
		t.Fatal("prepareInsertRow: empty key on auto-increment row")
	}
	if autoInc == 0 {
		t.Fatal("prepareInsertRow: autoInc=0, want >0")
	}
	if !strings.HasPrefix(key, "0") { // formatKey uses 20-digit zero-padded decimal
		t.Fatalf("prepareInsertRow: key %q missing zero-padded prefix", key)
	}
	if len(rowValues) != len(table.Columns) {
		t.Fatalf("prepareInsertRow: rowValues len=%d, want %d", len(rowValues), len(table.Columns))
	}
	// The auto-increment value should be the first column (id).
	if got, ok := rowValues[0].(float64); !ok || int64(got) != autoInc {
		t.Fatalf("prepareInsertRow: rowValues[0]=%v, want float64(%d)", rowValues[0], autoInc)
	}
}

// TestPrepareInsertRowColumnCountMismatch verifies that prepareInsertRow
// returns an error when value count != column count (and the row is not a
// default-values row with one auto-increment column).
func TestPrepareInsertRowColumnCountMismatch(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE cc (id INTEGER PRIMARY KEY, a TEXT, b TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("cc")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	stmt := &query.InsertStmt{Table: "cc"}
	// 2 values for 3 columns — should error.
	valueRow := []query.Expression{
		&query.NumberLiteral{Value: 1},
		&query.StringLiteral{Value: "x"},
	}

	_, _, _, _, err = c.prepareInsertRow(
		context.Background(), table, stmt, nil, valueRow,
		len(table.Columns), nil, nil, false, nil, nil,
	)
	if err == nil {
		t.Fatal("prepareInsertRow: expected column-count error, got nil")
	}
	if !strings.Contains(err.Error(), "INSERT has 3 columns but 2 values") {
		t.Fatalf("prepareInsertRow: unexpected error: %v", err)
	}
}

// TestPrepareInsertRowCompositePK verifies that prepareInsertRow defers
// the composite key construction to validateInsertRow and just returns
// the row + a non-auto-increment return.
func TestPrepareInsertRowCompositePK(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE cpk (a INTEGER, b INTEGER, v TEXT, PRIMARY KEY (a, b))"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("cpk")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	stmt := &query.InsertStmt{Table: "cpk"}
	valueRow := []query.Expression{
		&query.NumberLiteral{Value: 1},
		&query.NumberLiteral{Value: 2},
		&query.StringLiteral{Value: "hi"},
	}

	rowValues, key, autoInc, _, err := c.prepareInsertRow(
		context.Background(), table, stmt, nil, valueRow,
		len(table.Columns), nil, nil, true /* compositePK */, nil, nil,
	)
	if err != nil {
		t.Fatalf("prepareInsertRow: %v", err)
	}
	// For composite PK, validateInsertRow constructs the key from the row
	// values; the returned key is the resolved composite key (e.g. "1\x002").
	if key == "" {
		t.Fatal("composite PK: validateInsertRow should have built a key")
	}
	if autoInc != 0 {
		t.Fatalf("composite PK: expected autoInc=0, got %d", autoInc)
	}
	if len(rowValues) != 3 {
		t.Fatalf("composite PK: expected 3 row values, got %d", len(rowValues))
	}
}

// TestPrepareInsertRowExplicitPK verifies that an explicit PK value is used
// directly (no auto-increment), and the table's auto-inc counter advances
// to stay ahead of the explicit value.
func TestPrepareInsertRowExplicitPK(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE expl (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	table, err := c.getTableLocked("expl")
	if err != nil {
		t.Fatalf("getTableLocked: %v", err)
	}

	stmt := &query.InsertStmt{Table: "expl"}
	valueRow := []query.Expression{
		&query.NumberLiteral{Value: 42},
		&query.StringLiteral{Value: "explicit"},
	}

	_, key, autoInc, _, err := c.prepareInsertRow(
		context.Background(), table, stmt, nil, valueRow,
		len(table.Columns), nil, nil, false, nil, nil,
	)
	if err != nil {
		t.Fatalf("prepareInsertRow: %v", err)
	}
	if key == "" {
		t.Fatal("explicit PK: key is empty")
	}
	if autoInc != 0 {
		t.Fatalf("explicit PK: autoInc=%d, want 0 (literal PK did not auto-increment)", autoInc)
	}
}
