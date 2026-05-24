package catalog

import (
	"io"
	"reflect"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/fdw"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

type captureStreamingFDW struct {
	options fdw.ScanOptions
	rows    [][]interface{}
}

func (m *captureStreamingFDW) Name() string { return "capture" }
func (m *captureStreamingFDW) Open(options map[string]string) error {
	return nil
}
func (m *captureStreamingFDW) Scan(table string, columns []string) ([][]interface{}, error) {
	return m.rows, nil
}
func (m *captureStreamingFDW) OpenScan(table string, options fdw.ScanOptions) (fdw.RowCursor, error) {
	m.options = options
	return &sliceFDWCursor{rows: m.rows}, nil
}
func (m *captureStreamingFDW) Close() error { return nil }

type sliceFDWCursor struct {
	rows [][]interface{}
	pos  int
}

func (c *sliceFDWCursor) Next() ([]interface{}, error) {
	if c.pos >= len(c.rows) {
		return nil, io.EOF
	}
	row := c.rows[c.pos]
	c.pos++
	return row, nil
}
func (c *sliceFDWCursor) Close() error { return nil }

func TestFDWScanOptionsCarrySimpleWherePredicates(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("new btree: %v", err)
	}
	c := New(tree, pool, nil)

	wrapper := &captureStreamingFDW{
		rows: [][]interface{}{
			{int64(1), "alice", int64(95)},
			{int64(2), "bob", int64(87)},
		},
	}
	reg := fdw.NewRegistry()
	reg.Register("capture", func() fdw.ForeignDataWrapper { return wrapper })
	c.SetFDWRegistry(reg)

	if err := c.CreateForeignTable(&query.CreateForeignTableStmt{
		Table: "ext_users",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
			{Name: "score", Type: query.TokenInteger},
		},
		Wrapper: "capture",
	}); err != nil {
		t.Fatalf("CreateForeignTable: %v", err)
	}

	parsed, err := query.Parse("SELECT id, name FROM ext_users u WHERE u.score >= ? AND name != 'bob'")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}
	stmt := parsed.(*query.SelectStmt)
	if _, _, err := c.Select(stmt, []interface{}{float64(90)}); err != nil {
		t.Fatalf("select: %v", err)
	}

	wantColumns := []string{"id", "name", "score"}
	if !reflect.DeepEqual(wrapper.options.Columns, wantColumns) {
		t.Fatalf("scan columns = %v, want %v", wrapper.options.Columns, wantColumns)
	}
	wantPredicates := []fdw.Predicate{
		{Column: "score", Operator: ">=", Value: float64(90)},
		{Column: "name", Operator: "!=", Value: "bob"},
	}
	if !reflect.DeepEqual(wrapper.options.Predicates, wantPredicates) {
		t.Fatalf("predicates = %#v, want %#v", wrapper.options.Predicates, wantPredicates)
	}
}
