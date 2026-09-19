package catalog

import (
	"math"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// ==================== cloneJoinClause tests (0% coverage) ====================

func TestCloneJoinClause_Nil(t *testing.T) {
	cloned := cloneJoinClause(nil)
	if cloned != nil {
		t.Error("expected nil for nil input")
	}
}

func TestCloneJoinClause_Basic(t *testing.T) {
	join := &query.JoinClause{
		Type:    query.TokenInner,
		Natural: false,
		Table:   &query.TableRef{Name: "t1"},
		Using:   []string{"id", "name"},
	}

	cloned := cloneJoinClause(join)
	if cloned == nil {
		t.Fatal("expected non-nil result")
	}
	if cloned.Type != query.TokenInner {
		t.Errorf("expected Type=TokenInner, got %v", cloned.Type)
	}
	if cloned.Table == nil || cloned.Table.Name != "t1" {
		t.Errorf("expected Table.Name=t1, got %v", cloned.Table)
	}
	if len(cloned.Using) != 2 || cloned.Using[0] != "id" || cloned.Using[1] != "name" {
		t.Errorf("unexpected Using slice: %v", cloned.Using)
	}
}

func TestCloneJoinClause_NilTable(t *testing.T) {
	join := &query.JoinClause{
		Type:  query.TokenLeft,
		Table: nil,
	}

	cloned := cloneJoinClause(join)
	if cloned == nil {
		t.Fatal("expected non-nil result")
	}
	if cloned.Table != nil {
		t.Error("expected nil Table")
	}
}

func TestCloneJoinClause_NilUsing(t *testing.T) {
	join := &query.JoinClause{
		Type:  query.TokenRight,
		Table: &query.TableRef{Name: "t2"},
		Using: nil,
	}

	cloned := cloneJoinClause(join)
	if cloned == nil {
		t.Fatal("expected non-nil result")
	}
	if cloned.Using != nil {
		t.Error("expected nil Using")
	}
}

// ==================== cloneInterfaceSlice tests (0% coverage) ====================

func TestCloneInterfaceSlice_Nil(t *testing.T) {
	cloned := cloneInterfaceSlice(nil)
	if cloned != nil {
		t.Error("expected nil for nil input")
	}
}

func TestCloneInterfaceSlice_Empty(t *testing.T) {
	cloned := cloneInterfaceSlice([]interface{}{})
	if cloned == nil {
		t.Fatal("expected non-nil result")
	}
	if len(cloned) != 0 {
		t.Errorf("expected empty slice, got len %d", len(cloned))
	}
}

func TestCloneInterfaceSlice_SimpleTypes(t *testing.T) {
	src := []interface{}{"hello", int64(42), 3.14, true}
	cloned := cloneInterfaceSlice(src)
	if len(cloned) != len(src) {
		t.Fatalf("expected len %d, got %d", len(src), len(cloned))
	}
	for i, v := range src {
		if cloned[i] != v {
			t.Errorf("index %d: expected %v, got %v", i, v, cloned[i])
		}
	}
}

func TestCloneInterfaceSlice_ByteSlice(t *testing.T) {
	src := []interface{}{[]byte{1, 2, 3}}
	cloned := cloneInterfaceSlice(src)
	if len(cloned) != 1 {
		t.Fatalf("expected len 1, got %d", len(cloned))
	}
	orig := src[0].([]byte)
	copy := cloned[0].([]byte)
	if len(copy) != 3 || copy[0] != 1 || copy[1] != 2 || copy[2] != 3 {
		t.Errorf("unexpected cloned byte slice: %v", copy)
	}
	// Ensure it's a deep copy
	orig[0] = 99
	if copy[0] == 99 {
		t.Error("cloneInterfaceSlice should deep-copy byte slices")
	}
}

func TestCloneInterfaceSlice_NestedSlice(t *testing.T) {
	src := []interface{}{
		[]interface{}{"a", "b"},
	}
	cloned := cloneInterfaceSlice(src)
	if len(cloned) != 1 {
		t.Fatalf("expected len 1, got %d", len(cloned))
	}
	nested, ok := cloned[0].([]interface{})
	if !ok {
		t.Fatal("expected nested []interface{}")
	}
	if len(nested) != 2 || nested[0] != "a" || nested[1] != "b" {
		t.Errorf("unexpected nested slice: %v", nested)
	}
	// Modify original to verify deep copy
	src[0].([]interface{})[0] = "z"
	if nested[0] == "z" {
		t.Error("cloneInterfaceSlice should deep-copy nested slices")
	}
}

// ==================== recordForUpdateReads tests (0% coverage) ====================

func TestRecordForUpdateReads_NoActiveTxn(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree failed: %v", err)
	}
	c := New(tree, pool, nil)

	// Create a table so the table lookup succeeds
	createCoverageTestTable(t, c, "test_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	table, ok := c.tables["test_tbl"]
	if !ok {
		t.Fatal("table not found after CreateTable")
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "test_tbl"},
	}

	// No active transaction → should return nil immediately
	err = c.recordForUpdateReads(table, stmt, nil)
	if err != nil {
		t.Errorf("expected nil error with no active txn, got %v", err)
	}
}

func TestRecordForUpdateReads_MissingTableTree(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree failed: %v", err)
	}
	c := New(tree, pool, nil)

	// A table definition that is NOT in c.tableTrees
	table := &TableDef{
		Name: "phantom_tbl",
		Columns: []ColumnDef{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
		},
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "id"}},
		From:    &query.TableRef{Name: "phantom_tbl"},
	}

	// No active transaction → returns nil
	err = c.recordForUpdateReads(table, stmt, nil)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

// ==================== buildGroupByGroupsFromRows tests (0% coverage) ====================

func TestBuildGroupByGroupsFromRows_BasicGrouping(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree failed: %v", err)
	}
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "gb_test", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	table, ok := c.tables["gb_test"]
	if !ok {
		t.Fatal("table not found")
	}

	rows := [][]interface{}{
		{int64(1), "a", int64(10)},
		{int64(2), "b", int64(20)},
		{int64(3), "a", int64(30)},
		{int64(4), "c", int64(40)},
		{int64(5), "b", int64(50)},
	}

	specs := []groupBySpec{
		{index: 1}, // group by "category" column (index 1)
	}

	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "category"}},
		From:    &query.TableRef{Name: "gb_test"},
		Where:   nil, // no filter
	}

	groups, order, buildErr := c.buildGroupByGroupsFromRows(table, stmt, nil, specs, rows)
	if buildErr != nil {
		t.Fatalf("buildGroupByGroupsFromRows: %v", buildErr)
	}
	if len(groups) != 3 {
		t.Errorf("expected 3 groups, got %d", len(groups))
	}
	if len(order) != 3 {
		t.Errorf("expected 3 group order entries, got %d", len(order))
	}

	// Verify group "a" has 2 rows
	if rowsA, ok := groups[typeTaggedKey("a")]; ok {
		if len(rowsA) != 2 {
			t.Errorf("expected 2 rows in group 'a', got %d", len(rowsA))
		}
	} else {
		t.Error("group 'a' not found")
	}

	// Verify group "b" has 2 rows
	if rowsB, ok := groups[typeTaggedKey("b")]; ok {
		if len(rowsB) != 2 {
			t.Errorf("expected 2 rows in group 'b', got %d", len(rowsB))
		}
	} else {
		t.Error("group 'b' not found")
	}
}

func TestBuildGroupByGroupsFromRows_WithWhereFilter(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, err := btree.NewBTree(pool)
	if err != nil {
		t.Fatalf("NewBTree failed: %v", err)
	}
	c := New(tree, pool, nil)

	createCoverageTestTable(t, c, "gb_filter", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "category", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	table, ok := c.tables["gb_filter"]
	if !ok {
		t.Fatal("table not found")
	}

	rows := [][]interface{}{
		{int64(1), "a", int64(10)},
		{int64(2), "b", int64(20)},
		{int64(3), "a", int64(30)},
		{int64(4), "c", int64(40)},
		{int64(5), "b", int64(50)},
	}

	specs := []groupBySpec{
		{index: 1},
	}

	// WHERE val > 25 — only rows with val=30,40,50 should pass
	stmt := &query.SelectStmt{
		Columns: []query.Expression{&query.Identifier{Name: "category"}},
		From:    &query.TableRef{Name: "gb_filter"},
		Where: &query.BinaryExpr{
			Operator: query.TokenGt,
			Left:     &query.Identifier{Name: "val"},
			Right:    &query.NumberLiteral{Value: 25},
		},
	}

	groups, order, buildErr := c.buildGroupByGroupsFromRows(table, stmt, nil, specs, rows)
	if buildErr != nil {
		t.Fatalf("buildGroupByGroupsFromRows: %v", buildErr)
	}
	// Only groups "a" (val=30) and "c" (val=40) and "b" (val=50) have v > 25
	if len(groups) != 3 {
		t.Errorf("expected 3 groups after WHERE filter, got %d", len(groups))
	}
	if len(order) != 3 {
		t.Errorf("expected 3 group order entries, got %d", len(order))
	}

	// Group "a" should have only 1 row (val=30, not val=10)
	if rowsA, ok := groups[typeTaggedKey("a")]; ok {
		if len(rowsA) != 1 {
			t.Errorf("expected 1 row in group 'a' after filter, got %d", len(rowsA))
		}
	} else {
		t.Error("group 'a' not found")
	}
}

// ==================== computeStdevVar tests (0% coverage) ====================

func TestComputeStdevVar_EmptyValues(t *testing.T) {
	result := computeStdevVar(nil, "STDDEV")
	if result != nil {
		t.Errorf("expected nil for empty values, got %v", result)
	}
}

func TestComputeStdevVar_AllNil(t *testing.T) {
	result := computeStdevVar([]interface{}{nil, nil}, "STDDEV")
	if result != nil {
		t.Errorf("expected nil for all-nil values, got %v", result)
	}
}

func TestComputeStdevVar_StddevPop(t *testing.T) {
	// STDDEV_POP: population standard deviation
	// values: [2, 4, 4, 4, 5, 5, 7, 9], mean = 5
	// variance = (9+1+1+1+0+0+4+16)/8 = 32/8 = 4
	// stdev = sqrt(4) = 2
	values := []interface{}{int64(2), int64(4), int64(4), int64(4), int64(5), int64(5), int64(7), int64(9)}
	result := computeStdevVar(values, "STDDEV_POP")
	stdev, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if math.Abs(stdev-2.0) > 1e-9 {
		t.Errorf("expected STDDEV_POP=2.0, got %v", stdev)
	}
}

func TestComputeStdevVar_VarPop(t *testing.T) {
	values := []interface{}{int64(2), int64(4), int64(4), int64(4), int64(5), int64(5), int64(7), int64(9)}
	result := computeStdevVar(values, "VAR_POP")
	variance, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if math.Abs(variance-4.0) > 1e-9 {
		t.Errorf("expected VAR_POP=4.0, got %v", variance)
	}
}

func TestComputeStdevVar_StddevSamp(t *testing.T) {
	// STDDEV_SAMP: sample standard deviation
	// values: [2, 4, 4, 4, 5, 5, 7, 9], mean = 5
	// variance = 32/7 ≈ 4.5714
	// stdev = sqrt(32/7) ≈ 2.138
	values := []interface{}{int64(2), int64(4), int64(4), int64(4), int64(5), int64(5), int64(7), int64(9)}
	result := computeStdevVar(values, "STDDEV_SAMP")
	stdev, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	expected := math.Sqrt(32.0 / 7.0)
	if math.Abs(stdev-expected) > 1e-9 {
		t.Errorf("expected STDDEV_SAMP=%v, got %v", expected, stdev)
	}
}

func TestComputeStdevVar_VarSamp(t *testing.T) {
	values := []interface{}{int64(2), int64(4), int64(4), int64(4), int64(5), int64(5), int64(7), int64(9)}
	result := computeStdevVar(values, "VAR_SAMP")
	variance, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	expected := 32.0 / 7.0
	if math.Abs(variance-expected) > 1e-9 {
		t.Errorf("expected VAR_SAMP=%v, got %v", expected, variance)
	}
}

func TestComputeStdevVar_VarianceAlias(t *testing.T) {
	values := []interface{}{int64(1), int64(2), int64(3), int64(4)}
	result := computeStdevVar(values, "VARIANCE")
	variance, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	// mean = 2.5, ss = (1.5^2 + 0.5^2 + 0.5^2 + 1.5^2) = 2.25+0.25+0.25+2.25 = 5
	// pop variance = 5/4 = 1.25
	if math.Abs(variance-1.25) > 1e-9 {
		t.Errorf("expected VARIANCE=1.25, got %v", variance)
	}
}

func TestComputeStdevVar_StddevAlias(t *testing.T) {
	values := []interface{}{int64(1), int64(2), int64(3), int64(4)}
	result := computeStdevVar(values, "STDDEV")
	stdev, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	expected := math.Sqrt(1.25)
	if math.Abs(stdev-expected) > 1e-9 {
		t.Errorf("expected STDDEV=%v, got %v", expected, stdev)
	}
}

func TestComputeStdevVar_SingleValuePop(t *testing.T) {
	// Population with n=1: variance = 0/1 = 0, stdev = 0
	values := []interface{}{int64(42)}
	result := computeStdevVar(values, "STDDEV_POP")
	stdev, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	if stdev != 0 {
		t.Errorf("expected STDDEV_POP=0 for single value, got %v", stdev)
	}
}

func TestComputeStdevVar_SingleValueSamp(t *testing.T) {
	// Sample with n=1: n < 2 → nil
	values := []interface{}{int64(42)}
	result := computeStdevVar(values, "STDDEV_SAMP")
	if result != nil {
		t.Errorf("expected nil for STDDEV_SAMP with single value, got %v", result)
	}
}

func TestComputeStdevVar_StringValues(t *testing.T) {
	// String values that can be converted to float64
	values := []interface{}{"2.5", "3.5", "4.0"}
	result := computeStdevVar(values, "STDDEV_POP")
	stdev, ok := result.(float64)
	if !ok {
		t.Fatalf("expected float64, got %T", result)
	}
	// mean = 10/3 ≈ 3.333, ss = (0.833^2 + 0.167^2 + 0.667^2) ≈ 0.694+0.028+0.444 ≈ 1.1667
	// variance = 1.1667/3 ≈ 0.3889, stdev ≈ 0.624
	if stdev <= 0 {
		t.Errorf("expected positive STDDEV for string values, got %v", stdev)
	}
}

// ==================== compareOrderByValues tests (0% coverage) ====================

func TestCompareOrderByValues_BothNilNoNullsSpec(t *testing.T) {
	ob := &query.OrderByExpr{Desc: false}
	cmp := compareOrderByValues(nil, nil, ob)
	if cmp != 0 {
		t.Errorf("expected 0 for both nil with no nulls spec, got %d", cmp)
	}
}

func TestCompareOrderByValues_LeftNilNullsFirst(t *testing.T) {
	ob := &query.OrderByExpr{
		Desc:           false,
		NullsFirst:     true,
		NullsSpecified: true,
	}
	cmp := compareOrderByValues(nil, int64(5), ob)
	if cmp != -1 {
		t.Errorf("expected -1 for nil left with NullsFirst, got %d", cmp)
	}
}

func TestCompareOrderByValues_LeftNilNullsNotFirst(t *testing.T) {
	ob := &query.OrderByExpr{
		Desc:           false,
		NullsFirst:     false,
		NullsSpecified: true,
	}
	cmp := compareOrderByValues(nil, int64(5), ob)
	if cmp != 1 {
		t.Errorf("expected 1 for nil left with !NullsFirst, got %d", cmp)
	}
}

func TestCompareOrderByValues_RightNilNullsFirst(t *testing.T) {
	ob := &query.OrderByExpr{
		Desc:           false,
		NullsFirst:     true,
		NullsSpecified: true,
	}
	cmp := compareOrderByValues(int64(5), nil, ob)
	if cmp != 1 {
		t.Errorf("expected 1 for nil right with NullsFirst, got %d", cmp)
	}
}

func TestCompareOrderByValues_RightNilNullsNotFirst(t *testing.T) {
	ob := &query.OrderByExpr{
		Desc:           false,
		NullsFirst:     false,
		NullsSpecified: true,
	}
	cmp := compareOrderByValues(int64(5), nil, ob)
	if cmp != -1 {
		t.Errorf("expected -1 for nil right with !NullsFirst, got %d", cmp)
	}
}

func TestCompareOrderByValues_Ascending(t *testing.T) {
	ob := &query.OrderByExpr{Desc: false}
	cmp := compareOrderByValues(int64(3), int64(7), ob)
	if cmp != -1 {
		t.Errorf("expected -1 for 3 vs 7 ascending, got %d", cmp)
	}

	cmp = compareOrderByValues(int64(7), int64(3), ob)
	if cmp != 1 {
		t.Errorf("expected 1 for 7 vs 3 ascending, got %d", cmp)
	}

	cmp = compareOrderByValues(int64(5), int64(5), ob)
	if cmp != 0 {
		t.Errorf("expected 0 for equal values ascending, got %d", cmp)
	}
}

func TestCompareOrderByValues_Descending(t *testing.T) {
	ob := &query.OrderByExpr{Desc: true}
	cmp := compareOrderByValues(int64(3), int64(7), ob)
	if cmp != 1 {
		t.Errorf("expected 1 for 3 vs 7 descending, got %d", cmp)
	}

	cmp = compareOrderByValues(int64(7), int64(3), ob)
	if cmp != -1 {
		t.Errorf("expected -1 for 7 vs 3 descending, got %d", cmp)
	}

	cmp = compareOrderByValues(int64(5), int64(5), ob)
	if cmp != 0 {
		t.Errorf("expected 0 for equal values descending, got %d", cmp)
	}
}

// ==================== literalStringValue tests (0% coverage) ====================

func TestLiteralStringValue_StringLiteral(t *testing.T) {
	expr := &query.StringLiteral{Value: "hello"}
	s, ok := literalStringValue(expr)
	if !ok {
		t.Error("expected ok=true for StringLiteral")
	}
	if s != "hello" {
		t.Errorf("expected 'hello', got '%s'", s)
	}
}

func TestLiteralStringValue_NumberLiteralWithRaw(t *testing.T) {
	expr := &query.NumberLiteral{Value: 42.0, Raw: "42"}
	s, ok := literalStringValue(expr)
	if !ok {
		t.Error("expected ok=true for NumberLiteral")
	}
	if s != "42" {
		t.Errorf("expected '42', got '%s'", s)
	}
}

func TestLiteralStringValue_NumberLiteralWithoutRaw(t *testing.T) {
	expr := &query.NumberLiteral{Value: 3.14}
	s, ok := literalStringValue(expr)
	if !ok {
		t.Error("expected ok=true for NumberLiteral")
	}
	if s != "3.14" {
		t.Errorf("expected '3.14', got '%s'", s)
	}
}

func TestLiteralStringValue_NumberLiteralInteger(t *testing.T) {
	expr := &query.NumberLiteral{Value: 100.0}
	s, ok := literalStringValue(expr)
	if !ok {
		t.Error("expected ok=true for NumberLiteral")
	}
	// strconv.FormatFloat(100, 'f', -1, 64) = "100"
	if s != "100" {
		t.Errorf("expected '100', got '%s'", s)
	}
}

func TestLiteralStringValue_BooleanLiteralTrue(t *testing.T) {
	expr := &query.BooleanLiteral{Value: true}
	s, ok := literalStringValue(expr)
	if !ok {
		t.Error("expected ok=true for BooleanLiteral true")
	}
	if s != "true" {
		t.Errorf("expected 'true', got '%s'", s)
	}
}

func TestLiteralStringValue_BooleanLiteralFalse(t *testing.T) {
	expr := &query.BooleanLiteral{Value: false}
	s, ok := literalStringValue(expr)
	if !ok {
		t.Error("expected ok=true for BooleanLiteral false")
	}
	if s != "false" {
		t.Errorf("expected 'false', got '%s'", s)
	}
}

func TestLiteralStringValue_NullLiteral(t *testing.T) {
	expr := &query.NullLiteral{}
	s, ok := literalStringValue(expr)
	if !ok {
		t.Error("expected ok=true for NullLiteral")
	}
	if s != "" {
		t.Errorf("expected '', got '%s'", s)
	}
}

func TestLiteralStringValue_UnknownExpr(t *testing.T) {
	expr := &query.Identifier{Name: "col1"}
	s, ok := literalStringValue(expr)
	if ok {
		t.Error("expected ok=false for Identifier")
	}
	if s != "" {
		t.Errorf("expected empty string for unknown expr, got '%s'", s)
	}
}
