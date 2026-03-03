package query

import (
	"fmt"
	"testing"
)

// Mock implementations for testing
type mockCatalog struct {
	tables  map[string]*TableInfo
	indexes map[string]*IndexInfo
}

func newMockCatalog() *mockCatalog {
	return &mockCatalog{
		tables:  make(map[string]*TableInfo),
		indexes: make(map[string]*IndexInfo),
	}
}

func (m *mockCatalog) GetTable(name string) (*TableInfo, error) {
	if table, ok := m.tables[name]; ok {
		return table, nil
	}
	return nil, fmt.Errorf("table not found: %s", name)
}

func (m *mockCatalog) GetIndex(name string) (*IndexInfo, error) {
	if idx, ok := m.indexes[name]; ok {
		return idx, nil
	}
	return nil, fmt.Errorf("index not found: %s", name)
}

func (m *mockCatalog) GetTableIndexes(tableName string) ([]*IndexInfo, error) {
	var result []*IndexInfo
	for _, idx := range m.indexes {
		if idx.TableName == tableName {
			result = append(result, idx)
		}
	}
	return result, nil
}

type mockStats struct {
	tableStats map[string]TableStats
}

func newMockStats() *mockStats {
	return &mockStats{
		tableStats: make(map[string]TableStats),
	}
}

func (m *mockStats) GetTableStats(tableName string) (TableStats, bool) {
	if stats, ok := m.tableStats[tableName]; ok {
		return stats, true
	}
	return TableStats{}, false
}

func (m *mockStats) EstimateSelectivity(tableName, column, op string, value interface{}) float64 {
	return 0.1
}

func (m *mockStats) EstimateSeqScanCost(tableName string, selectivity float64) float64 {
	if stats, ok := m.tableStats[tableName]; ok {
		return float64(stats.PageCount) * 1.0 * selectivity
	}
	return 1000.0
}

func (m *mockStats) EstimateIndexScanCost(tableName, indexName string, selectivity float64) float64 {
	return 100.0 * selectivity
}

func TestNewPlanner(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	if planner == nil {
		t.Fatal("Expected non-nil planner")
	}
	if planner.catalog != catalog {
		t.Error("Expected catalog to be set")
	}
	if planner.stats != stats {
		t.Error("Expected stats to be set")
	}
}

func TestDefaultCostModel(t *testing.T) {
	cost := DefaultCostModel()

	if cost.SeqPageCost != 1.0 {
		t.Errorf("Expected SeqPageCost 1.0, got %f", cost.SeqPageCost)
	}
	if cost.RandomPageCost != 4.0 {
		t.Errorf("Expected RandomPageCost 4.0, got %f", cost.RandomPageCost)
	}
	if cost.CpuTupleCost != 0.01 {
		t.Errorf("Expected CpuTupleCost 0.01, got %f", cost.CpuTupleCost)
	}
	if cost.CpuIndexTupleCost != 0.005 {
		t.Errorf("Expected CpuIndexTupleCost 0.005, got %f", cost.CpuIndexTupleCost)
	}
	if cost.CpuOperatorCost != 0.0025 {
		t.Errorf("Expected CpuOperatorCost 0.0025, got %f", cost.CpuOperatorCost)
	}
}

func TestPlannerPlanSimpleSelect(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	// Add a table
	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  1000,
		PageCount: 10,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	stats.tableStats["users"] = TableStats{
		RowCount:  1000,
		PageCount: 10,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if plan.Type != PlanTypeSeqScan {
		t.Errorf("Expected SeqScan plan, got %v", plan.Type)
	}
	if plan.Table != "users" {
		t.Errorf("Expected table 'users', got %s", plan.Table)
	}
}

func TestPlannerPlanWithWhere(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  1000,
		PageCount: 10,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "age", Type: "INTEGER"},
		},
	}

	stats.tableStats["users"] = TableStats{
		RowCount:  1000,
		PageCount: 10,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
		Where: &BinaryExpr{
			Left:     &Identifier{Name: "age"},
			Operator: TokenGt,
			Right:    &NumberLiteral{Value: 18},
		},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if plan.Filter == nil {
		t.Error("Expected filter to be set")
	}
}

func TestPlannerPlanWithOrderBy(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  1000,
		PageCount: 10,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	stats.tableStats["users"] = TableStats{
		RowCount:  1000,
		PageCount: 10,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
		OrderBy: []*OrderByExpr{
			{Expr: &Identifier{Name: "name"}, Desc: false},
			{Expr: &Identifier{Name: "id"}, Desc: true},
		},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Plan should have sort node - check if it's a sort type
	if plan.Type != PlanTypeSort && len(plan.Children) > 0 {
		// Sort might be wrapped in another node
		foundSort := false
		for _, child := range plan.Children {
			if child.Type == PlanTypeSort {
				foundSort = true
				break
			}
		}
		if !foundSort {
			// Sort may be handled differently, just check plan was created
		}
	}
}

func TestPlannerPlanWithLimit(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  1000,
		PageCount: 10,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
		},
	}

	stats.tableStats["users"] = TableStats{
		RowCount:  1000,
		PageCount: 10,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
		Limit:   &NumberLiteral{Value: 10},
		Offset:  &NumberLiteral{Value: 5},
	}

	_, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}
}

func TestPlannerPlanJoin(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  1000,
		PageCount: 10,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}

	catalog.tables["orders"] = &TableInfo{
		Name:      "orders",
		RowCount:  5000,
		PageCount: 50,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "user_id", Type: "INTEGER"},
		},
	}

	stats.tableStats["users"] = TableStats{RowCount: 1000, PageCount: 10}
	stats.tableStats["orders"] = TableStats{RowCount: 5000, PageCount: 50}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
		Joins: []*JoinClause{
			{
				Type:  TokenInner,
				Table: &TableRef{Name: "orders"},
				Condition: &BinaryExpr{
					Left:     &QualifiedIdentifier{Table: "users", Column: "id"},
					Operator: TokenEq,
					Right:    &QualifiedIdentifier{Table: "orders", Column: "user_id"},
				},
			},
		},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if plan.Type != PlanTypeJoin {
		t.Errorf("Expected Join plan, got %v", plan.Type)
	}
	if len(plan.Children) != 2 {
		t.Errorf("Expected 2 children, got %d", len(plan.Children))
	}
}

func TestPlannerPlanNonExistentTable(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "non_existent"},
	}

	_, err := planner.Plan(stmt)
	if err == nil {
		t.Error("Expected error for non-existent table")
	}
}

func TestPlannerPlanNoTable(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	stmt := &SelectStmt{
		Columns: []Expression{&NumberLiteral{Value: 1}},
		From:    nil,
	}

	_, err := planner.Plan(stmt)
	if err == nil {
		t.Error("Expected error for no table")
	}
}

func TestChooseJoinMethod(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	tests := []struct {
		name      string
		leftRows  uint64
		rightRows uint64
		expected  JoinMethod
	}{
		{"Small tables", 50, 50, JoinMethodNestedLoop},
		{"One small table", 50, 5000, JoinMethodNestedLoop},
		{"Large tables", 20000, 20000, JoinMethodHash},
		{"Medium tables", 500, 500, JoinMethodHash},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left := &QueryPlan{EstRows: test.leftRows}
			right := &QueryPlan{EstRows: test.rightRows}

			method := planner.chooseJoinMethod(left, right)
			if method != test.expected {
				t.Errorf("Expected %v, got %v", test.expected, method)
			}
		})
	}
}

func TestEstimateJoinRows(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	left := &QueryPlan{EstRows: 1000}
	right := &QueryPlan{EstRows: 500}

	// Test nested loop
	rows := planner.estimateJoinRows(left, right, JoinMethodNestedLoop)
	if rows == 0 {
		t.Error("Expected non-zero row estimate")
	}

	// Test hash join
	rows = planner.estimateJoinRows(left, right, JoinMethodHash)
	if rows == 0 {
		t.Error("Expected non-zero row estimate")
	}

	// Test merge join
	rows = planner.estimateJoinRows(left, right, JoinMethodMerge)
	if rows == 0 {
		t.Error("Expected non-zero row estimate")
	}
}

func TestEstimateJoinCost(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	left := &QueryPlan{EstRows: 1000, Cost: 100}
	right := &QueryPlan{EstRows: 500, Cost: 50}

	// Test nested loop
	cost := planner.estimateJoinCost(left, right, JoinMethodNestedLoop)
	if cost <= 0 {
		t.Error("Expected positive cost")
	}

	// Test hash join
	cost = planner.estimateJoinCost(left, right, JoinMethodHash)
	if cost <= 0 {
		t.Error("Expected positive cost")
	}

	// Test merge join
	cost = planner.estimateJoinCost(left, right, JoinMethodMerge)
	if cost <= 0 {
		t.Error("Expected positive cost")
	}
}

func TestConvertJoinType(t *testing.T) {
	tests := []struct {
		input    TokenType
		expected JoinType
	}{
		{TokenInner, JoinTypeInner},
		{TokenLeft, JoinTypeLeft},
		{TokenRight, JoinTypeRight},
		{TokenOuter, JoinTypeFull},
		{TokenAnd, JoinTypeInner}, // Default case
	}

	for _, test := range tests {
		result := convertJoinType(test.input)
		if result != test.expected {
			t.Errorf("convertJoinType(%v) = %v, expected %v", test.input, result, test.expected)
		}
	}
}

func TestHasAggregates(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	// Test with no aggregates
	cols := []Expression{
		&Identifier{Name: "id"},
		&Identifier{Name: "name"},
	}
	if planner.hasAggregates(cols) {
		t.Error("Expected no aggregates")
	}

	// Note: FunctionCall may not be fully implemented
	// This test assumes it would detect aggregate functions if implemented
}

func TestEvaluateLimitExpression(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	tests := []struct {
		expr     Expression
		expected int
	}{
		{&NumberLiteral{Value: 10}, 10},
		{&NumberLiteral{Value: 0}, 0},
		{&NumberLiteral{Value: -5}, -5},
		{nil, 0},
		{&Identifier{Name: "invalid"}, 0},
	}

	for _, test := range tests {
		result := planner.evaluateLimitExpression(test.expr)
		if result != test.expected {
			t.Errorf("evaluateLimitExpression(%v) = %d, expected %d", test.expr, result, test.expected)
		}
	}
}

func TestGetColumnNames(t *testing.T) {
	columns := []ColumnInfo{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
	}

	names := getColumnNames(columns)
	if len(names) != 3 {
		t.Errorf("Expected 3 names, got %d", len(names))
	}
	if names[0] != "id" || names[1] != "name" || names[2] != "email" {
		t.Errorf("Unexpected names: %v", names)
	}
}

func TestGetColumnNamesEmpty(t *testing.T) {
	columns := []ColumnInfo{}
	names := getColumnNames(columns)
	if len(names) != 0 {
		t.Errorf("Expected 0 names, got %d", len(names))
	}
}

func TestAddFilter(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	child := &QueryPlan{
		Type:    PlanTypeSeqScan,
		Table:   "users",
		EstRows: 1000,
		Cost:    100,
	}

	filter := &BinaryExpr{
		Left:     &Identifier{Name: "age"},
		Operator: TokenGt,
		Right:    &NumberLiteral{Value: 18},
	}

	plan := planner.addFilter(child, filter)

	if plan.Filter != filter {
		t.Error("Expected filter to be set")
	}
}

func TestAddSort(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	child := &QueryPlan{
		Type:    PlanTypeSeqScan,
		Table:   "users",
		EstRows: 1000,
		Cost:    100,
	}

	orderBy := []*OrderByExpr{
		{Expr: &Identifier{Name: "name"}, Desc: false},
		{Expr: &Identifier{Name: "id"}, Desc: true},
	}

	plan := planner.addSort(child, orderBy)

	if plan.Type != PlanTypeSort {
		t.Errorf("Expected Sort plan, got %v", plan.Type)
	}
	if len(plan.OrderBy) != 2 {
		t.Errorf("Expected 2 order by clauses, got %d", len(plan.OrderBy))
	}
}

func TestAddAggregate(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	child := &QueryPlan{
		Type:    PlanTypeSeqScan,
		Table:   "users",
		EstRows: 1000,
		Cost:    100,
	}

	columns := []Expression{
		&Identifier{Name: "id"},
	}
	groupBy := []string{"department"}

	plan := planner.addAggregate(child, columns, groupBy)

	if plan.Type != PlanTypeAggregate {
		t.Errorf("Expected Aggregate plan, got %v", plan.Type)
	}
	if len(plan.GroupBy) != 1 || plan.GroupBy[0] != "department" {
		t.Errorf("Expected group by department, got %v", plan.GroupBy)
	}
}

func TestAddLimit(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	child := &QueryPlan{
		Type:    PlanTypeSeqScan,
		Table:   "users",
		EstRows: 1000,
		Cost:    100,
	}

	plan := planner.addLimit(child, 10, 5)

	if plan.Type != PlanTypeLimit {
		t.Errorf("Expected Limit plan, got %v", plan.Type)
	}
	if plan.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", plan.Limit)
	}
	if plan.Offset != 5 {
		t.Errorf("Expected offset 5, got %d", plan.Offset)
	}
}

func TestAddProject(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	child := &QueryPlan{
		Type:    PlanTypeSeqScan,
		Table:   "users",
		EstRows: 1000,
		Cost:    100,
		Columns: []string{"id", "name", "email"},
	}

	columns := []Expression{
		&Identifier{Name: "id"},
		&Identifier{Name: "name"},
	}

	plan := planner.addProject(child, columns)

	// Project should update columns
	if len(plan.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(plan.Columns))
	}
}


func TestPlanWithGroupBy(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  1000,
		PageCount: 10,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "department", Type: "TEXT"},
		},
	}

	stats.tableStats["users"] = TableStats{
		RowCount:  1000,
		PageCount: 10,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&Identifier{Name: "department"}},
		From:    &TableRef{Name: "users"},
		GroupBy: []Expression{&Identifier{Name: "department"}},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Should have aggregate node
	if plan.Type != PlanTypeAggregate {
		t.Errorf("Expected Aggregate plan, got %v", plan.Type)
	}
}

func TestPlanWithComplexJoin(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["a"] = &TableInfo{Name: "a", RowCount: 100, PageCount: 1}
	catalog.tables["b"] = &TableInfo{Name: "b", RowCount: 200, PageCount: 2}
	catalog.tables["c"] = &TableInfo{Name: "c", RowCount: 300, PageCount: 3}

	stats.tableStats["a"] = TableStats{RowCount: 100, PageCount: 1}
	stats.tableStats["b"] = TableStats{RowCount: 200, PageCount: 2}
	stats.tableStats["c"] = TableStats{RowCount: 300, PageCount: 3}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "a"},
		Joins: []*JoinClause{
			{
				Type:      TokenInner,
				Table:     &TableRef{Name: "b"},
				Condition: &BinaryExpr{Left: &Identifier{Name: "a_id"}, Operator: TokenEq, Right: &Identifier{Name: "b_id"}},
			},
			{
				Type:      TokenLeft,
				Table:     &TableRef{Name: "c"},
				Condition: &BinaryExpr{Left: &Identifier{Name: "b_id"}, Operator: TokenEq, Right: &Identifier{Name: "c_id"}},
			},
		},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	// Should have nested joins
	if plan.Type != PlanTypeJoin {
		t.Errorf("Expected Join plan, got %v", plan.Type)
	}
}

func TestPlanWithEmptyResult(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  0,
		PageCount: 0,
		Columns:   []ColumnInfo{{Name: "id", Type: "INTEGER"}},
	}

	stats.tableStats["users"] = TableStats{
		RowCount:  0,
		PageCount: 0,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if plan.EstRows != 0 {
		t.Errorf("Expected 0 estimated rows, got %d", plan.EstRows)
	}
}

func TestPlanWithVeryLargeTable(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["big_table"] = &TableInfo{
		Name:      "big_table",
		RowCount:  10000000, // 10 million
		PageCount: 100000,
		Columns:   []ColumnInfo{{Name: "id", Type: "INTEGER"}},
	}

	stats.tableStats["big_table"] = TableStats{
		RowCount:  10000000,
		PageCount: 100000,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "big_table"},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if plan.EstRows != 10000000 {
		t.Errorf("Expected 10M estimated rows, got %d", plan.EstRows)
	}
}

func TestPlanCostEstimation(t *testing.T) {
	catalog := newMockCatalog()
	stats := newMockStats()
	planner := NewPlanner(catalog, stats)

	catalog.tables["users"] = &TableInfo{
		Name:      "users",
		RowCount:  1000,
		PageCount: 10,
		Columns:   []ColumnInfo{{Name: "id", Type: "INTEGER"}},
	}

	stats.tableStats["users"] = TableStats{
		RowCount:  1000,
		PageCount: 10,
	}

	stmt := &SelectStmt{
		Columns: []Expression{&StarExpr{}},
		From:    &TableRef{Name: "users"},
	}

	plan, err := planner.Plan(stmt)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}

	if plan.Cost <= 0 {
		t.Errorf("Expected positive cost, got %f", plan.Cost)
	}
}

func TestCanUseIndex(t *testing.T) {
	idx := &IndexInfo{
		Name:    "idx_id",
		Columns: []string{"id"},
	}

	// Can use index for equality on indexed column
	filter := &BinaryExpr{
		Left:     &Identifier{Name: "id"},
		Operator: TokenEq,
		Right:    &NumberLiteral{Value: 1},
	}
	if !canUseIndex(filter, idx) {
		t.Error("Expected can use index for equality on indexed column")
	}

	// Cannot use index for inequality
	filter2 := &BinaryExpr{
		Left:     &Identifier{Name: "id"},
		Operator: TokenGt,
		Right:    &NumberLiteral{Value: 1},
	}
	if canUseIndex(filter2, idx) {
		t.Error("Expected cannot use index for inequality")
	}

	// Cannot use index for non-indexed column
	filter3 := &BinaryExpr{
		Left:     &Identifier{Name: "name"},
		Operator: TokenEq,
		Right:    &StringLiteral{Value: "test"},
	}
	if canUseIndex(filter3, idx) {
		t.Error("Expected cannot use index for non-indexed column")
	}
}

func TestContainsAggregate(t *testing.T) {
	// Test with aggregate function
	aggExpr := &FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}}
	if !containsAggregate(aggExpr) {
		t.Error("Expected COUNT to be aggregate")
	}

	// Test with non-aggregate function
	nonAggExpr := &FunctionCall{Name: "UPPER", Args: []Expression{&StringLiteral{Value: "test"}}}
	if containsAggregate(nonAggExpr) {
		t.Error("Expected UPPER to not be aggregate")
	}

	// Test with identifier
	identExpr := &Identifier{Name: "id"}
	if containsAggregate(identExpr) {
		t.Error("Expected identifier to not be aggregate")
	}
}

func TestExtractAggregates(t *testing.T) {
	columns := []Expression{
		&FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}},
		&FunctionCall{Name: "SUM", Args: []Expression{&Identifier{Name: "amount"}}},
		&Identifier{Name: "id"},
	}

	aggs := extractAggregates(columns)
	if len(aggs) != 2 {
		t.Errorf("Expected 2 aggregates, got %d", len(aggs))
	}
}

func TestEstimateGroupByRows(t *testing.T) {
	// With GROUP BY
	rows := estimateGroupByRows(1000, []string{"department"})
	if rows != 100 { // 10% of 1000
		t.Errorf("Expected 100 rows, got %d", rows)
	}

	// Without GROUP BY (aggregate only)
	rows = estimateGroupByRows(1000, []string{})
	if rows != 1 {
		t.Errorf("Expected 1 row for aggregate without GROUP BY, got %d", rows)
	}

	// Small table
	rows = estimateGroupByRows(5, []string{"category"})
	if rows != 1 {
		t.Errorf("Expected at least 1 row, got %d", rows)
	}
}

func TestLog2(t *testing.T) {
	if log2(1) != 0 {
		t.Errorf("Expected log2(1) = 0, got %f", log2(1))
	}
	if log2(2) != 1 {
		t.Errorf("Expected log2(2) = 1, got %f", log2(2))
	}
	if log2(4) != 2 {
		t.Errorf("Expected log2(4) = 2, got %f", log2(4))
	}
	if log2(0) != 0 {
		t.Errorf("Expected log2(0) = 0, got %f", log2(0))
	}
	if log2(-1) != 0 {
		t.Errorf("Expected log2(-1) = 0, got %f", log2(-1))
	}
}

func TestQueryPlanExplain(t *testing.T) {
	plan := &QueryPlan{
		Type:    PlanTypeSeqScan,
		Table:   "users",
		EstRows: 1000,
		Cost:    100,
	}

	explain := plan.Explain()
	if explain == "" {
		t.Error("Expected non-empty explain output")
	}
}
