package optimizer

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()
	if !config.Enabled {
		t.Error("Optimizer should be enabled by default")
	}
	if !config.EnableIndexSelection {
		t.Error("Index selection should be enabled by default")
	}
	if config.MaxJoinReorderTables != 6 {
		t.Errorf("Expected max 6 tables for join reorder, got %d", config.MaxJoinReorderTables)
	}
}

func TestOptimizerCreation(t *testing.T) {
	opt := New(nil, nil)
	if opt == nil {
		t.Fatal("Failed to create optimizer")
	}
	if opt.config == nil {
		t.Error("Config should not be nil")
	}
	if opt.stats == nil {
		t.Error("Stats should not be nil")
	}
}

func TestOptimizeDisabled(t *testing.T) {
	config := DefaultConfig()
	config.Enabled = false

	opt := New(config, nil)

	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
	}

	optimized, err := opt.Optimize(stmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	if optimized != stmt {
		t.Error("Should return same statement when disabled")
	}
}

func TestReorderJoins(t *testing.T) {
	opt := New(DefaultConfig(), nil)

	// Create a statement with joins
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Joins: []*query.JoinClause{
			{Type: query.TokenCross, Table: &query.TableRef{Name: "orders"}},
			{Type: query.TokenInner, Table: &query.TableRef{Name: "products"}},
		},
	}

	// Add statistics to influence ordering
	opt.stats.TableStats["orders"] = &TableStatistics{
		TableName: "orders",
		RowCount:  100000,
	}
	opt.stats.TableStats["products"] = &TableStatistics{
		TableName: "products",
		RowCount:  1000,
	}

	optimized, err := opt.Optimize(stmt)
	if err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// Inner join should come before cross join
	if optimized.Joins[0].Type != query.TokenInner {
		t.Error("Inner join should be first (more selective)")
	}
}

func TestSelectBestIndex(t *testing.T) {
	stats := &Statistics{
		TableStats: map[string]*TableStatistics{
			"users": {
				TableName: "users",
				IndexStats: map[string]*IndexStatistics{
					"idx_name": {
						IndexName:   "idx_name",
						Columns:     []string{"name"},
						IsUnique:    false,
						Selectivity: 0.1,
					},
					"idx_email_unique": {
						IndexName:   "idx_email_unique",
						Columns:     []string{"email"},
						IsUnique:    true,
						Selectivity: 1.0,
					},
				},
			},
		},
	}

	opt := New(DefaultConfig(), stats)

	// WHERE condition on email
	where := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "email"},
		Operator: query.TokenEq,
		Right:    &query.StringLiteral{Value: "test@example.com"},
	}

	bestIndex := opt.SelectBestIndex("users", where)
	if bestIndex != "idx_email_unique" {
		t.Errorf("Expected unique email index, got %s", bestIndex)
	}
}

func TestSelectBestIndexDisabled(t *testing.T) {
	config := DefaultConfig()
	config.EnableIndexSelection = false

	opt := New(config, nil)

	bestIndex := opt.SelectBestIndex("users", nil)
	if bestIndex != "" {
		t.Error("Should return empty when index selection disabled")
	}
}

func TestExtractColumnReferences(t *testing.T) {
	opt := New(DefaultConfig(), nil)

	expr := &query.BinaryExpr{
		Left:     &query.Identifier{Name: "id"},
		Operator: query.TokenEq,
		Right:    &query.QualifiedIdentifier{Table: "users", Column: "user_id"},
	}

	columns := opt.extractColumnReferences(expr)
	if len(columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(columns))
	}

	found := make(map[string]bool)
	for _, col := range columns {
		found[col] = true
	}

	if !found["id"] {
		t.Error("Should find 'id' column")
	}
	if !found["user_id"] {
		t.Error("Should find 'user_id' column")
	}
}

func TestScoreIndex(t *testing.T) {
	opt := New(DefaultConfig(), nil)

	tests := []struct {
		name      string
		columns   []string
		index     IndexStatistics
		wantScore float64
	}{
		{
			name:      "matching single column",
			columns:   []string{"name"},
			index:     IndexStatistics{Columns: []string{"name"}, IsUnique: false, Selectivity: 0.1},
			wantScore: 11.0, // 10 * 1.1
		},
		{
			name:      "unique index bonus",
			columns:   []string{"id"},
			index:     IndexStatistics{Columns: []string{"id"}, IsUnique: true, Selectivity: 1.0},
			wantScore: 40.0, // 10 * 2 * 2
		},
		{
			name:      "no match",
			columns:   []string{"email"},
			index:     IndexStatistics{Columns: []string{"name"}, IsUnique: false},
			wantScore: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			score := opt.scoreIndex(test.columns, &test.index)
			if score != test.wantScore {
				t.Errorf("scoreIndex() = %f, want %f", score, test.wantScore)
			}
		})
	}
}

func TestEstimateJoinSelectivity(t *testing.T) {
	stats := &Statistics{
		TableStats: map[string]*TableStatistics{
			"large_table": {RowCount: 100000},
			"small_table": {RowCount: 100},
		},
	}

	opt := New(DefaultConfig(), stats)

	tests := []struct {
		tableName  string
		joinType   query.TokenType
		wantMaxSel float64
	}{
		{"large_table", query.TokenInner, 0.1},
		{"small_table", query.TokenLeft, 0.5},
		{"unknown_table", query.TokenCross, 1.0},
	}

	for _, test := range tests {
		join := &query.JoinClause{
			Type:  test.joinType,
			Table: &query.TableRef{Name: test.tableName},
		}

		sel := opt.estimateJoinSelectivity(join)
		if sel > test.wantMaxSel {
			t.Errorf("Selectivity for %s %v = %f, want <= %f",
				test.tableName, test.joinType, sel, test.wantMaxSel)
		}
	}
}

func TestUpdateStatistics(t *testing.T) {
	opt := New(DefaultConfig(), nil)

	stats := &TableStatistics{
		TableName:   "users",
		RowCount:    1000,
		ColumnStats: map[string]*ColumnStatistics{"id": {ColumnName: "id", DistinctValues: 1000}},
		IndexStats: map[string]*IndexStatistics{
			"idx_users_id": {IndexName: "idx_users_id", TableName: "users", Columns: []string{"id"}, Selectivity: 0.01},
		},
	}

	opt.UpdateStatistics("users", stats)
	stats.RowCount = 1
	stats.ColumnStats["id"].DistinctValues = 1
	stats.IndexStats["idx_users_id"].Columns[0] = "mutated"

	retrieved := opt.GetTableStatistics("users")
	if retrieved == nil {
		t.Fatal("Should retrieve statistics")
	}

	if retrieved.RowCount != 1000 {
		t.Errorf("Expected 1000 rows, got %d", retrieved.RowCount)
	}
	if retrieved.ColumnStats["id"].DistinctValues != 1000 {
		t.Errorf("UpdateStatistics retained caller-owned column stats: %+v", retrieved.ColumnStats["id"])
	}
	if retrieved.IndexStats["idx_users_id"].Columns[0] != "id" {
		t.Errorf("UpdateStatistics retained caller-owned index columns: %+v", retrieved.IndexStats["idx_users_id"].Columns)
	}

	retrieved.RowCount = 2
	retrieved.ColumnStats["id"].DistinctValues = 2
	retrieved.IndexStats["idx_users_id"].Columns[0] = "external"

	retrievedAgain := opt.GetTableStatistics("users")
	if retrievedAgain.RowCount != 1000 {
		t.Errorf("GetTableStatistics returned mutable stats: got %d", retrievedAgain.RowCount)
	}
	if retrievedAgain.ColumnStats["id"].DistinctValues != 1000 {
		t.Errorf("GetTableStatistics returned mutable column stats: %+v", retrievedAgain.ColumnStats["id"])
	}
	if retrievedAgain.IndexStats["idx_users_id"].Columns[0] != "id" {
		t.Errorf("GetTableStatistics returned mutable index columns: %+v", retrievedAgain.IndexStats["idx_users_id"].Columns)
	}
}

func TestNewCopiesConfigAndStatistics(t *testing.T) {
	config := DefaultConfig()
	config.EnableJoinReorder = true
	stats := &Statistics{TableStats: map[string]*TableStatistics{
		"users": {
			TableName: "users",
			RowCount:  500,
			IndexStats: map[string]*IndexStatistics{
				"idx": {IndexName: "idx", Columns: []string{"id"}},
			},
		},
	}}

	opt := New(config, stats)
	config.EnableJoinReorder = false
	stats.TableStats["users"].RowCount = 1
	stats.TableStats["users"].IndexStats["idx"].Columns[0] = "mutated"

	if !opt.config.EnableJoinReorder {
		t.Fatal("New retained caller-owned config")
	}
	retrieved := opt.GetTableStatistics("users")
	if retrieved.RowCount != 500 {
		t.Fatalf("New retained caller-owned stats: got row count %d", retrieved.RowCount)
	}
	if retrieved.IndexStats["idx"].Columns[0] != "id" {
		t.Fatalf("New retained caller-owned index stats: %v", retrieved.IndexStats["idx"].Columns)
	}
}
