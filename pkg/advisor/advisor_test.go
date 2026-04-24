package advisor

import (
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

func TestAdvisorAnalyzeSelect(t *testing.T) {
	a := NewIndexAdvisor()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "email"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "test"},
		},
		OrderBy: []*query.OrderByExpr{
			{Expr: &query.Identifier{Name: "created_at"}},
		},
	}

	a.Analyze(stmt)

	recs := a.Recommendations(nil)
	if len(recs) == 0 {
		t.Fatal("expected recommendations")
	}

	foundEmail := false
	foundCreatedAt := false
	for _, r := range recs {
		if r.TableName == "users" {
			if len(r.Columns) == 1 && r.Columns[0] == "email" {
				foundEmail = true
			}
			if len(r.Columns) == 1 && r.Columns[0] == "created_at" {
				foundCreatedAt = true
			}
		}
	}
	if !foundEmail {
		t.Error("expected recommendation for email column")
	}
	if !foundCreatedAt {
		t.Error("expected recommendation for created_at column")
	}
}

func TestAdvisorSkipsExistingIndex(t *testing.T) {
	a := NewIndexAdvisor()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "email"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "test"},
		},
	}

	a.Analyze(stmt)

	existing := map[string][][]string{
		"users": {{"email"}},
	}
	recs := a.Recommendations(existing)
	for _, r := range recs {
		if r.TableName == "users" && len(r.Columns) == 1 && r.Columns[0] == "email" {
			t.Error("should not recommend existing index")
		}
	}
}

func TestAdvisorCompositeRecommendation(t *testing.T) {
	a := NewIndexAdvisor()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "orders"},
		Where: &query.BinaryExpr{
			Left: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "user_id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: 1},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "status"},
				Operator: query.TokenEq,
				Right:    &query.StringLiteral{Value: "pending"},
			},
		},
	}

	a.Analyze(stmt)

	recs := a.Recommendations(nil)
	var composite *IndexRecommendation
	for _, r := range recs {
		if r.TableName == "orders" && len(r.Columns) > 1 {
			composite = r
			break
		}
	}
	if composite == nil {
		t.Fatal("expected composite index recommendation")
	}
	if len(composite.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(composite.Columns))
	}
}

func TestAdvisorAnalyzeUpdate(t *testing.T) {
	a := NewIndexAdvisor()
	stmt := &query.UpdateStmt{
		Table: "products",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "sku"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "ABC"},
		},
		Set: []*query.SetClause{
			{Column: "price"},
		},
	}

	a.Analyze(stmt)

	recs := a.Recommendations(nil)
	found := false
	for _, r := range recs {
		if r.TableName == "products" && len(r.Columns) == 1 && r.Columns[0] == "sku" {
			found = true
		}
	}
	if !found {
		t.Error("expected recommendation for sku in UPDATE WHERE")
	}
}

func TestAdvisorAnalyzeDelete(t *testing.T) {
	a := NewIndexAdvisor()
	stmt := &query.DeleteStmt{
		Table: "logs",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "level"},
			Operator: query.TokenEq,
			Right:    &query.StringLiteral{Value: "error"},
		},
	}

	a.Analyze(stmt)

	recs := a.Recommendations(nil)
	found := false
	for _, r := range recs {
		if r.TableName == "logs" && len(r.Columns) == 1 && r.Columns[0] == "level" {
			found = true
		}
	}
	if !found {
		t.Error("expected recommendation for level in DELETE WHERE")
	}
}

func TestAdvisorJoinColumns(t *testing.T) {
	a := NewIndexAdvisor()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Joins: []*query.JoinClause{
			{
				Table: &query.TableRef{Name: "orders"},
				Condition: &query.BinaryExpr{
					Left:     &query.QualifiedIdentifier{Table: "orders", Column: "user_id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "users", Column: "id"},
				},
			},
		},
	}

	a.Analyze(stmt)

	recs := a.Recommendations(nil)
	foundOrders := false
	for _, r := range recs {
		if r.TableName == "orders" && containsColumn(r.Columns, "user_id") {
			foundOrders = true
		}
	}
	if !foundOrders {
		t.Error("expected recommendation for orders.user_id join column")
	}
}

func TestAdvisorPrioritySorting(t *testing.T) {
	a := NewIndexAdvisor()

	// Run many queries against email, fewer against name
	for i := 0; i < 10; i++ {
		a.Analyze(&query.SelectStmt{
			From: &query.TableRef{Name: "users"},
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "email"},
				Operator: query.TokenEq,
				Right:    &query.StringLiteral{Value: "x"},
			},
		})
	}
	for i := 0; i < 2; i++ {
		a.Analyze(&query.SelectStmt{
			From: &query.TableRef{Name: "users"},
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "name"},
				Operator: query.TokenEq,
				Right:    &query.StringLiteral{Value: "y"},
			},
		})
	}

	recs := a.Recommendations(nil)
	if len(recs) < 2 {
		t.Fatal("expected at least 2 recommendations")
	}

	// email should have higher priority than name
	var emailIdx, nameIdx int
	for i, r := range recs {
		if r.TableName == "users" && len(r.Columns) == 1 {
			if r.Columns[0] == "email" {
				emailIdx = i
			}
			if r.Columns[0] == "name" {
				nameIdx = i
			}
		}
	}
	if emailIdx >= nameIdx {
		t.Error("expected email recommendation to have higher priority than name")
	}
}

func TestAdvisorReset(t *testing.T) {
	a := NewIndexAdvisor()
	a.Analyze(&query.SelectStmt{
		From:  &query.TableRef{Name: "t"},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "c"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	})

	if len(a.Recommendations(nil)) == 0 {
		t.Fatal("expected recommendations before reset")
	}

	a.Reset()
	if len(a.Recommendations(nil)) != 0 {
		t.Error("expected no recommendations after reset")
	}
}

func TestAdvisorSnapshot(t *testing.T) {
	a := NewIndexAdvisor()
	a.Analyze(&query.SelectStmt{
		From:  &query.TableRef{Name: "t"},
		Where: &query.BinaryExpr{Left: &query.Identifier{Name: "c"}, Operator: query.TokenEq, Right: &query.NumberLiteral{Value: 1}},
	})

	snap := a.Snapshot()
	if len(snap) == 0 {
		t.Fatal("expected snapshot data")
	}
	if _, ok := snap["t"]; !ok {
		t.Fatal("expected table t in snapshot")
	}
}

func TestAdvisorIgnoresInsert(t *testing.T) {
	a := NewIndexAdvisor()
	a.Analyze(&query.InsertStmt{Table: "users", Columns: []string{"email"}})
	recs := a.Recommendations(nil)
	for range rangeRecsForTable(recs, "users") {
		t.Error("INSERT should not generate recommendations")
	}
}

func TestAdvisorPrefixOfExisting(t *testing.T) {
	a := NewIndexAdvisor()
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "a"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}
	a.Analyze(stmt)

	stmt2 := &query.SelectStmt{
		From: &query.TableRef{Name: "users"},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "b"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 2},
		},
	}
	a.Analyze(stmt2)

	// Existing composite index (a, b) covers both single-column lookups
	existing := map[string][][]string{
		"users": {{"a", "b"}},
	}
	recs := a.Recommendations(existing)
	for _, r := range recs {
		if r.TableName == "users" && len(r.Columns) == 1 && (r.Columns[0] == "a" || r.Columns[0] == "b") {
			t.Error("single-column recommendations should be suppressed by prefix composite index")
		}
	}
}

func TestAdvisorExtractColumnsComplexExpressions(t *testing.T) {
	a := NewIndexAdvisor()

	// Test UnaryExpr (NOT), LikeExpr, IsNullExpr, BetweenExpr, CastExpr
	stmt := &query.SelectStmt{
		From: &query.TableRef{Name: "items"},
		Where: &query.BinaryExpr{
			Left: &query.UnaryExpr{
				Operator: query.TokenNot,
				Expr:     &query.Identifier{Name: "deleted"},
			},
			Operator: query.TokenAnd,
			Right: &query.BinaryExpr{
				Left: &query.LikeExpr{
					Expr:    &query.Identifier{Name: "name"},
					Pattern: &query.StringLiteral{Value: "%test%"},
				},
				Operator: query.TokenAnd,
				Right: &query.BinaryExpr{
					Left: &query.IsNullExpr{
						Expr: &query.Identifier{Name: "archived"},
						Not:  true,
					},
					Operator: query.TokenAnd,
					Right: &query.BetweenExpr{
						Expr:  &query.Identifier{Name: "price"},
						Lower: &query.NumberLiteral{Value: 10},
						Upper: &query.NumberLiteral{Value: 100},
					},
				},
			},
		},
	}
	a.Analyze(stmt)

	// Test FunctionCall, CaseExpr
	stmt2 := &query.SelectStmt{
		From: &query.TableRef{Name: "items"},
		Where: &query.FunctionCall{
			Name: "COALESCE",
			Args: []query.Expression{
				&query.Identifier{Name: "discount"},
				&query.NumberLiteral{Value: 0},
			},
		},
	}
	a.Analyze(stmt2)

	// Test CastExpr
	stmt3 := &query.SelectStmt{
		From: &query.TableRef{Name: "items"},
		Where: &query.CastExpr{
			Expr:     &query.Identifier{Name: "code"},
			DataType: query.TokenInteger,
		},
	}
	a.Analyze(stmt3)

	// Test CaseExpr
	stmt4 := &query.SelectStmt{
		From: &query.TableRef{Name: "items"},
		Where: &query.CaseExpr{
			Expr: &query.Identifier{Name: "status"},
			Whens: []*query.WhenClause{
				{Condition: &query.StringLiteral{Value: "active"}, Result: &query.Identifier{Name: "active_flag"}},
			},
			Else: &query.Identifier{Name: "default_flag"},
		},
	}
	a.Analyze(stmt4)

	// Test ExistsExpr with Subquery
	stmt5 := &query.SelectStmt{
		From: &query.TableRef{Name: "items"},
		Where: &query.ExistsExpr{
			Subquery: &query.SelectStmt{
				From: &query.TableRef{Name: "reviews"},
				Where: &query.BinaryExpr{
					Left:     &query.QualifiedIdentifier{Table: "reviews", Column: "item_id"},
					Operator: query.TokenEq,
					Right:    &query.QualifiedIdentifier{Table: "items", Column: "id"},
				},
			},
		},
	}
	a.Analyze(stmt5)

	// Test InExpr with Subquery
	stmt6 := &query.SelectStmt{
		From: &query.TableRef{Name: "items"},
		Where: &query.InExpr{
			Expr: &query.Identifier{Name: "category_id"},
			Subquery: &query.SelectStmt{
				From: &query.TableRef{Name: "categories"},
				Where: &query.BinaryExpr{
					Left:     &query.Identifier{Name: "active"},
					Operator: query.TokenEq,
					Right:    &query.BooleanLiteral{Value: true},
				},
			},
		},
	}
	a.Analyze(stmt6)

	recs := a.Recommendations(nil)
	expectedCols := map[string]bool{
		"deleted":      true,
		"name":         true,
		"archived":     true,
		"price":        true,
		"discount":     true,
		"code":         true,
		"status":       true,
		"active_flag":  true,
		"default_flag": true,
		"category_id":  true,
	}

	for _, r := range recs {
		if r.TableName == "items" && len(r.Columns) == 1 {
			delete(expectedCols, r.Columns[0])
		}
	}

	if len(expectedCols) > 0 {
		missing := make([]string, 0, len(expectedCols))
		for c := range expectedCols {
			missing = append(missing, c)
		}
		t.Errorf("missing recommendations for columns: %v", missing)
	}
}

func containsColumn(cols []string, target string) bool {
	for _, c := range cols {
		if strings.EqualFold(c, target) {
			return true
		}
	}
	return false
}

func rangeRecsForTable(recs []*IndexRecommendation, table string) []*IndexRecommendation {
	var out []*IndexRecommendation
	for _, r := range recs {
		if r.TableName == table {
			out = append(out, r)
		}
	}
	return out
}
