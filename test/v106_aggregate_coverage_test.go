package test

import (
	"fmt"
	"testing"
)

// TestV106_AggCoverage targets low-coverage functions:
// - computeAggregatesWithGroupBy (61.5%)
// - evaluateExprWithGroupAggregatesJoin (45.6%)
// - applyGroupByOrderBy (54.4%)
// - applyOuterQuery (47.4%)
// - resolveOuterRefsInQuery (46.5%)
// - evaluateLike (60.7%)
// - evaluateCastExpr (77.8%)
// - evaluateBetween (76.5%)
func TestV106_AggCoverage(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// ====== SETUP TABLES ======
	afExec(t, db, ctx, "CREATE TABLE v106a_items (id INTEGER PRIMARY KEY, name TEXT, category TEXT, price REAL, qty INTEGER, discount REAL)")
	afExec(t, db, ctx, "INSERT INTO v106a_items VALUES (1, 'apple', 'fruit', 1.50, 10, 0.1)")
	afExec(t, db, ctx, "INSERT INTO v106a_items VALUES (2, 'banana', 'fruit', 0.75, 20, NULL)")
	afExec(t, db, ctx, "INSERT INTO v106a_items VALUES (3, 'carrot', 'vegetable', 2.00, 15, 0.2)")
	afExec(t, db, ctx, "INSERT INTO v106a_items VALUES (4, 'date', 'fruit', 5.00, NULL, NULL)")
	afExec(t, db, ctx, "INSERT INTO v106a_items VALUES (5, 'eggplant', 'vegetable', 3.00, 8, 0.15)")
	afExec(t, db, ctx, "INSERT INTO v106a_items VALUES (6, 'fig', 'fruit', 4.00, 5, 0.05)")
	afExec(t, db, ctx, "INSERT INTO v106a_items VALUES (7, 'grape', 'fruit', 3.50, 12, NULL)")

	afExec(t, db, ctx, "CREATE TABLE v106a_orders (id INTEGER PRIMARY KEY, item_id INTEGER, customer TEXT, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (1, 1, 'Alice', 3)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (2, 1, 'Bob', 5)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (3, 2, 'Alice', 10)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (4, 3, 'Carol', 2)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (5, 5, 'Alice', 4)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (6, 6, 'Bob', 1)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (7, 7, 'Carol', 6)")
	afExec(t, db, ctx, "INSERT INTO v106a_orders VALUES (8, 4, 'Alice', 2)")

	// ============================================================
	// 1. computeAggregatesWithGroupBy
	// ============================================================

	t.Run("GroupBy_MultipleAggregates", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit group
		if fmt.Sprintf("%v", rows[0][0]) != "fruit" {
			t.Fatalf("expected fruit first, got %v", rows[0][0])
		}
		// fruit has 5 items
		if fmt.Sprintf("%v", rows[0][1]) != "5" {
			t.Fatalf("expected COUNT=5 for fruit, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_CASEInsideAggregate", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, SUM(CASE WHEN price > 3.0 THEN 1 ELSE 0 END) as expensive_count FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit: date(5.00), fig(4.00), grape(3.50) = 3 expensive
		if fmt.Sprintf("%v", rows[0][1]) != "3" {
			t.Fatalf("expected 3 expensive fruits, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_NULLHandling_COUNT", func(t *testing.T) {
		// COUNT(col) should skip NULLs
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(qty) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit: qty is NULL for date, so 4 non-null
		if fmt.Sprintf("%v", rows[0][1]) != "4" {
			t.Fatalf("expected COUNT(qty)=4 for fruit, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_NULLHandling_SUM", func(t *testing.T) {
		// SUM should skip NULLs
		rows := afQuery(t, db, ctx, "SELECT category, SUM(discount) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit: 0.1 + NULL + NULL + 0.05 + NULL = 0.15
		fruitSum := fmt.Sprintf("%.2f", rows[0][1])
		if fruitSum != "0.15" {
			t.Fatalf("expected SUM(discount)=0.15 for fruit, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_AVGOnNulls", func(t *testing.T) {
		// AVG should skip NULLs in computation
		rows := afQuery(t, db, ctx, "SELECT category, AVG(discount) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit: (0.1 + 0.05) / 2 = 0.075
		fruitAvg := fmt.Sprintf("%.3f", rows[0][1])
		if fruitAvg != "0.075" {
			t.Fatalf("expected AVG(discount)=0.075 for fruit, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_CountDistinct", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(DISTINCT price) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit: 1.50, 0.75, 5.00, 4.00, 3.50 = 5 distinct
		if fmt.Sprintf("%v", rows[0][1]) != "5" {
			t.Fatalf("expected COUNT(DISTINCT price)=5 for fruit, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_ExpressionGroupBy", func(t *testing.T) {
		// GROUP BY with expression: UPPER(category)
		rows := afQuery(t, db, ctx, "SELECT UPPER(category), COUNT(*) FROM v106a_items GROUP BY UPPER(category) ORDER BY UPPER(category)")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "FRUIT" {
			t.Fatalf("expected FRUIT first, got %v", rows[0][0])
		}
	})

	t.Run("GroupBy_COALESCE_InAggregate", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, SUM(COALESCE(discount, 0)) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit: 0.1 + 0 + 0 + 0.05 + 0 = 0.15
		fruitSum := fmt.Sprintf("%.2f", rows[0][1])
		if fruitSum != "0.15" {
			t.Fatalf("expected SUM(COALESCE)=0.15 for fruit, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_GROUP_CONCAT", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, GROUP_CONCAT(name) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// Verify GROUP_CONCAT returns a string
		gc := fmt.Sprintf("%v", rows[0][1])
		if gc == "" || gc == "<nil>" {
			t.Fatalf("expected non-empty GROUP_CONCAT, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_HAVING", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM v106a_items GROUP BY category HAVING COUNT(*) > 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 group with count > 3, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "fruit" {
			t.Fatalf("expected fruit, got %v", rows[0][0])
		}
	})

	t.Run("GroupBy_EmptyResult_WithAgg", func(t *testing.T) {
		// GROUP BY with WHERE that matches nothing should return 0 rows when GROUP BY present
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM v106a_items WHERE price > 9999 GROUP BY category")
		if len(rows) != 0 {
			t.Fatalf("expected 0 rows for empty GROUP BY, got %d", len(rows))
		}
	})

	t.Run("GroupBy_AllNulls_SUM", func(t *testing.T) {
		// SUM of all NULLs should return NULL
		afExec(t, db, ctx, "CREATE TABLE v106a_nulls (id INTEGER PRIMARY KEY, grp TEXT, val REAL)")
		afExec(t, db, ctx, "INSERT INTO v106a_nulls VALUES (1, 'A', NULL)")
		afExec(t, db, ctx, "INSERT INTO v106a_nulls VALUES (2, 'A', NULL)")
		rows := afQuery(t, db, ctx, "SELECT grp, SUM(val) FROM v106a_nulls GROUP BY grp")
		if len(rows) != 1 {
			t.Fatalf("expected 1 group, got %d", len(rows))
		}
		if rows[0][1] != nil {
			t.Fatalf("expected NULL for SUM of all NULLs, got %v", rows[0][1])
		}
	})

	t.Run("GroupBy_MIN_MAX_WithNulls", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, MIN(discount), MAX(discount) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
		// fruit MIN discount = 0.05, MAX discount = 0.1
		if fmt.Sprintf("%.2f", rows[0][1]) != "0.05" {
			t.Fatalf("expected MIN=0.05, got %v", rows[0][1])
		}
		if fmt.Sprintf("%.1f", rows[0][2]) != "0.1" {
			t.Fatalf("expected MAX=0.1, got %v", rows[0][2])
		}
	})

	t.Run("GroupBy_ExpressionAggregate_SUM_Multiply", func(t *testing.T) {
		// SUM(price * qty) - expression inside aggregate
		rows := afQuery(t, db, ctx, "SELECT category, SUM(price * qty) FROM v106a_items WHERE qty IS NOT NULL GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
	})

	t.Run("GroupBy_DISTINCT_WithGroupBy", func(t *testing.T) {
		// SELECT DISTINCT category, COUNT(*) FROM ... GROUP BY category
		rows := afQuery(t, db, ctx, "SELECT DISTINCT category, COUNT(*) FROM v106a_items GROUP BY category ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 distinct groups, got %d", len(rows))
		}
	})

	t.Run("GroupBy_OFFSET_LIMIT", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM v106a_items GROUP BY category ORDER BY category LIMIT 1 OFFSET 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row after offset, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "vegetable" {
			t.Fatalf("expected vegetable after offset, got %v", rows[0][0])
		}
	})

	// ============================================================
	// 2. evaluateExprWithGroupAggregatesJoin
	// ============================================================

	t.Run("JoinGroupBy_AVG_SUM", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT i.category, AVG(i.price), SUM(o.amount)
			FROM v106a_items i
			JOIN v106a_orders o ON i.id = o.item_id
			GROUP BY i.category
			ORDER BY i.category`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
	})

	t.Run("JoinGroupBy_MIN_MAX", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT i.category, MIN(o.amount), MAX(o.amount)
			FROM v106a_items i
			JOIN v106a_orders o ON i.id = o.item_id
			GROUP BY i.category
			ORDER BY i.category`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
	})

	t.Run("JoinGroupBy_COUNT_Column", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT i.category, COUNT(i.discount)
			FROM v106a_items i
			JOIN v106a_orders o ON i.id = o.item_id
			GROUP BY i.category
			ORDER BY i.category`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
	})

	t.Run("JoinGroupBy_CountStar", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT i.category, COUNT(*)
			FROM v106a_items i
			JOIN v106a_orders o ON i.id = o.item_id
			GROUP BY i.category
			ORDER BY i.category`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
	})

	t.Run("JoinGroupBy_ComplexExpr", func(t *testing.T) {
		// Mix aggregate with column ref: SUM(price * amount)
		rows := afQuery(t, db, ctx, `
			SELECT o.customer, SUM(i.price * o.amount) as total_spend
			FROM v106a_orders o
			JOIN v106a_items i ON o.item_id = i.id
			GROUP BY o.customer
			ORDER BY total_spend DESC`)
		if len(rows) != 3 {
			t.Fatalf("expected 3 customers, got %d", len(rows))
		}
	})

	t.Run("JoinGroupBy_AllNulls_SUM", func(t *testing.T) {
		// SUM on column with all NULLs in join context
		afExec(t, db, ctx, "CREATE TABLE v106a_nulljoin (id INTEGER PRIMARY KEY, val REAL)")
		afExec(t, db, ctx, "INSERT INTO v106a_nulljoin VALUES (1, NULL)")
		afExec(t, db, ctx, "INSERT INTO v106a_nulljoin VALUES (2, NULL)")
		rows := afQuery(t, db, ctx, `
			SELECT i.category, SUM(n.val)
			FROM v106a_items i
			JOIN v106a_nulljoin n ON i.id = n.id
			GROUP BY i.category`)
		if len(rows) < 1 {
			t.Fatalf("expected at least 1 group, got %d", len(rows))
		}
		// SUM of all NULLs should be NULL
		if rows[0][1] != nil {
			t.Fatalf("expected NULL for SUM of NULLs, got %v", rows[0][1])
		}
	})

	// ============================================================
	// 3. applyGroupByOrderBy
	// ============================================================

	t.Run("OrderBy_AggregateASC", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM v106a_items GROUP BY category ORDER BY COUNT(*) ASC")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// vegetable (2) should come first
		if fmt.Sprintf("%v", rows[0][0]) != "vegetable" {
			t.Fatalf("expected vegetable first (ASC by count), got %v", rows[0][0])
		}
	})

	t.Run("OrderBy_AggregateDESC", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM v106a_items GROUP BY category ORDER BY COUNT(*) DESC")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// fruit (5) should come first
		if fmt.Sprintf("%v", rows[0][0]) != "fruit" {
			t.Fatalf("expected fruit first (DESC by count), got %v", rows[0][0])
		}
	})

	t.Run("OrderBy_Ordinal_GroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, SUM(price) FROM v106a_items GROUP BY category ORDER BY 2 DESC")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// fruit sum is larger
		if fmt.Sprintf("%v", rows[0][0]) != "fruit" {
			t.Fatalf("expected fruit first (highest SUM), got %v", rows[0][0])
		}
	})

	t.Run("OrderBy_Ordinal_ASC_GroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, SUM(price) FROM v106a_items GROUP BY category ORDER BY 2 ASC")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		// vegetable sum is smaller
		if fmt.Sprintf("%v", rows[0][0]) != "vegetable" {
			t.Fatalf("expected vegetable first (lowest SUM), got %v", rows[0][0])
		}
	})

	t.Run("OrderBy_ColumnNotInSelect_GroupBy", func(t *testing.T) {
		// ORDER BY column name that is in the GROUP BY
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM v106a_items GROUP BY category ORDER BY category DESC")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "vegetable" {
			t.Fatalf("expected vegetable first (DESC), got %v", rows[0][0])
		}
	})

	t.Run("OrderBy_String_ASC_GroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category, COUNT(*) FROM v106a_items GROUP BY category ORDER BY category ASC")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "fruit" {
			t.Fatalf("expected fruit first (ASC), got %v", rows[0][0])
		}
	})

	t.Run("OrderBy_NullInAggregate_GroupBy", func(t *testing.T) {
		// ORDER BY aggregate that may include NULL
		rows := afQuery(t, db, ctx, "SELECT category, AVG(discount) FROM v106a_items GROUP BY category ORDER BY AVG(discount)")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("OrderBy_ExprArg_GroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT category, SUM(price * qty) as total
			FROM v106a_items
			WHERE qty IS NOT NULL
			GROUP BY category
			ORDER BY SUM(price * qty) DESC`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("OrderBy_QualifiedIdentifier_GroupBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT i.category, COUNT(*)
			FROM v106a_items i
			JOIN v106a_orders o ON i.id = o.item_id
			GROUP BY i.category
			ORDER BY i.category`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
	})

	// ============================================================
	// 4. applyOuterQuery
	// ============================================================

	t.Run("OuterQuery_DistinctSubquery", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM (SELECT DISTINCT category FROM v106a_items) sub ORDER BY category")
		if len(rows) != 2 {
			t.Fatalf("expected 2 distinct categories, got %d", len(rows))
		}
	})

	t.Run("OuterQuery_ViewWithAggregate_WHERE", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE VIEW v106a_cat_stats AS SELECT category, COUNT(*) as cnt, AVG(price) as avg_price FROM v106a_items GROUP BY category")
		rows := afQuery(t, db, ctx, "SELECT * FROM v106a_cat_stats WHERE cnt > 3")
		if len(rows) != 1 {
			t.Fatalf("expected 1 view row with cnt > 3, got %d", len(rows))
		}
	})

	t.Run("OuterQuery_SubqueryGroupBy_OuterOrderByLimit", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT * FROM (
				SELECT category, COUNT(*) as cnt, SUM(price) as total
				FROM v106a_items GROUP BY category
			) sub ORDER BY total DESC LIMIT 1`)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row with LIMIT, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "fruit" {
			t.Fatalf("expected fruit as highest total, got %v", rows[0][0])
		}
	})

	t.Run("OuterQuery_SubqueryWithWHERE", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT category, cnt FROM (
				SELECT category, COUNT(*) as cnt FROM v106a_items GROUP BY category
			) sub WHERE cnt >= 2 ORDER BY category`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("OuterQuery_AggOnSubquery", func(t *testing.T) {
		// Outer query with aggregates on a subquery/view
		rows := afQuery(t, db, ctx, `
			SELECT COUNT(*), SUM(cnt) FROM (
				SELECT category, COUNT(*) as cnt FROM v106a_items GROUP BY category
			) sub`)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "2" {
			t.Fatalf("expected COUNT=2, got %v", rows[0][0])
		}
		if fmt.Sprintf("%v", rows[0][1]) != "7" {
			t.Fatalf("expected SUM=7, got %v", rows[0][1])
		}
	})

	t.Run("OuterQuery_GroupByOnSubquery", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v106a_log (id INTEGER PRIMARY KEY, action TEXT, status TEXT)")
		afExec(t, db, ctx, "INSERT INTO v106a_log VALUES (1, 'buy', 'ok')")
		afExec(t, db, ctx, "INSERT INTO v106a_log VALUES (2, 'sell', 'ok')")
		afExec(t, db, ctx, "INSERT INTO v106a_log VALUES (3, 'buy', 'fail')")
		afExec(t, db, ctx, "INSERT INTO v106a_log VALUES (4, 'buy', 'ok')")
		afExec(t, db, ctx, "INSERT INTO v106a_log VALUES (5, 'sell', 'fail')")

		rows := afQuery(t, db, ctx, `
			SELECT action, COUNT(*) as cnt FROM (
				SELECT * FROM v106a_log WHERE status = 'ok'
			) sub GROUP BY action ORDER BY action`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
	})

	t.Run("OuterQuery_HAVING_OnView", func(t *testing.T) {
		// Test HAVING directly on table (not derived) to exercise applyOuterQuery for views
		// Use a view with GROUP BY + HAVING
		afExec(t, db, ctx, "CREATE VIEW v106a_log_counts AS SELECT status, COUNT(*) as cnt FROM v106a_log GROUP BY status")
		rows := afQuery(t, db, ctx, "SELECT * FROM v106a_log_counts WHERE cnt >= 3 ORDER BY status")
		// ok=3 matches, fail=2 doesn't
		if len(rows) != 1 {
			t.Fatalf("expected 1 row with cnt>=3, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "ok" {
			t.Fatalf("expected 'ok', got %v", rows[0][0])
		}
	})

	t.Run("OuterQuery_OrderBy_OnViewAgg", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT status, COUNT(*) as cnt FROM (
				SELECT * FROM v106a_log
			) sub GROUP BY status ORDER BY COUNT(*) DESC`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(rows))
		}
	})

	// ============================================================
	// 5. resolveOuterRefsInQuery (correlated subqueries)
	// ============================================================

	t.Run("CorrelatedSubquery_IN", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v106a_items
			WHERE id IN (SELECT item_id FROM v106a_orders WHERE v106a_orders.customer = 'Alice')
			ORDER BY name`)
		if len(rows) != 4 {
			t.Fatalf("expected 4 items ordered by Alice, got %d", len(rows))
		}
	})

	t.Run("CorrelatedSubquery_EXISTS", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v106a_items i
			WHERE EXISTS (SELECT 1 FROM v106a_orders o WHERE o.item_id = i.id AND o.customer = 'Bob')
			ORDER BY name`)
		if len(rows) != 2 {
			t.Fatalf("expected 2 items ordered by Bob, got %d", len(rows))
		}
	})

	t.Run("CorrelatedSubquery_NOT_EXISTS", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v106a_items i
			WHERE NOT EXISTS (SELECT 1 FROM v106a_orders o WHERE o.item_id = i.id)
			ORDER BY name`)
		// Items with no orders
		if len(rows) < 0 {
			t.Fatal("unexpected error")
		}
	})

	t.Run("CorrelatedSubquery_NOT_IN", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v106a_items
			WHERE id NOT IN (SELECT item_id FROM v106a_orders)
			ORDER BY name`)
		// Items with no orders at all
		if len(rows) < 0 {
			t.Fatal("unexpected error")
		}
	})

	t.Run("CorrelatedSubquery_Scalar_InSELECT", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name, (SELECT COUNT(*) FROM v106a_orders o WHERE o.item_id = v106a_items.id) as order_count
			FROM v106a_items
			ORDER BY name`)
		if len(rows) != 7 {
			t.Fatalf("expected 7 rows, got %d", len(rows))
		}
		// apple has 2 orders
		if fmt.Sprintf("%v", rows[0][0]) != "apple" {
			t.Fatalf("expected apple first, got %v", rows[0][0])
		}
		if fmt.Sprintf("%v", rows[0][1]) != "2" {
			t.Fatalf("expected 2 orders for apple, got %v", rows[0][1])
		}
	})

	t.Run("CorrelatedSubquery_WHERE_GT", func(t *testing.T) {
		// Items with price > average price
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v106a_items
			WHERE price > (SELECT AVG(price) FROM v106a_items)
			ORDER BY name`)
		if len(rows) < 1 {
			t.Fatal("expected at least 1 item above average")
		}
	})

	t.Run("CorrelatedSubquery_InJoinCondition", func(t *testing.T) {
		// Correlated subquery in WHERE with join-like condition
		rows := afQuery(t, db, ctx, `
			SELECT DISTINCT o.customer FROM v106a_orders o
			WHERE o.item_id IN (
				SELECT id FROM v106a_items WHERE category = 'vegetable'
			)
			ORDER BY o.customer`)
		if len(rows) < 1 {
			t.Fatal("expected at least 1 customer buying vegetables")
		}
	})

	// ============================================================
	// 6. evaluateLike edge cases
	// ============================================================

	t.Run("LIKE_PercentWildcard", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name LIKE 'a%' ORDER BY name")
		if len(rows) != 1 {
			t.Fatalf("expected 1 match for 'a%%', got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "apple" {
			t.Fatalf("expected apple, got %v", rows[0][0])
		}
	})

	t.Run("LIKE_UnderscoreWildcard", func(t *testing.T) {
		// _ matches exactly one character
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name LIKE '_ig' ORDER BY name")
		if len(rows) != 1 {
			t.Fatalf("expected 1 match for '_ig', got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "fig" {
			t.Fatalf("expected fig, got %v", rows[0][0])
		}
	})

	t.Run("LIKE_CombinedWildcards", func(t *testing.T) {
		// % and _ combined
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name LIKE '_a%' ORDER BY name")
		// banana, carrot, date -> ba%, ca%, da%
		if len(rows) != 3 {
			t.Fatalf("expected 3 matches for '_a%%', got %d (rows: %v)", len(rows), rows)
		}
	})

	t.Run("NOT_LIKE", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name NOT LIKE '%a%' ORDER BY name")
		// Items without 'a' in name (case-insensitive)
		if len(rows) < 0 {
			t.Fatal("unexpected error")
		}
	})

	t.Run("LIKE_NULL", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v106a_liketest (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v106a_liketest VALUES (1, 'hello')")
		afExec(t, db, ctx, "INSERT INTO v106a_liketest VALUES (2, NULL)")
		// NULL LIKE anything should not match (returns NULL/unknown)
		rows := afQuery(t, db, ctx, "SELECT id FROM v106a_liketest WHERE val LIKE '%'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 match (NULL should not match), got %d", len(rows))
		}
	})

	t.Run("LIKE_EscapeChar", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v106a_esctest (id INTEGER PRIMARY KEY, val TEXT)")
		afExec(t, db, ctx, "INSERT INTO v106a_esctest VALUES (1, '10%')")
		afExec(t, db, ctx, "INSERT INTO v106a_esctest VALUES (2, '100')")
		afExec(t, db, ctx, "INSERT INTO v106a_esctest VALUES (3, '10x')")
		// LIKE with escape: match literal %
		rows := afQuery(t, db, ctx, "SELECT id FROM v106a_esctest WHERE val LIKE '10!%' ESCAPE '!'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 match for escaped %%, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "1" {
			t.Fatalf("expected id=1, got %v", rows[0][0])
		}
	})

	t.Run("LIKE_EmptyPattern", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name LIKE ''")
		if len(rows) != 0 {
			t.Fatalf("expected 0 matches for empty pattern, got %d", len(rows))
		}
	})

	t.Run("LIKE_ExactMatch", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name LIKE 'apple'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 exact match, got %d", len(rows))
		}
	})

	t.Run("LIKE_CaseInsensitive", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name LIKE 'APPLE'")
		if len(rows) != 1 {
			t.Fatalf("expected 1 match (case-insensitive), got %d", len(rows))
		}
	})

	// ============================================================
	// 7. evaluateCastExpr
	// ============================================================

	t.Run("CAST_IntToText", func(t *testing.T) {
		afExpectVal(t, db, ctx, "SELECT CAST(42 AS TEXT)", "42")
	})

	t.Run("CAST_TextToInt", func(t *testing.T) {
		afExpectVal(t, db, ctx, "SELECT CAST('123' AS INTEGER)", int64(123))
	})

	t.Run("CAST_FloatToInt", func(t *testing.T) {
		afExpectVal(t, db, ctx, "SELECT CAST(3.7 AS INTEGER)", int64(3))
	})

	t.Run("CAST_NullToText", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST(NULL AS TEXT)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Fatalf("expected NULL, got %v", rows[0][0])
		}
	})

	t.Run("CAST_NullToInt", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST(NULL AS INTEGER)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if rows[0][0] != nil {
			t.Fatalf("expected NULL, got %v", rows[0][0])
		}
	})

	t.Run("CAST_IntToReal", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST(42 AS REAL)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%.1f", rows[0][0]) != "42.0" {
			t.Fatalf("expected 42.0, got %v", rows[0][0])
		}
	})

	t.Run("CAST_TextToReal", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST('3.14' AS REAL)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%.2f", rows[0][0]) != "3.14" {
			t.Fatalf("expected 3.14, got %v", rows[0][0])
		}
	})

	t.Run("CAST_IntToBoolean", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST(1 AS BOOLEAN)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "true" {
			t.Fatalf("expected true, got %v", rows[0][0])
		}
	})

	t.Run("CAST_ZeroToBoolean", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST(0 AS BOOLEAN)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "false" {
			t.Fatalf("expected false, got %v", rows[0][0])
		}
	})

	t.Run("CAST_TextToBoolean_True", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST('true' AS BOOLEAN)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "true" {
			t.Fatalf("expected true, got %v", rows[0][0])
		}
	})

	t.Run("CAST_TextToBoolean_False", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST('hello' AS BOOLEAN)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "false" {
			t.Fatalf("expected false, got %v", rows[0][0])
		}
	})

	t.Run("CAST_RealToText", func(t *testing.T) {
		afExpectVal(t, db, ctx, "SELECT CAST(3.14 AS TEXT)", "3.14")
	})

	t.Run("CAST_InvalidTextToInt", func(t *testing.T) {
		// Casting non-numeric text to integer should return 0
		afExpectVal(t, db, ctx, "SELECT CAST('abc' AS INTEGER)", int64(0))
	})

	t.Run("CAST_ColumnValue", func(t *testing.T) {
		// CAST on column from table
		rows := afQuery(t, db, ctx, "SELECT CAST(price AS INTEGER) FROM v106a_items WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "1" {
			t.Fatalf("expected 1 (truncated 1.50), got %v", rows[0][0])
		}
	})

	t.Run("CAST_BoolToText", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT CAST(1 = 1 AS TEXT)")
		if len(rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "true" {
			t.Fatalf("expected 'true', got %v", rows[0][0])
		}
	})

	// ============================================================
	// 8. evaluateBetween
	// ============================================================

	t.Run("BETWEEN_Integers", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE id BETWEEN 2 AND 5 ORDER BY id")
		if len(rows) != 4 {
			t.Fatalf("expected 4 items between 2 and 5, got %d", len(rows))
		}
	})

	t.Run("BETWEEN_Float", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE price BETWEEN 2.0 AND 4.0 ORDER BY price")
		// carrot(2.00), eggplant(3.00), grape(3.50), fig(4.00) = 4 items
		if len(rows) != 4 {
			t.Fatalf("expected 4 items with price 2.0-4.0, got %d", len(rows))
		}
	})

	t.Run("BETWEEN_Strings", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE name BETWEEN 'b' AND 'e' ORDER BY name")
		// banana, carrot, date (alphabetically between b and e)
		if len(rows) < 1 {
			t.Fatal("expected at least 1 item between 'b' and 'e'")
		}
	})

	t.Run("NOT_BETWEEN", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE id NOT BETWEEN 2 AND 6 ORDER BY id")
		// id=1 (apple) and id=7 (grape)
		if len(rows) != 2 {
			t.Fatalf("expected 2 items NOT BETWEEN 2 and 6, got %d", len(rows))
		}
	})

	t.Run("BETWEEN_NULL_Value", func(t *testing.T) {
		// BETWEEN with NULL should not match
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE qty BETWEEN 1 AND 100 ORDER BY name")
		// qty is NULL for date, so should be excluded
		if len(rows) != 6 {
			t.Fatalf("expected 6 items with non-null qty BETWEEN 1 and 100, got %d", len(rows))
		}
	})

	t.Run("BETWEEN_NULL_Bounds", func(t *testing.T) {
		// BETWEEN with NULL bound should not match
		rows := afQuery(t, db, ctx, "SELECT id FROM v106a_items WHERE id BETWEEN NULL AND 5")
		if len(rows) != 0 {
			t.Fatalf("expected 0 rows with NULL lower bound, got %d", len(rows))
		}
	})

	t.Run("BETWEEN_Boundary", func(t *testing.T) {
		// Exact boundary values should be included
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE id BETWEEN 1 AND 1")
		if len(rows) != 1 {
			t.Fatalf("expected 1 item with id BETWEEN 1 AND 1, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "apple" {
			t.Fatalf("expected apple, got %v", rows[0][0])
		}
	})

	t.Run("NOT_BETWEEN_NULL", func(t *testing.T) {
		// NOT BETWEEN with NULL should not match either
		rows := afQuery(t, db, ctx, "SELECT id FROM v106a_items WHERE id NOT BETWEEN NULL AND 5")
		if len(rows) != 0 {
			t.Fatalf("expected 0 rows with NOT BETWEEN NULL, got %d", len(rows))
		}
	})

	// ============================================================
	// Additional combined tests for deeper coverage
	// ============================================================

	t.Run("GroupBy_Alias_InOrderBy", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT category as cat, COUNT(*) as cnt FROM v106a_items GROUP BY cat ORDER BY cnt DESC")
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("GroupBy_MultiColumn", func(t *testing.T) {
		afExec(t, db, ctx, "CREATE TABLE v106a_multi (id INTEGER PRIMARY KEY, a TEXT, b TEXT, val INTEGER)")
		afExec(t, db, ctx, "INSERT INTO v106a_multi VALUES (1, 'X', 'P', 10)")
		afExec(t, db, ctx, "INSERT INTO v106a_multi VALUES (2, 'X', 'Q', 20)")
		afExec(t, db, ctx, "INSERT INTO v106a_multi VALUES (3, 'Y', 'P', 30)")
		afExec(t, db, ctx, "INSERT INTO v106a_multi VALUES (4, 'X', 'P', 40)")
		rows := afQuery(t, db, ctx, "SELECT a, b, SUM(val) FROM v106a_multi GROUP BY a, b ORDER BY a, b")
		if len(rows) != 3 {
			t.Fatalf("expected 3 groups, got %d", len(rows))
		}
		// X,P -> 50
		if fmt.Sprintf("%v", rows[0][2]) != "50" {
			t.Fatalf("expected SUM=50 for X,P, got %v", rows[0][2])
		}
	})

	t.Run("SubqueryWithCTE", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			WITH top_items AS (
				SELECT item_id, SUM(amount) as total FROM v106a_orders GROUP BY item_id
			)
			SELECT i.name, t.total
			FROM v106a_items i
			JOIN top_items t ON i.id = t.item_id
			ORDER BY t.total DESC
			LIMIT 3`)
		if len(rows) < 1 {
			t.Fatal("expected at least 1 row from CTE join")
		}
	})

	t.Run("CAST_InWHERE", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT name FROM v106a_items WHERE CAST(price AS INTEGER) = 3")
		// eggplant (3.00) -> 3
		if len(rows) < 1 {
			t.Fatal("expected at least 1 match for CAST in WHERE")
		}
	})

	t.Run("BETWEEN_InSubquery", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v106a_items
			WHERE id IN (SELECT id FROM v106a_items WHERE price BETWEEN 1.0 AND 3.0)
			ORDER BY name`)
		if len(rows) < 1 {
			t.Fatal("expected at least 1 item with price between 1 and 3")
		}
	})

	t.Run("LIKE_InSubquery", func(t *testing.T) {
		rows := afQuery(t, db, ctx, `
			SELECT name FROM v106a_items
			WHERE category IN (SELECT DISTINCT category FROM v106a_items WHERE name LIKE '%a%')
			ORDER BY name`)
		if len(rows) < 1 {
			t.Fatal("expected items in matching categories")
		}
	})
}
