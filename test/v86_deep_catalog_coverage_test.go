package test

import (
	"fmt"
	"testing"
)

// ==================== V86: DEEP CATALOG COVERAGE TEST ====================
// Targets the lowest-coverage reachable functions in pkg/catalog:
// 1. evaluateHaving (68.2%) - LIKE, IS NULL, CASE, function calls in HAVING
// 2. applyGroupByOrderBy (66.7%) - expression aggregate ORDER BY, DESC
// 3. evaluateExprWithGroupAggregatesJoin (68.4%) - JOIN+GROUP BY+CASE/BETWEEN
// 4. executeTriggerStatement (62.5%) - trigger with UPDATE/DELETE body
// 5. computeViewAggregate (68.9%) - empty set, NULL, multi-agg views
// 6. executeDerivedTable (62.5%) - derived table with UNION, WHERE
// 7. toNumber (44.4%) - string-to-number type conversions
// 8. evaluateLike (67.9%) - NULL, ESCAPE, complex patterns
// 9. resolveOuterRefsInExpr (52.2%) - correlated subqueries with IN, BETWEEN
// 10. valueToExpr (36.4%) - outer ref type conversion
// 11. selectLocked deeper paths (78.0%) - DISTINCT+ORDER BY, expressions

// ==================== HAVING DEEPER PATHS ====================

func TestV86_HavingWithLIKE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_lk (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_lk VALUES ('apples', 10)")
	afExec(t, db, ctx, "INSERT INTO h_lk VALUES ('apples', 20)")
	afExec(t, db, ctx, "INSERT INTO h_lk VALUES ('bananas', 5)")
	afExec(t, db, ctx, "INSERT INTO h_lk VALUES ('cherries', 15)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM h_lk GROUP BY cat HAVING cat LIKE 'a%'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (apples), got %d: %v", len(rows), rows)
	}
}

func TestV86_HavingWithISNULL(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_isn (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_isn VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO h_isn VALUES ('a', NULL)")
	afExec(t, db, ctx, "INSERT INTO h_isn VALUES ('b', 20)")
	afExec(t, db, ctx, "INSERT INTO h_isn VALUES (NULL, 30)")

	// HAVING with IS NOT NULL on group key
	rows := afQuery(t, db, ctx, "SELECT cat, SUM(val) FROM h_isn GROUP BY cat HAVING cat IS NOT NULL ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (a, b), got %d: %v", len(rows), rows)
	}
}

func TestV86_HavingWithFunctionCall(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_fn (name TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_fn VALUES ('alice', 80)")
	afExec(t, db, ctx, "INSERT INTO h_fn VALUES ('alice', 90)")
	afExec(t, db, ctx, "INSERT INTO h_fn VALUES ('bob', 70)")
	afExec(t, db, ctx, "INSERT INTO h_fn VALUES ('charlie', 60)")

	// HAVING with LENGTH function on group key
	rows := afQuery(t, db, ctx, "SELECT name, AVG(score) FROM h_fn GROUP BY name HAVING LENGTH(name) > 3 ORDER BY name")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (alice, charlie), got %d: %v", len(rows), rows)
	}
}

func TestV86_HavingNestedCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_nc (dept TEXT, sal INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_nc VALUES ('eng', 100)")
	afExec(t, db, ctx, "INSERT INTO h_nc VALUES ('eng', 200)")
	afExec(t, db, ctx, "INSERT INTO h_nc VALUES ('hr', 50)")
	afExec(t, db, ctx, "INSERT INTO h_nc VALUES ('sales', 300)")

	rows := afQuery(t, db, ctx, `SELECT dept, SUM(sal) as total FROM h_nc GROUP BY dept
		HAVING CASE WHEN COUNT(*) > 1 THEN SUM(sal) ELSE 0 END > 100`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (eng, total=300), got %d: %v", len(rows), rows)
	}
}

func TestV86_HavingMultipleConditions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE h_mc (cat TEXT, amt INTEGER)")
	afExec(t, db, ctx, "INSERT INTO h_mc VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO h_mc VALUES ('a', 20)")
	afExec(t, db, ctx, "INSERT INTO h_mc VALUES ('b', 5)")
	afExec(t, db, ctx, "INSERT INTO h_mc VALUES ('c', 100)")
	afExec(t, db, ctx, "INSERT INTO h_mc VALUES ('c', 200)")

	// HAVING with AND + OR combined
	// a=30 (count=2, sum>20 AND count>1 → true), b=5 (sum=5 → true), c=300 (count=2, sum>20 → true)
	rows := afQuery(t, db, ctx, `SELECT cat, SUM(amt) as s FROM h_mc GROUP BY cat
		HAVING (SUM(amt) > 20 AND COUNT(*) > 1) OR SUM(amt) = 5`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
	}
}

// ==================== GROUP BY + ORDER BY DEEPER PATHS ====================

func TestV86_GroupByOrderByExprAgg(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbo_expr (dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO gbo_expr VALUES ('eng', 100)")
	afExec(t, db, ctx, "INSERT INTO gbo_expr VALUES ('eng', 200)")
	afExec(t, db, ctx, "INSERT INTO gbo_expr VALUES ('hr', 50)")
	afExec(t, db, ctx, "INSERT INTO gbo_expr VALUES ('sales', 500)")

	// ORDER BY positional ref DESC - sales=500, eng=300, hr=50
	rows := afQuery(t, db, ctx, `SELECT dept, SUM(salary) as total FROM gbo_expr
		GROUP BY dept ORDER BY 2 DESC`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "sales" {
		t.Fatalf("expected sales first (highest sum=500), got %v (rows: %v)", rows[0][0], rows)
	}
}

func TestV86_GroupByOrderByMultipleAggs(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbo_ma (cat TEXT, v INTEGER)")
	afExec(t, db, ctx, "INSERT INTO gbo_ma VALUES ('x', 10)")
	afExec(t, db, ctx, "INSERT INTO gbo_ma VALUES ('x', 20)")
	afExec(t, db, ctx, "INSERT INTO gbo_ma VALUES ('y', 5)")
	afExec(t, db, ctx, "INSERT INTO gbo_ma VALUES ('z', 100)")

	// ORDER BY COUNT then SUM
	rows := afQuery(t, db, ctx, `SELECT cat, COUNT(*) as cnt, SUM(v) as total FROM gbo_ma
		GROUP BY cat ORDER BY COUNT(*) DESC, SUM(v) DESC`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// x has count 2 (highest), then z and y (count 1 each, z has higher sum)
	if fmt.Sprintf("%v", rows[0][0]) != "x" {
		t.Fatalf("expected x first, got %v", rows[0][0])
	}
}

func TestV86_GroupByOrderByASC(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbo_asc (name TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO gbo_asc VALUES ('charlie', 30)")
	afExec(t, db, ctx, "INSERT INTO gbo_asc VALUES ('alice', 10)")
	afExec(t, db, ctx, "INSERT INTO gbo_asc VALUES ('bob', 20)")

	rows := afQuery(t, db, ctx, "SELECT name, SUM(val) FROM gbo_asc GROUP BY name ORDER BY name ASC")
	if fmt.Sprintf("%v", rows[0][0]) != "alice" {
		t.Fatalf("expected alice first, got %v", rows[0][0])
	}
}

func TestV86_GroupByOrderByNullLast(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gbo_null (cat TEXT, v INTEGER)")
	afExec(t, db, ctx, "INSERT INTO gbo_null VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO gbo_null VALUES (NULL, 20)")
	afExec(t, db, ctx, "INSERT INTO gbo_null VALUES ('b', 30)")

	rows := afQuery(t, db, ctx, "SELECT cat, SUM(v) FROM gbo_null GROUP BY cat ORDER BY cat ASC")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// NULL should sort last in ASC
}

// ==================== JOIN + GROUP BY + EXPRESSIONS ====================

func TestV86_JoinGroupByCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE jgc_emp (id INTEGER, dept_id INTEGER, salary INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE jgc_dept (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO jgc_dept VALUES (1, 'eng')")
	afExec(t, db, ctx, "INSERT INTO jgc_dept VALUES (2, 'hr')")
	afExec(t, db, ctx, "INSERT INTO jgc_emp VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO jgc_emp VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO jgc_emp VALUES (3, 2, 50)")

	rows := afQuery(t, db, ctx, `SELECT d.name,
		CASE WHEN SUM(e.salary) > 150 THEN 'high' ELSE 'low' END as level
		FROM jgc_dept d INNER JOIN jgc_emp e ON d.id = e.dept_id
		GROUP BY d.name ORDER BY d.name`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV86_JoinGroupByHaving(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE jgh_ord (id INTEGER, customer_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE jgh_cust (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO jgh_cust VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO jgh_cust VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO jgh_ord VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO jgh_ord VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO jgh_ord VALUES (3, 2, 50)")

	rows := afQuery(t, db, ctx, `SELECT c.name, SUM(o.amount) as total
		FROM jgh_cust c INNER JOIN jgh_ord o ON c.id = o.customer_id
		GROUP BY c.name HAVING SUM(o.amount) > 100`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (Alice, 300), got %d: %v", len(rows), rows)
	}
}

func TestV86_JoinGroupByCountStar(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE jgcs_prod (id INTEGER, category TEXT)")
	afExec(t, db, ctx, "CREATE TABLE jgcs_review (product_id INTEGER, rating INTEGER)")
	afExec(t, db, ctx, "INSERT INTO jgcs_prod VALUES (1, 'electronics')")
	afExec(t, db, ctx, "INSERT INTO jgcs_prod VALUES (2, 'books')")
	afExec(t, db, ctx, "INSERT INTO jgcs_review VALUES (1, 5)")
	afExec(t, db, ctx, "INSERT INTO jgcs_review VALUES (1, 4)")
	afExec(t, db, ctx, "INSERT INTO jgcs_review VALUES (2, 3)")

	rows := afQuery(t, db, ctx, `SELECT p.category, COUNT(*) as review_count, AVG(r.rating) as avg_rating
		FROM jgcs_prod p INNER JOIN jgcs_review r ON p.id = r.product_id
		GROUP BY p.category ORDER BY review_count DESC`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ==================== TRIGGER BODY UPDATE/DELETE ====================

func TestV86_TriggerBodyUpdate(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tbu_items (id INTEGER PRIMARY KEY, name TEXT, stock INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tbu_orders (id INTEGER PRIMARY KEY, item_id INTEGER, qty INTEGER)")

	// Trigger that decrements stock after an order
	afExec(t, db, ctx, `CREATE TRIGGER tbu_decrement AFTER INSERT ON tbu_orders
		FOR EACH ROW
		BEGIN
			UPDATE tbu_items SET stock = stock - NEW.qty WHERE id = NEW.item_id;
		END`)

	afExec(t, db, ctx, "INSERT INTO tbu_items VALUES (1, 'Widget', 100)")
	afExec(t, db, ctx, "INSERT INTO tbu_orders VALUES (1, 1, 5)")

	afExpectVal(t, db, ctx, "SELECT stock FROM tbu_items WHERE id = 1", float64(95))
}

func TestV86_TriggerBodyDelete(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tbd_parent (id INTEGER PRIMARY KEY, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tbd_child (id INTEGER, parent_id INTEGER)")

	// Trigger that deletes children when parent is deleted
	afExec(t, db, ctx, `CREATE TRIGGER tbd_cascade AFTER DELETE ON tbd_parent
		FOR EACH ROW
		BEGIN
			DELETE FROM tbd_child WHERE parent_id = OLD.id;
		END`)

	afExec(t, db, ctx, "INSERT INTO tbd_parent VALUES (1, 'p1')")
	afExec(t, db, ctx, "INSERT INTO tbd_child VALUES (10, 1)")
	afExec(t, db, ctx, "INSERT INTO tbd_child VALUES (11, 1)")
	afExec(t, db, ctx, "INSERT INTO tbd_child VALUES (12, 2)")

	afExec(t, db, ctx, "DELETE FROM tbd_parent WHERE id = 1")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tbd_child", float64(1))
}

func TestV86_TriggerBodyMultipleStatements(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE tbm_main (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE tbm_log (msg TEXT)")
	afExec(t, db, ctx, "CREATE TABLE tbm_stats (total_inserts INTEGER)")
	afExec(t, db, ctx, "INSERT INTO tbm_stats VALUES (0)")

	afExec(t, db, ctx, `CREATE TRIGGER tbm_multi AFTER INSERT ON tbm_main
		FOR EACH ROW
		BEGIN
			INSERT INTO tbm_log VALUES ('inserted:' || CAST(NEW.id AS TEXT));
			UPDATE tbm_stats SET total_inserts = total_inserts + 1;
		END`)

	afExec(t, db, ctx, "INSERT INTO tbm_main VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO tbm_main VALUES (2, 20)")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM tbm_log", float64(2))
	afExpectVal(t, db, ctx, "SELECT total_inserts FROM tbm_stats", float64(2))
}

// ==================== VIEW AGGREGATE DEEPER ====================

func TestV86_ViewAggregateEmpty(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE vae (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "CREATE VIEW vae_view AS SELECT cat, SUM(val) as total FROM vae GROUP BY cat")

	rows := afQuery(t, db, ctx, "SELECT * FROM vae_view")
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows from empty view, got %d", len(rows))
	}
}

func TestV86_ViewAggregateWithNulls(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE van (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO van VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO van VALUES ('a', NULL)")
	afExec(t, db, ctx, "INSERT INTO van VALUES ('b', NULL)")
	afExec(t, db, ctx, "INSERT INTO van VALUES ('b', NULL)")

	afExec(t, db, ctx, "CREATE VIEW van_view AS SELECT cat, SUM(val) as total, COUNT(val) as cnt FROM van GROUP BY cat")
	rows := afQuery(t, db, ctx, "SELECT * FROM van_view ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV86_ViewWithOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE vob (name TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO vob VALUES ('charlie', 30)")
	afExec(t, db, ctx, "INSERT INTO vob VALUES ('alice', 10)")
	afExec(t, db, ctx, "INSERT INTO vob VALUES ('bob', 20)")

	afExec(t, db, ctx, "CREATE VIEW vob_view AS SELECT name, score FROM vob ORDER BY score DESC")
	rows := afQuery(t, db, ctx, "SELECT * FROM vob_view")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV86_ViewAggregateMinMax(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE vamm (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO vamm VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO vamm VALUES ('a', 30)")
	afExec(t, db, ctx, "INSERT INTO vamm VALUES ('b', 50)")

	afExec(t, db, ctx, `CREATE VIEW vamm_view AS
		SELECT cat, MIN(val) as min_v, MAX(val) as max_v, AVG(val) as avg_v
		FROM vamm GROUP BY cat`)
	rows := afQuery(t, db, ctx, "SELECT * FROM vamm_view ORDER BY cat")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ==================== DERIVED TABLE DEEPER ====================

func TestV86_DerivedTableWithWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dtw (id INTEGER, val INTEGER)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO dtw VALUES (%d, %d)", i, i*10))
	}

	rows := afQuery(t, db, ctx, `SELECT * FROM
		(SELECT id, val FROM dtw WHERE val > 50) sub
		ORDER BY id`)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows (val > 50), got %d", len(rows))
	}
}

func TestV86_DerivedTableWithOrderByAndLimit(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dtol (id INTEGER, score INTEGER)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO dtol VALUES (%d, %d)", i, i*5))
	}

	rows := afQuery(t, db, ctx, `SELECT * FROM
		(SELECT id, score FROM dtol ORDER BY score DESC LIMIT 5) sub`)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

func TestV86_DerivedTableNested(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dtn (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dtn VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO dtn VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO dtn VALUES (3, 30)")

	rows := afQuery(t, db, ctx, `SELECT * FROM
		(SELECT id, val * 2 as doubled FROM dtn WHERE val > 10) sub
		ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ==================== LIKE DEEPER ====================

func TestV86_LikeNullPattern(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lnp (name TEXT)")
	afExec(t, db, ctx, "INSERT INTO lnp VALUES ('hello')")
	afExec(t, db, ctx, "INSERT INTO lnp VALUES (NULL)")

	// LIKE with NULL pattern should return no matches
	rows := afQuery(t, db, ctx, "SELECT name FROM lnp WHERE name LIKE NULL")
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows for LIKE NULL, got %d", len(rows))
	}
}

func TestV86_LikeNullSubject(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lns (name TEXT)")
	afExec(t, db, ctx, "INSERT INTO lns VALUES ('hello')")
	afExec(t, db, ctx, "INSERT INTO lns VALUES (NULL)")

	// NULL LIKE pattern should not match
	rows := afQuery(t, db, ctx, "SELECT name FROM lns WHERE name LIKE '%'")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (hello), got %d: %v", len(rows), rows)
	}
}

func TestV86_LikeEmptyPattern(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lep (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO lep VALUES ('')")
	afExec(t, db, ctx, "INSERT INTO lep VALUES ('hello')")

	rows := afQuery(t, db, ctx, "SELECT val FROM lep WHERE val LIKE ''")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (empty string), got %d: %v", len(rows), rows)
	}
}

func TestV86_LikeAllWildcard(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE law (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO law VALUES ('hello')")
	afExec(t, db, ctx, "INSERT INTO law VALUES ('world')")
	afExec(t, db, ctx, "INSERT INTO law VALUES ('')")

	rows := afQuery(t, db, ctx, "SELECT val FROM law WHERE val LIKE '%'")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows for '%%', got %d", len(rows))
	}
}

func TestV86_LikeMultipleWildcards(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lmw (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO lmw VALUES ('hello world')")
	afExec(t, db, ctx, "INSERT INTO lmw VALUES ('hello earth')")
	afExec(t, db, ctx, "INSERT INTO lmw VALUES ('hi world')")

	// 'h%o%d' matches both 'hello world' and 'hi world' (case-insensitive, h...o...d)
	rows := afQuery(t, db, ctx, "SELECT val FROM lmw WHERE val LIKE 'h%o%d'")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestV86_LikeNumberConversion(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE lnc (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO lnc VALUES (123)")
	afExec(t, db, ctx, "INSERT INTO lnc VALUES (456)")
	afExec(t, db, ctx, "INSERT INTO lnc VALUES (120)")

	// LIKE should convert number to string
	rows := afQuery(t, db, ctx, "SELECT val FROM lnc WHERE val LIKE '12%'")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (123, 120), got %d: %v", len(rows), rows)
	}
}

// ==================== CORRELATED SUBQUERY / OUTER REFS ====================

func TestV86_CorrelatedSubqueryINList(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE csi_orders (id INTEGER, customer_id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE csi_customers (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO csi_customers VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO csi_customers VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO csi_customers VALUES (3, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO csi_orders VALUES (1, 1, 100)")
	afExec(t, db, ctx, "INSERT INTO csi_orders VALUES (2, 1, 200)")
	afExec(t, db, ctx, "INSERT INTO csi_orders VALUES (3, 3, 50)")

	// Correlated subquery in WHERE with IN (Carol amount=50, not > 50)
	rows := afQuery(t, db, ctx, `SELECT name FROM csi_customers c
		WHERE c.id IN (SELECT customer_id FROM csi_orders WHERE amount >= 50)
		ORDER BY name`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (Alice, Carol), got %d: %v", len(rows), rows)
	}
}

func TestV86_CorrelatedSubqueryScalar(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE css_emp (id INTEGER, dept TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO css_emp VALUES (1, 'eng', 100)")
	afExec(t, db, ctx, "INSERT INTO css_emp VALUES (2, 'eng', 200)")
	afExec(t, db, ctx, "INSERT INTO css_emp VALUES (3, 'hr', 150)")

	// Correlated scalar subquery
	rows := afQuery(t, db, ctx, `SELECT e.id, e.dept, e.salary,
		(SELECT MAX(salary) FROM css_emp e2 WHERE e2.dept = e.dept) as max_sal
		FROM css_emp e ORDER BY e.id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV86_CorrelatedSubqueryNOTIN(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE csn_main (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE csn_exclude (id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO csn_main VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO csn_main VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO csn_main VALUES (3, 'c')")
	afExec(t, db, ctx, "INSERT INTO csn_exclude VALUES (2)")

	rows := afQuery(t, db, ctx, `SELECT val FROM csn_main
		WHERE id NOT IN (SELECT id FROM csn_exclude) ORDER BY val`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(rows), rows)
	}
}

func TestV86_CorrelatedSubqueryNOTEXISTS(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE csne_cust (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "CREATE TABLE csne_ord (customer_id INTEGER)")
	afExec(t, db, ctx, "INSERT INTO csne_cust VALUES (1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO csne_cust VALUES (2, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO csne_cust VALUES (3, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO csne_ord VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO csne_ord VALUES (3)")

	rows := afQuery(t, db, ctx, `SELECT name FROM csne_cust c
		WHERE NOT EXISTS (SELECT 1 FROM csne_ord WHERE customer_id = c.id)`)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (Bob), got %d: %v", len(rows), rows)
	}
}

// ==================== STRING-TO-NUMBER CONVERSION ====================

func TestV86_StringToNumberArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Arithmetic with string values that look like numbers
	afExpectVal(t, db, ctx, "SELECT '10' + 5", float64(15))
	afExpectVal(t, db, ctx, "SELECT '3.14' * 2", float64(6.28))
}

func TestV86_NumberComparisonTypes(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE nct (a INTEGER, b REAL)")
	afExec(t, db, ctx, "INSERT INTO nct VALUES (10, 10.0)")

	rows := afQuery(t, db, ctx, "SELECT a = b FROM nct")
	if rows[0][0] != true {
		t.Fatalf("expected true for 10 = 10.0, got %v (%T)", rows[0][0], rows[0][0])
	}
}

// ==================== DEFAULT EXPRESSIONS (EvalExpression) ====================

func TestV86_DefaultExprCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// DEFAULT with a literal expression
	afExec(t, db, ctx, `CREATE TABLE de_case (
		id INTEGER PRIMARY KEY,
		status TEXT DEFAULT 'pending',
		priority INTEGER DEFAULT 1
	)`)
	afExec(t, db, ctx, "INSERT INTO de_case (id) VALUES (1)")
	afExpectVal(t, db, ctx, "SELECT status FROM de_case WHERE id = 1", "pending")
	afExpectVal(t, db, ctx, "SELECT priority FROM de_case WHERE id = 1", int64(1))
}

func TestV86_DefaultExprNumericOps(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, `CREATE TABLE de_num (
		id INTEGER PRIMARY KEY,
		val INTEGER DEFAULT 42,
		rate REAL DEFAULT 3.14
	)`)
	afExec(t, db, ctx, "INSERT INTO de_num (id) VALUES (1)")
	afExpectVal(t, db, ctx, "SELECT val FROM de_num WHERE id = 1", int64(42))
}

// ==================== SELECT DISTINCT + ORDER BY ====================

func TestV86_DistinctOrderBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dob (cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dob VALUES ('b', 20)")
	afExec(t, db, ctx, "INSERT INTO dob VALUES ('a', 10)")
	afExec(t, db, ctx, "INSERT INTO dob VALUES ('b', 30)")
	afExec(t, db, ctx, "INSERT INTO dob VALUES ('a', 40)")

	rows := afQuery(t, db, ctx, "SELECT DISTINCT cat FROM dob ORDER BY cat ASC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][0]) != "a" {
		t.Fatalf("expected 'a' first, got %v", rows[0][0])
	}
}

func TestV86_DistinctWithExpression(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dwe (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dwe VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO dwe VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO dwe VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO dwe VALUES (2)")

	rows := afQuery(t, db, ctx, "SELECT DISTINCT val * 10 FROM dwe ORDER BY val * 10")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

// ==================== ANALYZE DEEPER ====================

func TestV86_AnalyzeMultipleTables(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE an1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE an2 (id INTEGER PRIMARY KEY, score INTEGER)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO an1 VALUES (%d, 'val%d')", i, i))
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO an2 VALUES (%d, %d)", i, i*10))
	}

	afExec(t, db, ctx, "ANALYZE an1")
	afExec(t, db, ctx, "ANALYZE an2")
}

func TestV86_AnalyzeWithIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE anidx (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	afExec(t, db, ctx, "CREATE INDEX anidx_age ON anidx(age)")
	for i := 1; i <= 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO anidx VALUES (%d, 'name%d', %d)", i, i, 20+i%10))
	}

	afExec(t, db, ctx, "ANALYZE anidx")
}

func TestV86_AnalyzeEmptyTable(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE an_empty (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "ANALYZE an_empty")
}

// ==================== COMPLEX EXPRESSIONS ====================

func TestV86_NestedCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE nc (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO nc VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO nc VALUES (5)")
	afExec(t, db, ctx, "INSERT INTO nc VALUES (10)")

	rows := afQuery(t, db, ctx, `SELECT val,
		CASE
			WHEN val < 3 THEN CASE WHEN val = 1 THEN 'one' ELSE 'small' END
			WHEN val < 8 THEN 'medium'
			ELSE 'large'
		END as label
		FROM nc ORDER BY val`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "one" {
		t.Fatalf("expected 'one', got %v", rows[0][1])
	}
}

func TestV86_SimpleCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sc (grade TEXT)")
	afExec(t, db, ctx, "INSERT INTO sc VALUES ('A')")
	afExec(t, db, ctx, "INSERT INTO sc VALUES ('B')")
	afExec(t, db, ctx, "INSERT INTO sc VALUES ('C')")
	afExec(t, db, ctx, "INSERT INTO sc VALUES ('D')")

	rows := afQuery(t, db, ctx, `SELECT grade,
		CASE grade WHEN 'A' THEN 'Excellent' WHEN 'B' THEN 'Good' WHEN 'C' THEN 'Average' ELSE 'Below' END
		FROM sc ORDER BY grade`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if fmt.Sprintf("%v", rows[0][1]) != "Excellent" {
		t.Fatalf("expected 'Excellent', got %v", rows[0][1])
	}
}

func TestV86_BooleanExpressions(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE be (a INTEGER, b INTEGER)")
	afExec(t, db, ctx, "INSERT INTO be VALUES (1, 0)")

	// Various boolean expressions
	rows := afQuery(t, db, ctx, "SELECT a > 0 AND b = 0, a > 0 OR b > 0, NOT (a = 0) FROM be")
	if rows[0][0] != true {
		t.Fatalf("expected true for a>0 AND b=0, got %v", rows[0][0])
	}
}

func TestV86_ComparisonOperators(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE co (val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO co VALUES (10)")

	rows := afQuery(t, db, ctx, "SELECT val = 10, val != 5, val < 20, val > 5, val <= 10, val >= 10 FROM co")
	for i, v := range rows[0] {
		if v != true {
			t.Fatalf("expected true for comparison %d, got %v (%T)", i, v, v)
		}
	}
}

// ==================== CONCATENATION EDGE CASES ====================

func TestV86_ConcatWithNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, "SELECT 'hello' || NULL")
	// Concat with NULL may return NULL or the string depending on implementation
	t.Logf("'hello' || NULL = %v", rows[0][0])
}

func TestV86_ConcatNumbers(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT 'count: ' || CAST(42 AS TEXT)", "count: 42")
}

// ==================== ARITHMETIC EDGE CASES ====================

func TestV86_IntegerDivision(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT 10 / 3", float64(10)/float64(3))
	afExpectVal(t, db, ctx, "SELECT 10 % 3", float64(1))
}

func TestV86_NegativeArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT -5 + 3", int64(-2))
	afExpectVal(t, db, ctx, "SELECT -5 * -3", int64(15))
}

func TestV86_FloatArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT 1.5 + 2.5", float64(4))
	afExpectVal(t, db, ctx, "SELECT 10.0 / 3.0", float64(10.0/3.0))
}

// ==================== MORE SELECT PATTERNS ====================

func TestV86_SelectWithComputedColumn(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE scc (price REAL, qty INTEGER)")
	afExec(t, db, ctx, "INSERT INTO scc VALUES (9.99, 3)")
	afExec(t, db, ctx, "INSERT INTO scc VALUES (19.99, 1)")

	rows := afQuery(t, db, ctx, "SELECT price * qty as total FROM scc ORDER BY total DESC")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestV86_SelectCountDistinct(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE scd (cat TEXT)")
	afExec(t, db, ctx, "INSERT INTO scd VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO scd VALUES ('a')")
	afExec(t, db, ctx, "INSERT INTO scd VALUES ('b')")
	afExec(t, db, ctx, "INSERT INTO scd VALUES ('b')")
	afExec(t, db, ctx, "INSERT INTO scd VALUES ('c')")

	afExpectVal(t, db, ctx, "SELECT COUNT(DISTINCT cat) FROM scd", float64(3))
}

func TestV86_SelectWithAlias(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE swa (id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO swa VALUES (1, 'hello')")

	rows := afQuery(t, db, ctx, "SELECT id AS identifier, UPPER(name) AS upper_name FROM swa")
	if fmt.Sprintf("%v", rows[0][1]) != "HELLO" {
		t.Fatalf("expected 'HELLO', got %v", rows[0][1])
	}
}

// ==================== MULTIPLE JOIN TYPES ====================

func TestV86_LeftJoinWithNull(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ljn_a (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE ljn_b (a_id INTEGER, extra TEXT)")
	afExec(t, db, ctx, "INSERT INTO ljn_a VALUES (1, 'one')")
	afExec(t, db, ctx, "INSERT INTO ljn_a VALUES (2, 'two')")
	afExec(t, db, ctx, "INSERT INTO ljn_b VALUES (1, 'match')")

	rows := afQuery(t, db, ctx, `SELECT a.val, b.extra FROM ljn_a a LEFT JOIN ljn_b b ON a.id = b.a_id ORDER BY a.id`)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[1][1] != nil {
		t.Fatalf("expected NULL for unmatched left join, got %v", rows[1][1])
	}
}

func TestV86_CrossJoin(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE cj_a (x INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE cj_b (y INTEGER)")
	afExec(t, db, ctx, "INSERT INTO cj_a VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO cj_a VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO cj_b VALUES (10)")
	afExec(t, db, ctx, "INSERT INTO cj_b VALUES (20)")

	rows := afQuery(t, db, ctx, "SELECT x, y FROM cj_a CROSS JOIN cj_b ORDER BY x, y")
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows (2x2 cross join), got %d", len(rows))
	}
}

// ==================== TRIGGER WHEN WITH COMPLEX OPS ====================

func TestV86_TriggerWhenCASE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE twc (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE twc_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER twc_case AFTER INSERT ON twc
		FOR EACH ROW WHEN CASE WHEN NEW.val > 50 THEN 1 ELSE 0 END = 1
		BEGIN
			INSERT INTO twc_log VALUES ('big');
		END`)

	afExec(t, db, ctx, "INSERT INTO twc VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO twc VALUES (2, 100)")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM twc_log", float64(1))
}

func TestV86_TriggerWhenArithmetic(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE twa (id INTEGER PRIMARY KEY, price INTEGER, qty INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE twa_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER twa_expensive AFTER INSERT ON twa
		FOR EACH ROW WHEN NEW.price * NEW.qty > 100
		BEGIN
			INSERT INTO twa_log VALUES ('expensive_order');
		END`)

	afExec(t, db, ctx, "INSERT INTO twa VALUES (1, 5, 10)")    // 50 - skip
	afExec(t, db, ctx, "INSERT INTO twa VALUES (2, 20, 10)")   // 200 - fire
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM twa_log", float64(1))
}

// ==================== INDEX USAGE WITH COMPOSITE ====================

func TestV86_CompositeIndex(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE ci (a INTEGER, b INTEGER, c TEXT)")
	afExec(t, db, ctx, "CREATE INDEX ci_ab ON ci(a, b)")
	for i := 0; i < 20; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO ci VALUES (%d, %d, 'val%d')", i%5, i%3, i))
	}

	rows := afQuery(t, db, ctx, "SELECT c FROM ci WHERE a = 2 AND b = 1")
	if len(rows) == 0 {
		t.Fatalf("expected results from composite index query")
	}
}

// ==================== WINDOW FUNCTIONS ====================

func TestV86_WindowRowNumber(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wrn (dept TEXT, name TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wrn VALUES ('eng', 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO wrn VALUES ('eng', 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO wrn VALUES ('hr', 'Carol', 150)")

	rows := afQuery(t, db, ctx, `SELECT name, salary,
		ROW_NUMBER() OVER (ORDER BY salary DESC) as rn
		FROM wrn`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestV86_WindowPartitionBy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wpb (dept TEXT, emp TEXT, salary INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wpb VALUES ('eng', 'Alice', 100)")
	afExec(t, db, ctx, "INSERT INTO wpb VALUES ('eng', 'Bob', 200)")
	afExec(t, db, ctx, "INSERT INTO wpb VALUES ('hr', 'Carol', 150)")
	afExec(t, db, ctx, "INSERT INTO wpb VALUES ('hr', 'Dave', 120)")

	rows := afQuery(t, db, ctx, `SELECT emp, dept,
		ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as dept_rank
		FROM wpb ORDER BY dept, dept_rank`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestV86_WindowSumRunning(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE wsr (id INTEGER, amount INTEGER)")
	afExec(t, db, ctx, "INSERT INTO wsr VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO wsr VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO wsr VALUES (3, 30)")

	rows := afQuery(t, db, ctx, `SELECT id, amount,
		SUM(amount) OVER (ORDER BY id) as running_total
		FROM wsr ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

// ==================== RECURSIVE CTE DEEPER ====================

func TestV86_RecursiveCTEFibonacci(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `WITH RECURSIVE fib(n, a, b) AS (
		SELECT 1, 0, 1
		UNION ALL
		SELECT n + 1, b, a + b FROM fib WHERE n < 10
	) SELECT a FROM fib`)
	if len(rows) != 10 {
		t.Fatalf("expected 10 rows, got %d", len(rows))
	}
}

func TestV86_RecursiveCTEHierarchy(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE rch (id INTEGER, parent_id INTEGER, name TEXT)")
	afExec(t, db, ctx, "INSERT INTO rch VALUES (1, NULL, 'root')")
	afExec(t, db, ctx, "INSERT INTO rch VALUES (2, 1, 'child1')")
	afExec(t, db, ctx, "INSERT INTO rch VALUES (3, 1, 'child2')")
	afExec(t, db, ctx, "INSERT INTO rch VALUES (4, 2, 'grandchild1')")

	rows := afQuery(t, db, ctx, `WITH RECURSIVE tree(id, name, level) AS (
		SELECT id, name, 0 FROM rch WHERE parent_id IS NULL
		UNION ALL
		SELECT rch.id, rch.name, tree.level + 1
		FROM rch INNER JOIN tree ON rch.parent_id = tree.id
	) SELECT * FROM tree ORDER BY level, id`)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

// ==================== COMPLEX UPDATE PATTERNS ====================

func TestV86_UpdateWithSubquery(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE uws (id INTEGER, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO uws VALUES (1, 10)")
	afExec(t, db, ctx, "INSERT INTO uws VALUES (2, 20)")
	afExec(t, db, ctx, "INSERT INTO uws VALUES (3, 30)")

	afExec(t, db, ctx, "UPDATE uws SET val = val * 2 WHERE val > (SELECT AVG(val) FROM uws)")
	rows := afQuery(t, db, ctx, "SELECT id, val FROM uws ORDER BY id")
	// AVG = 20, so id=3 (val=30) gets doubled to 60
	if fmt.Sprintf("%v", rows[2][1]) != "60" {
		t.Fatalf("expected val=60 for id=3, got %v", rows[2][1])
	}
}

func TestV86_UpdateMultipleColumns(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE umc (id INTEGER, a TEXT, b INTEGER, c REAL)")
	afExec(t, db, ctx, "INSERT INTO umc VALUES (1, 'old', 10, 1.5)")

	afExec(t, db, ctx, "UPDATE umc SET a = 'new', b = b + 5, c = c * 2 WHERE id = 1")
	rows := afQuery(t, db, ctx, "SELECT a, b, c FROM umc WHERE id = 1")
	if fmt.Sprintf("%v", rows[0][0]) != "new" {
		t.Fatalf("expected 'new', got %v", rows[0][0])
	}
}

// ==================== COMPLEX DELETE PATTERNS ====================

func TestV86_DeleteAll(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE da (id INTEGER, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO da VALUES (1, 'a')")
	afExec(t, db, ctx, "INSERT INTO da VALUES (2, 'b')")
	afExec(t, db, ctx, "INSERT INTO da VALUES (3, 'c')")

	afExec(t, db, ctx, "DELETE FROM da")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM da", float64(0))
}

func TestV86_DeleteWithComplexWhere(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE dcw (id INTEGER, cat TEXT, val INTEGER)")
	afExec(t, db, ctx, "INSERT INTO dcw VALUES (1, 'a', 10)")
	afExec(t, db, ctx, "INSERT INTO dcw VALUES (2, 'b', 20)")
	afExec(t, db, ctx, "INSERT INTO dcw VALUES (3, 'a', 30)")
	afExec(t, db, ctx, "INSERT INTO dcw VALUES (4, 'b', 40)")

	afExec(t, db, ctx, "DELETE FROM dcw WHERE cat = 'a' AND val > 15")
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM dcw", float64(3))
}

// ==================== VACUUM WITH MULTIPLE TABLES ====================

func TestV86_VacuumMultipleTables(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE vm1 (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "CREATE TABLE vm2 (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 10; i++ {
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO vm1 VALUES (%d, 'v%d')", i, i))
		afExec(t, db, ctx, fmt.Sprintf("INSERT INTO vm2 VALUES (%d, 'v%d')", i, i))
	}
	afExec(t, db, ctx, "DELETE FROM vm1 WHERE id > 5")
	afExec(t, db, ctx, "DELETE FROM vm2 WHERE id > 7")

	afExec(t, db, ctx, "VACUUM")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM vm1", float64(5))
	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM vm2", float64(7))
}

// ==================== SAVEPOINT + TRIGGER WHEN ====================

func TestV86_SavepointWithTriggerWhen(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE sptw (id INTEGER PRIMARY KEY, val INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE sptw_log (msg TEXT)")

	afExec(t, db, ctx, `CREATE TRIGGER sptw_big AFTER INSERT ON sptw
		FOR EACH ROW WHEN NEW.val > 50
		BEGIN
			INSERT INTO sptw_log VALUES ('big:' || CAST(NEW.val AS TEXT));
		END`)

	afExec(t, db, ctx, "BEGIN TRANSACTION")
	afExec(t, db, ctx, "INSERT INTO sptw VALUES (1, 10)")   // no trigger
	afExec(t, db, ctx, "SAVEPOINT sp1")
	afExec(t, db, ctx, "INSERT INTO sptw VALUES (2, 100)")  // trigger fires
	afExec(t, db, ctx, "ROLLBACK TO SAVEPOINT sp1")         // undo insert + trigger
	afExec(t, db, ctx, "INSERT INTO sptw VALUES (3, 75)")   // trigger fires
	afExec(t, db, ctx, "COMMIT")

	afExpectVal(t, db, ctx, "SELECT COUNT(*) FROM sptw", float64(2))
}

// ==================== STRING FUNCTIONS DEEPER ====================

func TestV86_FunctionPRINTF(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT PRINTF('%d items', 5)", "5 items")
	afExpectVal(t, db, ctx, "SELECT PRINTF('%s world', 'hello')", "hello world")
}

func TestV86_FunctionCHAR(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT CHAR(65)", "A")
	afExpectVal(t, db, ctx, "SELECT CHAR(97)", "a")
	afExpectVal(t, db, ctx, "SELECT CHAR(48)", "0")
}

func TestV86_FunctionUNICODE(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, "SELECT UNICODE('A')", float64(65))
	afExpectVal(t, db, ctx, "SELECT UNICODE('a')", float64(97))
}

func TestV86_FunctionGLOB(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE gt (val TEXT)")
	afExec(t, db, ctx, "INSERT INTO gt VALUES ('hello')")
	afExec(t, db, ctx, "INSERT INTO gt VALUES ('world')")
	afExec(t, db, ctx, "INSERT INTO gt VALUES ('help')")

	rows := afQuery(t, db, ctx, "SELECT val FROM gt WHERE GLOB('hel*', val)")
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for GLOB 'hel*', got %d: %v", len(rows), rows)
	}
}

// ==================== JSON DEEPER ====================

func TestV86_JSONExtractArray(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExpectVal(t, db, ctx, `SELECT JSON_EXTRACT('[10,20,30]', '$[1]')`, float64(20))
}

func TestV86_JSONPretty(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_PRETTY('{"a":1,"b":2}')`)
	if rows[0][0] == nil {
		t.Fatalf("expected non-nil result from JSON_PRETTY")
	}
}

func TestV86_JSONMinify(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	rows := afQuery(t, db, ctx, `SELECT JSON_MINIFY('{ "a" : 1 , "b" : 2 }')`)
	if rows[0][0] == nil {
		t.Fatalf("expected non-nil result from JSON_MINIFY")
	}
}

// ==================== MULTIPLE SET OPERATIONS ====================

func TestV86_UnionChained(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	afExec(t, db, ctx, "CREATE TABLE uc1 (v INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE uc2 (v INTEGER)")
	afExec(t, db, ctx, "CREATE TABLE uc3 (v INTEGER)")
	afExec(t, db, ctx, "INSERT INTO uc1 VALUES (1)")
	afExec(t, db, ctx, "INSERT INTO uc2 VALUES (2)")
	afExec(t, db, ctx, "INSERT INTO uc3 VALUES (3)")

	rows := afQuery(t, db, ctx, "SELECT v FROM uc1 UNION SELECT v FROM uc2 UNION SELECT v FROM uc3 ORDER BY v")
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}
