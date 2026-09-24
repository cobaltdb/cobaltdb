package advisor

import (
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
)

// TestSubqueryColumnsNotAttributedToOuterTable pins the contract that columns
// inside an IN/EXISTS subquery are never attributed to the outer query's
// primary table.
//
// Regression: extractColumnsRecursive merged subquery-internal WHERE columns
// into the outer expression map including the unqualified ("" table) bucket,
// so analyzeSelect recorded them under the outer FROM table and
// Recommendations proposed indexes like orders(region) for a column that
// belongs to the subquery's own table (customers). Qualified subquery
// references (customers.tier) must keep counting under their own table.
func TestSubqueryColumnsNotAttributedToOuterTable(t *testing.T) {
	a := NewIndexAdvisor()
	analyze := func(sql string) {
		t.Helper()
		stmt, err := query.Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		a.Analyze(stmt)
	}

	analyze("SELECT * FROM orders WHERE cust_id IN (SELECT id FROM customers WHERE region = 'EU')")
	analyze("SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE tier = 'gold')")
	// Qualified subquery columns must keep counting under their own table.
	analyze("SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM customers WHERE customers.tier = 'gold')")

	recs := a.Recommendations(nil)
	var sawCustID, sawRegionOnOrders, sawTierOnOrders, sawTierOnCustomers bool
	for _, rec := range recs {
		joined := strings.Join(rec.Columns, ",")
		switch {
		case rec.TableName == "orders" && strings.Contains(joined, "cust_id"):
			sawCustID = true
		case rec.TableName == "orders" && strings.Contains(joined, "region"):
			sawRegionOnOrders = true
		case rec.TableName == "orders" && strings.Contains(joined, "tier"):
			sawTierOnOrders = true
		case rec.TableName == "customers" && strings.Contains(joined, "tier"):
			sawTierOnCustomers = true
		}
	}

	if !sawCustID {
		t.Fatal("control failed: outer column cust_id was not counted on orders")
	}
	if sawRegionOnOrders {
		t.Fatal("orders(region) recommended — subquery column attributed to the outer table")
	}
	if sawTierOnOrders {
		t.Fatal("orders(tier) recommended — subquery column attributed to the outer table")
	}
	if !sawTierOnCustomers {
		t.Fatal("qualified subquery column tier no longer counted under customers (over-correction)")
	}

	// Direct pattern inspection (same package): orders must not know the
	// subquery-internal columns at all; customers keeps the qualified one.
	a.mu.RLock()
	defer a.mu.RUnlock()
	orders := a.patterns["orders"]
	if orders == nil {
		t.Fatal("orders pattern missing entirely")
	}
	if _, ok := orders.Columns["region"]; ok {
		t.Fatal("orders pattern recorded subquery column region")
	}
	if _, ok := orders.Columns["tier"]; ok {
		t.Fatal("orders pattern recorded subquery column tier")
	}
	if _, ok := orders.Columns["cust_id"]; !ok {
		t.Fatal("orders pattern missing outer column cust_id")
	}
	customers := a.patterns["customers"]
	if customers == nil {
		t.Fatal("customers pattern missing entirely")
	}
	if _, ok := customers.Columns["tier"]; !ok {
		t.Fatal("customers pattern missing qualified subquery column tier")
	}
	if _, ok := customers.Columns["region"]; ok {
		t.Fatal("customers pattern recorded unqualified subquery column region (attribution guess)")
	}
}
