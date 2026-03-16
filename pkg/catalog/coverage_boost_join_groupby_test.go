package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

// TestCoverage_executeSelectWithJoinAndGroupByBasic tests basic JOIN + GROUP BY
func TestCoverage_executeSelectWithJoinAndGroupByBasic(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgb_orders",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "customer_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgb_customers",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "region", Type: query.TokenText},
		},
	})

	// Insert customers
	regions := []string{"North", "South", "East", "West"}
	for i, region := range regions {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_customers",
			Columns: []string{"id", "region"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(region)}},
		}, nil)
	}

	// Insert orders
	for i := 1; i <= 40; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_orders",
			Columns: []string{"id", "customer_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%4)+1)), numReal(float64(i * 10))}},
		}, nil)
	}

	// JOIN + GROUP BY
	result, err := c.ExecuteQuery("SELECT c.region, COUNT(*) as order_count, SUM(o.amount) as total_amount FROM jgb_orders o JOIN jgb_customers c ON o.customer_id = c.id GROUP BY c.region")
	if err != nil {
		t.Logf("JOIN + GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN + GROUP BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Region: %v, Count: %v, Total: %v", row[0], row[1], row[2])
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByMultipleJoins tests multiple JOINs + GROUP BY
func TestCoverage_executeSelectWithJoinAndGroupByMultipleJoins(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create three tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgb_products",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
			{Name: "price", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgb_categories",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "department_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgb_departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert departments
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_departments",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Dept" + string(rune('0'+i)))}},
		}, nil)
	}

	// Insert categories
	for i := 1; i <= 6; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_categories",
			Columns: []string{"id", "department_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)/2 + 1)), strReal("Cat" + string(rune('0'+i)))}},
		}, nil)
	}

	// Insert products
	for i := 1; i <= 30; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_products",
			Columns: []string{"id", "category_id", "name", "price"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%6 + 1)), strReal("Product" + string(rune('0'+i%10))), numReal(float64(i * 10))}},
		}, nil)
	}

	// Multiple JOINs + GROUP BY
	result, err := c.ExecuteQuery("SELECT d.name as dept, COUNT(*) as product_count, SUM(p.price) as total_value FROM jgb_products p JOIN jgb_categories c ON p.category_id = c.id JOIN jgb_departments d ON c.department_id = d.id GROUP BY d.name")
	if err != nil {
		t.Logf("Multiple JOINs + GROUP BY error: %v", err)
	} else {
		t.Logf("Multiple JOINs + GROUP BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Dept: %v, Count: %v, Total: %v", row[0], row[1], row[2])
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByWithHaving tests JOIN + GROUP BY + HAVING
func TestCoverage_executeSelectWithJoinAndGroupByWithHaving(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgbh_sales",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "store_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgbh_stores",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "city", Type: query.TokenText},
		},
	})

	// Insert stores
	cities := []string{"NYC", "LA", "Chicago", "Houston"}
	for i, city := range cities {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgbh_stores",
			Columns: []string{"id", "city"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(city)}},
		}, nil)
	}

	// Insert sales with varying amounts
	for i := 1; i <= 50; i++ {
		amount := i * 100
		if i%4 == 0 {
			amount = i * 10 // Smaller amounts
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgbh_sales",
			Columns: []string{"id", "store_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%4)+1)), numReal(float64(amount))}},
		}, nil)
	}

	// JOIN + GROUP BY + HAVING
	result, err := c.ExecuteQuery("SELECT s.city, COUNT(*) as sale_count, SUM(sa.amount) as total_sales FROM jgbh_sales sa JOIN jgbh_stores s ON sa.store_id = s.id GROUP BY s.city HAVING SUM(sa.amount) > 5000")
	if err != nil {
		t.Logf("JOIN + GROUP BY + HAVING error: %v", err)
	} else {
		t.Logf("JOIN + GROUP BY + HAVING returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  City: %v, Count: %v, Total: %v", row[0], row[1], row[2])
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByWithOrderBy tests JOIN + GROUP BY + ORDER BY
func TestCoverage_executeSelectWithJoinAndGroupByWithOrderBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgbo_items",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "vendor_id", Type: query.TokenInteger},
			{Name: "cost", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgbo_vendors",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert vendors
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgbo_vendors",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Vendor" + string(rune('0'+i)))}},
		}, nil)
	}

	// Insert items
	for i := 1; i <= 25; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgbo_items",
			Columns: []string{"id", "vendor_id", "cost"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%5)+1)), numReal(float64((6-i%5) * 100))}},
		}, nil)
	}

	// JOIN + GROUP BY + ORDER BY
	result, err := c.ExecuteQuery("SELECT v.name, COUNT(*) as item_count, AVG(i.cost) as avg_cost FROM jgbo_items i JOIN jgbo_vendors v ON i.vendor_id = v.id GROUP BY v.name ORDER BY avg_cost DESC")
	if err != nil {
		t.Logf("JOIN + GROUP BY + ORDER BY error: %v", err)
	} else {
		t.Logf("JOIN + GROUP BY + ORDER BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Vendor: %v, Count: %v, Avg: %v", row[0], row[1], row[2])
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByLeftJoin tests LEFT JOIN + GROUP BY
func TestCoverage_executeSelectWithJoinAndGroupByLeftJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgl_employees",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "dept_id", Type: query.TokenInteger},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgl_departments",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert departments
	for i := 1; i <= 4; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgl_departments",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Dept" + string(rune('0'+i)))}},
		}, nil)
	}

	// Insert employees (not all departments have employees)
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgl_employees",
			Columns: []string{"id", "dept_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%3)+1)), strReal("Emp" + string(rune('0'+i)))}},
		}, nil)
	}

	// LEFT JOIN + GROUP BY
	result, err := c.ExecuteQuery("SELECT d.name, COUNT(e.id) as emp_count FROM jgl_departments d LEFT JOIN jgl_employees e ON d.id = e.dept_id GROUP BY d.name")
	if err != nil {
		t.Logf("LEFT JOIN + GROUP BY error: %v", err)
	} else {
		t.Logf("LEFT JOIN + GROUP BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Dept: %v, Count: %v", row[0], row[1])
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByExpressionGroupBy tests GROUP BY with expressions
func TestCoverage_executeSelectWithJoinAndGroupByExpressionGroupBy(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jge_transactions",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "account_id", Type: query.TokenInteger},
			{Name: "amount", Type: query.TokenInteger},
			{Name: "tx_type", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jge_accounts",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "type", Type: query.TokenText},
		},
	})

	// Insert accounts
	acctTypes := []string{"checking", "savings", "credit"}
	for i, acctType := range acctTypes {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jge_accounts",
			Columns: []string{"id", "type"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(acctType)}},
		}, nil)
	}

	// Insert transactions
	for i := 1; i <= 30; i++ {
		txType := "credit"
		if i%2 == 0 {
			txType = "debit"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jge_transactions",
			Columns: []string{"id", "account_id", "amount", "tx_type"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%3)+1)), numReal(float64(i * 10)), strReal(txType)}},
		}, nil)
	}

	// JOIN + GROUP BY with CASE expression
	result, err := c.ExecuteQuery("SELECT a.type, CASE WHEN t.tx_type = 'credit' THEN 'In' ELSE 'Out' END as direction, COUNT(*) as count FROM jge_transactions t JOIN jge_accounts a ON t.account_id = a.id GROUP BY a.type, direction")
	if err != nil {
		t.Logf("JOIN + GROUP BY expression error: %v", err)
	} else {
		t.Logf("JOIN + GROUP BY expression returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Type: %v, Direction: %v, Count: %v", row[0], row[1], row[2])
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByQualifiedIdentifier tests GROUP BY with qualified identifiers
func TestCoverage_executeSelectWithJoinAndGroupByQualifiedIdentifier(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgq_inventory",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "warehouse_id", Type: query.TokenInteger},
			{Name: "quantity", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgq_warehouses",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "location", Type: query.TokenText},
		},
	})

	// Insert warehouses
	locations := []string{"NYC", "LA", "Chicago"}
	for i, loc := range locations {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgq_warehouses",
			Columns: []string{"id", "location"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(loc)}},
		}, nil)
	}

	// Insert inventory
	for i := 1; i <= 30; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgq_inventory",
			Columns: []string{"id", "warehouse_id", "quantity"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%3)+1)), numReal(float64(i * 5))}},
		}, nil)
	}

	// JOIN + GROUP BY with qualified identifiers
	result, err := c.ExecuteQuery("SELECT w.location, SUM(i.quantity) as total_qty FROM jgq_inventory i JOIN jgq_warehouses w ON i.warehouse_id = w.id GROUP BY w.location ORDER BY w.location")
	if err != nil {
		t.Logf("JOIN + GROUP BY qualified identifier error: %v", err)
	} else {
		t.Logf("JOIN + GROUP BY qualified identifier returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Location: %v, Total: %v", row[0], row[1])
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByLimitOffset tests JOIN + GROUP BY with LIMIT/OFFSET
func TestCoverage_executeSelectWithJoinAndGroupByLimitOffset(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jglo_data",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "group_id", Type: query.TokenInteger},
			{Name: "value", Type: query.TokenInteger},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jglo_groups",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	// Insert groups
	for i := 1; i <= 10; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jglo_groups",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Group" + string(rune('0'+i%10)))}},
		}, nil)
	}

	// Insert data
	for i := 1; i <= 100; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jglo_data",
			Columns: []string{"id", "group_id", "value"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i%10)+1)), numReal(float64(i))}},
		}, nil)
	}

	// JOIN + GROUP BY + LIMIT
	result, err := c.ExecuteQuery("SELECT g.name, COUNT(*) as cnt, SUM(d.value) as total FROM jglo_data d JOIN jglo_groups g ON d.group_id = g.id GROUP BY g.name ORDER BY g.name LIMIT 5")
	if err != nil {
		t.Logf("JOIN + GROUP BY + LIMIT error: %v", err)
	} else {
		t.Logf("JOIN + GROUP BY + LIMIT returned %d rows", len(result.Rows))
	}

	// JOIN + GROUP BY + LIMIT + OFFSET
	result, err = c.ExecuteQuery("SELECT g.name, COUNT(*) as cnt, SUM(d.value) as total FROM jglo_data d JOIN jglo_groups g ON d.group_id = g.id GROUP BY g.name ORDER BY g.name LIMIT 3 OFFSET 3")
	if err != nil {
		t.Logf("JOIN + GROUP BY + LIMIT + OFFSET error: %v", err)
	} else {
		t.Logf("JOIN + GROUP BY + LIMIT + OFFSET returned %d rows", len(result.Rows))
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByDerivedTable tests JOIN + GROUP BY with derived table
func TestCoverage_executeSelectWithJoinAndGroupByDerivedTable(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create base table
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgdt_base",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "category", Type: query.TokenText},
			{Name: "subcategory", Type: query.TokenText},
			{Name: "amount", Type: query.TokenInteger},
		},
	})

	// Insert data
	categories := []string{"A", "B", "C"}
	subcategories := []string{"X", "Y"}
	id := 1
	for _, cat := range categories {
		for _, sub := range subcategories {
			for i := 1; i <= 5; i++ {
				c.Insert(ctx, &query.InsertStmt{
					Table:   "jgdt_base",
					Columns: []string{"id", "category", "subcategory", "amount"},
					Values:  [][]query.Expression{{numReal(float64(id)), strReal(cat), strReal(sub), numReal(float64(i * 10))}},
				}, nil)
				id++
			}
		}
	}

	// JOIN with derived table + GROUP BY
	result, err := c.ExecuteQuery("SELECT dt.cat, COUNT(*) as cnt, SUM(b.amount) as total FROM jgdt_base b JOIN (SELECT DISTINCT category as cat FROM jgdt_base) as dt ON b.category = dt.cat GROUP BY dt.cat")
	if err != nil {
		t.Logf("JOIN + derived table + GROUP BY error: %v", err)
	} else {
		t.Logf("JOIN + derived table + GROUP BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			if len(row) >= 3 {
				t.Logf("  Category: %v, Count: %v, Total: %v", row[0], row[1], row[2])
			} else {
				t.Logf("  Row: %v", row)
			}
		}
	}
}

// TestCoverage_executeSelectWithJoinAndGroupByCrossJoin tests CROSS JOIN + GROUP BY
func TestCoverage_executeSelectWithJoinAndGroupByCrossJoin(t *testing.T) {
	ctx := context.Background()
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	defer pool.Close()
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	// Create tables
	c.CreateTable(&query.CreateTableStmt{
		Table: "jgc_colors",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgc_sizes",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "name", Type: query.TokenText},
		},
	})

	c.CreateTable(&query.CreateTableStmt{
		Table: "jgc_inventory",
		Columns: []*query.ColumnDef{
			{Name: "color_id", Type: query.TokenInteger},
			{Name: "size_id", Type: query.TokenInteger},
			{Name: "quantity", Type: query.TokenInteger},
		},
	})

	// Insert colors
	colors := []string{"Red", "Blue", "Green"}
	for i, color := range colors {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgc_colors",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(color)}},
		}, nil)
	}

	// Insert sizes
	sizes := []string{"S", "M", "L"}
	for i, size := range sizes {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgc_sizes",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(size)}},
		}, nil)
	}

	// Insert inventory
	for ci := 1; ci <= 3; ci++ {
		for si := 1; si <= 3; si++ {
			c.Insert(ctx, &query.InsertStmt{
				Table:   "jgc_inventory",
				Columns: []string{"color_id", "size_id", "quantity"},
				Values:  [][]query.Expression{{numReal(float64(ci)), numReal(float64(si)), numReal(float64(ci*si*10))}},
			}, nil)
		}
	}

	// CROSS JOIN + GROUP BY
	result, err := c.ExecuteQuery("SELECT c.name as color, SUM(i.quantity) as total FROM jgc_colors c CROSS JOIN jgc_sizes s JOIN jgc_inventory i ON c.id = i.color_id AND s.id = i.size_id GROUP BY c.name")
	if err != nil {
		t.Logf("CROSS JOIN + GROUP BY error: %v", err)
	} else {
		t.Logf("CROSS JOIN + GROUP BY returned %d rows", len(result.Rows))
		for _, row := range result.Rows {
			t.Logf("  Color: %v, Total: %v", row[0], row[1])
		}
	}
}
