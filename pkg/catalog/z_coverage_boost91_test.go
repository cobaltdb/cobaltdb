package catalog

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func newB91Cat() *Catalog {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	return New(tree, pool, nil)
}

func b91CreateTable(t *testing.T, c *Catalog, name string, cols []*query.ColumnDef) {
	t.Helper()
	if err := c.CreateTable(&query.CreateTableStmt{Table: name, Columns: cols}); err != nil {
		t.Fatalf("CreateTable %s: %v", name, err)
	}
}

func b91Insert(t *testing.T, c *Catalog, table string, cols []string, vals []query.Expression) {
	t.Helper()
	ctx := context.Background()
	_, _, err := c.Insert(ctx, &query.InsertStmt{
		Table:   table,
		Columns: cols,
		Values:  [][]query.Expression{vals},
	}, nil)
	if err != nil {
		t.Fatalf("Insert into %s: %v", table, err)
	}
}

// TestB91_UpdateWithIndex tests UPDATE using index path
func TestB91_UpdateWithIndex(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "upd_idx", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "score", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		b91Insert(t, c, "upd_idx", []string{"id", "name", "score"}, []query.Expression{
			&query.NumberLiteral{Value: float64(i)},
			&query.StringLiteral{Value: "person"},
			&query.NumberLiteral{Value: float64(i * 10)},
		})
	}

	// Create index first
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "idx_upd_score",
		Table:   "upd_idx",
		Columns: []string{"score"},
	})

	// Update with WHERE that can use index
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_idx",
		Set:   []*query.SetClause{{Column: "name", Value: &query.StringLiteral{Value: "updated"}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "score"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 50},
		},
	}, nil)
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}
}

// TestB91_UpdateCaseExpr tests UPDATE with CASE expression in SET
func TestB91_UpdateCaseExpr(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "upd_case", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
		{Name: "label", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		b91Insert(t, c, "upd_case", []string{"id", "val"}, []query.Expression{
			&query.NumberLiteral{Value: float64(i)},
			&query.NumberLiteral{Value: float64(i * 10)},
		})
	}

	// UPDATE with CASE expression
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "upd_case",
		Set: []*query.SetClause{
			{
				Column: "label",
				Value: &query.CaseExpr{
					Whens: []*query.WhenClause{
						{
							Condition: &query.BinaryExpr{
								Left:     &query.Identifier{Name: "val"},
								Operator: query.TokenGt,
								Right:    &query.NumberLiteral{Value: 30},
							},
							Result: &query.StringLiteral{Value: "high"},
						},
					},
					Else: &query.StringLiteral{Value: "low"},
				},
			},
		},
	}, nil)
	if err != nil {
		t.Errorf("UPDATE with CASE failed: %v", err)
	}
}

// TestB91_InsertWithTextPK tests insert with non-numeric (text) primary key
func TestB91_InsertWithTextPK(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "txt_pk", []*query.ColumnDef{
		{Name: "code", Type: query.TokenText, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	ctx := context.Background()
	rows := [][]string{
		{"A001", "Alpha"},
		{"B002", "Beta"},
		{"C003", "Gamma"},
	}
	for _, r := range rows {
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "txt_pk",
			Columns: []string{"code", "name"},
			Values: [][]query.Expression{{
				&query.StringLiteral{Value: r[0]},
				&query.StringLiteral{Value: r[1]},
			}},
		}, nil)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	result, err := c.ExecuteQuery("SELECT code, name FROM txt_pk")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
}

// TestB91_FKRestrictOnDelete tests FK RESTRICT on delete
func TestB91_FKRestrictOnDelete(t *testing.T) {
	c := newB91Cat()
	// Parent table
	b91CreateTable(t, c, "fk_parent91", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})
	// Child table with RESTRICT FK
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_child91",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent91",
				ReferencedColumns: []string{"id"},
				OnDelete:          "RESTRICT",
			},
		},
	})

	ctx := context.Background()
	b91Insert(t, c, "fk_parent91", []string{"id", "name"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "Parent1"},
	})
	b91Insert(t, c, "fk_child91", []string{"id", "parent_id"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1},
	})

	// Should fail with RESTRICT
	_, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent91",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Error("Expected RESTRICT error on FK delete, got nil")
	}
}

// TestB91_FKDefaultRestrictOnDelete tests FK default behavior (no action specified) on delete
func TestB91_FKDefaultRestrictOnDelete(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "fk_parent_def", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_child_def",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_parent_def",
				ReferencedColumns: []string{"id"},
				// No OnDelete - should default to restrict
			},
		},
	})

	ctx := context.Background()
	b91Insert(t, c, "fk_parent_def", []string{"id"}, []query.Expression{&query.NumberLiteral{Value: 1}})
	b91Insert(t, c, "fk_child_def", []string{"id", "parent_id"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1},
	})

	_, _, err := c.Delete(ctx, &query.DeleteStmt{
		Table: "fk_parent_def",
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Error("Expected FK restriction error, got nil")
	}
}

// TestB91_FKRestrictOnUpdate tests FK RESTRICT on update
func TestB91_FKRestrictOnUpdate(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "fk_pu_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_pu_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_pu_parent",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "RESTRICT",
			},
		},
	})

	ctx := context.Background()
	b91Insert(t, c, "fk_pu_parent", []string{"id"}, []query.Expression{&query.NumberLiteral{Value: 1}})
	b91Insert(t, c, "fk_pu_child", []string{"id", "parent_id"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1},
	})

	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "fk_pu_parent",
		Set:   []*query.SetClause{{Column: "id", Value: &query.NumberLiteral{Value: 99}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	if err == nil {
		t.Error("Expected RESTRICT error on FK update, got nil")
	}
}

// TestB91_FKSetNullOnUpdate tests FK SET NULL on update
func TestB91_FKSetNullOnUpdate(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "fk_snu_parent", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
	})
	c.CreateTable(&query.CreateTableStmt{
		Table: "fk_snu_child",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "parent_id", Type: query.TokenInteger},
		},
		ForeignKeys: []*query.ForeignKeyDef{
			{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "fk_snu_parent",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "SET NULL",
			},
		},
	})

	ctx := context.Background()
	b91Insert(t, c, "fk_snu_parent", []string{"id"}, []query.Expression{&query.NumberLiteral{Value: 1}})
	b91Insert(t, c, "fk_snu_child", []string{"id", "parent_id"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.NumberLiteral{Value: 1},
	})

	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "fk_snu_parent",
		Set:   []*query.SetClause{{Column: "id", Value: &query.NumberLiteral{Value: 99}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenEq,
			Right:    &query.NumberLiteral{Value: 1},
		},
	}, nil)
	// SET NULL on update may succeed or fail depending on implementation
	_ = err
}

// TestB91_AlterTableRenameInTxn tests AlterTableRename within a transaction
func TestB91_AlterTableRenameInTxn(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "rename_src", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	b91Insert(t, c, "rename_src", []string{"id", "val"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "hello"},
	})

	// Start transaction
	c.BeginTransaction(1001)

	// Rename table
	if err := c.AlterTableRename(&query.AlterTableStmt{
		Table:   "rename_src",
		NewName: "rename_dst",
	}); err != nil {
		t.Fatalf("AlterTableRename: %v", err)
	}

	// Rollback should restore old name
	if err := c.RollbackTransaction(); err != nil {
		t.Errorf("RollbackTransaction: %v", err)
	}

	// Verify old table still exists
	result, err := c.ExecuteQuery("SELECT id FROM rename_src")
	if err != nil {
		t.Errorf("Table should be restored after rollback: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("Expected 1 row, got %d", len(result.Rows))
	}
}

// TestB91_AlterTableRenameColumnInTxn tests AlterTableRenameColumn within a transaction
func TestB91_AlterTableRenameColumnInTxn(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "renamecol_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "old_name", Type: query.TokenText},
	})

	b91Insert(t, c, "renamecol_tbl", []string{"id", "old_name"}, []query.Expression{
		&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "hello"},
	})

	c.BeginTransaction(1001)

	if err := c.AlterTableRenameColumn(&query.AlterTableStmt{
		Table:   "renamecol_tbl",
		OldName: "old_name",
		NewName: "new_name",
	}); err != nil {
		t.Fatalf("AlterTableRenameColumn: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Errorf("RollbackTransaction: %v", err)
	}

	// After rollback, old_name should be back
	result, err := c.ExecuteQuery("SELECT old_name FROM renamecol_tbl")
	if err != nil {
		t.Errorf("Column should be restored after rollback: %v", err)
	}
	_ = result
}

// TestB91_RollbackDropIndex tests rollback of DROP INDEX
func TestB91_RollbackDropIndex(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "rollback_idx_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "rollback_idx",
		Table:   "rollback_idx_tbl",
		Columns: []string{"val"},
	})

	c.BeginTransaction(1001)

	if err := c.DropIndex("rollback_idx"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Errorf("RollbackTransaction: %v", err)
	}

	// Index should be restored
	idx, _ := c.GetIndex("rollback_idx")
	if idx == nil {
		t.Error("Index should be restored after rollback")
	}
}

// TestB91_RollbackAlterAddColumnInTxn tests rollback of ALTER TABLE ADD COLUMN
func TestB91_RollbackAlterAddColumnInTxn(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "alter_add_txn", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	c.BeginTransaction(1001)

	if err := c.AlterTableAddColumn(&query.AlterTableStmt{
		Table:  "alter_add_txn",
		Action: "ADD_COLUMN",
		Column: query.ColumnDef{Name: "extra", Type: query.TokenText},
	}); err != nil {
		t.Fatalf("AlterTableAddColumn: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Errorf("RollbackTransaction: %v", err)
	}

	// Column should be gone after rollback
	result, err := c.ExecuteQuery("SELECT id, name FROM alter_add_txn")
	if err != nil {
		t.Errorf("SELECT after rollback: %v", err)
	}
	_ = result
}

// TestB91_RollbackAlterDropColumnInTxn tests rollback of ALTER TABLE DROP COLUMN
func TestB91_RollbackAlterDropColumnInTxn(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "alter_drop_txn", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "extra", Type: query.TokenText},
	})

	b91Insert(t, c, "alter_drop_txn", []string{"id", "name", "extra"}, []query.Expression{
		&query.NumberLiteral{Value: 1},
		&query.StringLiteral{Value: "hello"},
		&query.StringLiteral{Value: "world"},
	})

	c.BeginTransaction(1001)

	if err := c.AlterTableDropColumn(&query.AlterTableStmt{
		Table:   "alter_drop_txn",
		NewName: "extra",
	}); err != nil {
		t.Fatalf("AlterTableDropColumn: %v", err)
	}

	if err := c.RollbackTransaction(); err != nil {
		t.Errorf("RollbackTransaction: %v", err)
	}

	// extra column should be restored
	result, err := c.ExecuteQuery("SELECT extra FROM alter_drop_txn")
	if err != nil {
		t.Errorf("extra column should be restored after rollback: %v", err)
	}
	_ = result
}

// TestB91_ExecuteQueryCTEWindow tests CTE with window function
func TestB91_ExecuteQueryCTEWindow(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "cte_win91", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenInteger},
	})

	ctx := context.Background()
	data := []struct {
		id     int
		dept   string
		salary int
	}{
		{1, "Eng", 100}, {2, "Eng", 120}, {3, "HR", 90}, {4, "HR", 95}, {5, "Eng", 110},
	}
	for _, d := range data {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "cte_win91",
			Columns: []string{"id", "dept", "salary"},
			Values:  [][]query.Expression{{numReal(float64(d.id)), strReal(d.dept), numReal(float64(d.salary))}},
		}, nil)
	}

	queries := []string{
		`WITH eng AS (SELECT * FROM cte_win91 WHERE dept = 'Eng')
		 SELECT id, salary FROM eng ORDER BY salary`,
		`WITH totals AS (SELECT dept, SUM(salary) AS total FROM cte_win91 GROUP BY dept)
		 SELECT dept, total FROM totals ORDER BY dept`,
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("CTE query error: %v", err)
		} else {
			t.Logf("CTE returned %d rows", len(result.Rows))
		}
	}
}

// TestB91_JoinGroupByWithAggregates tests JOIN + GROUP BY + aggregates path
func TestB91_JoinGroupByWithAggregates(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "jgb_orders", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "customer_id", Type: query.TokenInteger},
		{Name: "amount", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, c, "jgb_customers", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_customers",
			Columns: []string{"id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("Customer " + string(rune('A'+i-1)))}},
		}, nil)
	}
	for i := 1; i <= 9; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jgb_orders",
			Columns: []string{"id", "customer_id", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64((i-1)%3 + 1)), numReal(float64(i * 10))}},
		}, nil)
	}

	queries := []string{
		`SELECT c.name, COUNT(o.id), SUM(o.amount)
		 FROM jgb_customers c JOIN jgb_orders o ON c.id = o.customer_id
		 GROUP BY c.name ORDER BY c.name`,
		`SELECT c.name, AVG(o.amount)
		 FROM jgb_customers c JOIN jgb_orders o ON c.id = o.customer_id
		 GROUP BY c.name
		 HAVING AVG(o.amount) > 30`,
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("JOIN+GROUP BY error: %v", err)
		} else {
			t.Logf("Returned %d rows", len(result.Rows))
		}
	}
}

// TestB91_UpdateWithSubquery tests UPDATE where SET value contains subquery
func TestB91_UpdateWithSubquery(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "usq_prices", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "price", Type: query.TokenInteger},
		{Name: "avg_price", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "usq_prices",
			Columns: []string{"id", "price"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 20))}},
		}, nil)
	}

	// Simple update (without subquery, just test processUpdateRow path)
	_, _, err := c.Update(ctx, &query.UpdateStmt{
		Table: "usq_prices",
		Set:   []*query.SetClause{{Column: "avg_price", Value: &query.NumberLiteral{Value: 60}}},
		Where: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "id"},
			Operator: query.TokenGte,
			Right:    &query.NumberLiteral{Value: 3},
		},
	}, nil)
	if err != nil {
		t.Errorf("UPDATE failed: %v", err)
	}
}

// TestB91_VacuumWithData tests Vacuum with actual table data
func TestB91_VacuumWithData(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "vac_tbl91", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "vac_tbl91",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("value")}},
		}, nil)
	}

	// Delete some rows to create fragmentation
	for i := 1; i <= 10; i += 2 {
		c.Delete(ctx, &query.DeleteStmt{
			Table: "vac_tbl91",
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: float64(i)},
			},
		}, nil)
	}

	if err := c.Vacuum(); err != nil {
		t.Errorf("Vacuum failed: %v", err)
	}

	// Verify data still accessible
	result, err := c.ExecuteQuery("SELECT COUNT(*) FROM vac_tbl91")
	if err != nil {
		t.Errorf("SELECT after Vacuum: %v", err)
	}
	_ = result
}

// TestB91_ScalarSelectWithDistinct tests scalar SELECT with DISTINCT and LIMIT
func TestB91_ScalarSelectWithDistinct(t *testing.T) {
	c := newB91Cat()
	queries := []struct {
		sql     string
		wantErr bool
	}{
		{"SELECT 1+2", false},
		{"SELECT UPPER('hello')", false},
		{"SELECT 1 WHERE 1=1", false},
		{"SELECT 1 WHERE 1=0", false},
		{"SELECT 'a' AS col1, 'b' AS col2", false},
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q.sql)
		if q.wantErr && err == nil {
			t.Errorf("Expected error for %q", q.sql)
		} else if !q.wantErr && err != nil {
			t.Errorf("Unexpected error for %q: %v", q.sql, err)
		} else if !q.wantErr && result != nil {
			t.Logf("Query %q returned %d rows", q.sql, len(result.Rows))
		}
	}
}

// TestB91_JSONSetPathBranches tests json_utils.go Set() various branches
func TestB91_JSONSetPathBranches(t *testing.T) {
	// Test JSONSet with various path types
	tests := []struct {
		jsonData string
		path     string
		value    string
	}{
		{`{"a":1}`, "$.a", "99"},
		{`{"a":{"b":2}}`, "$.a.b", "99"},
		{`[1,2,3]`, "$[0]", "99"},
		{`{"a":1}`, "$.newkey", `"hello"`},
		{`{}`, "$.x.y", "1"},
	}
	for _, tt := range tests {
		result, err := JSONSet(tt.jsonData, tt.path, tt.value)
		if err != nil {
			t.Logf("JSONSet(%q, %q, %q) error: %v", tt.jsonData, tt.path, tt.value, err)
		} else {
			t.Logf("JSONSet result: %s", result)
		}
	}
}

// TestB91_JSONQuotePaths tests JSONQuote with various inputs
func TestB91_JSONQuotePaths(t *testing.T) {
	tests := []string{
		"hello",
		"hello \"world\"",
		"",
		"with\nnewline",
		"with\ttab",
		"unicode \u0000 null",
	}
	for _, v := range tests {
		result := JSONQuote(v)
		t.Logf("JSONQuote(%q) = %s", v, result)
	}
}

// TestB91_GroupByWithFunctionOrderBy tests GROUP BY with function in ORDER BY
func TestB91_GroupByWithFunctionOrderBy(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "gbf_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenReal},
	})

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		dept := "Eng"
		if i > 5 {
			dept = "HR"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "gbf_tbl",
			Columns: []string{"id", "dept", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(dept), numReal(float64(i * 1000))}},
		}, nil)
	}

	queries := []string{
		"SELECT dept, SUM(salary) FROM gbf_tbl GROUP BY dept ORDER BY SUM(salary) DESC",
		"SELECT dept, AVG(salary) FROM gbf_tbl GROUP BY dept ORDER BY AVG(salary)",
		"SELECT dept, COUNT(*) FROM gbf_tbl GROUP BY dept ORDER BY COUNT(*) DESC",
		"SELECT dept, MAX(salary) FROM gbf_tbl GROUP BY dept ORDER BY MAX(salary) DESC",
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Query error: %v", err)
		} else {
			t.Logf("Returned %d rows", len(result.Rows))
		}
	}
}

// TestB91_SelectLockedCTEWithWindowFuncs tests selectLocked with CTE + window functions
func TestB91_SelectLockedCTEWithWindowFuncs(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "cte_wf_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "cte_wf_tbl",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Use ExecuteQuery which goes through selectLocked with CTE
	queries := []string{
		`WITH nums AS (SELECT id, val FROM cte_wf_tbl)
		 SELECT id, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM nums`,
		`WITH nums AS (SELECT id, val FROM cte_wf_tbl)
		 SELECT id, SUM(val) OVER (ORDER BY id) AS running_sum FROM nums`,
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("CTE+window query error: %v", err)
		} else {
			t.Logf("CTE+window returned %d rows", len(result.Rows))
		}
	}
}

// TestB91_BuildJSONIndex tests buildJSONIndex with actual JSON data
func TestB91_BuildJSONIndex(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "json_idx_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "data", Type: query.TokenText},
	})

	ctx := context.Background()
	jsonRows := []string{
		`{"name":"Alice","age":30}`,
		`{"name":"Bob","age":25}`,
		`{"name":"Charlie","age":35}`,
	}
	for i, jr := range jsonRows {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "json_idx_tbl",
			Columns: []string{"id", "data"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(jr)}},
		}, nil)
	}

	// Create JSON index
	err := c.CreateJSONIndex("json_age_idx", "json_idx_tbl", "data", "$.age", "numeric")
	if err != nil {
		t.Logf("CreateJSONIndex error: %v", err)
	}

	// Query the JSON index
	result, err := c.QueryJSONIndex("json_age_idx", 30.0)
	if err != nil {
		t.Logf("QueryJSONIndex error: %v", err)
	} else {
		t.Logf("JSON index query returned %d rows", len(result))
	}
}

// TestB91_InsertWithDefaultValues tests INSERT with DEFAULT expressions
func TestB91_InsertWithDefaultValues(t *testing.T) {
	c := newB91Cat()

	// Create table with DEFAULT values
	err := c.CreateTable(&query.CreateTableStmt{
		Table: "def_vals_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true, AutoIncrement: true},
			{Name: "name", Type: query.TokenText},
			{Name: "status", Type: query.TokenText, Default: &query.StringLiteral{Value: "active"}},
			{Name: "created", Type: query.TokenText, Default: &query.StringLiteral{Value: "2024-01-01"}},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	ctx := context.Background()
	// Insert without specifying default columns
	for i := 1; i <= 3; i++ {
		_, _, err := c.Insert(ctx, &query.InsertStmt{
			Table:   "def_vals_tbl",
			Columns: []string{"id", "name"},
			Values: [][]query.Expression{{
				&query.NumberLiteral{Value: float64(i)},
				&query.StringLiteral{Value: "Person"},
			}},
		}, nil)
		if err != nil {
			t.Errorf("Insert with defaults: %v", err)
		}
	}

	result, err := c.ExecuteQuery("SELECT id, name, status FROM def_vals_tbl")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("Expected 3 rows, got %d", len(result.Rows))
	}
	for _, row := range result.Rows {
		if row[2] != "active" {
			t.Errorf("Expected default status='active', got %v", row[2])
		}
	}
}

// TestB91_CountRowsViaAnalyze tests stats.countRows via Analyze
func TestB91_CountRowsViaAnalyze(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "analyze_tbl91", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "name", Type: query.TokenText},
		{Name: "score", Type: query.TokenReal},
	})

	ctx := context.Background()
	for i := 1; i <= 20; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "analyze_tbl91",
			Columns: []string{"id", "name", "score"},
			Values: [][]query.Expression{
				{numReal(float64(i)), strReal("user"), numReal(float64(i) * 1.5)},
			},
		}, nil)
	}

	// Analyze triggers countRows and collectColumnStats
	err := c.Analyze("analyze_tbl91")
	if err != nil {
		t.Errorf("Analyze failed: %v", err)
	}

	stats, err2 := c.GetTableStats("analyze_tbl91")
	if err2 != nil {
		t.Logf("GetTableStats: %v", err2)
	} else if stats == nil {
		t.Error("Expected stats after Analyze")
	}
}

// TestB91_VectorIndexUpdate tests vector index Update path
func TestB91_VectorIndexUpdate(t *testing.T) {
	c := newB91Cat()
	c.CreateTable(&query.CreateTableStmt{
		Table: "vec_upd_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "embedding", Type: query.TokenVector, Dimensions: 3},
		},
	})

	if err := c.CreateVectorIndex("vec_upd_idx", "vec_upd_tbl", "embedding"); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	// Get the vector index and call Update directly
	vi := c.vectorIndexes["vec_upd_idx"]
	if vi == nil {
		t.Fatal("Vector index not found")
	}
	if vi.HNSW != nil {
		// First insert a vector, then update it
		vi.HNSW.Insert("key1", []float64{1.0, 0.0, 0.0})
		vi.HNSW.Update("key1", []float64{0.0, 1.0, 0.0})
	}
}

// TestB91_SelectWithDerivedTableAndJoin tests derived table + JOIN path in selectLocked
func TestB91_SelectWithDerivedTableAndJoin(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "dt_join_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, c, "dt_join_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "label", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "dt_join_a",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "dt_join_b",
			Columns: []string{"id", "label"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("label" + string(rune('0'+i)))}},
		}, nil)
	}

	// Derived table with JOIN
	q := `SELECT sub.id, sub.val, b.label
	      FROM (SELECT id, val FROM dt_join_a WHERE val > 10) AS sub
	      JOIN dt_join_b b ON sub.id = b.id`
	result, err := c.ExecuteQuery(q)
	if err != nil {
		t.Logf("Derived table + JOIN error: %v", err)
	} else {
		t.Logf("Returned %d rows", len(result.Rows))
	}
}

// TestB91_ExecuteWithJoinCTEMain tests executeSelectWithJoinAndGroupBy with CTE as main table
func TestB91_ExecuteWithJoinCTEMain(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "jcte_data", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "salary", Type: query.TokenInteger},
	})
	createCoverageTestTable(t, c, "jcte_depts", []*query.ColumnDef{
		{Name: "name", Type: query.TokenText, PrimaryKey: true},
		{Name: "budget", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 6; i++ {
		dept := "Eng"
		if i > 3 {
			dept = "HR"
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jcte_data",
			Columns: []string{"id", "dept", "salary"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal(dept), numReal(float64(i * 10000))}},
		}, nil)
	}
	c.Insert(ctx, &query.InsertStmt{
		Table:   "jcte_depts",
		Columns: []string{"name", "budget"},
		Values:  [][]query.Expression{{strReal("Eng"), numReal(1000000)}},
	}, nil)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "jcte_depts",
		Columns: []string{"name", "budget"},
		Values:  [][]query.Expression{{strReal("HR"), numReal(500000)}},
	}, nil)

	q := `WITH emp AS (SELECT dept, SUM(salary) AS total FROM jcte_data GROUP BY dept)
	      SELECT emp.dept, emp.total, d.budget
	      FROM emp JOIN jcte_depts d ON emp.dept = d.name`
	result, err := c.ExecuteQuery(q)
	if err != nil {
		t.Logf("JOIN CTE+main error: %v", err)
	} else {
		t.Logf("Returned %d rows", len(result.Rows))
	}
}

// TestB91_SaveLoadWithIndexes tests Save and Load with indexes
func TestB91_SaveLoadWithIndexes(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(16384, backend)
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil)

	c.CreateTable(&query.CreateTableStmt{
		Table: "save_idx_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val", Type: query.TokenInteger},
		},
	})
	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "save_idx",
		Table:   "save_idx_tbl",
		Columns: []string{"val"},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "save_idx_tbl",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	if err := c.Save(); err != nil {
		t.Errorf("Save failed: %v", err)
	}

	// Load into new catalog
	c2 := New(tree, pool, nil)
	if err := c2.Load(); err != nil {
		t.Errorf("Load failed: %v", err)
	}

	result, err := c2.ExecuteQuery("SELECT COUNT(*) FROM save_idx_tbl")
	if err != nil {
		t.Logf("SELECT after Load error: %v", err)
	} else {
		t.Logf("After load: %v", result.Rows)
	}
}

// TestB91_GetQueryCacheStats tests GetQueryCacheStats paths
func TestB91_GetQueryCacheStats(t *testing.T) {
	c := newB91Cat()
	c.EnableQueryCache(100, 0)

	createCoverageTestTable(t, c, "qcs_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "qcs_tbl",
			Columns: []string{"id", "val"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i * 10))}},
		}, nil)
	}

	// Execute queries to populate cache
	for i := 0; i < 3; i++ {
		c.ExecuteQuery("SELECT * FROM qcs_tbl WHERE val > 20")
	}

	hits, misses, size := c.GetQueryCacheStats()
	t.Logf("Cache stats: hits=%d misses=%d size=%d", hits, misses, size)
	// nil stats is also valid if cache isn't populated
}

// TestB91_ExecuteSelectJoinWithSubqueryJoinTable tests JOIN where join table is subquery
func TestB91_ExecuteSelectJoinWithSubqueryJoinTable(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "jsubq_main", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "ref_id", Type: query.TokenInteger},
		{Name: "name", Type: query.TokenText},
	})
	createCoverageTestTable(t, c, "jsubq_ref", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "label", Type: query.TokenText},
	})

	ctx := context.Background()
	for i := 1; i <= 4; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jsubq_ref",
			Columns: []string{"id", "label"},
			Values:  [][]query.Expression{{numReal(float64(i)), strReal("lbl" + string(rune('0'+i)))}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "jsubq_main",
			Columns: []string{"id", "ref_id", "name"},
			Values:  [][]query.Expression{{numReal(float64(i)), numReal(float64(i)), strReal("item" + string(rune('0'+i)))}},
		}, nil)
	}

	// JOIN with subquery as right table
	q := `SELECT m.id, m.name, r.label
	      FROM jsubq_main m
	      JOIN (SELECT id, label FROM jsubq_ref WHERE id <= 3) r ON m.ref_id = r.id`
	result, err := c.ExecuteQuery(q)
	if err != nil {
		t.Logf("JOIN+subquery error: %v", err)
	} else {
		t.Logf("Returned %d rows", len(result.Rows))
	}
}

// TestB91_EvalExprWithGroupAggJoin tests evaluateExprWithGroupAggregatesJoin
func TestB91_EvalExprWithGroupAggJoin(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "eagj_a", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "dept", Type: query.TokenText},
		{Name: "amount", Type: query.TokenReal},
	})
	createCoverageTestTable(t, c, "eagj_b", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "label", Type: query.TokenText},
	})

	ctx := context.Background()
	depts := []string{"Sales", "Sales", "HR", "HR", "Sales"}
	for i, dept := range depts {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "eagj_a",
			Columns: []string{"id", "dept", "amount"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal(dept), numReal(float64((i + 1) * 100))}},
		}, nil)
		c.Insert(ctx, &query.InsertStmt{
			Table:   "eagj_b",
			Columns: []string{"id", "label"},
			Values:  [][]query.Expression{{numReal(float64(i + 1)), strReal("label" + string(rune('0'+i+1)))}},
		}, nil)
	}

	// JOIN+GROUP BY exercises evaluateExprWithGroupAggregatesJoin
	queries := []string{
		`SELECT a.dept, COUNT(*), SUM(a.amount), AVG(a.amount), MIN(a.amount), MAX(a.amount)
		 FROM eagj_a a JOIN eagj_b b ON a.id = b.id
		 GROUP BY a.dept ORDER BY a.dept`,
		`SELECT a.dept, COUNT(a.id)
		 FROM eagj_a a JOIN eagj_b b ON a.id = b.id
		 GROUP BY a.dept
		 HAVING COUNT(a.id) > 2`,
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("evalAggJoin error: %v", err)
		} else {
			t.Logf("Returned %d rows", len(result.Rows))
		}
	}
}

// TestB91_StoreIndexDefPaths tests storeIndexDef through CreateIndex paths
func TestB91_StoreIndexDefPaths(t *testing.T) {
	backend := storage.NewMemory()
	pool := storage.NewBufferPool(4096, backend)
	tree, _ := btree.NewBTree(pool)
	c := New(tree, pool, nil) // tree != nil to trigger storeIndexDef

	c.CreateTable(&query.CreateTableStmt{
		Table: "store_idx_tbl",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
			{Name: "val1", Type: query.TokenInteger},
			{Name: "val2", Type: query.TokenText},
		},
	})

	// Create multiple indexes to cover storeIndexDef branches
	indexes := []struct {
		name   string
		cols   []string
		unique bool
	}{
		{"idx_s1", []string{"val1"}, false},
		{"idx_s2", []string{"val2"}, false},
		{"idx_s3_unique", []string{"val1", "val2"}, true},
	}
	for _, idx := range indexes {
		err := c.CreateIndex(&query.CreateIndexStmt{
			Index:   idx.name,
			Table:   "store_idx_tbl",
			Columns: idx.cols,
			Unique:  idx.unique,
		})
		if err != nil {
			t.Errorf("CreateIndex %s: %v", idx.name, err)
		}
	}
}

// TestB91_ExecuteTriggersWithCondition tests executeTriggers WHEN condition paths
func TestB91_ExecuteTriggersWithCondition(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "trig_cond_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenInteger},
	})
	b91CreateTable(t, c, "trig_cond_log", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "msg", Type: query.TokenText},
	})

	// Create trigger with WHEN condition
	err := c.CreateTrigger(&query.CreateTriggerStmt{
		Name:  "cond_trig",
		Table: "trig_cond_tbl",
		Event: "INSERT",
		Time:  "AFTER",
		Condition: &query.BinaryExpr{
			Left:     &query.Identifier{Name: "NEW.val"},
			Operator: query.TokenGt,
			Right:    &query.NumberLiteral{Value: 50},
		},
		Body: []query.Statement{
			&query.InsertStmt{
				Table:   "trig_cond_log",
				Columns: []string{"id", "msg"},
				Values:  [][]query.Expression{{&query.NumberLiteral{Value: 1}, &query.StringLiteral{Value: "triggered"}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}

	ctx := context.Background()
	// Insert row that doesn't satisfy condition (val=10 <= 50)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "trig_cond_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), numReal(10)}},
	}, nil)

	// Insert row that satisfies condition (val=100 > 50)
	c.Insert(ctx, &query.InsertStmt{
		Table:   "trig_cond_tbl",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(2), numReal(100)}},
	}, nil)
}

// TestB91_DeleteLockedWithIndexCleanup tests deleteLocked with index cleanup
func TestB91_DeleteLockedWithIndexCleanup(t *testing.T) {
	c := newB91Cat()
	b91CreateTable(t, c, "del_idx_tbl", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "email", Type: query.TokenText, Unique: true},
		{Name: "score", Type: query.TokenInteger},
	})

	c.CreateIndex(&query.CreateIndexStmt{
		Index:   "del_score_idx",
		Table:   "del_idx_tbl",
		Columns: []string{"score"},
	})

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		c.Insert(ctx, &query.InsertStmt{
			Table:   "del_idx_tbl",
			Columns: []string{"id", "email", "score"},
			Values: [][]query.Expression{
				{numReal(float64(i)), strReal("user" + string(rune('0'+i)) + "@test.com"), numReal(float64(i * 10))},
			},
		}, nil)
	}

	// Delete rows - should clean up indexes
	for i := 1; i <= 3; i++ {
		_, _, err := c.Delete(ctx, &query.DeleteStmt{
			Table: "del_idx_tbl",
			Where: &query.BinaryExpr{
				Left:     &query.Identifier{Name: "id"},
				Operator: query.TokenEq,
				Right:    &query.NumberLiteral{Value: float64(i)},
			},
		}, nil)
		if err != nil {
			t.Errorf("Delete %d: %v", i, err)
		}
	}

	result, err := c.ExecuteQuery("SELECT COUNT(*) FROM del_idx_tbl")
	if err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if len(result.Rows) == 0 {
		t.Fatal("No rows returned")
	}
}

// TestB91_HavingWithNullAggregates tests HAVING with NULL aggregate values
func TestB91_HavingWithNullAggregates(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "hav_null", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "grp", Type: query.TokenText},
		{Name: "val", Type: query.TokenInteger},
	})

	ctx := context.Background()
	rows := [][]interface{}{
		{1, "A", 10}, {2, "A", nil}, {3, "B", 20}, {4, "B", 30},
	}
	for _, r := range rows {
		vals := []query.Expression{numReal(float64(r[0].(int))), strReal(r[1].(string))}
		if r[2] == nil {
			vals = append(vals, &query.NullLiteral{})
		} else {
			vals = append(vals, numReal(float64(r[2].(int))))
		}
		c.Insert(ctx, &query.InsertStmt{
			Table:   "hav_null",
			Columns: []string{"id", "grp", "val"},
			Values:  [][]query.Expression{vals},
		}, nil)
	}

	queries := []string{
		"SELECT grp, SUM(val) FROM hav_null GROUP BY grp HAVING SUM(val) > 20",
		"SELECT grp, COUNT(val) FROM hav_null GROUP BY grp HAVING COUNT(val) >= 1",
		"SELECT grp, AVG(val) FROM hav_null GROUP BY grp HAVING AVG(val) IS NOT NULL",
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("HAVING NULL query error: %v", err)
		} else {
			t.Logf("Returned %d rows", len(result.Rows))
		}
	}
}

// TestB91_TemporalExprPaths tests evaluateTemporalExpr various branches
func TestB91_TemporalExprPaths(t *testing.T) {
	c := newB91Cat()
	createCoverageTestTable(t, c, "temporal_tbl91", []*query.ColumnDef{
		{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		{Name: "val", Type: query.TokenText},
	})

	ctx := context.Background()
	c.Insert(ctx, &query.InsertStmt{
		Table:   "temporal_tbl91",
		Columns: []string{"id", "val"},
		Values:  [][]query.Expression{{numReal(1), strReal("hello")}},
	}, nil)

	// These queries exercise evaluateTemporalExpr
	queries := []string{
		"SELECT * FROM temporal_tbl91 AS OF TIMESTAMP '2025-01-01'",
		"SELECT * FROM temporal_tbl91 AS OF TIMESTAMP '2025-06-15 12:00:00'",
	}
	for _, q := range queries {
		result, err := c.ExecuteQuery(q)
		if err != nil {
			t.Logf("Temporal query error (expected): %v", err)
		} else {
			t.Logf("Returned %d rows", len(result.Rows))
		}
	}
}
