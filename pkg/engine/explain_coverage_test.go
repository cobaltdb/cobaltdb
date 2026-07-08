package engine

import (
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/replication"
)

func TestBuildInsertPlan(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name  string
		stmt  *query.InsertStmt
		check func(t *testing.T, plan *QueryPlan)
	}{
		{
			name: "basic insert",
			stmt: &query.InsertStmt{
				Table:   "users",
				Columns: []string{"name", "email"},
				Values:  [][]query.Expression{{}, {}},
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 1 {
					t.Fatalf("expected 1 node, got %d", len(plan.Nodes))
				}
				node := plan.Nodes[0]
				if node.Operation != "Insert" {
					t.Errorf("expected Insert, got %s", node.Operation)
				}
				if node.Rows != 2 {
					t.Errorf("expected 2 rows, got %d", node.Rows)
				}
				if node.Detail != "users (name, email)" {
					t.Errorf("expected 'users (name, email)', got '%s'", node.Detail)
				}
			},
		},
		{
			name: "insert empty table name",
			stmt: &query.InsertStmt{
				Table:  "",
				Values: [][]query.Expression{{}},
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 1 {
					t.Fatalf("expected 1 node, got %d", len(plan.Nodes))
				}
				node := plan.Nodes[0]
				if node.Detail != "<unknown>" {
					t.Errorf("expected '<unknown>', got '%s'", node.Detail)
				}
				if node.Rows != 1 {
					t.Errorf("expected 1 row, got %d", node.Rows)
				}
			},
		},
		{
			name: "insert no values",
			stmt: &query.InsertStmt{
				Table: "orders",
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 1 {
					t.Fatalf("expected 1 node, got %d", len(plan.Nodes))
				}
				node := plan.Nodes[0]
				if node.Rows != 1 {
					t.Errorf("expected 1 row (default), got %d", node.Rows)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := db.buildInsertPlan(tt.stmt)
			tt.check(t, plan)
		})
	}
}

func TestBuildUpdatePlan(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name  string
		stmt  *query.UpdateStmt
		check func(t *testing.T, plan *QueryPlan)
	}{
		{
			name: "basic update no where",
			stmt: &query.UpdateStmt{
				Table: "users",
				Set:   []*query.SetClause{{Column: "name"}},
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 1 {
					t.Fatalf("expected 1 node, got %d", len(plan.Nodes))
				}
				node := plan.Nodes[0]
				if node.Operation != "Update" {
					t.Errorf("expected Update, got %s", node.Operation)
				}
				if node.Detail != "users SET name" {
					t.Errorf("expected 'users SET name', got '%s'", node.Detail)
				}
			},
		},
		{
			name: "update with where clause",
			stmt: &query.UpdateStmt{
				Table: "users",
				Set:   []*query.SetClause{{Column: "name"}},
				Where: &query.BinaryExpr{
					Operator: query.TokenEq,
					Left:     &query.ColumnRef{Column: "id"},
					Right:    &query.NumberLiteral{Value: 1},
				},
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 2 {
					t.Fatalf("expected 2 nodes, got %d", len(plan.Nodes))
				}
				if plan.Nodes[1].Operation != "Filter" {
					t.Errorf("expected Filter child, got %s", plan.Nodes[1].Operation)
				}
				if plan.Nodes[0].Detail != "users SET name" {
					t.Errorf("expected 'users SET name', got '%s'", plan.Nodes[0].Detail)
				}
			},
		},
		{
			name: "update empty table name",
			stmt: &query.UpdateStmt{
				Table: "",
				Set:   []*query.SetClause{{Column: "x"}},
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 1 {
					t.Fatalf("expected 1 node, got %d", len(plan.Nodes))
				}
				if plan.Nodes[0].Detail != "<unknown> SET x" {
					t.Errorf("expected '<unknown> SET x', got '%s'", plan.Nodes[0].Detail)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := db.buildUpdatePlan(tt.stmt)
			tt.check(t, plan)
		})
	}
}

func TestBuildDeletePlan(t *testing.T) {
	db := &DB{}

	tests := []struct {
		name  string
		stmt  *query.DeleteStmt
		check func(t *testing.T, plan *QueryPlan)
	}{
		{
			name: "basic delete no where",
			stmt: &query.DeleteStmt{
				Table: "users",
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 1 {
					t.Fatalf("expected 1 node, got %d", len(plan.Nodes))
				}
				node := plan.Nodes[0]
				if node.Operation != "Delete" {
					t.Errorf("expected Delete, got %s", node.Operation)
				}
				if node.Detail != "users" {
					t.Errorf("expected 'users', got '%s'", node.Detail)
				}
			},
		},
		{
			name: "delete with where clause",
			stmt: &query.DeleteStmt{
				Table: "users",
				Where: &query.BinaryExpr{
					Operator: query.TokenEq,
					Left:     &query.ColumnRef{Column: "id"},
					Right:    &query.NumberLiteral{Value: 1},
				},
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 2 {
					t.Fatalf("expected 2 nodes, got %d", len(plan.Nodes))
				}
				if plan.Nodes[1].Operation != "Filter" {
					t.Errorf("expected Filter child, got %s", plan.Nodes[1].Operation)
				}
			},
		},
		{
			name: "delete empty table name",
			stmt: &query.DeleteStmt{
				Table: "",
			},
			check: func(t *testing.T, plan *QueryPlan) {
				if len(plan.Nodes) != 1 {
					t.Fatalf("expected 1 node, got %d", len(plan.Nodes))
				}
				if plan.Nodes[0].Detail != "<unknown>" {
					t.Errorf("expected '<unknown>', got '%s'", plan.Nodes[0].Detail)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := db.buildDeletePlan(tt.stmt)
			tt.check(t, plan)
		})
	}
}

// --- replication tests ---

func TestReplicateStatementNoMaster(t *testing.T) {
	db := &DB{}
	// With no replication manager, replicateStatement returns false, nil
	needSync, err := db.replicateStatement("INSERT INTO users VALUES (1)", nil, false)
	if err != nil {
		t.Fatalf("replicateStatement failed: %v", err)
	}
	if needSync {
		t.Error("expected needSyncWait=false when no replication master")
	}
}

func TestReplicateStatementEmptySQL(t *testing.T) {
	db := &DB{}
	// Empty SQL returns false, nil even with replication
	needSync, err := db.replicateStatement("", nil, false)
	if err != nil {
		t.Fatalf("replicateStatement failed: %v", err)
	}
	if needSync {
		t.Error("expected needSyncWait=false for empty SQL")
	}
}

func TestApplyReplicatedStatementBadPayload(t *testing.T) {
	db := &DB{}
	// Applying a replicated statement with invalid data should return an error
	entry := &replication.WALEntry{
		Data: []byte("invalid payload data"),
		LSN:  42,
	}
	err := db.applyReplicatedStatement(entry)
	if err == nil {
		t.Error("expected error for invalid WAL entry data")
	}
}

func TestReplicationSyncWaitAsync(t *testing.T) {
	// With async mode, replicationSyncWait should return nil immediately
	mgr := replication.NewManager(&replication.Config{
		Role: replication.RoleStandalone,
		Mode: replication.ModeAsync,
	})
	db := &DB{
		options: &Options{
			Replication: ReplicationConfig{
				SyncTimeout: time.Second,
			},
		},
	}
	err := db.replicationSyncWait(mgr)
	if err != nil {
		t.Fatalf("replicationSyncWait failed for async mode: %v", err)
	}
}
