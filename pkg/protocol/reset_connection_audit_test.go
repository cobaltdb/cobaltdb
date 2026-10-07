package protocol

import (
	"context"
	"github.com/cobaltdb/cobaltdb/pkg/engine"
	"testing"
)

func TestAuditResetConnectionReleasesStatementState(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, tc := range []struct {
		name      string
		count     int
		failWrite bool
	}{{"empty", 0, false}, {"single", 1, false}, {"multiple", 3, false}, {"write failure", 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			conn := newMockConn()
			c := &MySQLClient{conn: conn, server: NewMySQLServer(db, "test"), database: "test", nextStmtID: 5, stmts: make(map[uint32]*preparedStmt)}
			var old []*preparedStmt
			var staleRows []*engine.Rows
			for i := 0; i < tc.count; i++ {
				rows, err := db.Query(context.Background(), "SELECT 1 AS n")
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				id := uint32(i + 1)
				stmt := &preparedStmt{id: id, numParams: 1, cursor: &stmtCursor{rows: rows, colTypes: []byte{MySQLTypeLongLong}}}
				c.stmts[id] = stmt
				if err := c.handleStmtSendLongData(buildStmtLongDataPacket(id, 0, []byte("abc"))); err != nil {
					t.Fatal(err)
				}
				old = append(old, stmt)
				staleRows = append(staleRows, rows)
			}
			entered, release, done := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
			go func() {
				close(entered)
				<-release
				usable := false
				for _, rows := range staleRows {
					if rows.Next() {
						usable = true
					}
				}
				done <- usable
			}()
			<-entered
			if tc.failWrite {
				conn.closed = true
			}
			err := c.handleResetConnection()
			close(release)
			if usable := <-done; usable {
				t.Fatal("gated old cursor remains usable after reset")
			}
			if (err != nil) != tc.failWrite {
				t.Fatalf("reset error=%v wantWriteFailure=%t", err, tc.failWrite)
			}
			if c.longDataTotal != 0 || c.database != "" || c.nextStmtID != 0 || len(c.stmts) != 0 {
				t.Fatalf("reset state: longData=%d database=%q stmtID=%d stmts=%d", c.longDataTotal, c.database, c.nextStmtID, len(c.stmts))
			}
			for _, stmt := range old {
				if stmt.cursor != nil || len(stmt.longData) != 0 {
					t.Fatal("old statement retained owned data or cursor")
				}
			}
			if tc.failWrite {
				return
			}
			if err := c.handleResetConnection(); err != nil {
				t.Fatal(err)
			} // repeated reset
			c.stmts[1] = &preparedStmt{id: 1, numParams: 1}
			if err := c.handleStmtSendLongData(buildStmtLongDataPacket(1, 0, []byte("xyz"))); err != nil {
				t.Fatal(err)
			}
			if c.longDataTotal != 3 || string(c.stmts[1].longData[0]) != "xyz" {
				t.Fatal("reset/reuse has stale accounting")
			}
			if err := c.handleResetConnection(); err != nil {
				t.Fatal(err)
			}
			if c.longDataTotal != 0 {
				t.Fatal("second reset retains accounting")
			}
		})
	}
}
