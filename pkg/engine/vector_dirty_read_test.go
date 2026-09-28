package engine

import (
	"context"
	"testing"
)

// Regression for the direct-path dirty read (refactor.md §1.16, Phase 1):
// vector-INDEXED table writes used to bypass the pending-write buffer inside
// explicit transactions (the useBuffer exemption), landing immediately in the
// shared B-tree — so a concurrent SELECT observed uncommitted values. Under
// the ratified option A, vector-table writes buffer like every other table
// and become visible only at COMMIT.
func TestVectorTableUpdateNoDirtyRead(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	exec := func(sql string, args ...interface{}) {
		t.Helper()
		if _, err := db.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("exec %q: %v", sql, err)
		}
	}
	exec(`CREATE TABLE plain (id INTEGER PRIMARY KEY, tag TEXT)`)
	exec(`INSERT INTO plain VALUES (1, 'committed')`)
	exec(`CREATE TABLE vitems (id INTEGER PRIMARY KEY, tag TEXT, embedding VECTOR(3))`)
	exec(`INSERT INTO vitems VALUES (1, 'committed', '[1.0, 0.0, 0.0]')`)
	exec(`CREATE VECTOR INDEX vitems_embedding ON vitems(embedding)`)

	// probe runs the uncommitted UPDATE in a second goroutine (its own
	// transaction) while this goroutine SELECTs, then lets it commit and
	// reports both observations.
	probe := func(table string) (observed, afterCommit string) {
		updateDone := make(chan error)
		readDone := make(chan string)
		commitDone := make(chan error)
		go func() {
			if _, err := db.Exec(ctx, "BEGIN"); err != nil {
				updateDone <- err
				return
			}
			if _, err := db.Exec(ctx, "UPDATE "+table+" SET tag = 'uncommitted' WHERE id = 1"); err != nil {
				updateDone <- err
				return
			}
			close(updateDone) // statement applied, transaction still open
			obs := <-readDone
			commitDone <- func() error {
				if _, err := db.Exec(ctx, "COMMIT"); err != nil {
					return err
				}
				return nil
			}()
			_ = obs
		}()
		if err := <-updateDone; err != nil {
			t.Fatalf("writer goroutine failed: %v", err)
		}
		var got string
		rows, err := db.Query(ctx, "SELECT tag FROM "+table+" WHERE id = 1")
		if err != nil {
			t.Fatalf("concurrent query: %v", err)
		}
		if rows.Next() {
			if err := rows.Scan(&got); err != nil {
				t.Fatalf("scan: %v", err)
			}
		}
		rows.Close()
		readDone <- got
		if err := <-commitDone; err != nil {
			t.Fatalf("commit: %v", err)
		}
		var after string
		rows2, err := db.Query(ctx, "SELECT tag FROM "+table+" WHERE id = 1")
		if err != nil {
			t.Fatalf("post-commit query: %v", err)
		}
		if rows2.Next() {
			_ = rows2.Scan(&after)
		}
		rows2.Close()
		return got, after
	}

	// CONTROL: plain tables were never affected — both observations correct.
	obs, after := probe("plain")
	if obs != "committed" {
		t.Fatalf("CONTROL plain table: concurrent SELECT observed %q (dirty read on an unaffected path — harness is wrong)", obs)
	}
	if after != "uncommitted" {
		t.Fatalf("CONTROL plain table: post-commit SELECT observed %q, want uncommitted", after)
	}

	// PROBE: the vector-indexed table must behave identically.
	obs, after = probe("vitems")
	if obs != "committed" {
		t.Fatalf("DIRTY READ on vector-indexed table: concurrent SELECT observed %q while the UPDATE was still uncommitted; vector-table writes must buffer and become visible only at COMMIT (refactor.md §1.16 option A)", obs)
	}
	if after != "uncommitted" {
		t.Fatalf("vector table: post-commit SELECT observed %q, want uncommitted (read-your-committed-writes)", after)
	}
}
