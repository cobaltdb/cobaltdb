package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Regression for the migrateUp/migrateDown version-skip gap: both loops made
// their skip decision from getCurrentVersion (MAX(version)) while showStatus
// uses per-migration records (getAppliedMigrations). A migration whose record
// is absent but whose version is <= MAX — e.g. a backfilled migration file
// arriving after later migrations were applied, exactly what merging branches
// produces — was silently skipped by `up` (schema drift, reported as
// "Migrations complete!") and blindly reverted by `down` (its destructive
// DownSQL executed on a never-applied migration). Skip decisions must use the
// per-migration applied set, making up/down consistent with status.

type fakeMigState struct {
	mu       sync.Mutex
	applied  map[int64]bool
	executed []string
}

func (s *fakeMigState) snapshotExecuted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.executed...)
}

var (
	fakeMigStateMu sync.Mutex
	fakeMigCur     *fakeMigState
	fakeMigRegOnce sync.Once
)

type fakeMigDriver struct{}

func (d *fakeMigDriver) Open(string) (driver.Conn, error) {
	fakeMigStateMu.Lock()
	defer fakeMigStateMu.Unlock()
	return &fakeMigConn{state: fakeMigCur}, nil
}

type fakeMigConn struct{ state *fakeMigState }

func (c *fakeMigConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare not used by the fake")
}
func (c *fakeMigConn) Close() error              { return nil }
func (c *fakeMigConn) Begin() (driver.Tx, error) { return &fakeMigTx{state: c.state}, nil }

func (c *fakeMigConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	switch {
	case strings.Contains(query, "INSERT INTO schema_migrations"):
		v, _ := args[0].(int64)
		c.state.applied[v] = true
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "DELETE FROM schema_migrations"):
		v, _ := args[0].(int64)
		delete(c.state.applied, v)
		return driver.RowsAffected(1), nil
	default:
		c.state.executed = append(c.state.executed, query)
		return driver.RowsAffected(1), nil
	}
}

func (c *fakeMigConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	switch {
	case strings.Contains(query, "MAX(version)"):
		max := int64(0)
		for v := range c.state.applied {
			if v > max {
				max = v
			}
		}
		return &fakeMigRows{cols: []string{"v"}, rows: [][]driver.Value{{max}}}, nil
	case strings.Contains(query, "SELECT version, name, applied_at"):
		var rows [][]driver.Value
		for v := range c.state.applied {
			rows = append(rows, []driver.Value{v, fmt.Sprintf("mig%d", v), time.Now()})
		}
		return &fakeMigRows{cols: []string{"version", "name", "applied_at"}, rows: rows}, nil
	}
	return nil, fmt.Errorf("unexpected query: %s", query)
}

type fakeMigTx struct{ state *fakeMigState }

func (t *fakeMigTx) Commit() error   { return nil }
func (t *fakeMigTx) Rollback() error { return nil }

type fakeMigRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *fakeMigRows) Columns() []string { return r.cols }
func (r *fakeMigRows) Close() error      { return nil }
func (r *fakeMigRows) Next(dest []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.i])
	r.i++
	return nil
}

func openFakeMigrateDB(t *testing.T, state *fakeMigState) *sql.DB {
	t.Helper()
	fakeMigRegOnce.Do(func() {
		sql.Register("cobaltdb-migrate-fake", &fakeMigDriver{})
	})
	fakeMigStateMu.Lock()
	fakeMigCur = state
	fakeMigStateMu.Unlock()
	db, err := sql.Open("cobaltdb-migrate-fake", "")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func writeMigrations(t *testing.T, dir string, versions ...int64) {
	t.Helper()
	for _, v := range versions {
		up := filepath.Join(dir, fmt.Sprintf("%04d_add_m%d_up.sql", v, v))
		down := filepath.Join(dir, fmt.Sprintf("%04d_add_m%d_down.sql", v, v))
		if err := os.WriteFile(up, []byte(fmt.Sprintf("CREATE TABLE m%d (id INTEGER)", v)), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(down, []byte(fmt.Sprintf("DROP TABLE m%d", v)), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMigrateUpAppliesBackfilledMiddleMigration(t *testing.T) {
	state := &fakeMigState{applied: map[int64]bool{}}
	db := openFakeMigrateDB(t, state)
	defer db.Close()

	dir := t.TempDir()
	writeMigrations(t, dir, 1, 3)

	// Control: first up applies both available migrations.
	if err := migrateUp(db, dir, 0); err != nil {
		t.Fatalf("initial up: %v", err)
	}
	state.mu.Lock()
	n := len(state.applied)
	state.mu.Unlock()
	if n != 2 {
		t.Fatalf("control: applied %d migrations, want 2", n)
	}

	// The backfill: migration 2 arrives after 3 was already applied.
	writeMigrations(t, dir, 2)

	if err := migrateUp(db, dir, 0); err != nil {
		t.Fatalf("up after backfill: %v", err)
	}

	state.mu.Lock()
	applied2 := state.applied[2]
	state.mu.Unlock()
	if !applied2 {
		t.Fatalf("schema drift: backfilled migration 2 reported complete but was never applied (skipped by the MAX(version) criterion)")
	}
}

func TestMigrateDownSkipsUnappliedMigrations(t *testing.T) {
	state := &fakeMigState{applied: map[int64]bool{1: true, 3: true}}
	db := openFakeMigrateDB(t, state)
	defer db.Close()

	dir := t.TempDir()
	writeMigrations(t, dir, 1, 2, 3)

	if err := migrateDown(db, dir, 1); err != nil {
		t.Fatalf("down: %v", err)
	}

	exec := state.snapshotExecuted()
	for _, stmt := range exec {
		if strings.Contains(stmt, "DROP TABLE m2") {
			t.Fatalf("down executed the DownSQL of migration 2, which was never applied (destructive revert of unapplied migration): executed=%v", exec)
		}
	}
	if !stateContains(exec, "DROP TABLE m3") {
		t.Fatalf("control: applied migration 3 was not reverted: executed=%v", exec)
	}
}

func stateContains(stmts []string, needle string) bool {
	for _, s := range stmts {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}
