package catalog

import (
	"testing"
)

// TestUpdateLockedResolvesTargetRows covers the resolveUpdateTargetRows
// phase of updateLocked: a full-table-scan WHERE match produces the
// expected number of updateEntry rows. We use the public Update API so
// the test exercises the end-to-end call site that updateLocked now
// dispatches to, including the three extracted helpers.
func TestUpdateLockedResolvesTargetRows(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_resolve (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := int64(1); i <= 5; i++ {
		if _, err := c.ExecuteQuery("INSERT INTO upd_resolve VALUES (" + itoa(i) + ", " + itoa(i*10) + ")"); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}

	// Update rows where v >= 30 → ids 3,4,5 should match. We assert via
	// the observable table state, which exercises the end-to-end call
	// site that updateLocked now dispatches to.
	if _, err := c.ExecuteQuery("UPDATE upd_resolve SET v = v + 1 WHERE v >= 30"); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Confirm only the matching rows were updated.
	check, err := c.ExecuteQuery("SELECT id, v FROM upd_resolve ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	want := []string{"1|10", "2|20", "3|31", "4|41", "5|51"}
	if len(check.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(check.Rows), len(want))
	}
	for i, row := range check.Rows {
		got := row[0].(int64)
		if v, ok := row[1].(int64); !ok {
			t.Fatalf("row[%d] v type: %T", i, row[1])
		} else if gotStr := formatRowIDValue(got) + "|" + formatRowIDValue(v); gotStr != want[i] {
			t.Fatalf("row[%d]: got %s, want %s", i, gotStr, want[i])
		}
	}
}

// TestUpdateLockedIndexPathResolvesTargetRows verifies the indexed
// path of resolveUpdateTargetRows. When an indexed column is used in
// WHERE, the helper should restrict the scan to indexedRows and still
// produce the same end-state. We assert via the observable table state.
func TestUpdateLockedIndexPathResolvesTargetRows(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_idx (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE INDEX upd_idx_v ON upd_idx (v)"); err != nil {
		t.Fatalf("create index: %v", err)
	}
	for i := int64(1); i <= 5; i++ {
		if _, err := c.ExecuteQuery("INSERT INTO upd_idx VALUES (" + itoa(i) + ", " + itoa(i*10) + ")"); err != nil {
			t.Fatalf("seed insert %d: %v", i, err)
		}
	}

	// Equality on an indexed column — useIndexForQueryWithArgs should
	// fire and resolve via the index path.
	if _, err := c.ExecuteQuery("UPDATE upd_idx SET v = 0 WHERE v = 30"); err != nil {
		t.Fatalf("update: %v", err)
	}
	check, err := c.ExecuteQuery("SELECT id, v FROM upd_idx WHERE v = 0")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(check.Rows) != 1 {
		t.Fatalf("got %d rows with v=0, want 1", len(check.Rows))
	}
	if got := check.Rows[0][0].(int64); got != 3 {
		t.Fatalf("row id: got %d, want 3", got)
	}
}

// TestUpdateLockedValidateConstraintsReturning covers phase 2: the
// validateUpdateConstraints helper evaluates the RETURNING projection
// and the setLastReturning call inside applyUpdateIndexes publishes
// the result. A subsequent GetLastReturningRows must return the
// projected rows.
func TestUpdateLockedValidateConstraintsReturning(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_ret (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_ret VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := c.ExecuteQuery("UPDATE upd_ret SET name = 'updated' WHERE id = 1 RETURNING id, name"); err != nil {
		t.Fatalf("update returning: %v", err)
	}
	rows := c.GetLastReturningRows()
	cols := c.GetLastReturningColumns()
	if len(rows) != 1 {
		t.Fatalf("returning rows: got %d, want 1", len(rows))
	}
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Fatalf("returning cols: got %v, want [id name]", cols)
	}
	if got, ok := rows[0][0].(int64); !ok || got != 1 {
		t.Fatalf("rows[0][0]: got %v, want int64(1)", rows[0][0])
	}
	if got, ok := rows[0][1].(string); !ok || got != "updated" {
		t.Fatalf("rows[0][1]: got %v, want 'updated'", rows[0][1])
	}
}

// TestUpdateLockedApplyIndexesPublishesReturning covers phase 3: after
// a buffered UPDATE the applyUpdateIndexes helper must invalidate the
// query cache and publish the RETURNING result. A follow-up SELECT
// reads the post-update state from the cache-invalidated catalog, not
// from a stale cached plan.
func TestUpdateLockedApplyIndexesPublishesReturning(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_pub (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_pub VALUES (1, 100), (2, 200)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// First update — populates returning state.
	if _, err := c.ExecuteQuery("UPDATE upd_pub SET v = v + 50 WHERE id = 1 RETURNING v"); err != nil {
		t.Fatalf("update: %v", err)
	}
	rows := c.GetLastReturningRows()
	if len(rows) != 1 {
		t.Fatalf("first update: got %d returning rows, want 1", len(rows))
	}

	// Second update without RETURNING should clear the returning state
	// because applyUpdateIndexes calls setLastReturning with empty args
	// (or the helper publishes only on first call). We assert the new
	// state via SELECT instead of relying on cache state.
	if _, err := c.ExecuteQuery("UPDATE upd_pub SET v = v + 50 WHERE id = 2"); err != nil {
		t.Fatalf("second update: %v", err)
	}
	check, err := c.ExecuteQuery("SELECT id, v FROM upd_pub ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(check.Rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(check.Rows))
	}
	// Both rows should have +50 from their seeded value.
	if v := check.Rows[0][1].(int64); v != 150 {
		t.Fatalf("id=1 v=%d, want 150", v)
	}
	if v := check.Rows[1][1].(int64); v != 250 {
		t.Fatalf("id=2 v=%d, want 250", v)
	}
}

// TestUpdateLockedNoMatchLeavesTableUntouched verifies that when
// resolveUpdateTargetRows finds no matching rows, the validate and
// apply phases are no-ops: no rows change, and the post-state is
// what we seeded. The end-to-end path through ExecuteQuery proves
// that all three extracted methods cooperate when entries is empty.
func TestUpdateLockedNoMatchLeavesTableUntouched(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE upd_empty (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO upd_empty VALUES (1, 100)"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if _, err := c.ExecuteQuery("UPDATE upd_empty SET v = 999 WHERE id = 99"); err != nil {
		t.Fatalf("update: %v", err)
	}
	check, err := c.ExecuteQuery("SELECT v FROM upd_empty WHERE id = 1")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(check.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(check.Rows))
	}
	if v := check.Rows[0][0].(int64); v != 100 {
		t.Fatalf("id=1 v=%d, want 100 (untouched)", v)
	}
}

// itoa is a tiny helper to keep the test compact without importing
// strconv at the top — we need to format small positive integers in
// many places.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// formatRowIDValue formats an int64 for the want[] string comparison
// without pulling strconv into this test file.
func formatRowIDValue(n int64) string {
	if n < 0 {
		return "-" + itoa(-n)
	}
	return itoa(n)
}
