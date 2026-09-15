package protocol

// Regression test for the binary result-set zero-column gap:
//
// sendResultSetFromRows (text path) answers a zero-column result set with an
// OK packet, because the column-count packet (0x00) collides with the OK
// packet header and crashes real drivers (slice bounds out of range in
// handleOkPacket — found end-to-end, fixed in 873e555 for the text path).
//
// sendBinaryResultSetFromRows (COM_STMT_EXECUTE path) had no such guard: the
// same zero-column result produced a bare 0x00 column-count packet. The
// construction is "SELECT missing_col FROM <existing table>", which the engine
// answers with a zero-column result set (the same query the integration test
// exercises over the text path).

import (
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestBinaryResultSetZeroColumnRespondsWithOK(t *testing.T) {
	engineDB, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("engine.Open: %v", err)
	}
	defer engineDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := engineDB.Exec(ctx, "CREATE TABLE robust (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	// The construction: a read statement referencing a missing column yields a
	// zero-column result set. If the engine ever changes to reject this, the
	// test must be re-derived — do not silently pass.
	rows, err := engineDB.Query(ctx, "SELECT missing_col FROM robust")
	if err != nil {
		t.Fatalf("construction changed: engine Query errors for missing column: %v", err)
	}
	defer rows.Close()
	if got := len(rows.Columns()); got != 0 {
		t.Fatalf("construction changed: expected zero columns, got %d", got)
	}

	srv := NewMySQLServer(engineDB, "5.7.0-CobaltDB-test")
	conn := newMockConn()
	client := &MySQLClient{
		conn:        conn,
		reader:      bufio.NewReader(conn),
		server:      srv,
		connID:      1,
		connectTime: time.Now(),
	}

	// THE regression: the binary path must answer zero-column results with an
	// OK packet, exactly like the text path. Pre-fix it wrote a bare 0x00
	// column-count packet, which clients parse as a malformed OK packet.
	if err := client.sendBinaryResultSetFromRows(rows); err != nil {
		t.Fatalf("sendBinaryResultSetFromRows: %v", err)
	}

	frame := firstPacketPayload(t, conn.writeBuf.Bytes())
	if len(frame) < 7 || frame[0] != 0x00 {
		t.Fatalf("zero-column binary result must respond with an OK packet (>=7-byte payload), got first payload len=%d bytes=% X", len(frame), frame)
	}
}

// firstPacketPayload extracts the payload of the first MySQL frame written to
// the mock connection: [len:3 LE][seq:1][payload].
func firstPacketPayload(t *testing.T, raw []byte) []byte {
	t.Helper()
	if len(raw) < 4 {
		t.Fatalf("no full packet header in %d bytes of output: % X", len(raw), raw)
	}
	n := int(raw[0]) | int(raw[1])<<8 | int(raw[2])<<16
	if len(raw) < 4+n {
		t.Fatalf("truncated first packet: header claims %d payload bytes, got %d total", n, len(raw))
	}
	return raw[4 : 4+n]
}
