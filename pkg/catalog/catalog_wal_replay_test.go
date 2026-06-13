package catalog

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func makeReplayData(key string, value []byte) []byte {
	data := make([]byte, 4+len(key)+len(value))
	binary.LittleEndian.PutUint32(data[:4], uint32(len(key)))
	copy(data[4:], key)
	copy(data[4+len(key):], value)
	return data
}

func recoverReplayOpsFromWAL(t *testing.T, walPath string, pool *storage.BufferPool) []storage.WALReplayOp {
	t.Helper()
	reopened, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL for recovery: %v", err)
	}
	defer reopened.Close()
	if err := reopened.Recover(pool); err != nil {
		t.Fatalf("Recover WAL: %v", err)
	}
	return reopened.GetReplayOps()
}

func requireReplayOp(t *testing.T, ops []storage.WALReplayOp, typ storage.WALRecordType, replayKey string) []byte {
	t.Helper()
	for _, op := range ops {
		if op.Type != typ {
			continue
		}
		key, value, err := parseReplayWALKeyValue(op.Data)
		if err != nil {
			t.Fatalf("parse replay op: %v", err)
		}
		if key == replayKey {
			return value
		}
	}
	t.Fatalf("missing replay op type=%d key=%q in %+v", typ, replayKey, ops)
	return nil
}

func TestEncodeLogicalWALDataRoundTrip(t *testing.T) {
	data, err := encodeLogicalWALData("users", []byte(formatKey(42)), []byte("row-data"))
	if err != nil {
		t.Fatalf("encodeLogicalWALData: %v", err)
	}
	key, value, err := parseReplayWALKeyValue(data)
	if err != nil {
		t.Fatalf("parseReplayWALKeyValue: %v", err)
	}
	if key != "users:"+formatKey(42) {
		t.Fatalf("unexpected WAL replay key %q", key)
	}
	if string(value) != "row-data" {
		t.Fatalf("unexpected WAL replay value %q", value)
	}
	tableName, rowKey, err := parseReplayWALKey(key)
	if err != nil {
		t.Fatalf("parseReplayWALKey: %v", err)
	}
	if tableName != "users" || rowKey != formatKey(42) {
		t.Fatalf("unexpected parsed key parts table=%q row=%q", tableName, rowKey)
	}
}

func TestJoinUpdateWritesLogicalWALBeforeCommit(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE wal_join_update (id INTEGER PRIMARY KEY, status TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO wal_join_update (id, status) VALUES (1, 'old')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	walPath := filepath.Join(t.TempDir(), "join-update.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	c.SetWAL(wal)

	table := c.tables["wal_join_update"]
	tree := c.tableTrees["wal_join_update"]
	rowKey := []byte(formatKey(1))
	valueData, err := tree.Get(rowKey)
	if err != nil {
		t.Fatalf("get existing row: %v", err)
	}
	oldVersioned, err := decodeVersionedRow(valueData, len(table.Columns))
	if err != nil {
		t.Fatalf("decode existing row: %v", err)
	}
	newRow := append([]interface{}(nil), oldVersioned.Data...)
	newRow[1] = "new"

	c.BeginTransaction(101)
	if err := c.applyJoinUpdateEntries("wal_join_update", table, tree, []joinUpdateEntry{{
		key:    rowKey,
		oldRow: oldVersioned.Data,
		newRow: newRow,
	}}); err != nil {
		t.Fatalf("applyJoinUpdateEntries: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	c.SetWAL(nil)
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	value := requireReplayOp(t, recoverReplayOpsFromWAL(t, walPath, pool), storage.WALUpdate, "wal_join_update:"+formatKey(1))
	if len(value) == 0 {
		t.Fatal("WAL update replay value is empty")
	}
}

func TestDeleteUsingWritesLogicalWALBeforeSoftDelete(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE wal_delete_using (id INTEGER PRIMARY KEY, status TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO wal_delete_using (id, status) VALUES (1, 'old')"); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	walPath := filepath.Join(t.TempDir(), "delete-using.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	c.SetWAL(wal)

	table := c.tables["wal_delete_using"]
	tree := c.tableTrees["wal_delete_using"]
	rowKey := []byte(formatKey(1))
	valueData, err := tree.Get(rowKey)
	if err != nil {
		t.Fatalf("get existing row: %v", err)
	}
	versioned, err := decodeVersionedRow(valueData, len(table.Columns))
	if err != nil {
		t.Fatalf("decode existing row: %v", err)
	}

	c.BeginTransaction(102)
	if err := c.softDeleteJoinEntries("wal_delete_using", table, tree, []joinDelEntry{{
		key:   rowKey,
		value: append([]byte(nil), valueData...),
		row:   versioned.Data,
	}}); err != nil {
		t.Fatalf("softDeleteJoinEntries: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	c.SetWAL(nil)
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	value := requireReplayOp(t, recoverReplayOpsFromWAL(t, walPath, pool), storage.WALDelete, "wal_delete_using:"+formatKey(1))
	if len(value) != 0 {
		t.Fatalf("WAL delete replay value should be empty, got %q", string(value))
	}
}

func TestForeignKeyCascadeUpdateWritesLogicalWAL(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE wal_fk_parent_update (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE wal_fk_child_update (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES wal_fk_parent_update(id) ON UPDATE CASCADE)"); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO wal_fk_parent_update VALUES (1)"); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO wal_fk_child_update VALUES (10, 1)"); err != nil {
		t.Fatalf("insert child: %v", err)
	}

	walPath := filepath.Join(t.TempDir(), "fk-update.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	c.SetWAL(wal)

	c.BeginTransaction(201)
	if _, err := c.ExecuteQuery("UPDATE wal_fk_parent_update SET id = 2 WHERE id = 1"); err != nil {
		t.Fatalf("cascade parent update: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	c.SetWAL(nil)
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	value := requireReplayOp(t, recoverReplayOpsFromWAL(t, walPath, pool), storage.WALUpdate, "wal_fk_child_update:"+formatKey(10))
	if len(value) == 0 {
		t.Fatal("FK cascade update replay value is empty")
	}
}

func TestForeignKeyCascadeDeleteWritesLogicalWAL(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if _, err := c.ExecuteQuery("CREATE TABLE wal_fk_parent_delete (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if _, err := c.ExecuteQuery("CREATE TABLE wal_fk_child_delete (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY (parent_id) REFERENCES wal_fk_parent_delete(id) ON DELETE CASCADE)"); err != nil {
		t.Fatalf("create child: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO wal_fk_parent_delete VALUES (1)"); err != nil {
		t.Fatalf("insert parent: %v", err)
	}
	if _, err := c.ExecuteQuery("INSERT INTO wal_fk_child_delete VALUES (10, 1)"); err != nil {
		t.Fatalf("insert child: %v", err)
	}

	walPath := filepath.Join(t.TempDir(), "fk-delete.wal")
	wal, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	c.SetWAL(wal)

	c.BeginTransaction(202)
	if _, err := c.ExecuteQuery("DELETE FROM wal_fk_parent_delete WHERE id = 1"); err != nil {
		t.Fatalf("cascade parent delete: %v", err)
	}
	if err := c.CommitTransaction(); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}
	c.SetWAL(nil)
	if err := wal.Close(); err != nil {
		t.Fatalf("Close WAL: %v", err)
	}

	value := requireReplayOp(t, recoverReplayOpsFromWAL(t, walPath, pool), storage.WALDelete, "wal_fk_child_delete:"+formatKey(10))
	if len(value) != 0 {
		t.Fatalf("FK cascade delete replay value should be empty, got %q", string(value))
	}
}

func TestEncodeLogicalWALDataRejectsInvalidOrOversizedInput(t *testing.T) {
	if _, err := encodeLogicalWALData("", []byte("key"), nil); err == nil {
		t.Fatal("expected empty tree name to be rejected")
	}
	if _, err := encodeLogicalWALData("tree", nil, nil); err == nil {
		t.Fatal("expected empty row key to be rejected")
	}

	keyLen := len("tree") + 1 + len("key")
	valueLen := maxCatalogLogicalWALDataBytes - 4 - keyLen + 1
	if _, err := encodeLogicalWALData("tree", []byte("key"), make([]byte, valueLen)); err == nil {
		t.Fatal("expected oversized WAL logical value to be rejected")
	}
}

func TestReplayWALOpsReturnsPutFailure(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "wal_put_fail",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c.tableTrees["wal_put_fail"] = &putFailTree{
		TreeStore: c.tableTrees["wal_put_fail"],
		err:       errors.New("put failed"),
	}

	err := c.ReplayWALOps([]storage.WALReplayOp{{
		Type: storage.WALInsert,
		Data: makeReplayData("wal_put_fail:"+formatKey(1), []byte("row")),
	}})
	if err == nil || !strings.Contains(err.Error(), "put failed") {
		t.Fatalf("expected WAL put failure, got %v", err)
	}
}

func TestReplayWALOpsReturnsDeleteFailure(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	if err := c.CreateTable(&query.CreateTableStmt{
		Table: "wal_delete_fail",
		Columns: []*query.ColumnDef{
			{Name: "id", Type: query.TokenInteger, PrimaryKey: true},
		},
	}); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	c.tableTrees["wal_delete_fail"] = &deleteFailTree{
		TreeStore: c.tableTrees["wal_delete_fail"],
		err:       errors.New("delete failed"),
	}

	err := c.ReplayWALOps([]storage.WALReplayOp{{
		Type: storage.WALDelete,
		Data: makeReplayData("wal_delete_fail:"+formatKey(1), nil),
	}})
	if err == nil || !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected WAL delete failure, got %v", err)
	}
}

func TestReplayWALOpsRejectsOverflowedKeyLength(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data[:4], ^uint32(0))

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ReplayWALOps panicked on overflowed key length: %v", r)
		}
	}()

	err := c.ReplayWALOps([]storage.WALReplayOp{
		{TxnID: 7, Type: storage.WALInsert, Data: data},
	})
	if err == nil || !strings.Contains(err.Error(), "key length") {
		t.Fatalf("expected corrupt WAL key length error, got %v", err)
	}
}

func TestReplayWALOpsRejectsUnroutableKey(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	err := c.ReplayWALOps([]storage.WALReplayOp{{
		TxnID: 8,
		Type:  storage.WALUpdateCommit,
		Data:  makeReplayData("legacy-key-without-table", []byte("row")),
	}})
	if err == nil || !strings.Contains(err.Error(), "missing table prefix") {
		t.Fatalf("expected unroutable WAL key error, got %v", err)
	}
}

func TestReplayWALOpsRejectsEmptyKeyParts(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	err := c.ReplayWALOps([]storage.WALReplayOp{{
		TxnID: 10,
		Type:  storage.WALInsert,
		Data:  makeReplayData(":row", []byte("row")),
	}})
	if err == nil || !strings.Contains(err.Error(), "empty table name") {
		t.Fatalf("expected empty table name error, got %v", err)
	}

	err = c.ReplayWALOps([]storage.WALReplayOp{{
		TxnID: 11,
		Type:  storage.WALDelete,
		Data:  makeReplayData("table:", nil),
	}})
	if err == nil || !strings.Contains(err.Error(), "empty row key") {
		t.Fatalf("expected empty row key error, got %v", err)
	}
}

func TestReplayWALOpsRejectsUnknownType(t *testing.T) {
	c, pool := newMetadataIsolationCatalog(t)
	defer pool.Close()

	err := c.ReplayWALOps([]storage.WALReplayOp{{
		TxnID: 9,
		Type:  storage.WALRecordType(0xff),
		Data:  makeReplayData("table:key", []byte("row")),
	}})
	if err == nil || !strings.Contains(err.Error(), "invalid WAL replay record type") {
		t.Fatalf("expected invalid WAL replay type error, got %v", err)
	}
}
