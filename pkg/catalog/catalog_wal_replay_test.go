package catalog

import (
	"encoding/binary"
	"errors"
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
