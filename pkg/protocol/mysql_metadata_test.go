package protocol

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

type parsedColumnDefinition struct {
	name     string
	table    string
	charset  uint16
	length   uint32
	typ      byte
	flags    uint16
	decimals byte
}

func parseColumnDefinitionForTest(t *testing.T, payload []byte) parsedColumnDefinition {
	t.Helper()
	offset := 0
	readString := func(field string) string {
		t.Helper()
		length, n := readLenEncInt(payload[offset:])
		if n == 0 {
			t.Fatalf("malformed %s length at offset %d", field, offset)
		}
		offset += n
		if length > uint64(len(payload)-offset) {
			t.Fatalf("%s length exceeds payload", field)
		}
		value := string(payload[offset : offset+int(length)])
		offset += int(length)
		return value
	}

	_ = readString("catalog")
	_ = readString("schema")
	table := readString("table")
	_ = readString("org_table")
	name := readString("name")
	_ = readString("org_name")

	if offset+13 > len(payload) {
		t.Fatalf("column definition fixed fields truncated: offset=%d len=%d", offset, len(payload))
	}
	if payload[offset] != 0x0c {
		t.Fatalf("fixed-field length = 0x%02x, want 0x0c", payload[offset])
	}
	offset++

	charset := binary.LittleEndian.Uint16(payload[offset:])
	offset += 2
	length := binary.LittleEndian.Uint32(payload[offset:])
	offset += 4
	typ := payload[offset]
	offset++
	flags := binary.LittleEndian.Uint16(payload[offset:])
	offset += 2
	decimals := payload[offset]

	return parsedColumnDefinition{
		name:     name,
		table:    table,
		charset:  charset,
		length:   length,
		typ:      typ,
		flags:    flags,
		decimals: decimals,
	}
}

func readWrittenPacketsForTest(t *testing.T, data []byte) [][]byte {
	t.Helper()
	var packets [][]byte
	for len(data) > 0 {
		if len(data) < 4 {
			t.Fatalf("truncated packet header: %d bytes", len(data))
		}
		length := int(data[0]) | int(data[1])<<8 | int(data[2])<<16
		if len(data) < 4+length {
			t.Fatalf("packet length %d exceeds remaining %d", length, len(data)-4)
		}
		payload := append([]byte(nil), data[4:4+length]...)
		packets = append(packets, payload)
		data = data[4+length:]
	}
	return packets
}

func TestBuildColumnDefPacketWithSQLMetadata(t *testing.T) {
	client, _ := newTestClient(nil)
	row := []interface{}{"id", "INTEGER", "NO", "PRI", "NULL", "auto_increment"}

	payload := client.buildColumnDefPacketWithDefinition(mysqlColumnDefinitionFromDescribe("users", row))
	def := parseColumnDefinitionForTest(t, payload)

	if def.name != "id" || def.table != "users" {
		t.Fatalf("unexpected identity metadata: %#v", def)
	}
	if def.typ != MySQLTypeLong {
		t.Fatalf("type = 0x%02x, want INTEGER type 0x%02x", def.typ, MySQLTypeLong)
	}
	if def.charset != mysqlCharsetBinary {
		t.Fatalf("charset = %d, want binary charset %d", def.charset, mysqlCharsetBinary)
	}
	if def.flags&mysqlColumnFlagPriKey == 0 || def.flags&mysqlColumnFlagNotNull == 0 || def.flags&mysqlColumnFlagAutoIncrement == 0 {
		t.Fatalf("flags = 0x%04x, want primary/not-null/auto-increment", def.flags)
	}
}

func TestBuildColumnDefPacketSanitizesOversizedMetadataNames(t *testing.T) {
	client, _ := newTestClient(nil)
	oversized := strings.Repeat("a", maxMySQLIdentifierBytes+128)

	payload := client.buildColumnDefPacketWithDefinition(mysqlColumnDefinition{
		name:     oversized + "\x00",
		orgName:  oversized + "\n",
		table:    oversized + "\x7f",
		orgTable: oversized + "\r",
		charset:  mysqlCharsetUTF8GeneralCI,
		length:   65535,
		typ:      MySQLTypeVarString,
	})
	def := parseColumnDefinitionForTest(t, payload)

	if len(def.name) != maxMySQLIdentifierBytes {
		t.Fatalf("column name length = %d, want %d", len(def.name), maxMySQLIdentifierBytes)
	}
	if len(def.table) != maxMySQLIdentifierBytes {
		t.Fatalf("table name length = %d, want %d", len(def.table), maxMySQLIdentifierBytes)
	}
	if strings.ContainsAny(def.name, "\x00\n\r\x7f") || strings.ContainsAny(def.table, "\x00\n\r\x7f") {
		t.Fatalf("metadata names contain control characters: name=%q table=%q", def.name, def.table)
	}
}

func TestSanitizeMySQLMetadataNameReplacesControlCharacters(t *testing.T) {
	got := sanitizeMySQLMetadataName("a\x00b\nc\rd\x7f")
	if got != "a?b?c?d?" {
		t.Fatalf("sanitizeMySQLMetadataName = %q", got)
	}
}

type fakeColumnTypeHints struct {
	hints []string
}

func (f fakeColumnTypeHints) ColumnTypeHints() []string {
	return append([]string(nil), f.hints...)
}

func TestBuildColumnDefinitionsForRowsUsesTypeHints(t *testing.T) {
	client, _ := newTestClient(nil)
	defs := client.buildColumnDefinitionsForRows(
		[]string{"id", "name", "score", "payload", "empty"},
		fakeColumnTypeHints{hints: []string{"BIGINT", "TEXT", "DOUBLE", "BLOB", ""}},
	)

	if defs[0].typ != MySQLTypeLongLong || defs[0].charset != mysqlCharsetBinary {
		t.Fatalf("id metadata mismatch: %#v", defs[0])
	}
	if defs[1].typ != MySQLTypeVarString || defs[1].charset != mysqlCharsetUTF8GeneralCI {
		t.Fatalf("name metadata mismatch: %#v", defs[1])
	}
	if defs[2].typ != MySQLTypeDouble || defs[2].charset != mysqlCharsetBinary {
		t.Fatalf("score metadata mismatch: %#v", defs[2])
	}
	if defs[3].typ != MySQLTypeBlob || defs[3].flags&mysqlColumnFlagBlob == 0 {
		t.Fatalf("payload metadata mismatch: %#v", defs[3])
	}
	if defs[4].typ != MySQLTypeVarString {
		t.Fatalf("empty metadata should fall back to string: %#v", defs[4])
	}
}

func TestHandleFieldListEmitsDescribeColumnMetadata(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, score REAL, data BLOB)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	client, conn := newTestClient(db)
	if err := client.handleFieldList([]byte("users\x00")); err != nil {
		t.Fatalf("handleFieldList: %v", err)
	}

	packets := readWrittenPacketsForTest(t, conn.writeBuf.Bytes())
	if len(packets) != 5 {
		t.Fatalf("packet count = %d, want 4 columns + EOF", len(packets))
	}

	defs := make([]parsedColumnDefinition, 4)
	for i := range defs {
		defs[i] = parseColumnDefinitionForTest(t, packets[i])
		if defs[i].table != "users" {
			t.Fatalf("column %d table = %q, want users", i, defs[i].table)
		}
	}

	if defs[0].name != "id" || defs[0].typ != MySQLTypeLong || defs[0].flags&mysqlColumnFlagPriKey == 0 {
		t.Fatalf("id metadata mismatch: %#v", defs[0])
	}
	if defs[1].name != "name" || defs[1].typ != MySQLTypeVarString || defs[1].flags&mysqlColumnFlagNotNull == 0 {
		t.Fatalf("name metadata mismatch: %#v", defs[1])
	}
	if defs[2].name != "score" || defs[2].typ != MySQLTypeFloat || defs[2].charset != mysqlCharsetBinary {
		t.Fatalf("score metadata mismatch: %#v", defs[2])
	}
	if defs[3].name != "data" || defs[3].typ != MySQLTypeBlob || defs[3].flags&mysqlColumnFlagBlob == 0 {
		t.Fatalf("data metadata mismatch: %#v", defs[3])
	}
	if packets[4][0] != 0xfe {
		t.Fatalf("last packet = 0x%02x, want EOF", packets[4][0])
	}
}

func TestQuoteMySQLIdentifier(t *testing.T) {
	got, err := quoteMySQLIdentifier(`users"; DROP TABLE users;--`)
	if err != nil {
		t.Fatalf("quoteMySQLIdentifier returned error: %v", err)
	}
	want := `"users""; DROP TABLE users;--"`
	if got != want {
		t.Fatalf("quoteMySQLIdentifier = %q, want %q", got, want)
	}
	if _, err := quoteMySQLIdentifier(""); err == nil {
		t.Fatal("expected empty identifier to be rejected")
	}
	if _, err := quoteMySQLIdentifier(strings.Repeat("a", maxMySQLIdentifierBytes+1)); err == nil {
		t.Fatal("expected oversized identifier to be rejected")
	}
}

func TestHandleFieldListQuotesTableName(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{CoreStorage: engine.CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(t.Context(), "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatalf("create table: %v", err)
	}

	client, conn := newTestClient(db)
	err = client.handleFieldList([]byte("users; DROP TABLE users;--\x00"))
	if err != nil {
		t.Fatalf("handleFieldList should send an error packet, not return transport error: %v", err)
	}
	packets := readWrittenPacketsForTest(t, conn.writeBuf.Bytes())
	if len(packets) != 1 || len(packets[0]) == 0 || packets[0][0] != 0xff {
		t.Fatalf("expected one MySQL error packet, got %#v", packets)
	}

	rows, err := db.Query(t.Context(), "SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("users table should remain after malicious field list: %v", err)
	}
	rows.Close()
}
