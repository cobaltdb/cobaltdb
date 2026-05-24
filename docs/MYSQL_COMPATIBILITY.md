# MySQL Protocol Compatibility

This matrix documents the MySQL wire-protocol surface that is currently covered
by automated tests. Treat unlisted behavior as unsupported until it has an
explicit test and operational owner.

## Verified

| Area | Status | Coverage |
|---|---|---|
| Handshake | Supported | Protocol version, server version, connection ID |
| Authentication | Supported | Optional `mysql_native_password` verification |
| `COM_QUERY` | Supported | DDL, INSERT, UPDATE, DELETE, SELECT, SHOW, SET, USE |
| Result sets | Supported | Text result rows, column definitions, EOF packets |
| Errors | Supported | Error packets and post-error connection reuse |
| `COM_PING` / `COM_QUIT` | Supported | Liveness and connection shutdown |
| `COM_FIELD_LIST` | Supported | Basic table column descriptions |
| `COM_STATISTICS` / `COM_PROCESS_INFO` | Supported | Basic operational responses |
| `COM_RESET_CONNECTION` | Supported | Clears per-client prepared statement state |
| `COM_STMT_PREPARE` | Supported | Statement IDs, parameter count, basic column count |
| `COM_STMT_EXECUTE` | Supported | Bound scalar parameters for query and exec statements |
| Prepared result rows | Supported | Binary row packets for `COM_STMT_EXECUTE` result sets |
| `COM_STMT_CLOSE` / `COM_STMT_RESET` | Supported | Statement lifecycle |
| Go `database/sql` driver | Supported baseline | `github.com/go-sql-driver/mysql` ping, text query, prepared insert, prepared select |

## Prepared Statement Parameters

`COM_STMT_EXECUTE` currently decodes these binary parameter types:

| MySQL type | CobaltDB value |
|---|---|
| `NULL` | `nil` |
| `TINY`, `SHORT`, `LONG`, `INT24`, `LONGLONG`, `YEAR` | `int64` or `uint64` |
| `FLOAT`, `DOUBLE` | `float64` |
| `DECIMAL`, `NEWDECIMAL`, `VARCHAR`, `VAR_STRING`, `STRING` | `string` |
| `TINY_BLOB`, `BLOB`, `MEDIUM_BLOB`, `LONG_BLOB`, `JSON` | `string` |

The first execute packet must send parameter type metadata. Later execute
packets may reuse the cached parameter types for the same prepared statement.

## Not Yet Certified

| Area | Current behavior |
|---|---|
| Temporal binary parameters | Rejected with an unsupported parameter type error |
| `COM_STMT_SEND_LONG_DATA` | Unsupported command |
| Server-side cursors | Cursor flags are not implemented |
| Rich column metadata | Column definitions use coarse string-like metadata |
| Session variables | Common initialization queries are handled, broad MySQL semantics are not complete |
| External client matrix | Go `database/sql` + `go-sql-driver/mysql` is covered; each additional production driver/ORM still needs validation |

## Release Checks

```bash
go test ./pkg/protocol -run 'TestHandleStmt|TestPreparedStmt|TestCountPreparedParams' -count=1
go test ./test -run 'TestMySQLPreparedStatementExecuteWithParameters|TestMySQL' -count=1
go test ./integration -run 'TestMySQLGoSQLDriverCompatibility|TestMySQLProtocolE2E' -count=1
```
