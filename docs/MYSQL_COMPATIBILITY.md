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
| Result sets | Supported | Text result rows, value-derived column type hints, EOF packets |
| Errors | Supported | Error packets and post-error connection reuse |
| `COM_PING` / `COM_QUIT` | Supported | Liveness and connection shutdown |
| `COM_FIELD_LIST` | Supported | Table column descriptions with SQL type, key, nullability, BLOB, and auto-increment metadata |
| `COM_STATISTICS` / `COM_PROCESS_INFO` | Supported | Basic operational responses |
| `COM_RESET_CONNECTION` | Supported | Clears per-client prepared statement state |
| `COM_STMT_PREPARE` | Supported | Statement IDs, parameter count, basic column count |
| `COM_STMT_EXECUTE` | Supported | Bound scalar parameters for query and exec statements |
| `COM_STMT_EXECUTE` cursor flags | Rejected safely | Server-side cursor requests return an explicit unsupported error |
| Prepared result rows | Supported | Binary row packets for `COM_STMT_EXECUTE` result sets; metadata remains string-like for driver compatibility |
| `COM_STMT_CLOSE` / `COM_STMT_RESET` | Supported | Statement lifecycle |
| `COM_STMT_SEND_LONG_DATA` | Supported | Chunked prepared TEXT/BLOB parameters |
| Go `database/sql` driver | Supported baseline | `github.com/go-sql-driver/mysql` ping, text query, prepared insert, prepared select |

## Prepared Statement Parameters

`COM_STMT_EXECUTE` currently decodes these binary parameter types:

| MySQL type | CobaltDB value |
|---|---|
| `NULL` | `nil` |
| `TINY`, `SHORT`, `LONG`, `INT24`, `LONGLONG`, `YEAR` | `int64` or `uint64` |
| `FLOAT`, `DOUBLE` | `float64` |
| `DATE`, `NEWDATE` | `YYYY-MM-DD` string |
| `DATETIME`, `TIMESTAMP` | `YYYY-MM-DD HH:MM:SS[.ffffff]` string |
| `TIME` | `[H]HH:MM:SS[.ffffff]` string |
| `DECIMAL`, `NEWDECIMAL`, `VARCHAR`, `VAR_STRING`, `STRING` | `string` |
| `TINY_BLOB`, `BLOB`, `MEDIUM_BLOB`, `LONG_BLOB`, `JSON` | `string` |

The first execute packet must send parameter type metadata. Later execute
packets may reuse the cached parameter types for the same prepared statement.

## Not Yet Certified

| Area | Current behavior |
|---|---|
| Server-side cursor fetch lifecycle | `COM_STMT_FETCH` and incremental cursor result state are not implemented |
| Rich column metadata | Result-set metadata is inferred from materialized values; empty result sets and complex expressions still fall back to string-like metadata |
| Session variables | Common initialization queries are handled, broad MySQL semantics are not complete |
| External client matrix | Go `database/sql` + `go-sql-driver/mysql` is covered; each additional production driver/ORM still needs validation |

## Release Checks

```bash
go test ./pkg/protocol -run 'TestHandleStmt|TestPreparedStmt|TestCountPreparedParams' -count=1
go test ./test -run 'TestMySQLPreparedStatementExecuteWithParameters|TestMySQL' -count=1
go test ./integration -run 'TestMySQLGoSQLDriverCompatibility|TestMySQLProtocolE2E' -count=1
```
