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
| `COM_STMT_EXECUTE` cursor flags | Supported baseline | Read-only cursor flag opens a per-statement cursor for SELECT result sets |
| `COM_STMT_FETCH` | Supported baseline | Fetches binary result rows from the open prepared-statement cursor and closes it after the final row |
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

## Verified SQL Forms

These MySQL SQL forms are covered by parser and engine regression tests:

| SQL form | Status | Coverage |
|---|---|---|
| `INSERT INTO t DEFAULT VALUES` | Supported | Inserts a row using column defaults and auto-increment values |
| `SELECT ALL ...`, `COUNT(ALL col)`, `SUM(ALL col)` | Supported | `ALL` is accepted as the default select/aggregate quantifier |
| `CREATE OR REPLACE VIEW v AS SELECT ...` | Supported | Existing view definitions are atomically replaced and transaction rollback restores the prior definition |
| `CREATE TEMPORARY VIEW v AS SELECT ...` | Supported baseline | Session-local view metadata is queryable in the current DB handle and not persisted across reopen |
| `CREATE VIEW v (col, ...) AS SELECT ...` | Supported | View column lists override SELECT item names for projection metadata, alias lookup, and persisted view reload |
| `REFRESH MATERIALIZED VIEW CONCURRENTLY mv` | Supported baseline | `CONCURRENTLY` is accepted and uses the existing atomic materialized-view refresh path |
| `GROUP_CONCAT(expr ORDER BY ... SEPARATOR sep)`, `GROUP_CONCAT(expr, sep)` | Supported | MySQL ordering, separator syntax, and comma separator form use the requested delimiter |
| `aggregate(...) FILTER (WHERE predicate)` | Supported | Row-level aggregate filters are applied before aggregate DISTINCT/order/reduction, including window aggregate frames |
| `FROM t INDEXED BY idx`, `FROM t NOT INDEXED` | Supported baseline | SQLite-style table index hints are parsed; `INDEXED BY` is carried into plan hints and `NOT INDEXED` suppresses automatic index hints |
| `FROM (SELECT ...)` | Supported baseline | Anonymous derived tables receive an internal execution alias when the query omits one |
| `JSON_ARRAYAGG(expr)`, `JSON_OBJECTAGG(key, value)` | Supported | Aggregates rows into JSON array/object strings, preserving SQL NULL as JSON null |
| `MIN(a, b, ...)`, `MAX(a, b, ...)` | Supported | Multi-argument calls are evaluated row-wise as scalar functions; single-argument calls remain aggregates |
| `SELECT alias.* FROM ...` | Supported | Qualified star expands only the referenced table or alias |
| `INSERT INTO t SET col = expr [, ...]` | Supported | Normal insert path, omitted-column defaults |
| `INSERT IGNORE INTO t ...` | Supported | Primary-key and unique-key conflicts are skipped; non-conflicting rows insert normally |
| `REPLACE [INTO] t ...` | Supported | Primary-key and unique-key replacement via insert conflict replace |
| `INSERT OR ROLLBACK/ABORT/FAIL INTO t ...` | Supported baseline | `ABORT` and `FAIL` use normal constraint-error abort behavior; `ROLLBACK` rolls back the active transaction when an insert conflict/error occurs |
| `INSERT LOW_PRIORITY/HIGH_PRIORITY/DELAYED ...`, `REPLACE LOW_PRIORITY/DELAYED ...` | Supported | MySQL scheduling modifiers are accepted and ignored for embedded execution |
| `SHOW COLUMNS IN t`, `SHOW INDEXES IN t`, `SHOW KEYS IN t` | Supported | `IN` is accepted as a MySQL synonym for `FROM` in metadata SHOW statements |
| `MATCH(cols) AGAINST('term' IN BOOLEAN MODE)`, `MATCH(cols) AGAINST('term' IN NATURAL LANGUAGE MODE)` | Supported baseline | MySQL full-text search mode syntax is accepted and stored in the AST; current execution uses CobaltDB's existing AND-style token matching for both modes |
| `CREATE TEMP/TEMPORARY TABLE ...` | Supported baseline | Temporary tables use the normal table execution path for the current database handle and are excluded from catalog/index persistence across reopen |
| `CREATE COLLECTION c`, `DROP COLLECTION [IF EXISTS] c` | Supported baseline | Collection DDL is persisted in the catalog and participates in normal duplicate/missing-object checks |
| `CALL proc(arg_name => expr, ...)` | Supported baseline | Procedure arguments bind by parameter name, including out-of-order named arguments and validation for duplicate or unknown names |
| `CREATE TABLE t (col TEXT COLLATE name)` | Supported baseline | Column collation modifiers are accepted, stored in table metadata, and emitted by schema dumps |
| `CREATE TABLE t (..., CONSTRAINT name PRIMARY KEY (cols...))` | Supported baseline | The named table-level primary key form is accepted and enforced using the existing primary-key metadata |
| `CREATE TABLE t (..., CONSTRAINT name UNIQUE (cols...))` | Supported baseline | Named UNIQUE constraints are implemented through unique indexes and can be removed with `ALTER TABLE ... DROP CONSTRAINT name` |
| `CREATE TABLE t (col TYPE CONSTRAINT name UNIQUE)` | Supported baseline | Column-level named UNIQUE constraints are represented as named unique indexes and can be removed with `ALTER TABLE ... DROP CONSTRAINT name` |
| `ALTER TABLE t ADD COLUMN col TYPE CONSTRAINT name UNIQUE` | Supported baseline | ADD COLUMN creates the same named unique index used by DROP CONSTRAINT and future duplicate enforcement |
| `CREATE TABLE t (..., CONSTRAINT name CHECK (expr))` | Supported baseline | Named table-level CHECK constraints are persisted, emitted by schema dumps, and enforced on INSERT/UPDATE |
| `CREATE TABLE t (col TYPE CONSTRAINT name CHECK (expr))` | Supported baseline | Column-level named CHECK constraints are accepted, persisted, emitted by schema dumps, and enforced on INSERT/UPDATE |
| `ALTER TABLE t ADD/DROP CONSTRAINT name CHECK (expr)` | Supported baseline | Added CHECK constraints validate existing rows, enforce future INSERT/UPDATE, and roll back with transaction undo |
| `ALTER TABLE t ADD/DROP CONSTRAINT name UNIQUE (cols...)` | Supported baseline | Named UNIQUE constraints are implemented through unique indexes and reject duplicate existing or future rows |
| `ALTER TABLE t ADD/DROP CONSTRAINT name FOREIGN KEY (...) REFERENCES ...` | Supported baseline | Named foreign keys are stored in table metadata, validate existing rows on add, enforce future DML, and preserve ON DELETE/UPDATE actions |
| `CREATE INDEX idx ON t (col ASC/DESC [COLLATE name])` | Supported baseline | Column ordering and collation modifiers are accepted for MySQL DDL compatibility; current B-tree index storage records the column names only |
| `a MOD b` | Supported | MySQL/SQL modulo operator parses to the same arithmetic operation as `%`; `MOD(a,b)` remains supported as a scalar function |
| `~expr` | Supported | Unary bitwise NOT is accepted and evaluated on integer-compatible operands |
| `expr [NOT] GLOB pattern` | Supported | SQLite-style infix GLOB predicates map to the existing `GLOB(pattern, expr)` evaluator |
| `SELECT ... FOR UPDATE/FOR SHARE [OF table] [NOWAIT/SKIP LOCKED/WAIT n]` | Supported baseline | Locking clauses are parsed and accepted for client compatibility; embedded execution currently treats them as read-only no-ops |
| `UPDATE LOW_PRIORITY t SET ...` | Supported | MySQL scheduling modifier is accepted and ignored for embedded execution |
| `UPDATE t SET (a, b) = (expr1, expr2)` | Supported baseline | Tuple assignment expands to ordinary SET clauses and validates column/value arity |
| `UPDATE target AS alias SET alias.col = ...` | Supported | Target aliases resolve in `SET` and `WHERE` expressions |
| `UPDATE target JOIN source ... SET ...` | Supported baseline | Single-target MySQL joined update maps to the existing join update execution path |
| `UPDATE target, source SET ... WHERE ...` | Supported baseline | Single-target MySQL comma-source update maps to the existing join update execution path |
| `DELETE LOW_PRIORITY/QUICK FROM t ...` | Supported | MySQL scheduling/storage modifiers are accepted and ignored for embedded execution |
| `DELETE FROM target AS alias WHERE alias.col ...` | Supported | Target aliases resolve in `WHERE` expressions |
| `DELETE target FROM target JOIN ...` | Supported baseline | Single-target MySQL joined delete maps to the existing `DELETE ... USING` execution path |
| `DELETE FROM target USING source JOIN guard ON ... WHERE ...` | Supported baseline | JOIN `ON` predicates are combined with `WHERE` for target-row selection |
| `INSERT ... ON DUPLICATE KEY UPDATE ...` | Supported | Primary key, column-level `UNIQUE`, and unique-index conflicts |
| `VALUES(col)` in `ON DUPLICATE KEY UPDATE` | Supported | Reuses the incoming insert value in update expressions |

## Not Yet Certified

| Area | Current behavior |
|---|---|
| Server-side cursor fetch lifecycle | Basic read-only cursor fetch is covered and avoids a protocol-layer result copy; richer MySQL cursor behavior and engine-level streaming need additional certification |
| Rich column metadata | Result-set metadata is inferred from materialized values; empty result sets and complex expressions still fall back to string-like metadata |
| Session variables | Common initialization queries are handled, broad MySQL semantics are not complete |
| External client matrix | Go `database/sql` + `go-sql-driver/mysql` is covered; each additional production driver/ORM still needs validation |

## Release Checks

```bash
go test ./pkg/protocol -run 'TestHandleStmt|TestPreparedStmt|TestCountPreparedParams' -count=1
go test ./test -run 'TestMySQLPreparedStatementExecuteWithParameters|TestMySQL' -count=1
go test ./integration -run 'TestMySQLGoSQLDriverCompatibility|TestMySQLProtocolE2E' -count=1
go test ./pkg/query -run 'TestParseDeleteMySQLModifiers|TestParseMySQLTargetedDelete|TestParseMySQLUpdateCommaJoin|TestParseMySQLUpdateJoin|TestParseMySQLUpdateJoinTargetAlias|TestParseInsertDefaultValues|TestParseInsertSet|TestParseInsertIgnore|TestParseInsertMySQLPriorityModifiers|TestParseOnDuplicateKeyUpdate|TestParseReplace|TestParseUpdateMySQLLowPriorityModifier|TestParseUpdateTargetAlias' -count=1
go test ./pkg/engine -run 'TestRegression_DeleteTargetAlias|TestRegression_DeleteUsingJoinOnCondition|TestRegression_InsertDefaultValues|TestRegression_InsertSet|TestRegression_InsertIgnore|TestRegression_MySQL.*Modifiers|TestRegression_MySQL.*PriorityModifiers|TestRegression_MySQLTargetedDelete|TestRegression_MySQLUpdateCommaJoin|TestRegression_MySQLUpdateJoin|TestRegression_MySQLUpdateJoinTargetAlias|TestRegression_MySQLUpdateLowPriorityModifier|TestRegression_OnDuplicateKeyUpdate|TestRegression_Replace|TestRegression_UpdateTargetAlias' -count=1
```
