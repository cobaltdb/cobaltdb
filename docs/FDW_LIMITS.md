# Foreign Data Wrapper Limits

CobaltDB currently materializes FDW scan results into a temporary query-engine
B-tree. Wrappers that implement the streaming cursor API avoid building an
intermediate `[][]interface{}` result inside the wrapper. The CSV FDW implements
that cursor API and still enforces explicit limits so large external files fail
predictably.

Simple `WHERE` predicates of the form `column <op> literal_or_placeholder`,
combined with `AND`, are now passed to streaming wrappers through
`fdw.ScanOptions.Predicates`. The CSV wrapper applies safe equality and numeric
range predicates when headers map predicate columns to CSV fields. Unsupported
expressions are still evaluated by the local query engine, so pushdown remains
advisory and correctness does not depend on wrapper-side filtering.

For simple single-table SELECT queries, the catalog also passes the needed
SELECT/WHERE/ORDER BY columns through `fdw.ScanOptions.Columns`. Returned
projected rows are expanded back to the full table shape before local SQL
evaluation, preserving existing query semantics.

## CSV FDW Options

| Option | Default | Meaning |
|---|---:|---|
| `file` | required | CSV file path |
| `max_rows` | `1000000` | Maximum materialized data rows; `0` disables the row limit |
| `max_bytes` | `268435456` | Maximum CSV file size in bytes; `0` disables the byte limit |

Example:

```sql
CREATE FOREIGN TABLE ext_users (
  id INTEGER,
  name TEXT
) WRAPPER 'csv'
OPTIONS (
  file '/data/users.csv',
  max_rows '100000',
  max_bytes '67108864'
);
```

## Release Drill

```bash
go test ./pkg/fdw -count=1
go test -race ./pkg/fdw -count=1
go test ./pkg/catalog -run 'TestFDWScanOptionsCarrySimpleWherePredicates|TestFDWProjectionPushdownExpandsRowsForLocalEvaluation' -count=1
go test ./integration -run 'TestFDWCSVSelect|TestFDWCSVMaxRowsLimitViaSQL|TestFDWCSVProjectionPushdownViaSQL' -count=1
```

Remaining work:

- Broader projection pushdown for JOIN/GROUP BY queries.
- Per-query memory accounting across FDW and local execution.
