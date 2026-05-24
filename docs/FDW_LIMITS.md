# Foreign Data Wrapper Limits

CobaltDB currently materializes FDW scan results into the query engine. The CSV
FDW therefore has explicit limits so large external files fail predictably
instead of consuming unbounded memory.

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
go test ./integration -run 'TestFDWCSVSelect|TestFDWCSVMaxRowsLimitViaSQL' -count=1
```

Remaining work:

- A streaming FDW cursor interface that avoids materializing all rows.
- Predicate and projection pushdown for wrappers that support it.
- Per-query memory accounting across FDW and local execution.
