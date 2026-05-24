# Vector Index Persistence

CobaltDB vector indexes use in-process HNSW structures backed by catalog
metadata. The persisted catalog record includes the HNSW nodes and entry-point
key; load rebuilds the runtime entry-point pointer.

## Verified

| Area | Status |
|---|---|
| `CREATE VECTOR INDEX` metadata persistence | Written to catalog metadata immediately |
| Existing row indexing | Covered for disk reopen |
| HNSW node persistence | Covered for disk reopen |
| Entry point rebuild | Covered for disk reopen |
| `DROP` persistence | Dropped metadata does not reappear after reopen |

## Release Drill

```bash
go test ./pkg/catalog -run TestVectorIndexMetadataPersistsOnCreateAndDrop -count=1
go test ./pkg/engine -run TestVectorIndexPersistsAcrossReopen -count=1
go test -race ./pkg/catalog ./pkg/engine -run 'TestVectorIndexMetadataPersistsOnCreateAndDrop|TestVectorIndexPersistsAcrossReopen' -count=1
```

Remaining certification work:

- Larger HNSW rebuild tests with thousands of vectors.
- Delete/update workload drills across multiple reopen cycles.
- Backup/restore drills that include vector indexes.
