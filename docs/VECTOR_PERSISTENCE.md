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
| Post-index DML persistence | INSERT/UPDATE/DELETE changes persist vector metadata |
| Larger rebuild drill | Covered with 512 vector rows and DML across reopen |
| Backup/restore drill | Covered with vector index metadata and HNSW nodes |
| Mixed workload reopen drill | Covered with 1024 vectors, post-index UPDATE/DELETE, and two reopen cycles |
| `DROP` persistence | Dropped metadata does not reappear after reopen |

## Release Drill

```bash
go test ./pkg/catalog -run TestVectorIndexMetadataPersistsOnCreateAndDrop -count=1
go test ./pkg/engine -run 'TestVectorIndexPersistsAcrossReopen|TestVectorIndexLargeRebuildAndBackupRestore|TestVectorIndexThousandPlusMixedDMLReopen' -count=1
go test -race ./pkg/catalog ./pkg/engine -run 'TestVectorIndexMetadataPersistsOnCreateAndDrop|TestVectorIndexPersistsAcrossReopen|TestVectorIndexLargeRebuildAndBackupRestore|TestVectorIndexThousandPlusMixedDMLReopen' -count=1
```

Remaining certification work:

- Larger HNSW rebuild tests with tens of thousands of vectors.
- Long-running mixed vector DML workload drills under concurrent readers.
