package catalog

import (
	"encoding/json"
	"fmt"
	"sort"
)

// CreateVectorIndex creates a new HNSW vector index on a table column
func (c *Catalog) CreateVectorIndex(name, tableName, columnName string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	if _, exists := c.vectorIndexes[name]; exists {
		return fmt.Errorf("vector index %s already exists", name)
	}

	// Verify table exists
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	// Verify column exists and is VECTOR type
	colIdx := table.GetColumnIndex(columnName)
	if colIdx == -1 {
		return fmt.Errorf("column %s not found in table %s", columnName, tableName)
	}
	col := table.Columns[colIdx]
	if col.Type != "VECTOR" {
		return fmt.Errorf("column %s is not a VECTOR type", columnName)
	}
	if col.Dimensions == 0 {
		return fmt.Errorf("column %s has no dimensions specified", columnName)
	}

	// Create the HNSW index
	hnswIndex := NewHNSWIndex(name, tableName, columnName, col.Dimensions)

	vectorIndex := &VectorIndexDef{
		Name:       name,
		TableName:  tableName,
		ColumnName: columnName,
		Dimensions: col.Dimensions,
		IndexType:  "hnsw",
		HNSW:       hnswIndex,
	}

	// Build the index from existing data
	tree, exists := c.tableTrees[tableName]
	if exists {
		pendingWrites := c.pendingWritesForTable(tableName)
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table %s for vector index %s: %w", tableName, name, err)
		}
		defer iter.Close()
		for iter.HasNext() {
			rowKey, value, iterErr := iter.NextString()
			if iterErr != nil {
				return fmt.Errorf("failed to read row for vector index %s: %w", name, iterErr)
			}
			if rowKey == "" || len(value) == 0 {
				break
			}
			if _, shadowed := pendingWrites[rowKey]; shadowed {
				continue
			}
			if err := c.addRowToVectorIndexLocked(vectorIndex, table, value, rowKey, colIdx); err != nil {
				return fmt.Errorf("failed to add row %s to vector index %s: %w", rowKey, name, err)
			}
		}
		for rowKey, pw := range pendingWrites {
			if pw.Value == nil {
				continue
			}
			if err := c.addRowToVectorIndexLocked(vectorIndex, table, pw.Value, rowKey, colIdx); err != nil {
				return fmt.Errorf("failed to add pending row %s to vector index %s: %w", rowKey, name, err)
			}
		}
	}

	if err := c.storeVectorIndexDef(vectorIndex); err != nil {
		return fmt.Errorf("failed to persist vector index %s: %w", name, err)
	}

	c.vectorIndexes[name] = vectorIndex
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoCreateVectorIndex,
			indexName: name,
		})
	}
	return nil
}

func (c *Catalog) addRowToVectorIndexLocked(vectorIndex *VectorIndexDef, table *TableDef, value []byte, rowKey string, colIdx int) error {
	row, ok, err := decodeLiveRow(value, len(table.Columns))
	if err != nil {
		return fmt.Errorf("failed to decode row %s: %w", rowKey, err)
	}
	if !ok {
		return nil
	}
	return c.indexRowForVector(vectorIndex, row, rowKey, colIdx)
}

// indexRowForVector adds a row to the vector index.
func (c *Catalog) indexRowForVector(vectorIndex *VectorIndexDef, rowSlice []interface{}, rowKey string, colIdx int) error {

	// Get the vector value from the row
	if colIdx >= len(rowSlice) {
		return nil
	}

	vectorVal := rowSlice[colIdx]
	if vectorVal == nil {
		return nil
	}

	vector, err := toVector(vectorVal)
	if err != nil {
		return nil
	}

	// Validate dimensions
	if len(vector) != vectorIndex.Dimensions {
		return nil // Dimension mismatch
	}

	// Insert into HNSW index
	if vectorIndex.HNSW != nil {
		if err := vectorIndex.HNSW.Insert(rowKey, vector); err != nil {
			return err
		}
	}
	return nil
}

// DropVectorIndex removes a vector index
func (c *Catalog) DropVectorIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()

	vectorIndex, exists := c.vectorIndexes[name]
	if !exists {
		return fmt.Errorf("vector index %s not found", name)
	}

	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:         undoDropVectorIndex,
			indexName:      name,
			vectorIndexDef: cloneVectorIndexDef(vectorIndex),
		})
	}
	if c.tree != nil {
		if err := c.tree.Delete([]byte("vec:" + name)); err != nil {
			return fmt.Errorf("failed to delete vector index %s metadata: %w", name, err)
		}
	}

	delete(c.vectorIndexes, name)
	return nil
}

// SearchVectorKNN performs a K-nearest neighbor search on a vector index
func (c *Catalog) SearchVectorKNN(indexName string, queryVector []float64, k int) ([]string, []float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vectorIndex, exists := c.vectorIndexes[indexName]
	if !exists {
		return nil, nil, fmt.Errorf("vector index %s not found", indexName)
	}

	if vectorIndex.HNSW == nil {
		return nil, nil, fmt.Errorf("vector index %s has no HNSW structure", indexName)
	}

	// §1.16 option B: read-your-own-writes. The overlay can REMOVE keys
	// from the HNSW top-k (tombstones, superseded embeddings), so the graph
	// must be over-fetched by the pending count or the post-filter result
	// can under-fill k.
	fetchK := k
	if k > 0 {
		if p := c.pendingVectorFilterCount(vectorIndex); p > 0 {
			fetchK = k + p
		}
	}

	keys, dists, err := vectorIndex.HNSW.SearchKNN(queryVector, fetchK)
	if err != nil {
		return nil, nil, err
	}
	keys, dists = c.overlayPendingVectorResults(vectorIndex, keys, dists, queryVector, k)
	return keys, dists, nil
}

// pendingVectorFilterCount returns how many keys the current transaction's
// pending writes could remove or supersede from a vector search result —
// the over-fetch headroom SearchVectorKNN needs so the overlay's filtering
// cannot under-fill k. Zero on every fast path (no txn, no table, no
// pending writes, no indexed column).
func (c *Catalog) pendingVectorFilterCount(vi *VectorIndexDef) int {
	ts := c.getCurrentTxn()
	if ts == nil {
		return 0
	}
	table, ok := c.tables[vi.TableName]
	if !ok || table == nil {
		return 0
	}
	n := 0
	for _, m := range ts.pendingWriteMapsFor(table) {
		n += len(m)
	}
	return n
}

// overlayPendingVectorResults merges the current transaction's pending
// embeddings into HNSW search results (refactor.md §1.16 option B). Under
// option A the HNSW graph is only refreshed at COMMIT, so until then a
// transaction's own buffered writes are invisible to its own vector search.
// The overlay restores read-your-own-writes without touching HNSW:
//   - a pending live row with a valid embedding enters as a candidate at its
//     true l2Distance (superseding the stale HNSW entry for the same key);
//   - a pending tombstone (buffered delete, or a rekey's old key) drops the
//     key from the results entirely;
//   - a live row without a valid embedding is excluded (HNSW would not have
//     indexed it either).
//
// The HNSW pool must be over-fetched by the pending count (see
// pendingVectorFilterCount): filtering can remove keys from the returned
// top-k, so k alone can under-fill. Callers hold c.mu.RLock;
// pendingWriteMapsFor reads goroutine-local transaction state.
func (c *Catalog) overlayPendingVectorResults(vi *VectorIndexDef, keys []string, dists []float64, query []float64, k int) ([]string, []float64) {
	if k <= 0 {
		// SearchKNN already returned the empty result for k<=0.
		return []string{}, []float64{}
	}
	ts := c.getCurrentTxn()
	if ts == nil {
		return keys, dists
	}
	table, ok := c.tables[vi.TableName]
	if !ok || table == nil {
		return keys, dists
	}
	maps := ts.pendingWriteMapsFor(table)
	if len(maps) == 0 {
		return keys, dists
	}
	colIdx := table.GetColumnIndex(vi.ColumnName)
	if colIdx < 0 {
		return keys, dists
	}

	pending := make(map[string]PendingWrite)
	for _, m := range maps {
		for key, w := range m {
			pending[key] = w
		}
	}
	if len(pending) == 0 {
		return keys, dists
	}

	type candidate struct {
		key  string
		dist float64
	}
	cands := make([]candidate, 0, len(keys)+len(pending))
	touched := make(map[string]bool, len(pending))
	for key, w := range pending {
		touched[key] = true
		vrow, err := decodeVersionedRow(w.Value, len(table.Columns))
		if err != nil || vrow.Version.DeletedAt > 0 || colIdx >= len(vrow.Data) {
			continue // tombstone or undecodable: exclude from HNSW results
		}
		vec, verr := toVector(vrow.Data[colIdx])
		if verr != nil || len(vec) != vi.Dimensions {
			continue // no valid embedding: HNSW wouldn't index it either
		}
		cands = append(cands, candidate{key: key, dist: l2Distance(query, vec)})
	}
	for i, key := range keys {
		if touched[key] {
			continue // superseded or filtered by the pending state above
		}
		cands = append(cands, candidate{key: key, dist: dists[i]})
	}
	sort.Slice(cands, func(a, b int) bool { return cands[a].dist < cands[b].dist })
	if len(cands) > k {
		cands = cands[:k]
	}
	outKeys := make([]string, len(cands))
	outDists := make([]float64, len(cands))
	for i, cd := range cands {
		outKeys[i] = cd.key
		outDists[i] = cd.dist
	}
	return outKeys, outDists
}

// SearchVectorRange performs a range search on a vector index
func (c *Catalog) SearchVectorRange(indexName string, queryVector []float64, radius float64) ([]string, []float64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vectorIndex, exists := c.vectorIndexes[indexName]
	if !exists {
		return nil, nil, fmt.Errorf("vector index %s not found", indexName)
	}

	if vectorIndex.HNSW == nil {
		return nil, nil, fmt.Errorf("vector index %s has no HNSW structure", indexName)
	}

	// §1.16 option B: read-your-own-writes — the same over-fetch reasoning
	// as SearchVectorKNN: the overlay can REMOVE keys from the frontier
	// results (tombstones, superseded embeddings), so the level-0
	// exploration gets headroom for what will be filtered.
	keys, dists, err := vectorIndex.HNSW.SearchRangeWithEf(queryVector, radius, c.pendingVectorFilterCount(vectorIndex))
	if err != nil {
		return nil, nil, err
	}
	keys, dists = c.overlayPendingVectorResultsRange(vectorIndex, keys, dists, queryVector, radius)
	return keys, dists, nil
}

// overlayPendingVectorResultsRange is the SearchVectorRange variant of
// overlayPendingVectorResults (§1.16 option B): the same pending-embedding
// merge — a live pending row with a valid embedding enters at its true
// l2Distance (superseding the stale HNSW entry), a pending tombstone drops
// the key entirely, and untouched keys pass through — but the result keeps
// exactly the keys within radius instead of capping at k. There is no k to
// under-fill, though the caller must still over-fetch the frontier (see
// SearchRangeWithEf) so filtering cannot shrink the reachable pool.
// Callers hold c.mu.RLock; pendingWriteMapsFor reads goroutine-local
// transaction state.
func (c *Catalog) overlayPendingVectorResultsRange(vi *VectorIndexDef, keys []string, dists []float64, query []float64, radius float64) ([]string, []float64) {
	if radius < 0 {
		return []string{}, []float64{}
	}
	ts := c.getCurrentTxn()
	if ts == nil {
		return keys, dists
	}
	table, ok := c.tables[vi.TableName]
	if !ok || table == nil {
		return keys, dists
	}
	maps := ts.pendingWriteMapsFor(table)
	if len(maps) == 0 {
		return keys, dists
	}
	colIdx := table.GetColumnIndex(vi.ColumnName)
	if colIdx < 0 {
		return keys, dists
	}

	pending := make(map[string]PendingWrite)
	for _, m := range maps {
		for key, w := range m {
			pending[key] = w
		}
	}
	if len(pending) == 0 {
		return keys, dists
	}

	type candidate struct {
		key  string
		dist float64
	}
	cands := make([]candidate, 0, len(keys)+len(pending))
	touched := make(map[string]bool, len(pending))
	for key, w := range pending {
		touched[key] = true
		vrow, err := decodeVersionedRow(w.Value, len(table.Columns))
		if err != nil || vrow.Version.DeletedAt > 0 || colIdx >= len(vrow.Data) {
			continue // tombstone or undecodable: exclude from HNSW results
		}
		vec, verr := toVector(vrow.Data[colIdx])
		if verr != nil || len(vec) != vi.Dimensions {
			continue // no valid embedding: HNSW wouldn't index it either
		}
		cands = append(cands, candidate{key: key, dist: l2Distance(query, vec)})
	}
	for i, key := range keys {
		if touched[key] {
			continue // superseded or filtered by the pending state above
		}
		cands = append(cands, candidate{key: key, dist: dists[i]})
	}
	sort.Slice(cands, func(a, b int) bool { return cands[a].dist < cands[b].dist })
	outKeys := make([]string, 0, len(cands))
	outDists := make([]float64, 0, len(cands))
	for _, cd := range cands {
		if cd.dist > radius {
			continue // radius filter replaces the KNN k-cap
		}
		outKeys = append(outKeys, cd.key)
		outDists = append(outDists, cd.dist)
	}
	return outKeys, outDists
}

// GetVectorIndex retrieves a vector index definition
func (c *Catalog) GetVectorIndex(name string) (*VectorIndexDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	vectorIndex, exists := c.vectorIndexes[name]
	if !exists {
		return nil, fmt.Errorf("vector index %s not found", name)
	}
	return vectorIndex, nil
}

func (c *Catalog) ListVectorIndexDefs() []VectorIndexDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	defs := make([]VectorIndexDef, 0, len(c.vectorIndexes))
	for _, idx := range c.vectorIndexes {
		if idx == nil {
			continue
		}
		defs = append(defs, VectorIndexDef{
			Name:       idx.Name,
			TableName:  idx.TableName,
			ColumnName: idx.ColumnName,
			Dimensions: idx.Dimensions,
			IndexType:  idx.IndexType,
		})
	}
	sort.Slice(defs, func(i, j int) bool {
		return toLowerFast(defs[i].Name) < toLowerFast(defs[j].Name)
	})
	return defs
}

func cloneVectorIndexDef(idx *VectorIndexDef) *VectorIndexDef {
	if idx == nil {
		return nil
	}
	data, err := json.Marshal(idx)
	if err != nil {
		return &VectorIndexDef{
			Name:       idx.Name,
			TableName:  idx.TableName,
			ColumnName: idx.ColumnName,
			Dimensions: idx.Dimensions,
			IndexType:  idx.IndexType,
		}
	}
	var cloned VectorIndexDef
	if err := json.Unmarshal(data, &cloned); err != nil {
		return &VectorIndexDef{
			Name:       idx.Name,
			TableName:  idx.TableName,
			ColumnName: idx.ColumnName,
			Dimensions: idx.Dimensions,
			IndexType:  idx.IndexType,
		}
	}
	if cloned.HNSW != nil {
		cloned.HNSW.RebuildEntryPoint()
	}
	return &cloned
}

// ListVectorIndexes returns a sorted list of all vector index names
func (c *Catalog) ListVectorIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.vectorIndexes))
	for name := range c.vectorIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// updateVectorIndexesForInsert updates all vector indexes when a row is inserted
func (c *Catalog) updateVectorIndexesForInsert(tableName string, rowSlice []interface{}, key string) error {
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}

		// Find column index
		table, err := c.getTableLocked(tableName)
		if err != nil {
			return fmt.Errorf("failed to resolve table %s for vector index %s after insert: %w", tableName, vectorIndex.Name, err)
		}
		colIdx := table.GetColumnIndex(vectorIndex.ColumnName)
		if colIdx == -1 {
			return fmt.Errorf("column %s not found in table %s for vector index %s", vectorIndex.ColumnName, tableName, vectorIndex.Name)
		}

		if err := c.indexRowForVector(vectorIndex, rowSlice, key, colIdx); err != nil {
			return fmt.Errorf("failed to update vector index %s after insert: %w", vectorIndex.Name, err)
		}
		if err := c.storeVectorIndexDef(vectorIndex); err != nil {
			return fmt.Errorf("failed to persist vector index %s after insert: %w", vectorIndex.Name, err)
		}
	}
	return nil
}

// updateVectorIndexesForDelete updates all vector indexes when a row is deleted
func (c *Catalog) updateVectorIndexesForDelete(tableName string, rowKey string) error {
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}
		if vectorIndex.HNSW != nil {
			if err := vectorIndex.HNSW.Delete(rowKey); err != nil {
				return fmt.Errorf("failed to delete row from vector index %s: %w", vectorIndex.Name, err)
			}
		}
		if err := c.storeVectorIndexDef(vectorIndex); err != nil {
			return fmt.Errorf("failed to persist vector index %s after delete: %w", vectorIndex.Name, err)
		}
	}
	return nil
}

// updateVectorIndexesForUpdate updates all vector indexes when a row is updated
func (c *Catalog) updateVectorIndexesForUpdate(tableName string, rowSlice []interface{}, rowKey string) error {
	for _, vectorIndex := range c.vectorIndexes {
		if vectorIndex.TableName != tableName {
			continue
		}

		// Delete old entry
		if vectorIndex.HNSW != nil {
			if err := vectorIndex.HNSW.Delete(rowKey); err != nil {
				return fmt.Errorf("failed to delete row from vector index %s before update: %w", vectorIndex.Name, err)
			}
		}

		// Find column index and re-insert
		table, err := c.getTableLocked(tableName)
		if err != nil {
			return fmt.Errorf("failed to resolve table %s for vector index %s after update: %w", tableName, vectorIndex.Name, err)
		}
		colIdx := table.GetColumnIndex(vectorIndex.ColumnName)
		if colIdx == -1 {
			return fmt.Errorf("column %s not found in table %s for vector index %s", vectorIndex.ColumnName, tableName, vectorIndex.Name)
		}

		if err := c.indexRowForVector(vectorIndex, rowSlice, rowKey, colIdx); err != nil {
			return fmt.Errorf("failed to update vector index %s: %w", vectorIndex.Name, err)
		}
		if err := c.storeVectorIndexDef(vectorIndex); err != nil {
			return fmt.Errorf("failed to persist vector index %s after update: %w", vectorIndex.Name, err)
		}
	}
	return nil
}
