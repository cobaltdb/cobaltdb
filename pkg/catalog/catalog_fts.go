package catalog

import (
	"fmt"
	"sort"
)

func (c *Catalog) CreateFTSIndex(name, tableName string, columns []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	if _, exists := c.ftsIndexes[name]; exists {
		return fmt.Errorf("FTS index %s already exists", name)
	}

	// Verify table exists
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return err
	}

	// Verify columns exist
	for _, col := range columns {
		if table.GetColumnIndex(col) == -1 {
			return fmt.Errorf("column %s not found in table %s", col, tableName)
		}
	}

	ftsIndex := &FTSIndexDef{
		Name:      name,
		TableName: tableName,
		Columns:   cloneStringSlice(columns),
		Index:     make(map[string][]int64),
	}

	// Build the index from existing data
	tree, exists := c.tableTrees[tableName]
	if exists {
		pendingWrites := c.pendingWritesForTable(tableName)
		iter, err := tree.Scan(nil, nil)
		if err != nil {
			return fmt.Errorf("failed to scan table %s for FTS index %s: %w", tableName, name, err)
		}
		defer iter.Close()
		for iter.HasNext() {
			key, value, iterErr := iter.Next()
			if iterErr != nil {
				return fmt.Errorf("failed to read row for FTS index %s: %w", name, iterErr)
			}
			if key == nil || len(value) == 0 {
				break
			}
			if _, shadowed := pendingWrites[string(key)]; shadowed {
				continue
			}
			if err := c.addRowToFTSIndexLocked(ftsIndex, table, key, value); err != nil {
				return fmt.Errorf("failed to decode row for FTS index %s: %w", name, err)
			}
		}
		for key, pw := range pendingWrites {
			if pw.Value == nil {
				continue
			}
			if err := c.addRowToFTSIndexLocked(ftsIndex, table, []byte(key), pw.Value); err != nil {
				return fmt.Errorf("failed to decode pending row for FTS index %s: %w", name, err)
			}
		}
	}

	c.ftsIndexes[name] = ftsIndex
	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:    undoCreateFTSIndex,
			indexName: name,
		})
	}
	return nil
}

func (c *Catalog) addRowToFTSIndexLocked(ftsIndex *FTSIndexDef, table *TableDef, key, value []byte) error {
	vrow, err := decodeVersionedRow(value, len(table.Columns))
	if err != nil {
		return err
	}
	if vrow.Version.DeletedAt > 0 {
		return nil
	}
	row := make(map[string]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		if i < len(vrow.Data) {
			row[col.Name] = vrow.Data[i]
		}
	}
	c.indexRowForFTS(ftsIndex, row, key)
	return nil
}

func (c *Catalog) indexRowForFTS(ftsIndex *FTSIndexDef, row map[string]interface{}, key []byte) {
	// Extract row ID from key - use a simple hash of the key
	rowID := int64(0)
	for _, b := range key {
		rowID = rowID*31 + int64(b)
	}
	if rowID < 0 {
		rowID = -rowID
	}

	// Index each column
	for _, col := range ftsIndex.Columns {
		if val, exists := row[col]; exists && val != nil {
			text := ValueToStringKey(val)
			words := tokenize(text)
			for _, word := range words {
				word = toLowerFast(word)
				ftsIndex.Index[word] = append(ftsIndex.Index[word], rowID)
			}
		}
	}
}

func (c *Catalog) DropFTSIndex(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.invalidateSchemaCache()
	ftsIndex, exists := c.ftsIndexes[name]
	if !exists {
		return fmt.Errorf("FTS index %s not found", name)
	}

	if c.isCurrentTxnActive() {
		c.appendUndoEntry(undoEntry{
			action:      undoDropFTSIndex,
			indexName:   name,
			ftsIndexDef: cloneFTSIndexDef(ftsIndex),
		})
	}
	delete(c.ftsIndexes, name)
	return nil
}

func (c *Catalog) SearchFTS(indexName string, query string) ([]int64, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ftsIndex, exists := c.ftsIndexes[indexName]
	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", indexName)
	}

	words := tokenize(query)
	if len(words) == 0 {
		return []int64{}, nil
	}

	// Find rows matching all words (AND logic)
	var result []int64
	first := true

	for _, word := range words {
		word = toLowerFast(word)
		rows, exists := ftsIndex.Index[word]
		if !exists {
			return []int64{}, nil // Word not found, no matches
		}

		if first {
			result = append([]int64{}, rows...)
			first = false
		} else {
			// Intersection
			result = intersectSorted(result, rows)
			if len(result) == 0 {
				return []int64{}, nil
			}
		}
	}

	return result, nil
}

func (c *Catalog) GetFTSIndex(name string) (*FTSIndexDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ftsIndex, exists := c.ftsIndexes[name]
	if !exists {
		return nil, fmt.Errorf("FTS index %s not found", name)
	}
	return ftsIndex, nil
}

func (c *Catalog) ListFTSIndexDefs() []FTSIndexDef {
	c.mu.RLock()
	defer c.mu.RUnlock()

	defs := make([]FTSIndexDef, 0, len(c.ftsIndexes))
	for _, idx := range c.ftsIndexes {
		if idx == nil {
			continue
		}
		defs = append(defs, FTSIndexDef{
			Name:      idx.Name,
			TableName: idx.TableName,
			Columns:   cloneStringSlice(idx.Columns),
		})
	}
	sort.Slice(defs, func(i, j int) bool {
		return toLowerFast(defs[i].Name) < toLowerFast(defs[j].Name)
	})
	return defs
}

func cloneFTSIndexDef(idx *FTSIndexDef) *FTSIndexDef {
	if idx == nil {
		return nil
	}
	cloned := &FTSIndexDef{
		Name:      idx.Name,
		TableName: idx.TableName,
		Columns:   cloneStringSlice(idx.Columns),
	}
	if idx.Index != nil {
		cloned.Index = make(map[string][]int64, len(idx.Index))
		for word, rows := range idx.Index {
			cloned.Index[word] = append([]int64(nil), rows...)
		}
	}
	return cloned
}

func (c *Catalog) ListFTSIndexes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.ftsIndexes))
	for name := range c.ftsIndexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
