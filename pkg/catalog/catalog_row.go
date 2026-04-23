package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"strings"
)

func (c *Catalog) serializePK(pkValue interface{}, tree *btree.BTree) []byte {
	switch val := pkValue.(type) {
	case string:
		// Try direct string key first
		key := []byte(val)
		if tree != nil {
			if _, err := tree.Get(key); err == nil {
				return key
			}
		}
		// Try with "S:" prefix (Insert format for text PKs)
		if !strings.HasPrefix(val, "S:") {
			sKey := []byte("S:" + val)
			if tree != nil {
				if _, err := tree.Get(sKey); err == nil {
					return sKey
				}
			}
		}
		return key // Default to direct string key
	case int:
		return []byte(fmt.Sprintf("%020d", int64(val)))
	case int64:
		return []byte(fmt.Sprintf("%020d", val))
	case float64:
		return []byte(fmt.Sprintf("%020d", int64(val)))
	default:
		return []byte(fmt.Sprintf("%v", val))
	}
}

func (c *Catalog) GetRow(tableName string, pkValue interface{}) (map[string]interface{}, error) {
	table, err := c.getTableLocked(tableName)
	if err != nil {
		return nil, err
	}

	tree, exists := c.tableTrees[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s has no data", tableName)
	}

	// Serialize the primary key using the same format as Insert
	key := c.serializePK(pkValue, tree)

	// Get the row from BTree
	data, err := tree.Get(key)
	if err != nil {
		return nil, err
	}

	// Decode the row (Insert uses json.Marshal, so use decodeRow for consistency)
	values, err := decodeRow(data, len(table.Columns))
	if err != nil {
		return nil, err
	}

	// Convert to map
	row := make(map[string]interface{})
	for i, col := range table.Columns {
		if i < len(values) {
			row[col.Name] = values[i]
		}
	}

	return row, nil
}
