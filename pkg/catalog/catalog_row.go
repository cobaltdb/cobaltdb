package catalog

import (
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"strconv"
	"strings"
)

func (c *Catalog) serializePK(pkValue interface{}, tree btree.TreeStore) []byte {
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
		s := strconv.FormatInt(int64(val), 10)
		if len(s) < 20 {
			s = strings.Repeat("0", 20-len(s)) + s
		}
		return []byte(s)
	case int64:
		s := strconv.FormatInt(val, 10)
		if len(s) < 20 {
			s = strings.Repeat("0", 20-len(s)) + s
		}
		return []byte(s)
	case float64:
		// Must match formatFloatKey: a whole number uses the integer key; a
		// fractional value uses the "F:"-tagged exact float so distinct floats
		// don't collide on int64(val) (which lost the fractional part and made
		// e.g. 1.2 and 1.8 read/write the same row).
		if val == float64(int64(val)) {
			s := strconv.FormatInt(int64(val), 10)
			if len(s) < 20 {
				s = strings.Repeat("0", 20-len(s)) + s
			}
			return []byte(s)
		}
		return []byte("F:" + strconv.FormatFloat(val, 'g', -1, 64))
	default:
		return []byte(ValueToStringKey(val))
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
