package catalog

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// PartitionType defines the type of partitioning
type PartitionType int

const (
	PartitionTypeNone PartitionType = iota
	PartitionTypeRange
	PartitionTypeList
	PartitionTypeHash
)

func (p PartitionType) String() string {
	switch p {
	case PartitionTypeRange:
		return "RANGE"
	case PartitionTypeList:
		return "LIST"
	case PartitionTypeHash:
		return "HASH"
	default:
		return "NONE"
	}
}

// Partition represents a single partition
type Partition struct {
	ID         uint32
	Name       string
	TableName  string
	Type       PartitionType
	ColumnName string

	// For RANGE partitioning
	LowerBound interface{}
	UpperBound interface{}

	// For LIST partitioning
	Values []interface{}

	// For HASH partitioning
	HashModulus   int
	HashRemainder int

	// Metadata
	RowCount   uint64
	SizeBytes  uint64
	CreatedAt  time.Time
	LastAccess time.Time

	// Reference to underlying table storage
	TableID uint32
}

// ContainsValue checks if a value belongs to this partition
func (p *Partition) ContainsValue(value interface{}) bool {
	switch p.Type {
	case PartitionTypeRange:
		return p.containsRange(value)
	case PartitionTypeList:
		return p.containsList(value)
	case PartitionTypeHash:
		return p.containsHash(value)
	default:
		return false
	}
}

func (p *Partition) containsRange(value interface{}) bool {
	cmpLower := compareValues(value, p.LowerBound)
	cmpUpper := compareValues(value, p.UpperBound)

	// value >= LowerBound && value < UpperBound (for most cases)
	// Special case: UpperBound nil means unbounded
	if p.UpperBound == nil {
		return cmpLower >= 0
	}

	return cmpLower >= 0 && cmpUpper < 0
}

func (p *Partition) containsList(value interface{}) bool {
	for _, v := range p.Values {
		if compareValues(value, v) == 0 {
			return true
		}
	}
	return false
}

func (p *Partition) containsHash(value interface{}) bool {
	hash := hashValue(value)
	return int(hash%uint32(p.HashModulus)) == p.HashRemainder
}

// PartitionedTable represents a table with partitioning
type PartitionedTable struct {
	TableName  string
	ColumnName string
	Type       PartitionType
	Partitions []*Partition
	mu         sync.RWMutex

	// Auto-partitioning for time-series data
	AutoPartition         bool
	AutoPartitionInterval time.Duration
	MaxPartitions         int
}

// PartitionManager manages partitioned tables
type PartitionManager struct {
	partitionedTables map[string]*PartitionedTable
	mu                sync.RWMutex
}

// NewPartitionManager creates a new partition manager
func NewPartitionManager() *PartitionManager {
	return &PartitionManager{
		partitionedTables: make(map[string]*PartitionedTable),
	}
}

// CreateRangePartitionedTable creates a range-partitioned table
func (pm *PartitionManager) CreateRangePartitionedTable(tableName, columnName string, partitionDefs []RangePartitionDef) (*PartitionedTable, error) {
	if len(partitionDefs) == 0 {
		return nil, fmt.Errorf("at least one partition required")
	}

	pt := &PartitionedTable{
		TableName:  tableName,
		ColumnName: columnName,
		Type:       PartitionTypeRange,
		Partitions: make([]*Partition, 0, len(partitionDefs)),
	}

	for i, def := range partitionDefs {
		partition := &Partition{
			ID:         uint32(i + 1),
			Name:       def.Name,
			TableName:  tableName,
			Type:       PartitionTypeRange,
			ColumnName: columnName,
			LowerBound: def.LowerBound,
			UpperBound: def.UpperBound,
			CreatedAt:  time.Now(),
		}
		pt.Partitions = append(pt.Partitions, partition)
	}

	pm.mu.Lock()
	pm.partitionedTables[tableName] = pt
	pm.mu.Unlock()

	return pt, nil
}

// CreateListPartitionedTable creates a list-partitioned table
func (pm *PartitionManager) CreateListPartitionedTable(tableName, columnName string, partitionDefs []ListPartitionDef) (*PartitionedTable, error) {
	if len(partitionDefs) == 0 {
		return nil, fmt.Errorf("at least one partition required")
	}

	pt := &PartitionedTable{
		TableName:  tableName,
		ColumnName: columnName,
		Type:       PartitionTypeList,
		Partitions: make([]*Partition, 0, len(partitionDefs)),
	}

	for i, def := range partitionDefs {
		partition := &Partition{
			ID:         uint32(i + 1),
			Name:       def.Name,
			TableName:  tableName,
			Type:       PartitionTypeList,
			ColumnName: columnName,
			Values:     def.Values,
			CreatedAt:  time.Now(),
		}
		pt.Partitions = append(pt.Partitions, partition)
	}

	pm.mu.Lock()
	pm.partitionedTables[tableName] = pt
	pm.mu.Unlock()

	return pt, nil
}

// CreateHashPartitionedTable creates a hash-partitioned table
func (pm *PartitionManager) CreateHashPartitionedTable(tableName, columnName string, numPartitions int) (*PartitionedTable, error) {
	if numPartitions <= 0 {
		return nil, fmt.Errorf("number of partitions must be positive")
	}

	pt := &PartitionedTable{
		TableName:  tableName,
		ColumnName: columnName,
		Type:       PartitionTypeHash,
		Partitions: make([]*Partition, 0, numPartitions),
	}

	for i := 0; i < numPartitions; i++ {
		partition := &Partition{
			ID:            uint32(i + 1),
			Name:          fmt.Sprintf("p%d", i),
			TableName:     tableName,
			Type:          PartitionTypeHash,
			ColumnName:    columnName,
			HashModulus:   numPartitions,
			HashRemainder: i,
			CreatedAt:     time.Now(),
		}
		pt.Partitions = append(pt.Partitions, partition)
	}

	pm.mu.Lock()
	pm.partitionedTables[tableName] = pt
	pm.mu.Unlock()

	return pt, nil
}

// GetPartitionedTable returns a partitioned table by name
func (pm *PartitionManager) GetPartitionedTable(tableName string) (*PartitionedTable, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pt, exists := pm.partitionedTables[tableName]
	return pt, exists
}

// DropPartitionedTable removes a partitioned table
func (pm *PartitionManager) DropPartitionedTable(tableName string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.partitionedTables[tableName]; !exists {
		return fmt.Errorf("partitioned table %s not found", tableName)
	}

	delete(pm.partitionedTables, tableName)
	return nil
}

// GetPartitionForValue returns the partition that should contain a value
func (pt *PartitionedTable) GetPartitionForValue(value interface{}) *Partition {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	for _, partition := range pt.Partitions {
		if partition.ContainsValue(value) {
			return partition
		}
	}
	return nil
}

// GetPartitionsForRange returns partitions that might contain values in a range
func (pt *PartitionedTable) GetPartitionsForRange(lower, upper interface{}) []*Partition {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	var result []*Partition

	for _, partition := range pt.Partitions {
		// For range partitioning, check if ranges overlap
		if pt.Type == PartitionTypeRange {
			// Partition overlaps if: partition.Lower < upper && partition.Upper > lower
			lowerCmp := compareValues(partition.UpperBound, lower)
			upperCmp := compareValues(partition.LowerBound, upper)

			// If UpperBound is nil (unbounded), it overlaps if lower is not below partition
			if partition.UpperBound == nil {
				if upperCmp < 0 {
					result = append(result, partition)
				}
			} else if lowerCmp > 0 && upperCmp < 0 {
				result = append(result, partition)
			}
		} else {
			// For list and hash, we need to check all partitions
			result = append(result, partition)
		}
	}

	return result
}

// AddPartition adds a new partition (for range partitioning)
func (pt *PartitionedTable) AddPartition(name string, lowerBound, upperBound interface{}) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.Type != PartitionTypeRange {
		return fmt.Errorf("can only add partitions to RANGE partitioned tables")
	}

	id := uint32(len(pt.Partitions) + 1)
	partition := &Partition{
		ID:         id,
		Name:       name,
		TableName:  pt.TableName,
		Type:       PartitionTypeRange,
		ColumnName: pt.ColumnName,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		CreatedAt:  time.Now(),
	}

	pt.Partitions = append(pt.Partitions, partition)
	return nil
}

// DropPartition removes a partition
func (pt *PartitionedTable) DropPartition(partitionName string) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	for i, p := range pt.Partitions {
		if p.Name == partitionName {
			// Remove partition
			pt.Partitions = append(pt.Partitions[:i], pt.Partitions[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("partition %s not found", partitionName)
}

// GetAllPartitions returns all partitions
func (pt *PartitionedTable) GetAllPartitions() []*Partition {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	result := make([]*Partition, len(pt.Partitions))
	copy(result, pt.Partitions)
	return result
}

// RangePartitionDef defines a range partition
type RangePartitionDef struct {
	Name       string
	LowerBound interface{}
	UpperBound interface{}
}

// ListPartitionDef defines a list partition
type ListPartitionDef struct {
	Name   string
	Values []interface{}
}

// PartitionStats contains statistics for a partition
type PartitionStats struct {
	TotalPartitions int
	TotalRows       uint64
	TotalSize       uint64
	Partitions      []SinglePartitionStats
}

// SinglePartitionStats contains stats for a single partition
type SinglePartitionStats struct {
	Name      string
	RowCount  uint64
	SizeBytes uint64
	CreatedAt time.Time
}

// GetStats returns partition statistics
func (pt *PartitionedTable) GetStats() PartitionStats {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	stats := PartitionStats{
		TotalPartitions: len(pt.Partitions),
		Partitions:      make([]SinglePartitionStats, 0, len(pt.Partitions)),
	}

	for _, p := range pt.Partitions {
		stats.TotalRows += p.RowCount
		stats.TotalSize += p.SizeBytes
		stats.Partitions = append(stats.Partitions, SinglePartitionStats{
			Name:      p.Name,
			RowCount:  p.RowCount,
			SizeBytes: p.SizeBytes,
			CreatedAt: p.CreatedAt,
		})
	}

	return stats
}

// AutoCreateTimePartition creates a new time-based partition for time-series data
func (pt *PartitionedTable) AutoCreateTimePartition(now time.Time) (*Partition, error) {
	if !pt.AutoPartition || pt.Type != PartitionTypeRange {
		return nil, fmt.Errorf("auto-partitioning only supported for RANGE partitioned tables")
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	// Check if we need a new partition
	if len(pt.Partitions) == 0 {
		return nil, fmt.Errorf("no existing partitions to determine interval")
	}

	lastPartition := pt.Partitions[len(pt.Partitions)-1]

	// Check if the last partition covers the current time
	if lastPartition.UpperBound != nil {
		if upperTime, ok := lastPartition.UpperBound.(time.Time); ok {
			if now.Before(upperTime) {
				return nil, nil // No new partition needed
			}
		}
	}

	// Create a new partition
	id := uint32(len(pt.Partitions) + 1)
	name := fmt.Sprintf("p_%s", now.Format("20060102"))

	var lowerBound, upperBound time.Time
	if lastPartition.UpperBound != nil {
		if t, ok := lastPartition.UpperBound.(time.Time); ok {
			lowerBound = t
		}
	}

	// Determine interval
	interval := pt.AutoPartitionInterval
	if interval == 0 {
		interval = 24 * time.Hour // Default to daily
	}

	upperBound = lowerBound.Add(interval)

	partition := &Partition{
		ID:         id,
		Name:       name,
		TableName:  pt.TableName,
		Type:       PartitionTypeRange,
		ColumnName: pt.ColumnName,
		LowerBound: lowerBound,
		UpperBound: upperBound,
		CreatedAt:  now,
	}

	pt.Partitions = append(pt.Partitions, partition)

	// Prune old partitions if needed
	if pt.MaxPartitions > 0 && len(pt.Partitions) > pt.MaxPartitions {
		pt.Partitions = pt.Partitions[len(pt.Partitions)-pt.MaxPartitions:]
	}

	return partition, nil
}

// hashValue computes a hash for partitioning
func hashValue(v interface{}) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", v)))
	return h.Sum32()
}

// IsPartitionedTable returns true if the table is partitioned
func (pm *PartitionManager) IsPartitionedTable(tableName string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	_, exists := pm.partitionedTables[tableName]
	return exists
}

// GetPartitionType returns the partition type for a table
func (pm *PartitionManager) GetPartitionType(tableName string) PartitionType {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	if pt, exists := pm.partitionedTables[tableName]; exists {
		return pt.Type
	}
	return PartitionTypeNone
}

// PrunePartitions removes partitions that match a condition
func (pt *PartitionedTable) PrunePartitions(condition func(*Partition) bool) int {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	var newPartitions []*Partition
	pruned := 0

	for _, p := range pt.Partitions {
		if !condition(p) {
			newPartitions = append(newPartitions, p)
		} else {
			pruned++
		}
	}

	pt.Partitions = newPartitions
	return pruned
}

// PartitionPruner handles partition pruning during query optimization
type PartitionPruner struct {
	pm *PartitionManager
}

// NewPartitionPruner creates a new partition pruner
func NewPartitionPruner(pm *PartitionManager) *PartitionPruner {
	return &PartitionPruner{pm: pm}
}

// PrunePartitionsForQuery returns the partitions that need to be scanned for a query
// This is a simplified version - full implementation would parse WHERE clauses
func (pp *PartitionPruner) PrunePartitionsForQuery(tableName string, columnValue interface{}) ([]*Partition, error) {
	pt, exists := pp.pm.GetPartitionedTable(tableName)
	if !exists {
		return nil, fmt.Errorf("table %s is not partitioned", tableName)
	}

	if columnValue != nil {
		// Direct partition lookup
		partition := pt.GetPartitionForValue(columnValue)
		if partition != nil {
			return []*Partition{partition}, nil
		}
		return []*Partition{}, nil
	}

	// No pruning possible - return all partitions
	return pt.GetAllPartitions(), nil
}

// MergePartitions combines multiple partitions into one (for range partitioning)
func (pt *PartitionedTable) MergePartitions(partitionNames []string, newName string) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.Type != PartitionTypeRange {
		return fmt.Errorf("partition merging only supported for RANGE partitioned tables")
	}

	if len(partitionNames) < 2 {
		return fmt.Errorf("at least 2 partitions required for merging")
	}

	// Find partitions to merge
	var partitionsToMerge []*Partition
	var minLower, maxUpper interface{}

	for _, name := range partitionNames {
		for _, p := range pt.Partitions {
			if p.Name == name {
				partitionsToMerge = append(partitionsToMerge, p)

				// Track bounds
				if minLower == nil || compareValues(p.LowerBound, minLower) < 0 {
					minLower = p.LowerBound
				}
				if maxUpper == nil || (p.UpperBound != nil && compareValues(p.UpperBound, maxUpper) > 0) {
					maxUpper = p.UpperBound
				}
				break
			}
		}
	}

	if len(partitionsToMerge) != len(partitionNames) {
		return fmt.Errorf("not all partitions found")
	}

	// Create merged partition
	merged := &Partition{
		ID:         uint32(len(pt.Partitions) + 1),
		Name:       newName,
		TableName:  pt.TableName,
		Type:       PartitionTypeRange,
		ColumnName: pt.ColumnName,
		LowerBound: minLower,
		UpperBound: maxUpper,
		CreatedAt:  time.Now(),
	}

	// Remove old partitions and add merged one
	nameSet := make(map[string]bool)
	for _, name := range partitionNames {
		nameSet[name] = true
	}

	var newPartitions []*Partition
	for _, p := range pt.Partitions {
		if !nameSet[p.Name] {
			newPartitions = append(newPartitions, p)
		}
	}
	newPartitions = append(newPartitions, merged)

	pt.Partitions = newPartitions
	return nil
}

// SplitPartition splits a partition into two (for range partitioning)
func (pt *PartitionedTable) SplitPartition(partitionName string, splitPoint interface{}, newName1, newName2 string) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.Type != PartitionTypeRange {
		return fmt.Errorf("partition splitting only supported for RANGE partitioned tables")
	}

	// Find partition to split
	var partitionToSplit *Partition
	var partitionIndex int

	for i, p := range pt.Partitions {
		if p.Name == partitionName {
			partitionToSplit = p
			partitionIndex = i
			break
		}
	}

	if partitionToSplit == nil {
		return fmt.Errorf("partition %s not found", partitionName)
	}

	// Validate split point is within partition bounds
	if !partitionToSplit.ContainsValue(splitPoint) {
		return fmt.Errorf("split point is not within partition bounds")
	}

	// Create two new partitions
	p1 := &Partition{
		ID:         uint32(len(pt.Partitions) + 1),
		Name:       newName1,
		TableName:  pt.TableName,
		Type:       PartitionTypeRange,
		ColumnName: pt.ColumnName,
		LowerBound: partitionToSplit.LowerBound,
		UpperBound: splitPoint,
		CreatedAt:  time.Now(),
	}

	p2 := &Partition{
		ID:         uint32(len(pt.Partitions) + 2),
		Name:       newName2,
		TableName:  pt.TableName,
		Type:       PartitionTypeRange,
		ColumnName: pt.ColumnName,
		LowerBound: splitPoint,
		UpperBound: partitionToSplit.UpperBound,
		CreatedAt:  time.Now(),
	}

	// Replace old partition with new ones
	newPartitions := make([]*Partition, 0, len(pt.Partitions)+1)
	newPartitions = append(newPartitions, pt.Partitions[:partitionIndex]...)
	newPartitions = append(newPartitions, p1, p2)
	newPartitions = append(newPartitions, pt.Partitions[partitionIndex+1:]...)

	pt.Partitions = newPartitions
	return nil
}

// RebalanceHashPartitions redistributes data across hash partitions
func (pt *PartitionedTable) RebalanceHashPartitions(newNumPartitions int) error {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.Type != PartitionTypeHash {
		return fmt.Errorf("partition rebalancing only supported for HASH partitioned tables")
	}

	if newNumPartitions <= 0 {
		return fmt.Errorf("number of partitions must be positive")
	}

	// Create new partitions
	newPartitions := make([]*Partition, 0, newNumPartitions)
	for i := 0; i < newNumPartitions; i++ {
		partition := &Partition{
			ID:            uint32(i + 1),
			Name:          fmt.Sprintf("p%d", i),
			TableName:     pt.TableName,
			Type:          PartitionTypeHash,
			ColumnName:    pt.ColumnName,
			HashModulus:   newNumPartitions,
			HashRemainder: i,
			CreatedAt:     time.Now(),
		}
		newPartitions = append(newPartitions, partition)
	}

	pt.Partitions = newPartitions
	return nil
}
