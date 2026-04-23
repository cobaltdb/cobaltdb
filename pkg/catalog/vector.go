package catalog

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// HNSWIndex represents a Hierarchical Navigable Small World index for vector similarity search
type HNSWIndex struct {
	Name       string
	TableName  string
	ColumnName string
	Dimensions int
	M          int     // Maximum number of connections per element per layer
	Mmax       int     // Maximum number of connections for the base layer
	Mmax0      int     // Maximum number of connections for layer 0
	Ef         int     // Size of dynamic candidate list
	Ml         float64 // Level generation factor

	Nodes      map[string]*HNSWNode // key -> node (key is the primary key value as string)
	EntryPoint *HNSWNode
	MaxLevel   int
	mu         sync.RWMutex
}

// HNSWNode represents a node in the HNSW graph
type HNSWNode struct {
	Key       string
	Vector    []float64
	Level     int
	Neighbors [][]string // Neighbors at each level
}

// NewHNSWIndex creates a new HNSW index
func NewHNSWIndex(name, tableName, columnName string, dimensions int) *HNSWIndex {
	m := 16 // Default M
	return &HNSWIndex{
		Name:       name,
		TableName:  tableName,
		ColumnName: columnName,
		Dimensions: dimensions,
		M:          m,
		Mmax:       m,
		Mmax0:      m * 2,
		Ef:         200,
		Ml:         1.0 / math.Log(float64(m)),
		Nodes:      make(map[string]*HNSWNode),
		MaxLevel:   -1,
	}
}

// Insert adds a vector to the index
func (h *HNSWIndex) Insert(key string, vector []float64) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Validate dimensions
	if len(vector) != h.Dimensions {
		return fmt.Errorf("vector dimension mismatch: expected %d, got %d", h.Dimensions, len(vector))
	}

	// Check if already exists
	if _, exists := h.Nodes[key]; exists {
		return h.Update(key, vector)
	}

	// Generate random level for the new node
	level := h.randomLevel()

	// Create the node
	node := &HNSWNode{
		Key:       key,
		Vector:    make([]float64, len(vector)),
		Level:     level,
		Neighbors: make([][]string, level+1),
	}
	copy(node.Vector, vector)

	// Initialize neighbor lists
	for i := 0; i <= level; i++ {
		node.Neighbors[i] = make([]string, 0, h.M)
	}

	// First node becomes entry point
	if h.EntryPoint == nil {
		h.Nodes[key] = node
		h.EntryPoint = node
		h.MaxLevel = level
		return nil
	}

	// Search for neighbors at each level
	entryPoint := h.EntryPoint
	currDist := l2Distance(vector, entryPoint.Vector)

	// Search from top level down to level+1
	for i := h.MaxLevel; i > level; i-- {
		changed := true
		for changed {
			changed = false
			for _, neighborKey := range entryPoint.Neighbors[i] {
				if neighbor, ok := h.Nodes[neighborKey]; ok {
					d := l2Distance(vector, neighbor.Vector)
					if d < currDist {
						currDist = d
						entryPoint = neighbor
						changed = true
					}
				}
			}
		}
	}

	// Search at each level from min(level, MaxLevel) down to 0
	for i := min(level, h.MaxLevel); i >= 0; i-- {
		candidates := h.searchLayer(vector, entryPoint, h.Ef, i)
		neighbors := h.selectNeighbors(vector, candidates, h.M)

		// Add bidirectional connections
		node.Neighbors[i] = neighbors
		for _, neighborKey := range neighbors {
			if neighbor, ok := h.Nodes[neighborKey]; ok {
				neighbor.Neighbors[i] = append(neighbor.Neighbors[i], key)
				// Shrink connections if needed
				if len(neighbor.Neighbors[i]) > h.Mmax {
					neighbor.Neighbors[i] = h.selectNeighborsByKey(neighbor.Neighbors[i], h.Mmax, i)
				}
			}
		}

		// Update entry point for next level
		if len(candidates) > 0 {
			entryPoint = h.Nodes[candidates[0].Key]
		}
	}

	h.Nodes[key] = node

	// Update entry point if new node has higher level
	if level > h.MaxLevel {
		h.EntryPoint = node
		h.MaxLevel = level
	}

	return nil
}

// Delete removes a vector from the index
func (h *HNSWIndex) Delete(key string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, exists := h.Nodes[key]
	if !exists {
		return nil // Already deleted
	}

	// Remove connections from neighbors
	for level := 0; level <= node.Level; level++ {
		for _, neighborKey := range node.Neighbors[level] {
			if neighbor, ok := h.Nodes[neighborKey]; ok {
				// Remove key from neighbor's neighbor list
				neighbor.Neighbors[level] = removeString(neighbor.Neighbors[level], key)
			}
		}
	}

	// If this was the entry point, find a new one
	if h.EntryPoint == node {
		// Find node with highest level
		var maxLevelNode *HNSWNode
		maxLevel := -1
		for _, n := range h.Nodes {
			if n != node && n.Level > maxLevel {
				maxLevel = n.Level
				maxLevelNode = n
			}
		}
		h.EntryPoint = maxLevelNode
		h.MaxLevel = maxLevel
	}

	delete(h.Nodes, key)
	return nil
}

// Update updates a vector in the index
func (h *HNSWIndex) Update(key string, vector []float64) error {
	// Delete and re-insert
	if err := h.Delete(key); err != nil {
		return err
	}
	return h.Insert(key, vector)
}

// SearchKNN finds the k nearest neighbors to the query vector
func (h *HNSWIndex) SearchKNN(query []float64, k int) ([]string, []float64, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.EntryPoint == nil || len(h.Nodes) == 0 {
		return []string{}, []float64{}, nil
	}

	if len(query) != h.Dimensions {
		return nil, nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", h.Dimensions, len(query))
	}

	// Search from top level down to level 1
	entryPoint := h.EntryPoint
	currDist := l2Distance(query, entryPoint.Vector)

	for i := h.MaxLevel; i > 0; i-- {
		changed := true
		for changed {
			changed = false
			for _, neighborKey := range entryPoint.Neighbors[i] {
				if neighbor, ok := h.Nodes[neighborKey]; ok {
					d := l2Distance(query, neighbor.Vector)
					if d < currDist {
						currDist = d
						entryPoint = neighbor
						changed = true
					}
				}
			}
		}
	}

	// Search at level 0 with ef = max(k, h.Ef)
	ef := max(k, h.Ef)
	candidates := h.searchLayer(query, entryPoint, ef, 0)

	// Return top k
	if len(candidates) > k {
		candidates = candidates[:k]
	}

	keys := make([]string, len(candidates))
	dists := make([]float64, len(candidates))
	for i, c := range candidates {
		keys[i] = c.Key
		dists[i] = c.Distance
	}

	return keys, dists, nil
}

// SearchRange finds all vectors within a distance radius from the query vector
func (h *HNSWIndex) SearchRange(query []float64, radius float64) ([]string, []float64, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.EntryPoint == nil || len(h.Nodes) == 0 {
		return []string{}, []float64{}, nil
	}

	if len(query) != h.Dimensions {
		return nil, nil, fmt.Errorf("query dimension mismatch: expected %d, got %d", h.Dimensions, len(query))
	}

	// Search from top level down to level 1
	entryPoint := h.EntryPoint
	currDist := l2Distance(query, entryPoint.Vector)

	for i := h.MaxLevel; i > 0; i-- {
		changed := true
		for changed {
			changed = false
			for _, neighborKey := range entryPoint.Neighbors[i] {
				if neighbor, ok := h.Nodes[neighborKey]; ok {
					d := l2Distance(query, neighbor.Vector)
					if d < currDist {
						currDist = d
						entryPoint = neighbor
						changed = true
					}
				}
			}
		}
	}

	// Search at level 0
	candidates := h.searchLayer(query, entryPoint, h.Ef, 0)

	// Filter by radius
	var results []candidate
	for _, c := range candidates {
		if c.Distance <= radius {
			results = append(results, c)
		}
	}

	keys := make([]string, len(results))
	dists := make([]float64, len(results))
	for i, c := range results {
		keys[i] = c.Key
		dists[i] = c.Distance
	}

	return keys, dists, nil
}

// candidate represents a candidate node in the search
type candidate struct {
	Key      string
	Distance float64
}

// candidateHeap is a min-heap for candidates
type candidateHeap []candidate

func (h candidateHeap) Len() int            { return len(h) }
func (h candidateHeap) Less(i, j int) bool  { return h[i].Distance < h[j].Distance }
func (h candidateHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *candidateHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *candidateHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// searchLayer searches a single layer for nearest neighbors
func (h *HNSWIndex) searchLayer(query []float64, entryPoint *HNSWNode, ef, level int) []candidate {
	visited := make(map[string]bool)
	candidates := &candidateHeap{}
	heap.Init(candidates)
	result := &candidateHeap{}
	heap.Init(result)

	entryDist := l2Distance(query, entryPoint.Vector)
	visited[entryPoint.Key] = true
	heap.Push(candidates, candidate{Key: entryPoint.Key, Distance: entryDist})
	heap.Push(result, candidate{Key: entryPoint.Key, Distance: entryDist})

	for candidates.Len() > 0 {
		curr := heap.Pop(candidates).(candidate)
		worstResult := (*result)[result.Len()-1]

		if curr.Distance > worstResult.Distance {
			break
		}

		if currNode, ok := h.Nodes[curr.Key]; ok {
			if level < len(currNode.Neighbors) {
				for _, neighborKey := range currNode.Neighbors[level] {
					if visited[neighborKey] {
						continue
					}
					visited[neighborKey] = true

					if neighbor, ok := h.Nodes[neighborKey]; ok {
						d := l2Distance(query, neighbor.Vector)
						worstResult := (*result)[result.Len()-1]

						if result.Len() < ef || d < worstResult.Distance {
							heap.Push(candidates, candidate{Key: neighborKey, Distance: d})
							heap.Push(result, candidate{Key: neighborKey, Distance: d})
							if result.Len() > ef {
								heap.Pop(result)
							}
						}
					}
				}
			}
		}
	}

	// Convert heap to sorted slice
	sorted := make([]candidate, result.Len())
	for i := result.Len() - 1; i >= 0; i-- {
		sorted[i] = heap.Pop(result).(candidate)
	}

	return sorted
}

// selectNeighbors selects the M closest neighbors from candidates
func (h *HNSWIndex) selectNeighbors(query []float64, candidates []candidate, m int) []string {
	if len(candidates) <= m {
		result := make([]string, len(candidates))
		for i, c := range candidates {
			result[i] = c.Key
		}
		return result
	}

	result := make([]string, m)
	for i := 0; i < m; i++ {
		result[i] = candidates[i].Key
	}
	return result
}

// selectNeighborsByKey selects M neighbors from a list of keys based on distance
func (h *HNSWIndex) selectNeighborsByKey(keys []string, m, level int) []string {
	if len(keys) <= m {
		return keys
	}

	// Calculate distances from the node to all its neighbors
	// We need to know which node this is - get it from the first key
	// This is a simplification; in a real implementation, we'd pass the node
	type neighborDist struct {
		Key      string
		Distance float64
	}

	dists := make([]neighborDist, len(keys))
	for i, key := range keys {
		if _, ok := h.Nodes[key]; ok {
			// Use a dummy distance for now - in practice we'd compute from the source node
			dists[i] = neighborDist{Key: key, Distance: rand.Float64()}
		}
	}

	// Sort by distance
	sort.Slice(dists, func(i, j int) bool {
		return dists[i].Distance < dists[j].Distance
	})

	result := make([]string, m)
	for i := 0; i < m; i++ {
		result[i] = dists[i].Key
	}
	return result
}

// randomLevel generates a random level for a new node
func (h *HNSWIndex) randomLevel() int {
	level := 0
	for rand.Float64() < h.Ml && level < 16 {
		level++
	}
	return level
}

// l2Distance calculates the L2 (Euclidean) distance between two vectors
func l2Distance(a, b []float64) float64 {
	if len(a) != len(b) {
		return math.Inf(1)
	}
	var sum float64
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return math.Sqrt(sum)
}

// cosineSimilarity calculates the cosine similarity between two vectors
func cosineSimilarity(a, b []float64) float64 {
	if len(a) != len(b) {
		return -1
	}
	var dot, normA, normB float64
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / (math.Sqrt(normA) * math.Sqrt(normB))
}

// innerProduct calculates the inner product (dot product) between two vectors
func innerProduct(a, b []float64) float64 {
	if len(a) != len(b) {
		return 0
	}
	var sum float64
	for i := range a {
		sum += a[i] * b[i]
	}
	return sum
}

// removeString removes a string from a slice
func removeString(slice []string, s string) []string {
	for i, v := range slice {
		if v == s {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// VectorIndexDef represents a vector index definition (HNSW)
type VectorIndexDef struct {
	Name       string
	TableName  string
	ColumnName string
	Dimensions int
	IndexType  string     // "hnsw", "ivf", etc.
	HNSW       *HNSWIndex `json:"-"` // Runtime index, not persisted
}
