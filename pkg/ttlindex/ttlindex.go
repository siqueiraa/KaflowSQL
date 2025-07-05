package ttlindex

import (
	"slices"
	"sort"
	"sync"
	"time"
)

// Item represents an index entry, associating a key (e.g., joinKey) with the timestamp of the last update.
// timestamp da última atualização (LastUpdated).
type Item struct {
	Key string    // Associated key (e.g., joinKey)
	TS  time.Time // Timestamp of the last update
}

// Index manages items ordered by timestamp. Internally, it maintains a slice sorted by TS.
// Internamente, ele mantém um slice ordenado dos itens.
type Index struct {
	mu    sync.RWMutex
	items []Item // Keeps items sorted by the TS field (from oldest to newest)
}

// New creates a new empty index.
func New() *Index {
	return &Index{
		items: make([]Item, 0),
	}
}

// Add inserts a new item or updates the timestamp of an existing item.
// After insert or update, the slice is re-sorted to maintain timestamp order.
func (i *Index) Add(key string, ts time.Time) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Checks if the key already exists; if so, updates the timestamp.
	for idx, item := range i.items {
		if item.Key == key {
			i.items[idx].TS = ts
			i.sort() // Reorders to maintain correct order.
			return
		}
	}
	// If it doesn't exist, add the new item.
	i.items = append(i.items, Item{Key: key, TS: ts})
	i.sort()
}

// sort sorts the items slice by timestamp in ascending order (oldest first).
func (i *Index) sort() {
	sort.Slice(i.items, func(a, b int) bool {
		return i.items[a].TS.Before(i.items[b].TS)
	})
}

// GetExpired returns a slice with the keys of items whose timestamp is before or equal to 'expiration'.
// This function uses sort.Search to find the first index whose item is not expired, then removes and returns expired keys.
// retorna as chaves dos itens expirados, removendo-os do índice.
func (i *Index) GetExpired(expiration time.Time) []string {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Uses sort.Search to find the lowest index where the item's timestamp is AFTER expiration.
	// Meaning items with indices [0, idx) are expired.
	idx := sort.Search(len(i.items), func(j int) bool {
		return i.items[j].TS.After(expiration)
	})

	// Creates a slice to store expired keys.
	expired := make([]string, idx)
	for j := 0; j < idx; j++ {
		expired[j] = i.items[j].Key
	}

	// Removes expired items from the slice, keeping only non-expired ones.
	i.items = i.items[idx:]

	return expired
}

// Remove deletes a specific item from the index based on the given key.
func (i *Index) Remove(key string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	for idx, item := range i.items {
		if item.Key == key {
			// Removes the item using slicing.
			i.items = slices.Delete(i.items, idx, idx+1)
			break
		}
	}
}
