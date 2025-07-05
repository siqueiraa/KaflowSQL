package ttlindex

import (
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	index := New()

	if index == nil {
		t.Errorf("Expected non-nil Index")
		return
	}

	if index.items == nil {
		t.Errorf("Expected initialized items slice")
		return
	}

	if len(index.items) != 0 {
		t.Errorf("Expected empty items slice, got %d items", len(index.items))
	}
}

func TestIndexAdd(t *testing.T) {
	index := New()
	now := time.Now()

	// Test adding new items
	index.Add("key1", now)
	if len(index.items) != 1 {
		t.Errorf("Expected 1 item after first add, got %d", len(index.items))
	}

	if index.items[0].Key != "key1" {
		t.Errorf("Expected key 'key1', got '%s'", index.items[0].Key)
	}

	if !index.items[0].TS.Equal(now) {
		t.Errorf("Expected timestamp %v, got %v", now, index.items[0].TS)
	}

	// Test adding another item with later timestamp
	later := now.Add(time.Hour)
	index.Add("key2", later)
	if len(index.items) != 2 {
		t.Errorf("Expected 2 items after second add, got %d", len(index.items))
	}

	// Verify order (oldest first)
	if !index.items[0].TS.Before(index.items[1].TS) {
		t.Errorf("Items not sorted correctly: %v should be before %v", index.items[0].TS, index.items[1].TS)
	}

	// Test updating existing item
	updated := now.Add(2 * time.Hour)
	index.Add("key1", updated)
	if len(index.items) != 2 {
		t.Errorf("Expected 2 items after update (no new item), got %d", len(index.items))
	}

	// Find key1 and verify its timestamp was updated
	var found bool
	for _, item := range index.items {
		if item.Key == "key1" {
			if !item.TS.Equal(updated) {
				t.Errorf("Expected key1 timestamp to be updated to %v, got %v", updated, item.TS)
			}
			found = true
			break
		}
	}
	if !found {
		t.Errorf("key1 not found after update")
	}
}

func TestIndexSorting(t *testing.T) {
	index := New()
	now := time.Now()

	// Add items in reverse chronological order
	index.Add("newest", now.Add(3*time.Hour))
	index.Add("oldest", now)
	index.Add("middle", now.Add(time.Hour))

	if len(index.items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(index.items))
	}

	// Verify they are sorted oldest to newest
	if index.items[0].Key != "oldest" {
		t.Errorf("Expected first item to be 'oldest', got '%s'", index.items[0].Key)
	}

	if index.items[1].Key != "middle" {
		t.Errorf("Expected second item to be 'middle', got '%s'", index.items[1].Key)
	}

	if index.items[2].Key != "newest" {
		t.Errorf("Expected third item to be 'newest', got '%s'", index.items[2].Key)
	}

	// Verify timestamps are in ascending order
	for i := 1; i < len(index.items); i++ {
		if !index.items[i-1].TS.Before(index.items[i].TS) {
			t.Errorf("Items at indices %d and %d are not in correct order", i-1, i)
		}
	}
}

func TestIndexGetExpired(t *testing.T) {
	index := New()
	now := time.Now()

	// Add items with different timestamps
	index.Add("expired1", now.Add(-2*time.Hour)) // 2 hours ago
	index.Add("expired2", now.Add(-time.Hour))   // 1 hour ago
	index.Add("current", now)                    // now
	index.Add("future", now.Add(time.Hour))      // 1 hour from now

	// Get expired items (older than 30 minutes ago)
	expiration := now.Add(-30 * time.Minute)
	expired := index.GetExpired(expiration)

	if len(expired) != 2 {
		t.Errorf("Expected 2 expired items, got %d", len(expired))
	}

	// Verify expired keys (order should match insertion order of expired items)
	expectedExpired := []string{"expired1", "expired2"}
	for i, key := range expired {
		if key != expectedExpired[i] {
			t.Errorf("Expected expired key %s at index %d, got %s", expectedExpired[i], i, key)
		}
	}

	// Verify remaining items in index
	if len(index.items) != 2 {
		t.Errorf("Expected 2 remaining items in index, got %d", len(index.items))
	}

	remainingKeys := make([]string, len(index.items))
	for i, item := range index.items {
		remainingKeys[i] = item.Key
	}

	expectedRemaining := []string{"current", "future"}
	for i, key := range expectedRemaining {
		if remainingKeys[i] != key {
			t.Errorf("Expected remaining key %s at index %d, got %s", key, i, remainingKeys[i])
		}
	}
}

func TestIndexGetExpiredEdgeCases(t *testing.T) {
	index := New()
	now := time.Now()

	// Test with empty index
	expired := index.GetExpired(now)
	if len(expired) != 0 {
		t.Errorf("Expected 0 expired items from empty index, got %d", len(expired))
	}

	// Add some items
	index.Add("key1", now.Add(-time.Hour))
	index.Add("key2", now)
	index.Add("key3", now.Add(time.Hour))

	// Test when no items are expired
	future := now.Add(2 * time.Hour)
	expired = index.GetExpired(now.Add(-2 * time.Hour))
	if len(expired) != 0 {
		t.Errorf("Expected 0 expired items when expiration is very old, got %d", len(expired))
	}

	// Test when all items are expired
	expired = index.GetExpired(future)
	if len(expired) != 3 {
		t.Errorf("Expected 3 expired items when expiration is in future, got %d", len(expired))
	}

	if len(index.items) != 0 {
		t.Errorf("Expected 0 remaining items after all expired, got %d", len(index.items))
	}
}

func TestIndexRemove(t *testing.T) {
	index := New()
	now := time.Now()

	// Add some items
	index.Add("key1", now)
	index.Add("key2", now.Add(time.Hour))
	index.Add("key3", now.Add(2*time.Hour))

	if len(index.items) != 3 {
		t.Errorf("Expected 3 items before removal, got %d", len(index.items))
	}

	// Remove middle item
	index.Remove("key2")

	if len(index.items) != 2 {
		t.Errorf("Expected 2 items after removal, got %d", len(index.items))
	}

	// Verify correct items remain
	remainingKeys := make([]string, len(index.items))
	for i, item := range index.items {
		remainingKeys[i] = item.Key
	}

	expectedKeys := []string{"key1", "key3"}
	for i, expectedKey := range expectedKeys {
		if remainingKeys[i] != expectedKey {
			t.Errorf("Expected remaining key %s at index %d, got %s", expectedKey, i, remainingKeys[i])
		}
	}

	// Test removing non-existent key
	initialLen := len(index.items)
	index.Remove("nonexistent")
	if len(index.items) != initialLen {
		t.Errorf("Expected length to remain %d after removing non-existent key, got %d", initialLen, len(index.items))
	}

	// Test removing all items
	index.Remove("key1")
	index.Remove("key3")
	if len(index.items) != 0 {
		t.Errorf("Expected 0 items after removing all, got %d", len(index.items))
	}
}

func TestItem(t *testing.T) {
	now := time.Now()
	item := Item{
		Key: "test_key",
		TS:  now,
	}

	if item.Key != "test_key" {
		t.Errorf("Expected key 'test_key', got '%s'", item.Key)
	}

	if !item.TS.Equal(now) {
		t.Errorf("Expected timestamp %v, got %v", now, item.TS)
	}
}

func TestIndexConcurrency(t *testing.T) {
	index := New()
	now := time.Now()

	// Test concurrent access
	done := make(chan bool, 3)

	// Writer goroutine 1
	go func() {
		for i := 0; i < 50; i++ {
			index.Add("writer1", now.Add(time.Duration(i)*time.Millisecond))
		}
		done <- true
	}()

	// Writer goroutine 2
	go func() {
		for i := 0; i < 50; i++ {
			index.Add("writer2", now.Add(time.Duration(i)*time.Millisecond))
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 50; i++ {
			index.GetExpired(now.Add(-time.Hour))
		}
		done <- true
	}()

	// Wait for all goroutines
	<-done
	<-done
	<-done

	// Verify final state (should have 2 items)
	if len(index.items) != 2 {
		t.Errorf("Expected 2 items after concurrent access, got %d", len(index.items))
	}
}

func TestIndexTimestampPrecision(t *testing.T) {
	index := New()
	base := time.Now()

	// Add items with very close timestamps
	index.Add("key1", base)
	index.Add("key2", base.Add(time.Nanosecond))
	index.Add("key3", base.Add(2*time.Nanosecond))

	// Verify they are sorted correctly even with nanosecond precision
	if len(index.items) != 3 {
		t.Errorf("Expected 3 items, got %d", len(index.items))
	}

	for i := 1; i < len(index.items); i++ {
		if !index.items[i-1].TS.Before(index.items[i].TS) {
			t.Errorf("Items with nanosecond precision not sorted correctly at indices %d and %d", i-1, i)
		}
	}
}
