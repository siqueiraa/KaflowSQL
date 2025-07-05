package engine

import (
	"reflect"
	"testing"
	"time"

	"github.com/siqueiraa/KaflowSQL/pkg/config"
	"github.com/siqueiraa/KaflowSQL/pkg/duck"
	"github.com/siqueiraa/KaflowSQL/pkg/kafka"
	"github.com/siqueiraa/KaflowSQL/pkg/pipeline"
	"github.com/siqueiraa/KaflowSQL/pkg/schema"
	"github.com/siqueiraa/KaflowSQL/pkg/state"
	"github.com/siqueiraa/KaflowSQL/pkg/ttlindex"
)

func TestBuildPresenceMask(t *testing.T) {
	engine := &Engine{
		field2bit: map[string]uint64{
			"field1": 0,
			"field2": 1,
			"field3": 2,
		},
	}

	tests := []struct {
		name     string
		input    map[string]any
		expected uint64
	}{
		{
			name:     "empty map",
			input:    map[string]any{},
			expected: 0,
		},
		{
			name: "single field",
			input: map[string]any{
				"field1": "value1",
			},
			expected: 1, // bit 0 set
		},
		{
			name: "multiple fields",
			input: map[string]any{
				"field1": "value1",
				"field3": "value3",
			},
			expected: 5, // bits 0 and 2 set (1 + 4)
		},
		{
			name: "unknown field",
			input: map[string]any{
				"unknown": "value",
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.buildPresenceMask(tt.input)
			if result != tt.expected {
				t.Errorf("buildPresenceMask() = %d, want %d", result, tt.expected)
			}
		})
	}
}

func TestMeetsEmission(t *testing.T) {
	engine := &Engine{
		requireMask: 7, // bits 0, 1, 2 required (111 in binary)
	}

	tests := []struct {
		name     string
		mask     uint64
		expected bool
	}{
		{
			name:     "all required fields present",
			mask:     7, // 111 in binary
			expected: true,
		},
		{
			name:     "extra fields present",
			mask:     15, // 1111 in binary
			expected: true,
		},
		{
			name:     "missing one required field",
			mask:     3, // 011 in binary
			expected: false,
		},
		{
			name:     "no fields present",
			mask:     0,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := engine.meetsEmission(tt.mask)
			if result != tt.expected {
				t.Errorf("meetsEmission() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestHashJoinKey(t *testing.T) {
	data := map[string]any{
		"id":   123,
		"name": "test",
		"age":  25,
	}

	keys := []string{"id", "name"}

	// Test that same input produces same hash
	hash1 := hashJoinKey(data, keys)
	hash2 := hashJoinKey(data, keys)

	if hash1 != hash2 {
		t.Errorf("hashJoinKey should be deterministic, got %d and %d", hash1, hash2)
	}

	// Test that different key orders produce different hashes
	reverseKeys := []string{"name", "id"}
	hash3 := hashJoinKey(data, reverseKeys)

	if hash1 == hash3 {
		t.Errorf("hashJoinKey should be order-sensitive, got same hash %d", hash1)
	}
}

func BenchmarkBuildPresenceMask(b *testing.B) {
	engine := &Engine{
		field2bit: map[string]uint64{
			"field1": 0,
			"field2": 1,
			"field3": 2,
			"field4": 3,
			"field5": 4,
		},
	}

	data := map[string]any{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
		"other":  "ignored",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		engine.buildPresenceMask(data)
	}
}

func BenchmarkHashJoinKey(b *testing.B) {
	data := map[string]any{
		"user_id":    12345,
		"session_id": "abc123",
		"timestamp":  time.Now().Unix(),
	}
	keys := []string{"user_id", "session_id"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hashJoinKey(data, keys)
	}
}

// Test Row structure and operations
func TestRow(t *testing.T) {
	tests := []struct {
		name              string
		row               Row
		expectedTable     uint8
		expectedTombstone bool
	}{
		{
			name: "basic row",
			row: Row{
				Table:        1,
				Data:         map[string]any{"user_id": 123, "name": "test"},
				DedupeKey:    "test-key",
				PresenceMask: 3,
				Tombstone:    false,
			},
			expectedTable:     1,
			expectedTombstone: false,
		},
		{
			name: "tombstone row",
			row: Row{
				Table:        0,
				Data:         map[string]any{},
				DedupeKey:    "tombstone-key",
				PresenceMask: 0,
				Tombstone:    true,
			},
			expectedTable:     0,
			expectedTombstone: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.row.Table != tt.expectedTable {
				t.Errorf("Table mismatch: got %d, want %d", tt.row.Table, tt.expectedTable)
			}
			if tt.row.Tombstone != tt.expectedTombstone {
				t.Errorf("Tombstone mismatch: got %v, want %v", tt.row.Tombstone, tt.expectedTombstone)
			}
		})
	}
}

// Test EdgeEntry concurrent operations
func TestEdgeEntry(t *testing.T) {
	entry := &EdgeEntry{}

	// Test concurrent reads and writes
	done := make(chan bool)

	// Writer goroutine
	go func() {
		entry.mu.Lock()
		entry.leftIDs = append(entry.leftIDs, 1, 2, 3)
		entry.rightIDs = append(entry.rightIDs, 4, 5, 6)
		entry.mu.Unlock()
		done <- true
	}()

	// Reader goroutine
	go func() {
		time.Sleep(10 * time.Millisecond) // Give writer time to start
		entry.mu.RLock()
		_ = len(entry.leftIDs)
		_ = len(entry.rightIDs)
		entry.mu.RUnlock()
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	if len(entry.leftIDs) != 3 || len(entry.rightIDs) != 3 {
		t.Errorf("EdgeEntry concurrent operations failed")
	}
}

// Test shard distribution
func TestShardDistribution(t *testing.T) {
	engine := &Engine{}

	// Initialize edge shards
	for i := range engine.edges {
		engine.edges[i].table = make(map[joinKey]*EdgeEntry)
	}

	// Test that different join keys get distributed across shards
	// Use keys that actually map to different shards (last 8 bits determine shard)
	testKeys := []joinKey{0x00, 0x01, 0x02, 0x03, 0xFF}
	shardCounts := make(map[int]int)

	for _, key := range testKeys {
		shard := engine.shardOf(key)
		shardIndex := int(key & shardMask) //nolint:gosec // Test shard calculation is safe
		shardCounts[shardIndex]++

		if shard != &engine.edges[shardIndex] {
			t.Errorf("shardOf(%x) returned wrong shard", key)
		}
	}

	// Verify distribution - should have 5 unique shards
	if len(shardCounts) != len(testKeys) {
		t.Errorf("Shard distribution failed, expected %d unique shards, got %d", len(testKeys), len(shardCounts))
	}
}

// Test engine initialization
func TestEngineInitialization(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping engine integration test in short mode")
	}

	t.Run("ValidConfiguration", func(t *testing.T) {
		subTempDir := t.TempDir()
		testPipeline := createTestPipeline()

		engine, plan := createValidTestEngine(t, subTempDir, &testPipeline)
		defer close(engine.shutdown)

		verifyEngineStructure(t, engine, &testPipeline, plan)
		verifyEngineInitialization(t, engine)
	})

	t.Run("InvalidConfiguration", func(t *testing.T) {
		subTempDir := t.TempDir()

		engine := createInvalidConfigEngine(t, subTempDir)
		defer close(engine.shutdown)

		verifyInvalidConfigBehavior(t, engine)
	})

	t.Run("EmissionMaskValidation", func(t *testing.T) {
		subTempDir := t.TempDir()

		engine := createEmissionTestEngine(t, subTempDir)
		defer close(engine.shutdown)

		verifyEmissionMaskBehavior(t, engine)
	})
}

// Helper function to create a valid test engine
func createValidTestEngine(t *testing.T, tempDir string, testPipeline *pipeline.Pipeline) (*Engine, *pipeline.QueryPlan) {
	// Create query plan
	plan, err := pipeline.NewQueryPlan(testPipeline.Query)
	if err != nil {
		t.Fatalf("Failed to create query plan: %v", err)
	}

	// Create mock dependencies
	stateManager := createMockStateManager(t, tempDir)
	buffer := createMockBuffer(t)
	schemaManager := createMockSchemaManager(t)
	duckEngine := createMockDuckEngine(t, tempDir)
	producer := createMockProducer(t)

	// Test engine creation
	engine, err := NewEngine(
		*testPipeline,
		plan,
		stateManager,
		buffer,
		schemaManager,
		duckEngine,
		producer,
		[]string{"events", "users"},
		[]string{"users"}, // slow dimensions
		[]string{},        // fast dimensions
		nil,               // schema registry client
	)

	if err != nil {
		t.Fatalf("Engine initialization failed: %v", err)
	}

	return engine, plan
}

// Helper function to create an engine with invalid configuration
func createInvalidConfigEngine(t *testing.T, tempDir string) *Engine {
	// Test with invalid emission configuration (empty required fields)
	invalidPipeline := createTestPipeline()
	invalidPipeline.Emission = &pipeline.EmissionRules{
		Type:    "smart",
		Require: []string{}, // Empty required fields
	}

	plan, _ := pipeline.NewQueryPlan(invalidPipeline.Query)

	engine, err := NewEngine(
		invalidPipeline,
		plan,
		createMockStateManager(t, tempDir),
		createMockBuffer(t),
		createMockSchemaManager(t),
		createMockDuckEngine(t, tempDir),
		createMockProducer(t),
		[]string{"events"},
		[]string{},
		[]string{},
		nil,
	)

	if err != nil {
		t.Fatalf("Engine creation failed: %v", err)
	}

	return engine
}

// Helper function to create an engine for emission testing
func createEmissionTestEngine(t *testing.T, tempDir string) *Engine {
	testPipeline := createTestPipeline()
	plan, _ := pipeline.NewQueryPlan(testPipeline.Query)

	engine, err := NewEngine(
		testPipeline,
		plan,
		createMockStateManager(t, tempDir),
		createMockBuffer(t),
		createMockSchemaManager(t),
		createMockDuckEngine(t, tempDir),
		createMockProducer(t),
		[]string{"events", "users"},
		[]string{},
		[]string{},
		nil,
	)

	if err != nil {
		t.Fatalf("Engine creation failed: %v", err)
	}

	return engine
}

// Helper function to verify engine structure
func verifyEngineStructure(t *testing.T, engine *Engine, testPipeline *pipeline.Pipeline, plan *pipeline.QueryPlan) {
	if engine.pipeline.Name != testPipeline.Name {
		t.Errorf("Pipeline name mismatch: got %s, want %s", engine.pipeline.Name, testPipeline.Name)
	}

	if engine.plan != plan {
		t.Errorf("Query plan not properly set")
	}

	if len(engine.topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(engine.topics))
	}

	// Verify field mappings
	expectedFields := []string{"user_id", "name"}
	if len(engine.field2bit) != len(expectedFields) {
		t.Errorf("Field mapping incorrect: got %d fields, want %d", len(engine.field2bit), len(expectedFields))
	}
}

// Helper function to verify engine initialization
func verifyEngineInitialization(t *testing.T, engine *Engine) {
	// Verify TTL indices are initialized
	if engine.memoryTTLIndex == nil {
		t.Errorf("Memory TTL index not initialized")
	}
	if engine.persistentTTLIndex == nil {
		t.Errorf("Persistent TTL index not initialized")
	}

	// Verify edge shards are initialized
	for i := range engine.edges {
		if engine.edges[i].table == nil {
			t.Errorf("Edge shard %d not initialized", i)
		}
	}
}

// Helper function to verify invalid configuration behavior
func verifyInvalidConfigBehavior(t *testing.T, engine *Engine) {
	// Engine should still be created, but emission mask should be empty
	if engine.requireMask != 0 {
		t.Errorf("Expected empty require mask for invalid configuration, got %d", engine.requireMask)
	}
}

// Helper function to verify emission mask behavior
func verifyEmissionMaskBehavior(t *testing.T, engine *Engine) {
	// Test presence mask building
	testData := map[string]any{
		"user_id": 123,
		"name":    "test",
		"extra":   "ignored",
	}

	mask := engine.buildPresenceMask(testData)
	if !engine.meetsEmission(mask) {
		t.Errorf("Test data should meet emission requirements")
	}

	// Test incomplete data
	incompleteData := map[string]any{
		"user_id": 123,
		// missing "name"
	}

	incompleteMask := engine.buildPresenceMask(incompleteData)
	if engine.meetsEmission(incompleteMask) {
		t.Errorf("Incomplete data should not meet emission requirements")
	}
}

// Test processing incoming events - basic functionality
func TestProcessIncomingEventBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping engine event processing test in short mode")
	}

	tempDir := t.TempDir()

	// Create test engine
	testPipeline := createTestPipeline()
	plan, _ := pipeline.NewQueryPlan(testPipeline.Query)

	engine, err := NewEngine(
		testPipeline,
		plan,
		createMockStateManager(t, tempDir),
		createMockBuffer(t),
		createMockSchemaManager(t),
		createMockDuckEngine(t, tempDir),
		createMockProducer(t),
		[]string{"events", "users"},
		[]string{"users"}, // slow dimensions
		[]string{},        // fast dimensions
		nil,
	)
	if err != nil {
		t.Fatalf("Engine creation failed: %v", err)
	}
	defer close(engine.shutdown)

	// Test processing a user event
	userEvent := map[string]any{
		"user_id": 123,
		"name":    "John Doe",
		"email":   "john@example.com",
	}

	err = engine.ProcessIncomingEvent("users", userEvent, time.Now(), 0, 1)
	if err != nil {
		t.Errorf("Processing user event failed: %v", err)
	}

	// Verify row was stored
	engine.rowsMu.RLock()
	rowCount := len(engine.rows)
	engine.rowsMu.RUnlock()

	if rowCount != 1 {
		t.Errorf("Expected 1 row stored, got %d", rowCount)
	}

	// Test processing an event
	eventData := map[string]any{
		"user_id":    123,
		"event_type": "login",
		"timestamp":  time.Now().Unix(),
	}

	err = engine.ProcessIncomingEvent("events", eventData, time.Now(), 0, 2)
	if err != nil {
		t.Errorf("Processing event failed: %v", err)
	}

	// Verify second row was stored
	engine.rowsMu.RLock()
	finalRowCount := len(engine.rows)
	engine.rowsMu.RUnlock()

	if finalRowCount != 2 {
		t.Errorf("Expected 2 rows stored, got %d", finalRowCount)
	}
}

// Test processing incoming events - join functionality
func TestProcessIncomingEventJoins(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping engine event processing test in short mode")
	}
	tempDir := t.TempDir()

	// Create test engine
	testPipeline := createTestPipeline()
	plan, _ := pipeline.NewQueryPlan(testPipeline.Query)

	engine, err := NewEngine(
		testPipeline,
		plan,
		createMockStateManager(t, tempDir),
		createMockBuffer(t),
		createMockSchemaManager(t),
		createMockDuckEngine(t, tempDir),
		createMockProducer(t),
		[]string{"events", "users"},
		[]string{"users"},
		[]string{},
		nil,
	)
	if err != nil {
		t.Fatalf("Engine creation failed: %v", err)
	}
	defer close(engine.shutdown)

	// First, process a user (dimension data)
	userEvent := map[string]any{
		"user_id": 456,
		"name":    "Jane Smith",
		"email":   "jane@example.com",
	}

	err = engine.ProcessIncomingEvent("users", userEvent, time.Now(), 0, 1)
	if err != nil {
		t.Errorf("Processing user event failed: %v", err)
	}

	// Then process an event that should join with the user
	eventData := map[string]any{
		"user_id":    456,
		"event_type": "purchase",
		"amount":     99.99,
	}

	err = engine.ProcessIncomingEvent("events", eventData, time.Now(), 0, 2)
	if err != nil {
		t.Errorf("Processing event failed: %v", err)
	}

	// Verify both events were stored in the row store
	engine.rowsMu.RLock()
	rowCount := len(engine.rows)
	engine.rowsMu.RUnlock()

	if rowCount != 2 {
		t.Errorf("Expected 2 rows stored after join processing, got %d", rowCount)
	}

	// Verify the join key calculation works consistently
	userKey := hashJoinKey(userEvent, []string{"user_id"})
	eventKey := hashJoinKey(eventData, []string{"user_id"})
	if eventKey != userKey {
		t.Errorf("Join keys should match: user=%d, event=%d", userKey, eventKey)
	}

	// Verify the presence mask calculation for both events
	userMask := engine.buildPresenceMask(userEvent)
	eventMask := engine.buildPresenceMask(eventData)

	// User event has both user_id and name, so should meet emission
	if !engine.meetsEmission(userMask) {
		t.Errorf("User event should meet emission requirements")
	}

	// Event has only user_id, missing name, so should not meet emission
	if engine.meetsEmission(eventMask) {
		t.Errorf("Event should not meet emission requirements (missing name)")
	}
}

// Test processing incoming events - deduplication functionality
func TestProcessIncomingEventDeduplication(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping engine event processing test in short mode")
	}
	tempDir := t.TempDir()

	// Create test engine
	testPipeline := createTestPipeline()
	plan, _ := pipeline.NewQueryPlan(testPipeline.Query)

	engine, err := NewEngine(
		testPipeline,
		plan,
		createMockStateManager(t, tempDir),
		createMockBuffer(t),
		createMockSchemaManager(t),
		createMockDuckEngine(t, tempDir),
		createMockProducer(t),
		[]string{"events"},
		[]string{},
		[]string{},
		nil,
	)
	if err != nil {
		t.Fatalf("Engine creation failed: %v", err)
	}
	defer close(engine.shutdown)

	eventData := map[string]any{
		"user_id":    789,
		"event_type": "click",
	}

	// Process same event twice with same offset
	err1 := engine.ProcessIncomingEvent("events", eventData, time.Now(), 0, 1)
	err2 := engine.ProcessIncomingEvent("events", eventData, time.Now(), 0, 1)

	if err1 != nil || err2 != nil {
		t.Errorf("Event processing failed: err1=%v, err2=%v", err1, err2)
	}

	// Should only have one row due to deduplication
	engine.rowsMu.RLock()
	rowCount := len(engine.rows)
	engine.rowsMu.RUnlock()

	if rowCount != 1 {
		t.Errorf("Expected 1 row after deduplication, got %d", rowCount)
	}

	// Process with higher offset - should succeed
	err3 := engine.ProcessIncomingEvent("events", eventData, time.Now(), 0, 2)
	if err3 != nil {
		t.Errorf("Processing event with higher offset failed: %v", err3)
	}

	engine.rowsMu.RLock()
	finalRowCount := len(engine.rows)
	engine.rowsMu.RUnlock()

	if finalRowCount != 2 {
		t.Errorf("Expected 2 rows after processing higher offset, got %d", finalRowCount)
	}
}

// Test processing incoming events - presence mask functionality
func TestProcessIncomingEventPresenceMask(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping engine event processing test in short mode")
	}
	tempDir := t.TempDir()

	// Create test engine
	testPipeline := createTestPipeline()
	plan, _ := pipeline.NewQueryPlan(testPipeline.Query)

	engine, err := NewEngine(
		testPipeline,
		plan,
		createMockStateManager(t, tempDir),
		createMockBuffer(t),
		createMockSchemaManager(t),
		createMockDuckEngine(t, tempDir),
		createMockProducer(t),
		[]string{"events"},
		[]string{},
		[]string{},
		nil,
	)
	if err != nil {
		t.Fatalf("Engine creation failed: %v", err)
	}
	defer close(engine.shutdown)

	// Event with complete emission fields
	completeEvent := map[string]any{
		"user_id": 111,
		"name":    "Complete User",
		"extra":   "ignored",
	}

	err = engine.ProcessIncomingEvent("events", completeEvent, time.Now(), 0, 1)
	if err != nil {
		t.Errorf("Processing complete event failed: %v", err)
	}

	// Find the stored row and verify presence mask
	engine.rowsMu.RLock()
	var completeRow *Row
	for _, row := range engine.rows {
		if userID, ok := row.Data["user_id"]; ok && userID == 111 {
			completeRow = row
			break
		}
	}
	engine.rowsMu.RUnlock()

	if completeRow == nil {
		t.Fatalf("Could not find stored row for complete event")
	}

	// Should meet emission requirements
	if !engine.meetsEmission(completeRow.PresenceMask) {
		t.Errorf("Complete event should meet emission requirements")
	}

	// Event with incomplete emission fields
	incompleteEvent := map[string]any{
		"user_id": 222,
		// missing "name"
	}

	err = engine.ProcessIncomingEvent("events", incompleteEvent, time.Now(), 0, 2)
	if err != nil {
		t.Errorf("Processing incomplete event failed: %v", err)
	}

	// Find the stored row and verify presence mask
	engine.rowsMu.RLock()
	var incompleteRow *Row
	for _, row := range engine.rows {
		if userID, ok := row.Data["user_id"]; ok && userID == 222 {
			incompleteRow = row
			break
		}
	}
	engine.rowsMu.RUnlock()

	if incompleteRow == nil {
		t.Fatalf("Could not find stored row for incomplete event")
	}

	// Should not meet emission requirements
	if engine.meetsEmission(incompleteRow.PresenceMask) {
		t.Errorf("Incomplete event should not meet emission requirements")
	}
}

// Test TTL index operations
func TestTTLIndexOperations(t *testing.T) {
	index := ttlindex.New()
	now := time.Now()

	// Add some entries
	index.Add("key1", now.Add(-2*time.Hour))
	index.Add("key2", now.Add(-1*time.Hour))
	index.Add("key3", now.Add(-30*time.Minute))

	// Test expiration
	expired := index.GetExpired(now.Add(-90 * time.Minute))

	if len(expired) != 1 {
		t.Errorf("GetExpired returned %d entries, want 1", len(expired))
	}

	if expired[0] != "key1" {
		t.Errorf("GetExpired returned wrong key: %s, want key1", expired[0])
	}

	// Test removal
	index.Remove("key1")
	expired = index.GetExpired(now.Add(-90 * time.Minute))

	if len(expired) != 0 {
		t.Errorf("GetExpired after removal returned %d entries, want 0", len(expired))
	}
}

// Test binary encoding/decoding for edge persistence
func TestEdgeEncoding(t *testing.T) {
	original := []uint64{1, 2, 3, 4, 5}

	// Test encoding
	encoded := encodeUint64Slice(original)
	if len(encoded) != 4+8*len(original) {
		t.Errorf("Encoded length incorrect: got %d, want %d", len(encoded), 4+8*len(original))
	}

	// Test decoding
	decoded, err := decodeUint64Slice(encoded)
	if err != nil {
		t.Errorf("Decoding failed: %v", err)
	}

	if !reflect.DeepEqual(original, decoded) {
		t.Errorf("Decoded slice doesn't match original: got %v, want %v", decoded, original)
	}

	// Test error cases
	_, err = decodeUint64Slice([]byte{})
	if err == nil {
		t.Errorf("Expected error for empty slice")
	}

	_, err = decodeUint64Slice([]byte{1, 2, 3}) // too short
	if err == nil {
		t.Errorf("Expected error for short slice")
	}
}

// Test utility functions
func TestUtilityFunctions(t *testing.T) {
	// Test indexOf
	arr := []string{"a", "b", "c", "d"}
	if indexOf("c", arr) != 2 {
		t.Errorf("indexOf failed")
	}
	if indexOf("z", arr) != -1 {
		t.Errorf("indexOf should return -1 for missing element")
	}

	// Test edgeID
	id1 := edgeID("table1", "table2")
	id2 := edgeID("table2", "table1")
	if id1 != id2 {
		t.Errorf("edgeID should be consistent regardless of order: %s != %s", id1, id2)
	}

	expected := "table1|table2"
	if id1 != expected {
		t.Errorf("edgeID format incorrect: got %s, want %s", id1, expected)
	}
}

// Test sliceToSet conversion
func TestSliceToSet(t *testing.T) {
	slice := []string{"a", "b", "c", "a"} // duplicate 'a'
	set := sliceToSet(slice)

	expected := map[string]bool{
		"a": true,
		"b": true,
		"c": true,
	}

	if !reflect.DeepEqual(set, expected) {
		t.Errorf("sliceToSet failed: got %v, want %v", set, expected)
	}
}

// Helper functions for engine integration tests

func createTestPipeline() pipeline.Pipeline {
	return pipeline.Pipeline{
		Name:  "test_engine_pipeline",
		Query: "SELECT u.user_id, u.name, e.event_type FROM users u LEFT JOIN events e ON u.user_id = e.user_id",
		Output: pipeline.OutputConfig{
			Topic:  "output_events",
			Format: "json",
			Key:    "user_id",
		},
		Emission: &pipeline.EmissionRules{
			Type:    "smart",
			Require: []string{"user_id", "name"},
		},
	}
}

func createMockStateManager(t *testing.T, tempDir string) *state.RocksDBState {
	cfg := config.AppConfig{}
	cfg.State.RocksDB.Path = tempDir + "/rocks"
	cfg.State.RocksDB.Checkpoint.Enabled = false

	stateManager, err := state.NewRocksDBState("test_pipeline", &cfg, make(map[string]time.Duration))
	if err != nil {
		t.Fatalf("Failed to create mock state manager: %v", err)
	}
	return stateManager
}

func createMockBuffer(_ *testing.T) *duck.Buffer {
	buffer := duck.NewBuffer("test_table")
	return buffer
}

func createMockSchemaManager(_ *testing.T) *schema.Manager {
	return schema.NewSchemaManager()
}

func createMockDuckEngine(t *testing.T, tempDir string) *duck.DBEngine {
	engine, err := duck.NewDuckDBEngine(tempDir + "/duck")
	if err != nil {
		t.Fatalf("Failed to create mock DuckDB engine: %v", err)
	}
	return engine
}

func createMockProducer(_ *testing.T) *kafka.Producer {
	// Create a mock producer that doesn't require real Kafka
	producer := &kafka.Producer{} // This will be a minimal mock
	return producer
}
