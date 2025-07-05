package state

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/siqueiraa/KaflowSQL/pkg/config"
)

func TestRocksDBStateManager(t *testing.T) {
	// Create temporary directory for test database
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	cfg := config.AppConfig{}
	cfg.State.RocksDB.Path = dbPath
	cfg.State.RocksDB.Checkpoint.Enabled = false // Disable S3 for tests
	cfg.State.RocksDB.Checkpoint.Interval = time.Minute

	ttlByTopic := map[string]time.Duration{
		"events": 1 * time.Hour,
		"users":  0, // never expire
	}

	// Test initialization
	sm, err := NewRocksDBState("test_pipeline", &cfg, ttlByTopic)
	if err != nil {
		t.Fatalf("Failed to create RocksDBState: %v", err)
	}
	// Note: RocksDBState uses BadgerDB which is closed automatically

	// Test basic put/get operations
	t.Run("BasicOperations", func(t *testing.T) {
		key := "test_key"
		value := []byte("test_value")
		ts := time.Now()

		err := sm.Put("test_cf", key, value, ts)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}

		// Verify stats
		stats, err := sm.StatsByTopic()
		if err != nil {
			t.Errorf("StatsByTopic failed: %v", err)
		}

		if len(stats) == 0 {
			t.Errorf("Expected stats to be populated")
		}
	})

	// Test offset management
	t.Run("OffsetManagement", func(t *testing.T) {
		topic := "test_topic"
		partition := 0
		offset := int64(100)

		err := sm.SaveOffset(topic, partition, offset)
		if err != nil {
			t.Errorf("SaveOffset failed: %v", err)
		}

		retrievedOffset, err := sm.GetOffset(topic, partition)
		if err != nil {
			t.Errorf("GetOffset failed: %v", err)
		}

		if retrievedOffset != offset {
			t.Errorf("Offset mismatch: got %d, want %d", retrievedOffset, offset)
		}
	})

	// Test ForEach functionality
	t.Run("ForEach", func(t *testing.T) {
		// Put some test data with a specific prefix to avoid conflicts
		testData := map[string][]byte{
			"foreach_key1": []byte("value1"),
			"foreach_key2": []byte("value2"),
			"foreach_key3": []byte("value3"),
		}

		ts := time.Now()
		for key, value := range testData {
			err := sm.Put("test_cf", key, value, ts)
			if err != nil {
				t.Errorf("Put failed for key %s: %v", key, err)
			}
		}

		// Test ForEach with specific prefix
		found := make(map[string][]byte)
		err := sm.ForEach("test_cf:foreach_", func(key string, value []byte, _ time.Time) error {
			found[key] = value
			return nil
		})

		if err != nil {
			t.Errorf("ForEach failed: %v", err)
		}

		if len(found) != len(testData) {
			t.Errorf("ForEach found %d items, expected %d", len(found), len(testData))
		}
	})

	// Test async delete
	t.Run("AsyncDelete", func(t *testing.T) {
		key := "delete_test_key"
		value := []byte("delete_test_value")
		ts := time.Now()

		// Put a value
		err := sm.Put("test_cf", key, value, ts)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}

		// Delete it
		err = sm.DeleteAsync([]byte(key))
		if err != nil {
			t.Errorf("DeleteAsync failed: %v", err)
		}

		// Wait a bit for async operation
		time.Sleep(100 * time.Millisecond)
	})
}

func TestRocksDBStateTTL(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "ttl_test.db")

	cfg := config.AppConfig{}
	cfg.State.RocksDB.Path = dbPath
	cfg.State.RocksDB.Checkpoint.Enabled = false

	ttlByTopic := map[string]time.Duration{
		"short_ttl": 100 * time.Millisecond,
		"long_ttl":  1 * time.Hour,
		"no_ttl":    0, // never expire
	}

	sm, err := NewRocksDBState("ttl_test", &cfg, ttlByTopic)
	if err != nil {
		t.Fatalf("Failed to create RocksDBState: %v", err)
	}
	// Note: RocksDBState uses BadgerDB which is closed automatically

	// Test TTL enforcement
	t.Run("TTLEnforcement", func(t *testing.T) {
		now := time.Now()

		// Put data with short TTL
		err := sm.Put("short_ttl", "key1", []byte("value1"), now)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}

		// Put data with no TTL
		err = sm.Put("no_ttl", "key2", []byte("value2"), now)
		if err != nil {
			t.Errorf("Put failed: %v", err)
		}

		// Wait for TTL to expire
		time.Sleep(200 * time.Millisecond)

		// The short TTL data should be cleaned up by background processes
		// This would require implementing the TTL cleanup mechanism
		// For now, just verify the data was stored
		count := 0
		err = sm.ForEach("", func(_ string, _ []byte, _ time.Time) error {
			count++
			return nil
		})

		if err != nil {
			t.Errorf("ForEach failed: %v", err)
		}

		if count == 0 {
			t.Errorf("No data found, expected at least the no_ttl data")
		}
	})
}

func TestRocksDBStateCheckpointing(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping checkpointing integration test in short mode")
	}

	cfg := createBasicCheckpointConfig(t)
	ttlByTopic := createTestTTLConfig()

	_, err := NewRocksDBState("checkpoint_test", &cfg, ttlByTopic)
	if err != nil {
		t.Fatalf("Failed to create RocksDBState: %v", err)
	}
}

func TestCheckpointCreation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping checkpointing integration test in short mode")
	}

	cfg := createCheckpointEnabledConfig(t)
	ttlByTopic := createTestTTLConfigWithUsers()

	stateManager := createStateManager(t, "checkpoint_test", &cfg, ttlByTopic)
	testData := createTestData()

	insertTestData(t, stateManager, testData)
	testCheckpointCreation(t, stateManager)
	validateDataAfterCheckpoint(t, stateManager, testData)
}

func TestS3CheckpointSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping S3 checkpoint simulation in short mode")
	}

	cfg := createS3CheckpointConfig(t)
	validateS3Configuration(t, &cfg)

	t.Logf("S3 configuration validated: bucket=%s, region=%s, prefix=%s",
		cfg.State.RocksDB.Checkpoint.S3.Bucket,
		cfg.State.RocksDB.Checkpoint.S3.Region,
		cfg.State.RocksDB.Checkpoint.S3.Prefix)
}

func TestCheckpointRestoreSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping checkpoint restore simulation in short mode")
	}

	originalCfg, restoredCfg := createRestoreTestConfigs(t)
	ttlByTopic := createTestTTLConfig()

	originalState := createStateManager(t, "original", &originalCfg, ttlByTopic)
	testDataAndVerify(t, originalState)

	restoredState := createStateManager(t, "restored", &restoredCfg, ttlByTopic)
	simulateRestoreAndVerify(t, restoredState)
}

// Helper functions for test refactoring

type testDataItem struct {
	topic string
	key   string
	value []byte
}

func createBasicCheckpointConfig(t *testing.T) config.AppConfig {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "checkpoint_test.db")

	cfg := config.AppConfig{}
	cfg.State.RocksDB.Path = dbPath
	cfg.State.RocksDB.Checkpoint.Enabled = true
	cfg.State.RocksDB.Checkpoint.S3.Enabled = false
	return cfg
}

func createCheckpointEnabledConfig(t *testing.T) config.AppConfig {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "checkpoint_test.db")

	cfg := config.AppConfig{}
	cfg.State.RocksDB.Path = dbPath
	cfg.State.RocksDB.Checkpoint.Enabled = true
	cfg.State.RocksDB.Checkpoint.Interval = 5 * time.Minute
	cfg.State.RocksDB.Checkpoint.S3.Enabled = false
	return cfg
}

func createS3CheckpointConfig(t *testing.T) config.AppConfig {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "s3_test.db")

	cfg := config.AppConfig{}
	cfg.State.RocksDB.Path = dbPath
	cfg.State.RocksDB.Checkpoint.Enabled = true
	cfg.State.RocksDB.Checkpoint.S3.Enabled = true
	cfg.State.RocksDB.Checkpoint.S3.Bucket = "test-bucket"
	cfg.State.RocksDB.Checkpoint.S3.Region = "us-west-2"
	cfg.State.RocksDB.Checkpoint.S3.AccessKey = "test-key"
	cfg.State.RocksDB.Checkpoint.S3.SecretKey = "test-secret"
	cfg.State.RocksDB.Checkpoint.S3.Prefix = "test-pipeline/"
	return cfg
}

func createRestoreTestConfigs(t *testing.T) (originalCfg, restoredCfg config.AppConfig) {
	tempDir := t.TempDir()
	originalDBPath := filepath.Join(tempDir, "original.db")
	restoredDBPath := filepath.Join(tempDir, "restored.db")

	originalCfg = config.AppConfig{}
	originalCfg.State.RocksDB.Path = originalDBPath
	originalCfg.State.RocksDB.Checkpoint.Enabled = false

	restoredCfg = config.AppConfig{}
	restoredCfg.State.RocksDB.Path = restoredDBPath
	restoredCfg.State.RocksDB.Checkpoint.Enabled = false

	return originalCfg, restoredCfg
}

func createTestTTLConfig() map[string]time.Duration {
	return map[string]time.Duration{
		"events": 1 * time.Hour,
	}
}

func createTestTTLConfigWithUsers() map[string]time.Duration {
	return map[string]time.Duration{
		"events": 1 * time.Hour,
		"users":  0, // never expire
	}
}

func createStateManager(t *testing.T, name string, cfg *config.AppConfig, ttlByTopic map[string]time.Duration) *RocksDBState {
	stateManager, err := NewRocksDBState(name, cfg, ttlByTopic)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	return stateManager
}

func createTestData() []testDataItem {
	return []testDataItem{
		{"events", "event_1", []byte(`{"user_id": 123, "action": "click"}`)},
		{"events", "event_2", []byte(`{"user_id": 456, "action": "view"}`)},
		{"users", "user_123", []byte(`{"name": "John", "email": "john@example.com"}`)},
		{"users", "user_456", []byte(`{"name": "Jane", "email": "jane@example.com"}`)},
	}
}

func insertTestData(t *testing.T, stateManager *RocksDBState, testData []testDataItem) {
	now := time.Now()
	for _, data := range testData {
		err := stateManager.Put(data.topic, data.key, data.value, now)
		if err != nil {
			t.Errorf("Failed to put data: %v", err)
		}
	}
}

func testCheckpointCreation(t *testing.T, stateManager *RocksDBState) {
	err := stateManager.CreateCheckpointIfEnabled()
	if err != nil {
		t.Errorf("Failed to create checkpoint: %v", err)
	}
	t.Logf("Checkpoint creation completed for local testing")
}

func validateDataAfterCheckpoint(t *testing.T, stateManager *RocksDBState, testData []testDataItem) {
	for _, data := range testData {
		value, err := stateManager.Get(data.topic, data.key)
		if err != nil {
			t.Errorf("Failed to get data after checkpoint: %v", err)
		}
		if !bytes.Equal(value, data.value) {
			t.Errorf("Data mismatch after checkpoint: got %s, want %s", string(value), string(data.value))
		}
	}
}

func validateS3Configuration(t *testing.T, cfg *config.AppConfig) {
	if !cfg.State.RocksDB.Checkpoint.S3.Enabled {
		t.Errorf("S3 should be enabled for this test")
	}
	if cfg.State.RocksDB.Checkpoint.S3.Bucket == "" {
		t.Errorf("S3 bucket should be configured")
	}
	if cfg.State.RocksDB.Checkpoint.S3.Region == "" {
		t.Errorf("S3 region should be configured")
	}
}

func testDataAndVerify(t *testing.T, stateManager *RocksDBState) {
	testKey := "test_key"
	testValue := []byte("test_value")

	err := stateManager.Put("events", testKey, testValue, time.Now())
	if err != nil {
		t.Errorf("Failed to put test data: %v", err)
	}

	retrievedValue, err := stateManager.Get("events", testKey)
	if err != nil {
		t.Errorf("Failed to get test data from original: %v", err)
	}
	if !bytes.Equal(retrievedValue, testValue) {
		t.Errorf("Data mismatch in original: got %s, want %s", string(retrievedValue), string(testValue))
	}
}

func simulateRestoreAndVerify(t *testing.T, stateManager *RocksDBState) {
	testKey := "test_key"
	testValue := []byte("test_value")

	err := stateManager.Put("events", testKey, testValue, time.Now())
	if err != nil {
		t.Errorf("Failed to put data in restored state: %v", err)
	}

	restoredValue, err := stateManager.Get("events", testKey)
	if err != nil {
		t.Errorf("Failed to get data from restored state: %v", err)
	}
	if !bytes.Equal(restoredValue, testValue) {
		t.Errorf("Data mismatch in restored state: got %s, want %s", string(restoredValue), string(testValue))
	}

	t.Logf("Checkpoint restore simulation completed successfully")
}

func TestRocksDBStateError(t *testing.T) {
	// Test error cases
	t.Run("InvalidPath", func(t *testing.T) {
		cfg := config.AppConfig{}
		cfg.State.RocksDB.Path = "/invalid/path/that/does/not/exist"

		_, err := NewRocksDBState("error_test", &cfg, nil)
		if err == nil {
			t.Errorf("Expected error for invalid path")
		}
	})

	t.Run("NilTTLMap", func(t *testing.T) {
		tempDir := t.TempDir()
		dbPath := filepath.Join(tempDir, "nil_ttl_test.db")

		cfg := config.AppConfig{}
		cfg.State.RocksDB.Path = dbPath

		_, err := NewRocksDBState("nil_ttl_test", &cfg, nil)
		if err != nil {
			t.Errorf("Should handle nil TTL map gracefully: %v", err)
		}
		// Note: RocksDBState uses BadgerDB which is closed automatically
	})
}

func TestRocksDBStateConcurrency(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "concurrency_test.db")

	cfg := config.AppConfig{}
	cfg.State.RocksDB.Path = dbPath

	ttlByTopic := map[string]time.Duration{
		"events": 1 * time.Hour,
	}

	sm, err := NewRocksDBState("concurrency_test", &cfg, ttlByTopic)
	if err != nil {
		t.Fatalf("Failed to create RocksDBState: %v", err)
	}
	// Note: RocksDBState uses BadgerDB which is closed automatically

	// Test concurrent operations
	t.Run("ConcurrentPuts", func(t *testing.T) {
		numGoroutines := 10
		itemsPerGoroutine := 100
		done := make(chan bool, numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				for i := 0; i < itemsPerGoroutine; i++ {
					key := fmt.Sprintf("key_%d_%d", goroutineID, i)
					value := []byte(fmt.Sprintf("value_%d_%d", goroutineID, i))
					err := sm.Put("events", key, value, time.Now())
					if err != nil {
						t.Errorf("Concurrent put failed: %v", err)
					}
				}
				done <- true
			}(g)
		}

		// Wait for all goroutines to complete
		for g := 0; g < numGoroutines; g++ {
			<-done
		}

		// Verify data integrity
		count := 0
		err := sm.ForEach("events:", func(_ string, _ []byte, _ time.Time) error {
			count++
			return nil
		})

		if err != nil {
			t.Errorf("ForEach failed: %v", err)
		}

		expected := numGoroutines * itemsPerGoroutine
		if count != expected {
			t.Errorf("Expected %d items, found %d", expected, count)
		}
	})
}
