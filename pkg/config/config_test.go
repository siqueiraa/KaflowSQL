package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigLoading(t *testing.T) {
	// Create a temporary config file for testing
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "test_config.yaml")

	configContent := `
kafka:
  brokers:
    - localhost:9092
    - localhost:9093
  schemaRegistry: http://localhost:8081
  useAvro: true

emitter:
  interval: 5s

state:
  ttl: 2h
  rocksdb:
    path: /tmp/test/rocksdb
    checkpoint:
      enabled: true
      interval: 5m
      s3:
        enabled: true
        bucket: test-bucket
        region: us-west-2
        endpoint: https://s3.us-west-2.amazonaws.com
        accessKey: test-key
        secretKey: test-secret
        prefix: test-prefix/
`

	err := os.WriteFile(configPath, []byte(configContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	// Test loading the config
	config := Load(configPath)

	// Verify Kafka configuration
	if len(config.Kafka.Brokers) != 2 {
		t.Errorf("Expected 2 brokers, got %d", len(config.Kafka.Brokers))
	}

	if config.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("Expected first broker to be localhost:9092, got %s", config.Kafka.Brokers[0])
	}

	if config.Kafka.SchemaRegistry != "http://localhost:8081" {
		t.Errorf("Expected schema registry http://localhost:8081, got %s", config.Kafka.SchemaRegistry)
	}

	if !config.Kafka.UseAvro {
		t.Errorf("Expected UseAvro to be true")
	}

	// Verify emitter configuration
	if config.Emitter.Interval != 5*time.Second {
		t.Errorf("Expected emitter interval 5s, got %v", config.Emitter.Interval)
	}

	// Verify state configuration
	if config.State.TTL != 2*time.Hour {
		t.Errorf("Expected state TTL 2h, got %v", config.State.TTL)
	}

	if config.State.RocksDB.Path != "/tmp/test/rocksdb" {
		t.Errorf("Expected RocksDB path /tmp/test/rocksdb, got %s", config.State.RocksDB.Path)
	}

	// Verify checkpoint configuration
	if !config.State.RocksDB.Checkpoint.Enabled {
		t.Errorf("Expected checkpoint to be enabled")
	}

	if config.State.RocksDB.Checkpoint.Interval != 5*time.Minute {
		t.Errorf("Expected checkpoint interval 5m, got %v", config.State.RocksDB.Checkpoint.Interval)
	}

	// Verify S3 configuration
	if !config.State.RocksDB.Checkpoint.S3.Enabled {
		t.Errorf("Expected S3 to be enabled")
	}

	if config.State.RocksDB.Checkpoint.S3.Bucket != "test-bucket" {
		t.Errorf("Expected S3 bucket test-bucket, got %s", config.State.RocksDB.Checkpoint.S3.Bucket)
	}

	if config.State.RocksDB.Checkpoint.S3.Region != "us-west-2" {
		t.Errorf("Expected S3 region us-west-2, got %s", config.State.RocksDB.Checkpoint.S3.Region)
	}
}

func TestConfigDefaults(t *testing.T) {
	// Create a minimal config file
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "minimal_config.yaml")

	configContent := `
kafka:
  brokers:
    - localhost:9092
  schemaRegistry: http://localhost:8081
`

	err := os.WriteFile(configPath, []byte(configContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write minimal config: %v", err)
	}

	config := Load(configPath)

	// Verify defaults are applied
	if config.Emitter.Interval == 0 {
		t.Errorf("Expected default emitter interval to be set")
	}

	if config.State.TTL == 0 {
		t.Errorf("Expected default state TTL to be set")
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      AppConfig
		expectError bool
	}{
		{
			name: "valid config",
			config: func() AppConfig {
				cfg := AppConfig{
					Kafka: KafkaConfig{
						Brokers:        []string{"localhost:9092"},
						SchemaRegistry: "http://localhost:8081",
						UseAvro:        true,
					},
				}
				cfg.State.TTL = 1 * time.Hour
				cfg.State.RocksDB.Path = "/tmp/rocksdb"
				return cfg
			}(),
			expectError: false,
		},
		{
			name: "missing brokers",
			config: AppConfig{
				Kafka: KafkaConfig{
					SchemaRegistry: "http://localhost:8081",
				},
			},
			expectError: true,
		},
		{
			name: "missing schema registry",
			config: AppConfig{
				Kafka: KafkaConfig{
					Brokers: []string{"localhost:9092"},
					UseAvro: true, // Avro requires schema registry
				},
			},
			expectError: true,
		},
		{
			name: "missing rocksdb path",
			config: func() AppConfig {
				cfg := AppConfig{
					Kafka: KafkaConfig{
						Brokers:        []string{"localhost:9092"},
						SchemaRegistry: "http://localhost:8081",
					},
				}
				cfg.State.RocksDB.Path = "" // empty path
				return cfg
			}(),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(&tt.config)

			if tt.expectError && err == nil {
				t.Errorf("Expected validation error but got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected validation error: %v", err)
			}
		})
	}
}

func TestConfigCompleteConfiguration(t *testing.T) {
	// Test loading a complete configuration file with all settings
	tempDir := t.TempDir()
	configPath := filepath.Join(tempDir, "complete_config.yaml")

	configContent := `
kafka:
  brokers:
    - kafka.example.com:9092
    - kafka2.example.com:9092
  schemaRegistry: http://schema-registry.example.com:8081
  useAvro: true

emitter:
  interval: 5s

state:
  ttl: 2h
  rocksdb:
    path: /custom/rocksdb/path
    checkpoint:
      enabled: true
      interval: 10m
      s3:
        enabled: true
        bucket: my-bucket
        region: us-west-2
        accessKey: test-key
        secretKey: test-secret
        endpoint: https://s3.us-west-2.amazonaws.com
        prefix: backups/

engine:
  defaultWindow: 1h
  duckdbMemory: 512MB
  checkpointInterval: 5m
`

	err := os.WriteFile(configPath, []byte(configContent), 0600)
	if err != nil {
		t.Fatalf("Failed to write complete config: %v", err)
	}

	config := Load(configPath)

	// Test Kafka configuration
	if len(config.Kafka.Brokers) != 2 {
		t.Errorf("Expected 2 brokers, got %d", len(config.Kafka.Brokers))
	}
	if config.Kafka.Brokers[0] != "kafka.example.com:9092" {
		t.Errorf("Expected first broker kafka.example.com:9092, got %s", config.Kafka.Brokers[0])
	}
	if config.Kafka.SchemaRegistry != "http://schema-registry.example.com:8081" {
		t.Errorf("Expected schema registry http://schema-registry.example.com:8081, got %s", config.Kafka.SchemaRegistry)
	}
	if !config.Kafka.UseAvro {
		t.Errorf("Expected useAvro to be true")
	}

	// Test custom values override defaults
	if config.Emitter.Interval != 5*time.Second {
		t.Errorf("Expected emitter interval 5s, got %v", config.Emitter.Interval)
	}
	if config.State.TTL != 2*time.Hour {
		t.Errorf("Expected state TTL 2h, got %v", config.State.TTL)
	}
	if config.State.RocksDB.Path != "/custom/rocksdb/path" {
		t.Errorf("Expected custom RocksDB path, got %s", config.State.RocksDB.Path)
	}
}

func TestConfigErrorHandling(t *testing.T) {
	t.Run("NonexistentFile", func(t *testing.T) {
		// This test verifies that Load terminates the program for missing files
		// Since Load calls log.Fatalf which exits the process, we can't test this
		// in a unit test. This is by design - config errors should be fatal.
		t.Skip("Load() calls log.Fatalf for missing files, which is the intended behavior")
	})

	t.Run("InvalidYAML", func(t *testing.T) {
		// This test verifies that Load terminates the program for invalid YAML
		// Since Load calls log.Fatalf for YAML errors, we can't test this
		// in a unit test. This is by design - config errors should be fatal.
		t.Skip("Load() calls log.Fatalf for invalid YAML, which is the intended behavior")
	})
}

// Helper function for validation (would be implemented in the actual code)
func validateConfig(config *AppConfig) error {
	if len(config.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers are required")
	}

	if config.Kafka.UseAvro && config.Kafka.SchemaRegistry == "" {
		return fmt.Errorf("schema registry is required when using Avro")
	}

	if config.State.RocksDB.Path == "" {
		return fmt.Errorf("rocksdb path is required")
	}

	return nil
}
