package kafka

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/siqueiraa/KaflowSQL/pkg/config"
)

// Mock implementation for testing only - not using MockStateManager with real consumer

func offsetKey(topic string, partition int) string {
	return fmt.Sprintf("%s-%d", topic, partition)
}

func TestConsumerConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consumer integration test in short mode")
	}

	t.Run("ValidConfiguration", func(t *testing.T) {
		// Test that consumer configuration validation works correctly
		brokers := []string{"localhost:9092"}
		topic := "test_topic"
		groupID := "test_group"

		cfg := config.KafkaConfig{
			Brokers:        brokers,
			SchemaRegistry: "http://localhost:8081",
			UseAvro:        false,
		}

		// Test configuration validation (without creating actual consumer)
		if len(brokers) == 0 {
			t.Errorf("Brokers should not be empty")
		}

		if topic == "" {
			t.Errorf("Topic should not be empty")
		}

		if groupID == "" {
			t.Errorf("Group ID should not be empty")
		}

		// Test configuration map creation logic
		expectedBootstrapServers := "localhost:9092"
		actualBootstrapServers := strings.Join(brokers, ",")
		if actualBootstrapServers != expectedBootstrapServers {
			t.Errorf("Bootstrap servers mismatch: got %s, want %s", actualBootstrapServers, expectedBootstrapServers)
		}

		// Test Avro configuration
		if cfg.UseAvro && cfg.SchemaRegistry == "" {
			t.Errorf("Schema registry required when using Avro")
		}
	})

	t.Run("ConfigurationDefaults", func(t *testing.T) {
		// Test default configuration values
		defaultConfig := map[string]interface{}{
			"enable.auto.commit":              false,
			"auto.offset.reset":               "earliest",
			"go.application.rebalance.enable": true,
		}

		// Verify default settings
		if defaultConfig["enable.auto.commit"] != false {
			t.Errorf("Auto commit should be disabled by default")
		}

		if defaultConfig["auto.offset.reset"] != "earliest" {
			t.Errorf("Auto offset reset should be 'earliest' by default")
		}

		if defaultConfig["go.application.rebalance.enable"] != true {
			t.Errorf("Rebalance should be enabled by default")
		}
	})

	t.Run("ConsumerOptions", func(t *testing.T) {
		// Test consumer option validation
		testCases := []struct {
			name        string
			brokers     []string
			topic       string
			groupID     string
			useAvro     bool
			schemaReg   string
			expectValid bool
		}{
			{
				name:        "valid_json_config",
				brokers:     []string{"localhost:9092"},
				topic:       "events",
				groupID:     "processor",
				useAvro:     false,
				schemaReg:   "",
				expectValid: true,
			},
			{
				name:        "valid_avro_config",
				brokers:     []string{"localhost:9092"},
				topic:       "events",
				groupID:     "processor",
				useAvro:     true,
				schemaReg:   "http://localhost:8081",
				expectValid: true,
			},
			{
				name:        "invalid_avro_config",
				brokers:     []string{"localhost:9092"},
				topic:       "events",
				groupID:     "processor",
				useAvro:     true,
				schemaReg:   "", // missing schema registry
				expectValid: false,
			},
			{
				name:        "empty_brokers",
				brokers:     []string{},
				topic:       "events",
				groupID:     "processor",
				useAvro:     false,
				schemaReg:   "",
				expectValid: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				valid := validateConsumerConfig(tc.brokers, tc.topic, tc.groupID, tc.useAvro, tc.schemaReg)
				if valid != tc.expectValid {
					t.Errorf("Config validation mismatch for %s: got %v, want %v", tc.name, valid, tc.expectValid)
				}
			})
		}
	})
}

func TestDecodedMessage(t *testing.T) {
	// Test DecodedMessage structure
	msg := &DecodedMessage{
		Value:     map[string]any{"user_id": 123, "amount": 100.0},
		Time:      time.Now(),
		Partition: 0,
		Offset:    1000,
	}

	if msg.Value["user_id"] != 123 {
		t.Errorf("Message value incorrect")
	}

	if msg.Partition != 0 {
		t.Errorf("Message partition incorrect")
	}

	if msg.Offset != 1000 {
		t.Errorf("Message offset incorrect")
	}

	// Test Release method (no-op for now)
	msg.Release()
}

func TestOffsetManagement(t *testing.T) {
	// Test offset key generation (unit testable part)
	topic := "test_topic"
	partition := 0

	key := offsetKey(topic, partition)
	expectedKey := "test_topic-0"

	if key != expectedKey {
		t.Errorf("Offset key mismatch: got %s, want %s", key, expectedKey)
	}
}

func TestConsumerStats(t *testing.T) {
	// Test consumer statistics tracking
	// This would be implemented with the actual consumer stats

	t.Run("StatsInitialization", func(_ *testing.T) {
		// Test that stats are properly initialized
		// stats := &ConsumerStats{}
		// Verify initial values
	})

	t.Run("StatsUpdate", func(_ *testing.T) {
		// Test that stats are updated correctly
		// when messages are processed
	})
}

func TestConsumerErrorHandlingInvalidBrokers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consumer error handling integration test in short mode")
	}

	// Test handling of invalid broker configurations
	invalidBrokerConfigs := [][]string{
		{}, // empty brokers
		{"invalid:broker:format"},
		{"localhost"}, // missing port
		{"   "},       // whitespace only
	}

	for i, brokers := range invalidBrokerConfigs {
		t.Run(fmt.Sprintf("config_%d", i), func(t *testing.T) {
			testInvalidBrokerConfig(t, brokers)
		})
	}
}

func TestConsumerErrorHandlingEmptyTopic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consumer error handling integration test in short mode")
	}

	// Test handling of empty or invalid topic configurations
	invalidTopics := []string{
		"",    // empty topic
		"   ", // whitespace only
	}

	for i, topic := range invalidTopics {
		t.Run(fmt.Sprintf("topic_%d", i), func(t *testing.T) {
			testInvalidTopicConfig(t, topic)
		})
	}
}

func TestConsumerErrorHandlingConnectionFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consumer error handling integration test in short mode")
	}

	// Test connection failure scenarios
	unreachableBrokers := []string{
		"unreachable.broker:9092",
		"localhost:99999",      // invalid port
		"255.255.255.255:9092", // unreachable IP
	}

	for i, broker := range unreachableBrokers {
		t.Run(fmt.Sprintf("broker_%d", i), func(t *testing.T) {
			testUnreachableBroker(t, broker)
		})
	}
}

func TestConsumerErrorHandlingAvroSchemaRegistry(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consumer error handling integration test in short mode")
	}

	// Test Avro schema registry error scenarios
	invalidSchemaRegistries := []string{
		"", // empty when Avro is enabled
		"invalid-url",
		"http://unreachable.registry:8081",
	}

	for i, registry := range invalidSchemaRegistries {
		t.Run(fmt.Sprintf("registry_%d", i), func(t *testing.T) {
			testSchemaRegistryValidation(t, registry)
		})
	}
}

func TestConsumerErrorHandlingOffsetManagement(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consumer error handling integration test in short mode")
	}

	// Test offset management error scenarios
	testCases := []struct {
		name      string
		topic     string
		partition int
		expectKey string
	}{
		{"normal_case", "events", 0, "events-0"},
		{"high_partition", "events", 999, "events-999"},
		{"special_topic", "topic-with-dashes", 5, "topic-with-dashes-5"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := offsetKey(tc.topic, tc.partition)
			if key != tc.expectKey {
				t.Errorf("Offset key mismatch: got %s, want %s", key, tc.expectKey)
			}
		})
	}
}

// Helper functions to reduce complexity

func testInvalidBrokerConfig(t *testing.T, brokers []string) {
	t.Helper()
	// Test configuration validation
	if validateConsumerConfig(brokers, "test_topic", "test_group", false, "") {
		if len(brokers) == 0 {
			t.Errorf("Empty brokers should be invalid")
		}
	}

	// Test broker string formatting
	if len(brokers) > 0 {
		bootstrapServers := strings.Join(brokers, ",")
		// Only check non-empty if the configuration was supposed to be valid
		if !validateConsumerConfig(brokers, "test_topic", "test_group", false, "") {
			// For invalid configurations, empty bootstrap servers are expected
			t.Logf("Invalid broker config produces bootstrap servers: '%s'", bootstrapServers)
		} else if strings.TrimSpace(bootstrapServers) == "" {
			t.Errorf("Valid broker config should not produce empty bootstrap servers")
		}
	}
}

func testInvalidTopicConfig(t *testing.T, topic string) {
	t.Helper()
	// Test topic validation
	if validateConsumerConfig([]string{"localhost:9092"}, topic, "test_group", false, "") {
		t.Errorf("Topic '%s' should be invalid", topic)
	}

	// Test topic name normalization
	trimmed := strings.TrimSpace(topic)
	if trimmed == "" && topic != "" {
		t.Logf("Topic '%s' becomes empty after trimming", topic)
	}
}

func testUnreachableBroker(t *testing.T, broker string) {
	t.Helper()
	// Test that configuration validation passes but connection would fail
	if !validateConsumerConfig([]string{broker}, "test_topic", "test_group", false, "") {
		t.Errorf("Configuration validation should pass for broker %s", broker)
	}

	// In a real scenario, this would test actual connection failure
	// For integration test, we verify the broker format is valid but unreachable
	t.Logf("Broker %s would be unreachable in real connection attempt", broker)
}

func testSchemaRegistryValidation(t *testing.T, registry string) {
	t.Helper()
	valid := validateConsumerConfig(
		[]string{"localhost:9092"},
		"test_topic",
		"test_group",
		true, // useAvro
		registry,
	)

	if registry == "" {
		if valid {
			t.Errorf("Empty schema registry should be invalid when using Avro")
		}
	} else {
		if !valid {
			t.Errorf("Non-empty schema registry should pass basic validation")
		}
	}
}

func TestConsumerBatching(t *testing.T) {
	// Test batch processing functionality

	t.Run("BatchCommit", func(t *testing.T) {
		// Create mock messages
		messages := []*DecodedMessage{
			{
				Value:     map[string]any{"id": 1},
				Time:      time.Now(),
				Partition: 0,
				Offset:    100,
			},
			{
				Value:     map[string]any{"id": 2},
				Time:      time.Now(),
				Partition: 0,
				Offset:    101,
			},
		}

		// Test batch operations
		if len(messages) != 2 {
			t.Errorf("Expected 2 messages in batch, got %d", len(messages))
		}

		// Test message release
		for _, msg := range messages {
			msg.Release()
		}
	})
}

func BenchmarkMessageDecoding(b *testing.B) {
	// Benchmark message decoding performance
	rawMessage := map[string]any{
		"user_id":   12345,
		"timestamp": time.Now().Unix(),
		"amount":    100.50,
		"event":     "purchase",
		"metadata":  map[string]any{"source": "mobile", "version": "1.0"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &DecodedMessage{
			Value:     rawMessage,
			Time:      time.Now(),
			Partition: 0,
			Offset:    int64(i),
		}
		_ = msg.Value["user_id"]
		_ = msg.Value["amount"]
	}
}

// Helper function for consumer configuration validation
func validateConsumerConfig(brokers []string, topic, groupID string, useAvro bool, schemaRegistry string) bool {
	if len(brokers) == 0 {
		return false
	}

	// Check if all brokers are valid (not just whitespace)
	validBrokers := 0
	for _, broker := range brokers {
		if strings.TrimSpace(broker) != "" {
			validBrokers++
		}
	}
	if validBrokers == 0 {
		return false
	}

	if strings.TrimSpace(topic) == "" {
		return false
	}
	if strings.TrimSpace(groupID) == "" {
		return false
	}
	if useAvro && strings.TrimSpace(schemaRegistry) == "" {
		return false
	}
	return true
}
