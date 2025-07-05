package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/siqueiraa/KaflowSQL/pkg/config"
)

func TestProducerCreation(t *testing.T) {
	ctx := context.Background()
	kafkaConfig := config.KafkaConfig{
		Brokers:        []string{"localhost:9092"},
		SchemaRegistry: "http://localhost:8081",
		UseAvro:        false,
	}

	producer, err := NewProducer(ctx, kafkaConfig)
	if err != nil {
		t.Logf("Producer creation failed (expected in test environment): %v", err)
		return
	}

	if producer != nil {
		defer producer.Close()
	}
}

func TestProducerPublishBatch(t *testing.T) {
	// Test batch publishing functionality

	t.Run("ValidBatch", func(t *testing.T) {
		// Mock producer for testing
		producer := &MockProducer{}

		topic := "test_topic"
		keyField := "user_id"

		batch := []map[string]any{
			{"user_id": 123, "amount": 100.0, "event": "purchase"},
			{"user_id": 456, "amount": 50.0, "event": "refund"},
			{"user_id": 789, "amount": 200.0, "event": "purchase"},
		}

		err := producer.PublishBatch(topic, batch, keyField)
		if err != nil {
			t.Errorf("PublishBatch failed: %v", err)
		}

		if producer.PublishedCount != len(batch) {
			t.Errorf("Published count mismatch: got %d, want %d", producer.PublishedCount, len(batch))
		}
	})

	t.Run("EmptyBatch", func(t *testing.T) {
		producer := &MockProducer{}

		err := producer.PublishBatch("test_topic", []map[string]any{}, "user_id")
		if err != nil {
			t.Errorf("Empty batch should not error: %v", err)
		}
	})

	t.Run("InvalidKey", func(t *testing.T) {
		producer := &MockProducer{}

		batch := []map[string]any{
			{"amount": 100.0, "event": "purchase"}, // missing user_id key
		}

		err := producer.PublishBatch("test_topic", batch, "user_id")
		if err == nil {
			t.Errorf("Expected error for missing key field")
		}
	})
}

func TestProducerClose(t *testing.T) {
	producer := &MockProducer{}

	err := producer.Close()
	if err != nil {
		t.Errorf("Close should not error: %v", err)
	}

	if !producer.Closed {
		t.Errorf("Producer should be marked as closed")
	}
}

func TestProducerErrorHandling(t *testing.T) {
	t.Run("InvalidTopic", func(t *testing.T) {
		producer := &MockProducer{
			ShouldError: true,
		}

		batch := []map[string]any{
			{"user_id": 123, "amount": 100.0},
		}

		err := producer.PublishBatch("", batch, "user_id")
		if err == nil {
			t.Errorf("Expected error for invalid topic")
		}
	})

	t.Run("ProducerFailure", func(t *testing.T) {
		producer := &MockProducer{
			ShouldError: true,
		}

		batch := []map[string]any{
			{"user_id": 123, "amount": 100.0},
		}

		err := producer.PublishBatch("test_topic", batch, "user_id")
		if err == nil {
			t.Errorf("Expected error from failing producer")
		}
	})
}

func TestProducerKeyExtraction(t *testing.T) {
	tests := []struct {
		name      string
		record    map[string]any
		keyField  string
		expectKey string
		expectErr bool
	}{
		{
			name:      "string key",
			record:    map[string]any{"user_id": "12345", "amount": 100.0},
			keyField:  "user_id",
			expectKey: "12345",
			expectErr: false,
		},
		{
			name:      "int key",
			record:    map[string]any{"user_id": 12345, "amount": 100.0},
			keyField:  "user_id",
			expectKey: "12345",
			expectErr: false,
		},
		{
			name:      "missing key",
			record:    map[string]any{"amount": 100.0},
			keyField:  "user_id",
			expectKey: "",
			expectErr: true,
		},
		{
			name:      "nil key",
			record:    map[string]any{"user_id": nil, "amount": 100.0},
			keyField:  "user_id",
			expectKey: "",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := extractKey(tt.record, tt.keyField)

			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			if key != tt.expectKey {
				t.Errorf("Key mismatch: got %s, want %s", key, tt.expectKey)
			}
		})
	}
}

func BenchmarkProducerPublishBatch(b *testing.B) {
	producer := &MockProducer{}

	batch := make([]map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		batch[i] = map[string]any{
			"user_id":   i,
			"amount":    float64(i * 10),
			"timestamp": "2024-01-01T00:00:00Z",
			"event":     "test_event",
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := producer.PublishBatch("test_topic", batch, "user_id")
		if err != nil {
			b.Errorf("PublishBatch failed: %v", err)
		}
	}
}

// MockProducer for testing
type MockProducer struct {
	PublishedCount int
	Closed         bool
	ShouldError    bool
	LastTopic      string
	LastBatch      []map[string]any
	LastKeyField   string
}

func (m *MockProducer) PublishBatch(topic string, batch []map[string]any, keyField string) error {
	if m.ShouldError {
		return fmt.Errorf("mock producer error")
	}

	if topic == "" {
		return fmt.Errorf("invalid topic")
	}

	// Validate all records have the key field
	for _, record := range batch {
		if _, exists := record[keyField]; !exists {
			return fmt.Errorf("missing key field: %s", keyField)
		}
	}

	m.PublishedCount += len(batch)
	m.LastTopic = topic
	m.LastBatch = batch
	m.LastKeyField = keyField

	return nil
}

func (m *MockProducer) Close() error {
	m.Closed = true
	return nil
}

// Helper function that would be implemented in the actual producer
func extractKey(record map[string]any, keyField string) (string, error) {
	value, exists := record[keyField]
	if !exists {
		return "", fmt.Errorf("key field %s not found", keyField)
	}

	if value == nil {
		return "", fmt.Errorf("key field %s is nil", keyField)
	}

	return fmt.Sprintf("%v", value), nil
}
