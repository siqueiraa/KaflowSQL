package duck

import (
	"testing"
	"time"
)

func TestNewBuffer(t *testing.T) {
	buffer := NewBuffer("test_buffer")

	if buffer.Name != "test_buffer" {
		t.Errorf("Expected buffer name 'test_buffer', got '%s'", buffer.Name)
	}

	if buffer.Size() != 0 {
		t.Errorf("Expected empty buffer, got size %d", buffer.Size())
	}

	if buffer.totalRecords != 0 {
		t.Errorf("Expected totalRecords to be 0, got %d", buffer.totalRecords)
	}

	if buffer.flushCount != 0 {
		t.Errorf("Expected flushCount to be 0, got %d", buffer.flushCount)
	}
}

func TestBufferAdd(t *testing.T) {
	buffer := NewBuffer("test_buffer")

	record1 := Record{
		Table:     "users",
		Data:      map[string]any{"id": 1, "name": "John"},
		UniqueKey: "users:1",
	}

	record2 := Record{
		Table:     "users",
		Data:      map[string]any{"id": 2, "name": "Jane"},
		UniqueKey: "users:2",
	}

	// Add first record
	buffer.Add(record1)
	if buffer.Size() != 1 {
		t.Errorf("Expected buffer size 1 after adding first record, got %d", buffer.Size())
	}

	// Add second record
	buffer.Add(record2)
	if buffer.Size() != 2 {
		t.Errorf("Expected buffer size 2 after adding second record, got %d", buffer.Size())
	}

	// Add duplicate record (should be ignored)
	buffer.Add(record1)
	if buffer.Size() != 2 {
		t.Errorf("Expected buffer size to remain 2 after adding duplicate, got %d", buffer.Size())
	}
}

func TestBufferFlush(t *testing.T) {
	buffer := NewBuffer("test_buffer")

	record1 := Record{
		Table:     "users",
		Data:      map[string]any{"id": 1, "name": "John"},
		UniqueKey: "users:1",
	}

	record2 := Record{
		Table:     "users",
		Data:      map[string]any{"id": 2, "name": "Jane"},
		UniqueKey: "users:2",
	}

	// Test flush on empty buffer
	flushed := buffer.Flush()
	if flushed != nil {
		t.Errorf("Expected nil from flushing empty buffer, got %v", flushed)
	}

	// Add records and flush
	buffer.Add(record1)
	buffer.Add(record2)

	if buffer.Size() != 2 {
		t.Errorf("Expected buffer size 2 before flush, got %d", buffer.Size())
	}

	flushed = buffer.Flush()

	if len(flushed) != 2 {
		t.Errorf("Expected 2 flushed records, got %d", len(flushed))
	}

	if buffer.Size() != 0 {
		t.Errorf("Expected buffer size 0 after flush, got %d", buffer.Size())
	}

	// Test that duplicate detection is reset after flush
	buffer.Add(record1) // This should succeed now since buffer was flushed
	if buffer.Size() != 1 {
		t.Errorf("Expected buffer size 1 after adding record post-flush, got %d", buffer.Size())
	}
}

func TestBufferConcurrency(t *testing.T) {
	buffer := NewBuffer("concurrent_test")

	// This is a basic test - in a real scenario we'd use goroutines
	// But for basic unit testing, we'll just verify the methods work correctly
	record := Record{
		Table:     "test",
		Data:      map[string]any{"id": 1},
		UniqueKey: "test:1",
	}

	// Test that multiple operations work correctly
	buffer.Add(record)
	size := buffer.Size()
	flushed := buffer.Flush()

	if size != 1 {
		t.Errorf("Expected size 1, got %d", size)
	}

	if len(flushed) != 1 {
		t.Errorf("Expected 1 flushed record, got %d", len(flushed))
	}
}

func TestBufferMetrics(t *testing.T) {
	buffer := NewBuffer("metrics_test")

	record := Record{
		Table:     "test",
		Data:      map[string]any{"id": 1},
		UniqueKey: "test:1",
	}

	buffer.Add(record)

	// Metrics() should not panic and should update internal counters
	// We can't easily test the log output, but we can verify it doesn't crash
	buffer.Metrics()

	// Verify internal state is still correct
	if buffer.Size() != 1 {
		t.Errorf("Expected buffer size 1 after Metrics(), got %d", buffer.Size())
	}
}

func TestRecord(t *testing.T) {
	record := Record{
		Table:     "users",
		Data:      map[string]any{"id": 123, "name": "Alice", "active": true},
		UniqueKey: "users:123",
	}

	// Test that record fields are correctly set
	if record.Table != "users" {
		t.Errorf("Expected table 'users', got '%s'", record.Table)
	}

	if record.UniqueKey != "users:123" {
		t.Errorf("Expected unique key 'users:123', got '%s'", record.UniqueKey)
	}

	if len(record.Data) != 3 {
		t.Errorf("Expected 3 data fields, got %d", len(record.Data))
	}

	// Test data access
	if id, ok := record.Data["id"]; !ok || id != 123 {
		t.Errorf("Expected id=123, got %v", id)
	}

	if name, ok := record.Data["name"]; !ok || name != "Alice" {
		t.Errorf("Expected name=Alice, got %v", name)
	}

	if active, ok := record.Data["active"]; !ok || active != true {
		t.Errorf("Expected active=true, got %v", active)
	}
}

func TestBufferTiming(t *testing.T) {
	buffer := NewBuffer("timing_test")

	startTime := time.Now()

	record := Record{
		Table:     "test",
		Data:      map[string]any{"id": 1},
		UniqueKey: "test:1",
	}

	buffer.Add(record)

	// Verify that lastInsert time was updated
	if buffer.lastInsert.Before(startTime) {
		t.Errorf("lastInsert time should be after start time")
	}

	flushTime := time.Now()
	buffer.Flush()

	// Verify that lastFlush time was updated
	if buffer.lastFlush.Before(flushTime) {
		t.Errorf("lastFlush time should be after flush time")
	}
}
