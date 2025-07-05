package schema

import (
	"reflect"
	"testing"
	"time"
)

func TestNewSchemaManager(t *testing.T) {
	sm := NewSchemaManager()

	if sm == nil {
		t.Errorf("Expected non-nil SchemaManager")
		return
	}

	if sm.schemas == nil {
		t.Errorf("Expected initialized schemas map")
		return
	}

	if len(sm.schemas) != 0 {
		t.Errorf("Expected empty schemas map, got %d entries", len(sm.schemas))
	}
}

func TestSchemaManagerInferSchema(t *testing.T) {
	sm := NewSchemaManager()

	tests := []struct {
		name     string
		record   map[string]any
		expected map[string]string
	}{
		{
			name: "basic types",
			record: map[string]any{
				"id":     123,
				"name":   "John",
				"active": true,
				"score":  45.5,
			},
			expected: map[string]string{
				"active": "bool",
				"id":     "int",
				"name":   "string",
				"score":  "float",
			},
		},
		{
			name: "float without decimal as int",
			record: map[string]any{
				"count": 10.0,
				"rate":  3.14,
			},
			expected: map[string]string{
				"count": "int",
				"rate":  "float",
			},
		},
		{
			name: "timestamp types",
			record: map[string]any{
				"created_at": time.Now(),
				"iso_time":   "2023-01-01T12:00:00Z",
				"regular":    "not a timestamp",
			},
			expected: map[string]string{
				"created_at": "timestamp",
				"iso_time":   "timestamp",
				"regular":    "string",
			},
		},
		{
			name: "complex types",
			record: map[string]any{
				"tags":     []string{"a", "b"},
				"data":     []byte{1, 2, 3},
				"metadata": map[string]string{"key": "value"},
				"config":   struct{ Name string }{Name: "test"},
			},
			expected: map[string]string{
				"config":   "struct",
				"data":     "bytes",
				"metadata": "map",
				"tags":     "array",
			},
		},
		{
			name: "nil values",
			record: map[string]any{
				"nullable": nil,
			},
			expected: map[string]string{
				"nullable": "null",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema := sm.InferSchema(tt.record)

			if len(schema.Types) != len(tt.expected) {
				t.Errorf("Expected %d types, got %d", len(tt.expected), len(schema.Types))
			}

			for field, expectedType := range tt.expected {
				if actualType, exists := schema.Types[field]; !exists {
					t.Errorf("Expected field %s not found", field)
				} else if actualType != expectedType {
					t.Errorf("Field %s: expected type %s, got %s", field, expectedType, actualType)
				}
			}

			// Test field ordering (should be alphabetical)
			if len(schema.FieldOrder) != len(tt.record) {
				t.Errorf("Expected field order length %d, got %d", len(tt.record), len(schema.FieldOrder))
			}

			// Verify alphabetical order
			for i := 1; i < len(schema.FieldOrder); i++ {
				if schema.FieldOrder[i-1] >= schema.FieldOrder[i] {
					t.Errorf("Field order not alphabetical: %s >= %s", schema.FieldOrder[i-1], schema.FieldOrder[i])
				}
			}
		})
	}
}

func TestSchemaManagerIsSchemaDifferent(t *testing.T) {
	sm := NewSchemaManager()

	schema1 := TableSchema{
		Types: map[string]string{
			"id":   "int",
			"name": "string",
		},
		FieldOrder: []string{"id", "name"},
	}

	schema2 := TableSchema{
		Types: map[string]string{
			"id":   "int",
			"name": "string",
		},
		FieldOrder: []string{"id", "name"},
	}

	schema3 := TableSchema{
		Types: map[string]string{
			"id":    "int",
			"name":  "string",
			"email": "string",
		},
		FieldOrder: []string{"id", "name", "email"},
	}

	schema4 := TableSchema{
		Types: map[string]string{
			"id":   "string", // different type
			"name": "string",
		},
		FieldOrder: []string{"id", "name"},
	}

	tests := []struct {
		name     string
		old      TableSchema
		new      TableSchema
		expected bool
	}{
		{"identical schemas", schema1, schema2, false},
		{"different field count", schema1, schema3, true},
		{"different field type", schema1, schema4, true},
		{"empty vs filled", TableSchema{}, schema1, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sm.IsSchemaDifferent(tt.old, tt.new)
			if result != tt.expected {
				t.Errorf("IsSchemaDifferent() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSchemaManagerUpdateAndGet(t *testing.T) {
	sm := NewSchemaManager()

	schema := TableSchema{
		Types: map[string]string{
			"id":   "int",
			"name": "string",
		},
		FieldOrder: []string{"id", "name"},
	}

	// Test getting non-existent schema
	empty := sm.GetSchemaForTable("nonexistent")
	if len(empty.Types) != 0 {
		t.Errorf("Expected empty schema for nonexistent table, got %d types", len(empty.Types))
	}

	// Test update and get
	sm.Update("users", schema)
	retrieved := sm.GetSchemaForTable("users")

	if len(retrieved.Types) != len(schema.Types) {
		t.Errorf("Retrieved schema has different type count: got %d, want %d", len(retrieved.Types), len(schema.Types))
	}

	for field, expectedType := range schema.Types {
		if actualType, exists := retrieved.Types[field]; !exists {
			t.Errorf("Field %s not found in retrieved schema", field)
		} else if actualType != expectedType {
			t.Errorf("Field %s: expected type %s, got %s", field, expectedType, actualType)
		}
	}

	if len(retrieved.FieldOrder) != len(schema.FieldOrder) {
		t.Errorf("Retrieved schema has different field order length: got %d, want %d", len(retrieved.FieldOrder), len(schema.FieldOrder))
	}
}

func TestGoTypeToString(t *testing.T) {
	tests := []struct {
		name     string
		value    any
		expected string
	}{
		// Nil values
		{"nil value", nil, "null"},
		{"nil pointer", (*int)(nil), "null"},

		// Basic types
		{"string", "hello", "string"},
		{"int", 123, "int"},
		{"int64", int64(123), "int"},
		{"uint", uint(123), "int"},
		{"bool true", true, "bool"},
		{"bool false", false, "bool"},

		// Float handling
		{"float without decimal", 10.0, "int"},
		{"float with decimal", 3.14, "float"},
		{"float32", float32(2.5), "float"},

		// Time types
		{"time.Time", time.Now(), "timestamp"},
		{"RFC3339 string", "2023-01-01T12:00:00Z", "timestamp"},
		{"non-timestamp string", "hello world", "string"},

		// Byte slices
		{"byte slice", []byte{1, 2, 3}, "bytes"},
		{"string slice", []string{"a", "b"}, "array"},

		// Complex types
		{"map", map[string]int{"a": 1}, "map"},
		{"struct", struct{ Name string }{Name: "test"}, "struct"},

		// Pointers
		{"pointer to int", func() *int { i := 42; return &i }(), "int"},
		{"pointer to string", func() *string { s := "hello"; return &s }(), "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reflectType reflect.Type
			if tt.value != nil {
				reflectType = reflect.TypeOf(tt.value)
			}

			result := goTypeToString(reflectType, tt.value)
			if result != tt.expected {
				t.Errorf("goTypeToString() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestIsRFC3339(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"valid RFC3339", "2023-01-01T12:00:00Z", true},
		{"valid RFC3339 with offset", "2023-01-01T12:00:00+02:00", true},
		{"valid RFC3339 with nanoseconds", "2023-01-01T12:00:00.123456Z", true},
		{"invalid format", "2023-01-01 12:00:00", false},
		{"empty string", "", false},
		{"random string", "not a timestamp", false},
		{"partial date", "2023-01-01", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRFC3339(tt.input)
			if result != tt.expected {
				t.Errorf("isRFC3339(%s) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTableSchema(t *testing.T) {
	schema := TableSchema{
		Types: map[string]string{
			"id":   "int",
			"name": "string",
		},
		FieldOrder: []string{"id", "name"},
	}

	// Test that the struct fields are accessible
	if len(schema.Types) != 2 {
		t.Errorf("Expected 2 types, got %d", len(schema.Types))
	}

	if len(schema.FieldOrder) != 2 {
		t.Errorf("Expected 2 fields in order, got %d", len(schema.FieldOrder))
	}

	if schema.Types["id"] != "int" {
		t.Errorf("Expected id type 'int', got '%s'", schema.Types["id"])
	}

	if schema.FieldOrder[0] != "id" {
		t.Errorf("Expected first field 'id', got '%s'", schema.FieldOrder[0])
	}
}

func TestSchemaManagerConcurrency(t *testing.T) {
	sm := NewSchemaManager()

	schema := TableSchema{
		Types: map[string]string{
			"id": "int",
		},
		FieldOrder: []string{"id"},
	}

	// Test concurrent access
	done := make(chan bool, 2)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			sm.Update("test", schema)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			sm.GetSchemaForTable("test")
		}
		done <- true
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Verify final state
	retrieved := sm.GetSchemaForTable("test")
	if len(retrieved.Types) != 1 {
		t.Errorf("Expected 1 type after concurrent access, got %d", len(retrieved.Types))
	}
}
