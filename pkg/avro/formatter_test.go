package avro

import (
	"testing"

	havro "github.com/hamba/avro/v2"
)

func TestMapLogicalOrPrimitive(t *testing.T) {
	tests := []struct {
		name     string
		logType  string
		expected string
	}{
		// Primitive types
		{"string primitive", "string", "string"},
		{"boolean primitive", "boolean", "bool"},
		{"int primitive", "int", "int32"},
		{"long primitive", "long", "int64"},
		{"float primitive", "float", "float32"},
		{"double primitive", "double", "float64"},
		{"bytes primitive", "bytes", "bytes"},

		// Logical types
		{"uuid logical", "uuid", "uuid"},
		{"date logical", "date", "date"},
		{"time-millis logical", "time-millis", "time"},
		{"time-micros logical", "time-micros", "time"},
		{"timestamp-millis logical", "timestamp-millis", "timestamp"},
		{"timestamp-micros logical", "timestamp-micros", "timestamp"},
		{"local-timestamp-millis logical", "local-timestamp-millis", "timestamp"},
		{"duration logical", "duration", "interval"},

		// Unknown type fallback
		{"unknown type", "unknown", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock schema for testing - we'll use a simple string schema since
			// mapLogicalOrPrimitive doesn't deeply inspect the schema structure
			schema, err := havro.Parse(`"string"`)
			if err != nil {
				t.Fatalf("Failed to create test schema: %v", err)
			}

			result := mapLogicalOrPrimitive(tt.logType, schema)
			if result != tt.expected {
				t.Errorf("mapLogicalOrPrimitive(%s) = %s, want %s", tt.logType, result, tt.expected)
			}
		})
	}
}

func TestPropAsInt(t *testing.T) {
	// Test the propAsInt function with a mock PropertySchema
	// Since the havro library's property handling is complex, we'll test the function logic
	tests := []struct {
		name     string
		value    interface{}
		expected int
		wantOK   bool
	}{
		{"int value", 10, 10, true},
		{"float64 value", 45.0, 45, true},
		{"string number", "123", 123, true},
		{"invalid string", "not_a_number", 0, false},
		{"nil value", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock PropertySchema that returns our test value
			mockPS := &mockPropertySchema{props: map[string]interface{}{"testProp": tt.value}}

			result, ok := propAsInt(mockPS, "testProp")

			if ok != tt.wantOK {
				t.Errorf("propAsInt() ok = %v, want %v", ok, tt.wantOK)
			}

			if tt.wantOK && result != tt.expected {
				t.Errorf("propAsInt() = %d, want %d", result, tt.expected)
			}
		})
	}
}

// Mock PropertySchema for testing
type mockPropertySchema struct {
	props map[string]interface{}
}

func (m *mockPropertySchema) Prop(key string) interface{} {
	return m.props[key]
}

func TestDecimalDDL(t *testing.T) {
	tests := []struct {
		name     string
		props    map[string]interface{}
		expected string
	}{
		{
			name: "decimal with precision and scale",
			props: map[string]interface{}{
				"precision": 10,
				"scale":     2,
			},
			expected: "decimal(10,2)",
		},
		{
			name: "decimal with precision only",
			props: map[string]interface{}{
				"precision": 5,
			},
			expected: "decimal(5,0)",
		},
		{
			name:     "decimal without precision or scale",
			props:    map[string]interface{}{},
			expected: "decimal",
		},
		{
			name: "decimal with connect.parameters",
			props: map[string]interface{}{
				"connect.parameters": map[string]interface{}{
					"scale":                     "3",
					"connect.decimal.precision": "8",
				},
			},
			expected: "decimal(8,3)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPS := &mockPropertySchema{props: tt.props}
			result := decimalDDL(mockPS)
			if result != tt.expected {
				t.Errorf("decimalDDL() = %s, want %s", result, tt.expected)
			}
		})
	}
}

func TestWrapUnionValue(t *testing.T) {
	// Create a test union schema ["null", "string"]
	unionSchema, err := havro.Parse(`["null", "string"]`)
	if err != nil {
		t.Fatalf("Failed to create union schema: %v", err)
	}

	u, ok := unionSchema.(*havro.UnionSchema)
	if !ok {
		t.Fatalf("Expected UnionSchema, got %T", unionSchema)
	}

	tests := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{
			name:     "nil value",
			value:    nil,
			expected: nil,
		},
		{
			name:  "string value",
			value: "test",
			expected: map[string]interface{}{
				"string": "test",
			},
		},
		{
			name:  "number value",
			value: 123,
			expected: map[string]interface{}{
				"string": 123,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := wrapUnionValue(u, tt.value)

			if tt.value == nil {
				if result != nil {
					t.Errorf("Expected nil for nil input, got %v", result)
				}
				return
			}

			// For non-nil values, check the wrapper structure
			wrapper, ok := result.(map[string]interface{})
			if !ok {
				t.Errorf("Expected map[string]interface{}, got %T", result)
				return
			}

			expectedWrapper := tt.expected.(map[string]interface{})
			for key, expectedVal := range expectedWrapper {
				if actualVal, exists := wrapper[key]; !exists {
					t.Errorf("Expected key %s not found in result", key)
				} else if actualVal != expectedVal {
					t.Errorf("For key %s: expected %v, got %v", key, expectedVal, actualVal)
				}
			}
		})
	}
}

func TestWrapUnionValueWithBytes(t *testing.T) {
	// Create a bytes union schema ["null", "bytes"]
	unionSchema, err := havro.Parse(`["null", "bytes"]`)
	if err != nil {
		t.Fatalf("Failed to create bytes union schema: %v", err)
	}

	u, ok := unionSchema.(*havro.UnionSchema)
	if !ok {
		t.Fatalf("Expected UnionSchema, got %T", unionSchema)
	}

	// Test with byte slice
	data := []byte{1, 2, 3, 4, 5}

	result := wrapUnionValue(u, data)

	wrapper, ok := result.(map[string]interface{})
	if !ok {
		t.Errorf("Expected map[string]interface{}, got %T", result)
		return
	}

	if _, exists := wrapper["bytes"]; !exists {
		t.Errorf("Expected 'bytes' key in wrapper, got keys: %v", getKeys(wrapper))
	}

	// Verify the payload
	if payload, exists := wrapper["bytes"]; exists {
		if payloadBytes, ok := payload.([]byte); !ok {
			t.Errorf("Expected []byte for bytes payload, got %T", payload)
		} else if len(payloadBytes) != len(data) {
			t.Errorf("Expected payload length %d, got %d", len(data), len(payloadBytes))
		}
	}
}

// Helper function to get map keys for testing
func getKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
