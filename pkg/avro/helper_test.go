package avro

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNormalizeSchemaJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "simple object",
			input:    `{"name":"test","type":"record"}`,
			expected: `{"name":"test","type":"record"}`,
			wantErr:  false,
		},
		{
			name:     "different field order",
			input:    `{"type":"record","name":"test"}`,
			expected: `{"name":"test","type":"record"}`,
			wantErr:  false,
		},
		{
			name:     "nested object",
			input:    `{"name":"test","fields":[{"name":"id","type":"string"}],"type":"record"}`,
			expected: `{"fields":[{"name":"id","type":"string"}],"name":"test","type":"record"}`,
			wantErr:  false,
		},
		{
			name:    "invalid JSON",
			input:   `{"name":"test",}`,
			wantErr: true,
		},
		{
			name:     "empty object",
			input:    `{}`,
			expected: `{}`,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := normalizeSchemaJSON(tt.input)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Parse both to compare structure rather than exact string
			var expectedObj, resultObj interface{}
			if err := json.Unmarshal([]byte(tt.expected), &expectedObj); err != nil {
				t.Fatalf("Failed to parse expected JSON: %v", err)
			}
			if err := json.Unmarshal([]byte(result), &resultObj); err != nil {
				t.Fatalf("Failed to parse result JSON: %v", err)
			}

			if !compareJSON(expectedObj, resultObj) {
				t.Errorf("Normalized result doesn't match expected.\nExpected: %s\nGot: %s", tt.expected, result)
			}
		})
	}
}

func TestNormalizeSchemaJSONConsistency(t *testing.T) {
	// Test that normalizing the same schema twice produces identical results
	schema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	result1, err1 := normalizeSchemaJSON(schema)
	if err1 != nil {
		t.Fatalf("First normalization failed: %v", err1)
	}

	result2, err2 := normalizeSchemaJSON(schema)
	if err2 != nil {
		t.Fatalf("Second normalization failed: %v", err2)
	}

	if result1 != result2 {
		t.Errorf("Normalization is not consistent.\nFirst: %s\nSecond: %s", result1, result2)
	}
}

func TestNormalizeSchemaJSONDifferentOrdering(t *testing.T) {
	// Test that schemas with different field ordering normalize to the same result
	schema1 := `{"name":"User","type":"record","fields":[{"name":"id","type":"int"}]}`
	schema2 := `{"type":"record","fields":[{"name":"id","type":"int"}],"name":"User"}`

	result1, err1 := normalizeSchemaJSON(schema1)
	if err1 != nil {
		t.Fatalf("First normalization failed: %v", err1)
	}

	result2, err2 := normalizeSchemaJSON(schema2)
	if err2 != nil {
		t.Fatalf("Second normalization failed: %v", err2)
	}

	if result1 != result2 {
		t.Errorf("Different orderings should normalize to same result.\nFirst: %s\nSecond: %s", result1, result2)
	}
}

// Helper function to compare JSON objects for structural equality
func compareJSON(a, b interface{}) bool {
	aBytes, _ := json.Marshal(a)
	bBytes, _ := json.Marshal(b)
	return bytes.Equal(aBytes, bBytes)
}
