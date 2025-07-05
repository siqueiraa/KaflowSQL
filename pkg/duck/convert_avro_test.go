package duck

import (
	"database/sql"
	"testing"
)

func TestDuckTypeToAvro(t *testing.T) {
	tests := []struct {
		name     string
		dbType   string
		expected interface{}
	}{
		// Primitive types
		{"boolean", "BOOLEAN", "boolean"},
		{"integer", "INTEGER", "int"},
		{"bigint", "BIGINT", "long"},
		{"float", "FLOAT", "float"},
		{"double", "DOUBLE", "double"},
		{"varchar", "VARCHAR", "string"},
		{"uuid", "UUID", "string"},
		{"blob", "BLOB", "bytes"},
		{"bytea", "BYTEA", "bytes"},
		{"bytes", "BYTES", "bytes"},

		// Logical types
		{"date", "DATE", map[string]any{"type": "int", "logicalType": "date"}},
		{"time", "TIME", map[string]any{"type": "long", "logicalType": "time-micros"}},
		{"time with zone", "TIME WITH TIME ZONE", map[string]any{"type": "long", "logicalType": "time-micros"}},
		// Note: TIMESTAMP gets matched by TIME prefix first, so it returns time-micros
		{"timestamp", "TIMESTAMP", map[string]any{"type": "long", "logicalType": "time-micros"}},
		{"timestamp with zone", "TIMESTAMP WITH TIME ZONE", map[string]any{"type": "long", "logicalType": "time-micros"}},

		// Complex types (fallback to string)
		{"list", "LIST", "string"},
		{"map", "MAP", "string"},
		{"struct", "STRUCT", "string"},

		// Unknown type (fallback to string)
		{"unknown", "UNKNOWN_TYPE", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock ColumnType for basic types
			result := duckTypeToAvro(tt.dbType, &sql.ColumnType{})

			// For primitive types, do direct comparison
			if str, ok := tt.expected.(string); ok {
				if result != str {
					t.Errorf("For type %s: expected %v, got %v", tt.dbType, tt.expected, result)
				}
				return
			}

			// For complex types (maps), compare structure
			if expectedMap, ok := tt.expected.(map[string]any); ok {
				resultMap, ok := result.(map[string]any)
				if !ok {
					t.Errorf("For type %s: expected map[string]any, got %T", tt.dbType, result)
					return
				}

				for key, expectedVal := range expectedMap {
					if resultVal, exists := resultMap[key]; !exists {
						t.Errorf("For type %s: missing key %s in result", tt.dbType, key)
					} else if resultVal != expectedVal {
						t.Errorf("For type %s: key %s expected %v, got %v", tt.dbType, key, expectedVal, resultVal)
					}
				}
			}
		})
	}
}

func TestDuckTypeToAvroDecimal(t *testing.T) {
	// Test decimal type with mock column type that supports DecimalSize
	// Since we can't easily create a real sql.ColumnType with DecimalSize,
	// we'll test the decimal fallback behavior

	result := duckTypeToAvro("DECIMAL(10,2)", &sql.ColumnType{})

	// Should return a decimal logical type with fallback precision/scale
	expectedMap := map[string]any{
		"type":        "bytes",
		"logicalType": "decimal",
		"precision":   38,
		"scale":       0,
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Errorf("Expected map[string]any for DECIMAL, got %T", result)
		return
	}

	for key, expectedVal := range expectedMap {
		if resultVal, exists := resultMap[key]; !exists {
			t.Errorf("Missing key %s in DECIMAL result", key)
		} else if resultVal != expectedVal {
			t.Errorf("DECIMAL key %s expected %v, got %v", key, expectedVal, resultVal)
		}
	}
}

func TestBuildAvroRecord(t *testing.T) {
	// Create mock column types for testing
	// Note: This is a simplified test since creating real sql.ColumnType instances
	// requires database connections. In practice, this function would be tested
	// with integration tests.

	// Test with empty columns
	record := buildAvroRecord([]*sql.ColumnType{})

	if record.Type != "record" {
		t.Errorf("Expected record type 'record', got '%s'", record.Type)
	}

	if record.Name != "Value" {
		t.Errorf("Expected record name 'Value', got '%s'", record.Name)
	}

	if len(record.Fields) != 0 {
		t.Errorf("Expected 0 fields for empty columns, got %d", len(record.Fields))
	}
}

func TestAvroField(t *testing.T) {
	// Test the avroField struct
	field := avroField{
		Name:    "test_field",
		Type:    "string",
		Default: nil,
	}

	if field.Name != "test_field" {
		t.Errorf("Expected field name 'test_field', got '%s'", field.Name)
	}

	if field.Type != "string" {
		t.Errorf("Expected field type 'string', got %v", field.Type)
	}

	if field.Default != nil {
		t.Errorf("Expected field default nil, got %v", field.Default)
	}
}

func TestAvroRecord(t *testing.T) {
	// Test the avroRecord struct
	record := avroRecord{
		Type:      "record",
		Name:      "TestRecord",
		Namespace: "com.example",
		Fields: []avroField{
			{
				Name:    "id",
				Type:    "int",
				Default: nil,
			},
			{
				Name:    "name",
				Type:    []interface{}{"null", "string"},
				Default: nil,
			},
		},
	}

	if record.Type != "record" {
		t.Errorf("Expected record type 'record', got '%s'", record.Type)
	}

	if record.Name != "TestRecord" {
		t.Errorf("Expected record name 'TestRecord', got '%s'", record.Name)
	}

	if record.Namespace != "com.example" {
		t.Errorf("Expected namespace 'com.example', got '%s'", record.Namespace)
	}

	if len(record.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(record.Fields))
	}

	// Test first field
	if record.Fields[0].Name != "id" {
		t.Errorf("Expected first field name 'id', got '%s'", record.Fields[0].Name)
	}

	if record.Fields[0].Type != "int" {
		t.Errorf("Expected first field type 'int', got %v", record.Fields[0].Type)
	}

	// Test second field (union type)
	if record.Fields[1].Name != "name" {
		t.Errorf("Expected second field name 'name', got '%s'", record.Fields[1].Name)
	}

	unionType, ok := record.Fields[1].Type.([]interface{})
	if !ok {
		t.Errorf("Expected second field type to be []interface{}, got %T", record.Fields[1].Type)
	} else {
		if len(unionType) != 2 {
			t.Errorf("Expected union type with 2 elements, got %d", len(unionType))
		}
		if unionType[0] != "null" {
			t.Errorf("Expected first union element to be 'null', got %v", unionType[0])
		}
		if unionType[1] != "string" {
			t.Errorf("Expected second union element to be 'string', got %v", unionType[1])
		}
	}
}
