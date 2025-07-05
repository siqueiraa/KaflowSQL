package duck

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// Constants for DuckDB and Avro type names
const (
	duckBooleanType = "BOOLEAN"
	duckIntegerType = "INTEGER"
	duckBigintType  = "BIGINT"
	duckFloatType   = "FLOAT"
	duckDoubleType  = "DOUBLE"
	duckBlobType    = "BLOB"
	duckDateType    = "DATE"
	duckUUIDType    = "UUID"
	duckVarcharType = "VARCHAR"

	avroStringType  = "string"
	avroIntType     = "int"
	avroLongType    = "long"
	avroBooleanType = "boolean"
	avroFloatType   = "float"
	avroDoubleType  = "double"
	avroBytesType   = "bytes"

	// Decimal defaults
	defaultDecimalPrecision = 38
	defaultDecimalScale     = 0
)

// --------------- Public API -------------------------------------------

// GenerateAvroSchema runs the given SELECT (it must return zero rows),
// inspects the column metadata and builds a JSON Avro schema ready to
// be registered in a schema-registry.
//
// subject ── the record/subject name for the registry
// query   ── a “zero-row” SELECT such as `SELECT * FROM foo WHERE 1=0`
func (e *DBEngine) GenerateAvroSchema(
	_ context.Context,
	_, query string,
) (string, error) {
	rows, err := e.ExecuteSQL(query)
	if err != nil {
		return "", fmt.Errorf("duck: select failed: %w", err)
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return "", fmt.Errorf("duck: column types: %w", err)
	}

	// Check for any iteration errors
	if rowsErr := rows.Err(); rowsErr != nil {
		return "", fmt.Errorf("duck: row iteration error: %w", rowsErr)
	}

	rec := buildAvroRecord(columnTypes)

	out, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("duck: marshal avro: %w", err)
	}
	return string(out), nil
}

// --------------- Avro record builder ----------------------------------

type avroField struct {
	Name    string      `json:"name"`
	Type    any         `json:"type"`    // string or complex type
	Default interface{} `json:"default"` // always present (null)

}

type avroRecord struct {
	Type      string      `json:"type"` // always "record"
	Name      string      `json:"name"`
	Namespace string      `json:"namespace,omitempty"`
	Fields    []avroField `json:"fields"`
}

// buildAvroRecord converts sql.ColumnType metadata into an avroRecord.
func buildAvroRecord(cols []*sql.ColumnType) *avroRecord {
	fields := make([]avroField, 0, len(cols))

	for _, c := range cols {
		dbType := strings.ToUpper(c.DatabaseTypeName())
		fieldName := c.Name()

		baseType := duckTypeToAvro(dbType, c)

		// Always nullable + default null
		unionType := []interface{}{"null", baseType}

		fields = append(fields, avroField{
			Name:    fieldName,
			Type:    unionType,
			Default: nil, // <- required when first branch is "null"
		})
	}

	return &avroRecord{
		Type:   "record",
		Name:   "Value",
		Fields: fields,
	}
}

// --------------- DuckDB → Avro type mapping ---------------------------

// duckTypeToAvro maps DuckDB types to Avro primitives / logical types.
func duckTypeToAvro(dbType string, ct *sql.ColumnType) any {
	// Handle primitive types
	if primitive := mapPrimitiveType(dbType); primitive != nil {
		return primitive
	}

	// Handle logical types
	if logical := mapLogicalType(dbType, ct); logical != nil {
		return logical
	}

	// Handle complex types
	if complexType := mapComplexType(dbType); complexType != nil {
		return complexType
	}

	// Fallback to string
	return avroStringType
}

// mapPrimitiveType handles basic DuckDB to Avro primitive type mapping
func mapPrimitiveType(dbType string) any {
	switch dbType {
	case duckBooleanType:
		return avroBooleanType
	case duckIntegerType:
		return avroIntType
	case duckBigintType:
		return avroLongType
	case duckFloatType:
		return avroFloatType
	case duckDoubleType:
		return avroDoubleType
	case duckVarcharType, duckUUIDType:
		return avroStringType
	case duckBlobType, "BYTEA", "BYTES":
		return avroBytesType
	default:
		return nil
	}
}

// mapLogicalType handles DuckDB logical types (date, time, timestamp, decimal)
func mapLogicalType(dbType string, ct *sql.ColumnType) any {
	switch {
	case dbType == duckDateType:
		return map[string]any{
			"type":        avroIntType,
			"logicalType": "date",
		}
	case strings.HasPrefix(dbType, "TIME"):
		return map[string]any{
			"type":        avroLongType,
			"logicalType": "time-micros",
		}
	case strings.HasPrefix(dbType, "TIMESTAMP"):
		return map[string]any{
			"type":        avroLongType,
			"logicalType": "timestamp-micros",
		}
	case strings.HasPrefix(dbType, "DECIMAL"):
		return createDecimalType(ct)
	default:
		return nil
	}
}

// mapComplexType handles complex DuckDB types (LIST, MAP, STRUCT)
func mapComplexType(dbType string) any {
	if strings.HasPrefix(dbType, "LIST") ||
		strings.HasPrefix(dbType, "MAP") ||
		strings.HasPrefix(dbType, "STRUCT") {
		return avroStringType // Serialize as JSON string for simplicity
	}
	return nil
}

// createDecimalType creates an Avro decimal logical type with precision/scale
func createDecimalType(ct *sql.ColumnType) map[string]any {
	if prec, scale, ok := ct.DecimalSize(); ok {
		return map[string]any{
			"type":        avroBytesType,
			"logicalType": "decimal",
			"precision":   prec,
			"scale":       scale,
		}
	}
	// fallback with default precision/scale
	return map[string]any{
		"type":        avroBytesType,
		"logicalType": "decimal",
		"precision":   defaultDecimalPrecision,
		"scale":       defaultDecimalScale,
	}
}
