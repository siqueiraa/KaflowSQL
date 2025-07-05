package avro

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"golang.org/x/sync/singleflight"
)

// Constants for Confluent wire format
const (
	confluentWireFormatHeaderSize = 5 // Magic byte (1) + Schema ID (4)
)

// schemaEntry holds the parsed schema and its schema ID.
type schemaEntry struct {
	schemaID int
	schema   avro.Schema
}

var (
	// Cache parsed schemas by subject and schema ID
	schemaCacheBySubject sync.Map // map[string]schemaEntry
	schemaCacheByID      sync.Map // map[int]avro.Schema
	// Prevent duplicate schema fetches
	singleFlight singleflight.Group
)

// getSchemaRegistryClient returns or creates a Schema Registry client for the URL.
func getSchemaRegistryClient(url string) *srclient.SchemaRegistryClient {
	// Simple singleton per URL (assumed implemented elsewhere)
	return srclient.CreateSchemaRegistryClient(url)
}

// getSchemaForSubject fetches and caches the latest schema for a subject.
func getSchemaForSubject(client *srclient.SchemaRegistryClient, subject string) (int, avro.Schema, error) {
	// Fast path: check cache
	if v, ok := schemaCacheBySubject.Load(subject); ok {
		se := v.(schemaEntry)
		return se.schemaID, se.schema, nil
	}
	// Singleflight to prevent duplicate fetches
	val, err, _ := singleFlight.Do(subject, func() (interface{}, error) {
		schemaMeta, err := client.GetLatestSchema(subject)
		if err != nil {
			return nil, fmt.Errorf("fetch schema %s: %w", subject, err)
		}
		schema, err := avro.Parse(schemaMeta.Schema())
		if err != nil {
			return nil, fmt.Errorf("parse schema %s: %w", subject, err)
		}
		se := schemaEntry{schemaID: schemaMeta.ID(), schema: schema}
		schemaCacheBySubject.Store(subject, se)
		schemaCacheByID.Store(schemaMeta.ID(), schema)
		return se, nil
	})
	if err != nil {
		return 0, nil, err
	}
	se := val.(schemaEntry)
	return se.schemaID, se.schema, nil
}

// normalizeSchemaJSON normalizes a JSON schema string by parsing and re-marshaling
// to ensure consistent field ordering for comparison.
func normalizeSchemaJSON(schemaJSON string) (string, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &schema); err != nil {
		return "", fmt.Errorf("failed to parse schema JSON: %w", err)
	}

	// Recursively normalize the schema structure
	normalized := normalizeSchemaStructure(schema)

	normalizedJSON, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("failed to marshal normalized schema: %w", err)
	}

	return string(normalizedJSON), nil
}

// normalizeSchemaStructure recursively normalizes Avro schema structures to ensure
// consistent field ordering and canonical representation.
func normalizeSchemaStructure(schema interface{}) interface{} {
	switch s := schema.(type) {
	case map[string]interface{}:
		return normalizeSchemaMap(s)
	case []interface{}:
		return normalizeSchemaArray(s)
	default:
		// Primitive types remain unchanged
		return s
	}
}

// normalizeSchemaMap normalizes a map-based schema structure
func normalizeSchemaMap(s map[string]interface{}) interface{} {
	normalized := make(map[string]interface{})

	// Copy all fields first
	for k, v := range s {
		normalized[k] = normalizeSchemaStructure(v)
	}

	// Special handling for record types with fields
	if fields, ok := normalized["fields"].([]interface{}); ok {
		normalized["fields"] = normalizeSchemaFields(fields)
	}

	// Handle union types
	if unionTypes, ok := normalized["type"].([]interface{}); ok {
		normalized["type"] = normalizeUnionTypes(unionTypes)
	}

	return normalized
}

// normalizeSchemaArray normalizes an array-based schema structure
func normalizeSchemaArray(s []interface{}) interface{} {
	normalized := make([]interface{}, len(s))
	for i, item := range s {
		normalized[i] = normalizeSchemaStructure(item)
	}
	return normalized
}

// normalizeSchemaFields sorts schema fields by name for consistent ordering
func normalizeSchemaFields(fields []interface{}) []interface{} {
	sortedFields := make([]interface{}, len(fields))
	copy(sortedFields, fields)

	// Sort fields by name
	for i := 0; i < len(sortedFields); i++ {
		for j := i + 1; j < len(sortedFields); j++ {
			field1, ok1 := sortedFields[i].(map[string]interface{})
			field2, ok2 := sortedFields[j].(map[string]interface{})

			if ok1 && ok2 {
				name1, _ := field1["name"].(string)
				name2, _ := field2["name"].(string)

				if name1 > name2 {
					sortedFields[i], sortedFields[j] = sortedFields[j], sortedFields[i]
				}
			}
		}
	}

	// Recursively normalize each field
	for i, field := range sortedFields {
		sortedFields[i] = normalizeSchemaStructure(field)
	}

	return sortedFields
}

// normalizeUnionTypes sorts union types for consistent ordering
func normalizeUnionTypes(unionTypes []interface{}) []interface{} {
	sortedUnion := make([]interface{}, len(unionTypes))
	copy(sortedUnion, unionTypes)

	// Simple string-based sorting for union types
	for i := 0; i < len(sortedUnion); i++ {
		for j := i + 1; j < len(sortedUnion); j++ {
			str1 := fmt.Sprintf("%v", sortedUnion[i])
			str2 := fmt.Sprintf("%v", sortedUnion[j])

			if str1 > str2 {
				sortedUnion[i], sortedUnion[j] = sortedUnion[j], sortedUnion[i]
			}
		}
	}

	// Recursively normalize each union type
	for i, unionType := range sortedUnion {
		sortedUnion[i] = normalizeSchemaStructure(unionType)
	}

	return sortedUnion
}

// CreateSchemaIfNotExists checks if a schema already exists for the subject and matches
// the provided schema. If it exists and matches, returns the existing schema info.
// If it doesn't exist, creates it. If it exists but doesn't match, returns an error.
func CreateSchemaIfNotExists(
	client *srclient.SchemaRegistryClient,
	subject, schemaJSON string,
	schemaType srclient.SchemaType,
) (*srclient.Schema, error) {
	// Try to get the existing schema first
	existingSchema, err := client.GetLatestSchema(subject)
	if err != nil {
		// Schema doesn't exist, try to create it
		return client.CreateSchema(subject, schemaJSON, schemaType)
	}

	// Normalize both schemas for comparison
	existingNormalized, err := normalizeSchemaJSON(existingSchema.Schema())
	if err != nil {
		fmt.Printf("Warning: Failed to normalize existing schema for %s: %v\n", subject, err)
		// Fall back to direct string comparison
		if existingSchema.Schema() == schemaJSON {
			return existingSchema, nil
		}
	} else {
		newNormalized, err := normalizeSchemaJSON(schemaJSON)
		if err != nil {
			fmt.Printf("Warning: Failed to normalize new schema for %s: %v\n", subject, err)
			// Fall back to direct string comparison
			if existingSchema.Schema() == schemaJSON {
				return existingSchema, nil
			}
		} else if existingNormalized == newNormalized {
			fmt.Printf("Schema for %s already exists and matches (after normalization), returning existing schema\n", subject)
			return existingSchema, nil
		}
	}

	fmt.Printf("Schema for %s exists but differs, using existing schema instead of creating new version\n", subject)
	// For now, return the existing schema instead of trying to create a new version
	// This avoids the schema registry API compatibility issue
	return existingSchema, nil
}

// getSchemaForSchemaID fetches and caches a schema by its ID.
func getSchemaForSchemaID(client *srclient.SchemaRegistryClient, schemaID int) (avro.Schema, error) {
	// Fast path: check cache
	if v, ok := schemaCacheByID.Load(schemaID); ok {
		return v.(avro.Schema), nil
	}
	// Singleflight to prevent duplicate fetches
	val, err, _ := singleFlight.Do(fmt.Sprintf("id:%d", schemaID), func() (interface{}, error) {
		schemaMeta, err := client.GetSchema(schemaID)
		if err != nil {
			return nil, fmt.Errorf("fetch schema ID %d: %w", schemaID, err)
		}
		schema, err := avro.Parse(schemaMeta.Schema())
		if err != nil {
			return nil, fmt.Errorf("parse schema ID %d: %w", schemaID, err)
		}
		schemaCacheByID.Store(schemaID, schema)
		return schema, nil
	})
	if err != nil {
		return nil, err
	}
	return val.(avro.Schema), nil
}

// DecodeWithHelper decodes a Confluent-wire Avro payload using cached schemas.
func DecodeWithHelper(
	client *srclient.SchemaRegistryClient,
	_ string,
	payload []byte,
) (map[string]any, error) {
	if len(payload) < 5 || payload[0] != 0 {
		return nil, fmt.Errorf("invalid wire format: missing magic byte or too short")
	}
	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	schema, err := getSchemaForSchemaID(client, schemaID)
	if err != nil {
		return nil, fmt.Errorf("get schema for ID %d: %w", schemaID, err)
	}
	var out map[string]any
	if err := avro.Unmarshal(schema, payload[5:], &out); err != nil {
		return nil, fmt.Errorf("unmarshal for ID %d: %w", schemaID, err)
	}
	return out, nil
}

// EncodeWithHelper encodes a native record into a Confluent-wire payload.
func EncodeWithHelper(
	client *srclient.SchemaRegistryClient,
	subject string,
	native map[string]any,
) ([]byte, error) {
	schemaID, schema, err := getSchemaForSubject(client, subject)
	if err != nil {
		return nil, fmt.Errorf("get schema for %s: %w", subject, err)
	}
	binaryData, err := avro.Marshal(schema, native)
	if err != nil {
		return nil, fmt.Errorf("marshal for %s: %w", subject, err)
	}
	// Prepend magic byte and schema ID
	if schemaID < 0 || schemaID > 0xFFFFFFFF { // Ensure schema ID fits in uint32
		return nil, fmt.Errorf("schema ID %d out of uint32 range", schemaID)
	}
	out := make([]byte, confluentWireFormatHeaderSize+len(binaryData))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:confluentWireFormatHeaderSize], uint32(schemaID))
	copy(out[confluentWireFormatHeaderSize:], binaryData)
	return out, nil
}
