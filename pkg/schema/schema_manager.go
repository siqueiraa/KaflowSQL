package schema

import (
	"reflect"
	"sort"
	"sync"
	"time"
)

const (
	timestampTypeName = "timestamp"
	nullTypeName      = "null"
	intTypeName       = "int"
	stringTypeName    = "string"
	boolTypeName      = "bool"
	floatTypeName     = "float"
	arrayTypeName     = "array"
	mapTypeName       = "map"
	structTypeName    = "struct"
)

type TableSchema struct {
	Types      map[string]string
	FieldOrder []string
}

type Manager struct {
	mu      sync.RWMutex
	schemas map[string]TableSchema
}

func NewSchemaManager() *Manager {
	return &Manager{
		schemas: make(map[string]TableSchema),
	}
}

// InferSchema infers the schema of a single record and returns a TableSchema.
// To ensure consistent field ordering, field names are sorted alphabetically.
func (sm *Manager) InferSchema(record map[string]any) TableSchema {
	inferred := make(map[string]string)
	keys := make([]string, 0, len(record))
	for k := range record {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := record[k]
		inferred[k] = goTypeToString(reflect.TypeOf(v), v)
	}
	return TableSchema{
		Types:      inferred,
		FieldOrder: keys,
	}
}

// IsSchemaDifferent checks if two TableSchemas differ in structure.
func (sm *Manager) IsSchemaDifferent(old, newSchema TableSchema) bool {
	if len(newSchema.Types) != len(old.Types) {
		return true
	}
	for k, v := range newSchema.Types {
		if oldType, ok := old.Types[k]; !ok || oldType != v {
			return true
		}
	}
	return false
}

// Update replaces the schema for a given table name.
func (sm *Manager) Update(table string, newSchema TableSchema) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.schemas[table] = newSchema
}

// GetSchemaForTable retrieves the schema for a specific table.
func (sm *Manager) GetSchemaForTable(table string) TableSchema {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.schemas[table]
}

// handlePointerType dereferences pointer types and returns the underlying type and value
func handlePointerType(t reflect.Type, value any) (reflect.Type, any, bool) {
	if t.Kind() == reflect.Ptr {
		val := reflect.ValueOf(value)
		if val.IsNil() {
			return nil, nil, true // null value
		}
		val = val.Elem()
		return val.Type(), val.Interface(), false
	}
	return t, value, false
}

// handleFloatType processes float types and determines if they should be treated as int
func handleFloatType(value any) string {
	if f, ok := value.(float64); ok {
		if f == float64(int64(f)) {
			return intTypeName
		}
	}
	return floatTypeName
}

// handleStringType processes string types and checks for timestamp patterns
func handleStringType(value any) string {
	if str, ok := value.(string); ok && isRFC3339(str) {
		return timestampTypeName
	}
	return stringTypeName
}

// handleCollectionType processes slice and array types
func handleCollectionType(t reflect.Type) string {
	if t.Elem().Kind() == reflect.Uint8 {
		return "bytes"
	}
	return arrayTypeName
}

// goTypeToString converts a reflect.Type and its value to a string representation of the type.
// If the value is a float64 without a decimal part, it's treated as "int".
func goTypeToString(t reflect.Type, value any) string {
	if t == nil {
		return nullTypeName
	}

	// Handle pointer types
	t, value, isNull := handlePointerType(t, value)
	if isNull {
		return nullTypeName
	}

	// Handle time.Time specifically
	if t == reflect.TypeOf(time.Time{}) {
		return timestampTypeName
	}

	return handleTypeKind(t, value)
}

// handleTypeKind processes different reflect.Kind values
func handleTypeKind(t reflect.Type, value any) string {
	switch t.Kind() {
	case reflect.String:
		return handleStringType(value)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return intTypeName
	case reflect.Float32, reflect.Float64:
		return handleFloatType(value)
	case reflect.Bool:
		return boolTypeName
	case reflect.Slice, reflect.Array:
		return handleCollectionType(t)
	case reflect.Map:
		return mapTypeName
	case reflect.Struct:
		return structTypeName
	case reflect.Invalid:
		return nullTypeName
	case reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Pointer, reflect.UnsafePointer:
		return handleSpecialTypes(t.Kind())
	default:
		return stringTypeName
	}
}

// handleSpecialTypes processes less common reflect.Kind values
func handleSpecialTypes(kind reflect.Kind) string {
	if result := handleBasicTypes(kind); result != "" {
		return result
	}
	return handleAdvancedTypes(kind)
}

// handleBasicTypes processes basic reflect.Kind values
func handleBasicTypes(kind reflect.Kind) string {
	switch kind {
	case reflect.Invalid:
		return nullTypeName
	case reflect.Bool:
		return boolTypeName
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return intTypeName
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return intTypeName
	case reflect.Float32, reflect.Float64:
		return floatTypeName
	case reflect.Array, reflect.Slice:
		return arrayTypeName
	case reflect.Map:
		return mapTypeName
	case reflect.String:
		return stringTypeName
	case reflect.Struct:
		return structTypeName
	case reflect.Uintptr, reflect.Complex64, reflect.Complex128, reflect.Chan,
		reflect.Func, reflect.Interface, reflect.Pointer, reflect.UnsafePointer:
		return ""
	default:
		return ""
	}
}

// handleAdvancedTypes processes advanced reflect.Kind values
func handleAdvancedTypes(kind reflect.Kind) string {
	switch kind {
	case reflect.Invalid, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.Array, reflect.Map, reflect.Slice,
		reflect.String, reflect.Struct:
		return stringTypeName
	case reflect.Uintptr:
		return "uintptr"
	case reflect.Complex64, reflect.Complex128:
		return "complex"
	case reflect.Chan:
		return "channel"
	case reflect.Func:
		return "function"
	case reflect.Interface:
		return "interface"
	case reflect.Pointer:
		return "pointer"
	case reflect.UnsafePointer:
		return "unsafe_pointer"
	default:
		return stringTypeName
	}
}

// isRFC3339 checks if a string matches the RFC3339 timestamp format.
func isRFC3339(s string) bool {
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}
