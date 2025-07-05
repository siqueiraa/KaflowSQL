package avro

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"strconv"
	"time"

	havro "github.com/hamba/avro/v2"
	"github.com/marcboeker/go-duckdb/v2" // for Decimal type
	"github.com/riferrei/srclient"

	"github.com/siqueiraa/KaflowSQL/pkg/schema"
)

// Constants for common strings
const (
	stringTypeName              = "string"
	bytesTypeName               = "bytes"
	dateTypeName                = "date"
	timeTypeName                = "time"
	timestampTypeName           = "timestamp"
	uuidTypeName                = "uuid"
	longTimestampMicrosTypeName = "long.timestamp-micros"
	nullTypeName                = "null"
	decimalTypeName             = "decimal"
	intDateTypeName             = "int.date"
	longTimeMicrosTypeName      = "long.time-micros"

	// Time conversion constants
	secondsPerDay             = 86400
	nanosecondsPerMicrosecond = 1_000
)

// NormalizeForAvroWithSchema normalises an input map so that it fully
// respects the Avro schema registered for <topic>-value:
//  1. fetch & cache the schema text from the registry
//  2. parse it once and cache havro.Schema                        (fast path)
//  3. wrap nullable-union values so the encoder picks the branch
//  4. Marshal → Unmarshal to apply defaults & prune extra fields
func NormalizeForAvroWithSchema(
	topic string,
	input map[string]interface{},
	client *srclient.SchemaRegistryClient,
) (map[string]any, error) {
	subject := topic + "-value"

	// 1) Fetch & cache parsed schema
	_, schema, err := getSchemaForSubject(client, subject)
	if err != nil {
		return nil, fmt.Errorf("schema %s: %w", subject, err)
	}

	// 2) Build a new record containing ONLY the schema fields
	record := make(map[string]any)
	var recSchema *havro.RecordSchema
	if rs, ok := schema.(*havro.RecordSchema); ok {
		recSchema = rs
		for _, f := range rs.Fields() {
			name := f.Name()
			if val, exists := input[name]; exists {
				record[name] = val
			}
		}
	} else {
		// not a record schema—just fall back to the raw input
		record = input
	}

	// 3) Wrap unions (and only unions) in that pruned record
	if recSchema != nil {
		for _, f := range recSchema.Fields() {
			if u, isUnion := f.Type().(*havro.UnionSchema); isUnion {
				record[f.Name()] = wrapUnionValue(u, record[f.Name()])
			}
		}
	}

	bin, err := havro.Marshal(schema, record)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", subject, err)
	}

	// 5) Round-trip unmarshal to apply defaults & prune extras
	var out map[string]any
	if err := havro.Unmarshal(schema, bin, &out); err != nil {
		return nil, fmt.Errorf("decode %s: %w", subject, err)
	}

	return out, nil
}

// wrapUnionValue inspects the non-null branch of a ["null", T] union
// and then wraps your raw Go value in a map[branchName]value so that
// hamba/avro will pick the correct branch when encoding.
// wrapUnionValue inspects a ["null", T] union and wraps your raw Go value
// in a map whose key is the non-null branch’s **string** name.
func wrapUnionValue(u *havro.UnionSchema, val interface{}) interface{} {
	// 1) nil stays nil → use the "null" branch
	if val == nil {
		return nil
	}

	// 2) pick the first non-null branch schema
	var branch havro.Schema
	for _, t := range u.Types() {
		if t.Type() == nullTypeName {
			continue
		}
		branch = t
		break
	}
	if branch == nil {
		return nil // no non-null branch found
	}

	// 3) compute branchName: "primitive" or "primitive.logicalType"
	prim := string(branch.Type()) // e.g. "bytes" or "long"
	branchName := prim
	if lt, ok := branch.(havro.LogicalTypeSchema); ok {
		if l := lt.Logical(); l != nil {
			branchName = prim + "." + string(l.Type())
		}
	}

	// 4) prepare the payload according to branchName
	var payload interface{}
	switch branchName {
	case "bytes.decimal":
		payload = handleBytesDecimal(val)
	case longTimestampMicrosTypeName, longTimeMicrosTypeName, intDateTypeName:
		payload = handleTimeTypes(branchName, val)

	default:
		// all other primitives & logical types: pass through
		payload = val
	}

	// 5) wrap in a singleton map so hamba/avro picks the named branch
	return map[string]interface{}{branchName: payload}
}

// handleBytesDecimal converts decimal values to Avro bytes format
func handleBytesDecimal(val interface{}) interface{} {
	// Avro decimal on bytes expects a two's-complement big-endian []byte
	switch d := val.(type) {
	case duckdb.Decimal:
		// duckdb.Decimal holds the big.Int in .Value
		// get the absolute big-endian bytes
		b := d.Value.Bytes()
		// if the highest bit is set, prefix a 0x00 to keep sign correct
		if len(b) > 0 && b[0]&0x80 != 0 {
			b = append([]byte{0x00}, b...)
		}
		return b

	case *duckdb.Decimal:
		b := d.Value.Bytes()
		if len(b) > 0 && b[0]&0x80 != 0 {
			b = append([]byte{0x00}, b...)
		}
		return b

	case *big.Int:
		b := d.Bytes()
		if len(b) > 0 && b[0]&0x80 != 0 {
			b = append([]byte{0x00}, b...)
		}
		return b

	default:
		// assume it's already a []byte or compatible
		return val
	}
}

// handleTimeTypes converts time values to appropriate Avro time formats
func handleTimeTypes(branchName string, val interface{}) interface{} {
	switch v := val.(type) {
	case time.Time:
		switch branchName {
		case intDateTypeName:
			days := v.UTC().Unix() / secondsPerDay
			return int(days)
		case "long.time-micros":
			t := v.UTC()
			return int64(t.Hour())*3_600_000_000 +
				int64(t.Minute())*60_000_000 +
				int64(t.Second())*1_000_000 +
				int64(t.Nanosecond()/nanosecondsPerMicrosecond)
		case longTimestampMicrosTypeName:
			return v.UTC().UnixNano() / nanosecondsPerMicrosecond
		default:
			return val
		}
	default:
		return val
	}
}

// DecodeWithSchema decodes a Confluent‑formatted Avro payload (magic‑byte +
// schema‑id prefix) into a Go map, fetching the referenced schema on demand.
func DecodeWithSchema(
	payload []byte,
	client *srclient.SchemaRegistryClient,
) (map[string]any, error) {
	if len(payload) < 5 || payload[0] != 0 {
		return nil, fmt.Errorf("invalid wire format: missing magic byte or too short")
	}
	schemaID := int(binary.BigEndian.Uint32(payload[1:5]))
	avroSchema, err := getSchemaForSchemaID(client, schemaID)
	if err != nil {
		return nil, fmt.Errorf("get schema for ID %d: %w", schemaID, err)
	}
	var out map[string]any
	if err := havro.Unmarshal(avroSchema, payload[5:], &out); err != nil {
		return nil, fmt.Errorf("unmarshal for ID %d: %w", schemaID, err)
	}
	return out, nil
}

// SchemaToTableSchema derives a DuckDB‑compatible table schema (ordered
// field list + column types) from the *latest* Avro record schema registered
// for <topic>-value. It covers **all** Avro primitive and logical types plus
// the Confluent Connect / Debezium extensions.
func SchemaToTableSchema(
	topic string,
	client *srclient.SchemaRegistryClient,
) (schema.TableSchema, error) {
	subject := topic + "-value"

	// Fetch <topic>-value latest schema
	_, avroSchema, err := getSchemaForSubject(client, subject)
	if err != nil {
		return schema.TableSchema{}, fmt.Errorf("get schema for %s: %w", subject, err)
	}

	rec, ok := avroSchema.(*havro.RecordSchema)
	if !ok {
		return schema.TableSchema{}, fmt.Errorf("expected RecordSchema for %s, got %T", subject, avroSchema)
	}

	fieldOrder := make([]string, 0, len(rec.Fields()))
	columnTypes := make(map[string]string, len(rec.Fields()))

	for _, f := range rec.Fields() {
		name := f.Name()
		fieldOrder = append(fieldOrder, name)
		columnTypes[name] = duckType(f.Type())
	}

	return schema.TableSchema{FieldOrder: fieldOrder, Types: columnTypes}, nil
}

// -----------------------------------------------------------------------------
// Type‑mapping helpers (full coverage)
// -----------------------------------------------------------------------------

// handleUnionSchema unwraps union schemas to find non-null branches
func handleUnionSchema(u *havro.UnionSchema) string {
	for _, t := range u.Types() {
		if t.Type() != nullTypeName {
			return duckType(t)
		}
	}
	return stringTypeName // degenerate all‑null union
}

// handleContainerTypes processes Avro container types (arrays, maps, records, etc.)
func handleContainerTypes(s havro.Schema) (string, bool) {
	switch sc := s.(type) {
	case *havro.ArraySchema:
		return "list<" + duckType(sc.Items()) + ">", true
	case *havro.MapSchema:
		return "map<string," + duckType(sc.Values()) + ">", true
	case *havro.EnumSchema:
		return stringTypeName, true // store enum symbol text
	case *havro.RecordSchema:
		return "struct", true // keep simple – field expansion unnecessary for DDL
	case *havro.FixedSchema:
		// Fixed can be a duration or decimal; fall through so logic-type path evaluates.
		return "", false
	default:
		return "", false
	}
}

// handleConnectorSpecificTypes processes Confluent/Debezium connector types
func handleConnectorSpecificTypes(ps havro.PropertySchema, s havro.Schema) (string, bool) {
	if v, ok := ps.Prop("logicalType").(string); ok && v != "" {
		return mapLogicalOrPrimitive(v, s), true
	}
	if v, ok := ps.Prop("connect.name").(string); ok {
		switch v {
		case "org.apache.kafka.connect.data.Decimal":
			return decimalDDL(ps), true
		case "io.debezium.time.ZonedTimestamp", "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp":
			return timestampTypeName, true
		case "io.debezium.time.Date":
			return dateTypeName, true
		case "io.debezium.time.Time", "io.debezium.time.MicroTime":
			return timeTypeName, true
		case "io.debezium.data.UUID":
			return uuidTypeName, true
		}
	}
	return "", false
}

// duckType translates *any* Avro schema node into a DuckDB type keyword (or a
// composite description like `list<int>`). It understands the full Avro 1.11
// spec plus Confluent/Debezium extensions.
func duckType(s havro.Schema) string {
	// 1) Unwrap unions like ["null", {...}] – choose first non‑null branch.
	if u, ok := s.(*havro.UnionSchema); ok {
		return handleUnionSchema(u)
	}

	// 2) Container types recurse first so nested primitives get mapped.
	if result, handled := handleContainerTypes(s); handled {
		return result
	}

	// 3) Official Avro logical type exposed via LogicalTypeSchema.
	if lt := getStdLogicalType(s); lt != "" {
		return mapLogicalOrPrimitive(lt, s)
	}

	// 4) Connector‑specific hints within properties.
	if ps, ok := s.(havro.PropertySchema); ok {
		if result, handled := handleConnectorSpecificTypes(ps, s); handled {
			return result
		}
	}

	// 5) Primitive fallback.
	return mapLogicalOrPrimitive(string(s.Type()), s)
}

// mapLogicalOrPrimitive converts a primitive or logical type name into the
// DuckDB keyword. Decimal attempts to embed precision/scale when present.
func mapLogicalOrPrimitive(t string, s havro.Schema) string {
	switch t {
	// Primitives
	case stringTypeName:
		return stringTypeName
	case "boolean":
		return "bool"
	case "int":
		return "int32"
	case "long":
		return "int64"
	case "float":
		return "float32"
	case "double":
		return "float64"
	case bytesTypeName:
		return bytesTypeName

	// Logical types
	case decimalTypeName:
		if ps, ok := s.(havro.PropertySchema); ok {
			return decimalDDL(ps)
		}
		return decimalTypeName
	case uuidTypeName:
		return uuidTypeName
	case dateTypeName:
		return dateTypeName
	case "time-millis", "time-micros", timeTypeName:
		return timeTypeName
	case "timestamp-millis", "timestamp-micros", timestampTypeName, "local-timestamp-millis", "local-timestamp-micros":
		return timestampTypeName
	case "duration":
		return "interval" // Avro fixed(12) duration → DuckDB INTERVAL
	default:
		return stringTypeName
	}
}

// decimalDDL builds a DECIMAL(precision,scale) clause when metadata is present
// on the PropertySchema; otherwise returns plain "decimal".
// decimalDDL builds a DECIMAL(precision,scale) clause when metadata is present
// on the PropertySchema; otherwise returns plain "decimal".
func decimalDDL(ps havro.PropertySchema) string {
	prec, okP := propAsInt(ps, "precision")
	scale, okS := propAsInt(ps, "scale")

	if !okS {
		if params, ok := ps.Prop("connect.parameters").(map[string]any); ok {
			if v, ok := params["scale"].(string); ok {
				if i, err := strconv.Atoi(v); err == nil {
					scale, okS = i, true
				}
			}
			if v, ok := params["connect.decimal.precision"].(string); ok {
				if i, err := strconv.Atoi(v); err == nil {
					prec, okP = i, true
				}
			}
		}
	}

	if okP {
		if !okS {
			scale = 0
		}
		return fmt.Sprintf("decimal(%d,%d)", prec, scale)
	}
	return decimalTypeName
}

// propAsInt fetches a schema property and converts it to int if possible.
func propAsInt(ps havro.PropertySchema, key string) (int, bool) {
	v := ps.Prop(key)
	switch val := v.(type) {
	case int:
		return val, true
	case float64: // JSON numbers decode to float64
		return int(val), true
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i, true
		}
	}
	return 0, false
}

// getStdLogicalType walks the schema tree and returns an official Avro logical
// type recognized by hamba/avro, ignoring connector extensions.
func getStdLogicalType(s havro.Schema) string {
	if ls, ok := s.(havro.LogicalTypeSchema); ok {
		if l := ls.Logical(); l != nil {
			return string(l.Type())
		}
	}
	switch sc := s.(type) {
	case *havro.UnionSchema:
		for _, t := range sc.Types() {
			if t.Type() != nullTypeName {
				return getStdLogicalType(t)
			}
		}
	case *havro.ArraySchema:
		return getStdLogicalType(sc.Items())
	case *havro.MapSchema:
		return getStdLogicalType(sc.Values())
	}
	return ""
}
