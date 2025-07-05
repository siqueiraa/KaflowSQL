package duck

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/marcboeker/go-duckdb/v2"

	"github.com/siqueiraa/KaflowSQL/pkg/schema"
)

// Time and data conversion constants
const (
	secondsPerDay         = 86400
	hoursPerDay           = 24
	microsecondsPerSecond = 1_000_000
	nanosecondsPerMicro   = 1_000
	millisecondsPerSecond = 1_000
	uuidByteLength        = 16
	stringPartsCount      = 2
)

// quoteSQLIdentifier safely quotes a SQL identifier to prevent injection
func quoteSQLIdentifier(identifier string) string {
	// DuckDB uses double quotes for identifiers, escape any existing quotes
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// buildDeleteQuery builds a DELETE query with safe identifier quoting
func buildDeleteQuery(tableName string) string {
	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(quoteSQLIdentifier(tableName))
	sb.WriteString(";")
	return sb.String()
}

type DBEngine struct {
	db     *sql.DB
	mu     sync.Mutex
	dbPath string              // used to delete the file if not in-memory
	Tables map[string]struct{} // stores only the table names
}

// NewDuckDBEngine creates an engine with support for multiple tables
func NewDuckDBEngine(dbPath string) (*DBEngine, error) {
	dsn := ":memory:"
	if dbPath != "" {
		dsn = fmt.Sprintf("%s?access_mode=read_write", dbPath)
		os.Remove(dbPath)
	}

	connector, err := duckdb.NewConnector(dsn, func(execer driver.ExecerContext) error {
		bootQueries := []string{
			`SET schema='main'`,
			`SET search_path='main'`,
		}
		for _, q := range bootQueries {
			if _, err := execer.ExecContext(context.Background(), q, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connector: %w", err)
	}

	db := sql.OpenDB(connector)

	return &DBEngine{
		db:     db,
		dbPath: dbPath,
		Tables: make(map[string]struct{}),
	}, nil
}

// InsertBatch inserts multiple records using the appender.
func (e *DBEngine) InsertBatch(ctx context.Context, table string, records []map[string]any, schema schema.TableSchema) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(records) == 0 {
		return 0, nil
	}
	if table == "" {
		return 0, fmt.Errorf("table name cannot be empty")
	}

	conn, err := e.db.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	var appender *duckdb.Appender
	err = conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("failed to assert driver.Conn")
		}
		appender, err = duckdb.NewAppenderFromConn(driverConn, "main", table)
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("failed to create appender: %w", err)
	}
	defer appender.Close()

	count := e.appendRecords(appender, schema.FieldOrder, records, schema)

	if err := appender.Flush(); err != nil {
		return count, fmt.Errorf("failed to flush appender: %w", err)
	}

	log.Printf("[DuckDB] Inserted %d records into table %s", count, table)
	return count, nil
}

// appendRecords loops through the records and inserts them via appender.
// Returns how many were successfully inserted.
func (e *DBEngine) appendRecords(
	appender *duckdb.Appender,
	columns []string,
	records []map[string]any,
	schema schema.TableSchema,
) int {
	count := 0
	for idx, record := range records {
		values := make([]driver.Value, 0, len(columns))
		normalized := NormalizeRecord(record, schema.Types)
		for _, col := range columns {
			values = append(values, normalized[col])
		}

		if err := appender.AppendRow(values...); err != nil {
			log.Printf("[DuckDB] Failed to insert row %d: %v", idx, err)
			continue
		}
		count++
	}
	return count
}

// ExecuteSQL executes a custom SELECT (or other) query
func (e *DBEngine) ExecuteSQL(query string) (*sql.Rows, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.db.Query(query)
}

// Cleanup removes the physical database file (if not in-memory)
func (e *DBEngine) Cleanup() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	_ = e.db.Close()
	if e.dbPath != "" {
		if err := os.Remove(e.dbPath); err != nil {
			return fmt.Errorf("failed to delete DuckDB file: %w", err)
		}
	}
	return nil
}

// CreateTableFromSchema creates the table based on the defined schema
func (e *DBEngine) CreateTableFromSchema(table string, tblSchema schema.TableSchema) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.Tables[table]; exists {
		log.Printf("[DuckDB] Table %s already registered, skipping creation", table)
		return nil
	}

	var columns []string
	for _, fieldName := range tblSchema.FieldOrder {
		duckType := mapToDuckDBType(tblSchema.Types[fieldName])
		columns = append(columns, fmt.Sprintf("%s %s", fieldName, duckType))
	}
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %q (%s);", table, strings.Join(columns, ", "))

	if _, err := e.db.Exec(query); err != nil {
		return err
	}

	e.Tables[table] = struct{}{}
	log.Printf("[DuckDB] Table %s created", table)
	return nil
}

// ClearAllTables deletes all data from all registered tables
func (e *DBEngine) ClearAllTables() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for table := range e.Tables {
		// Use safe query builder to prevent injection
		query := buildDeleteQuery(table)
		if _, err := e.db.Exec(query); err != nil {
			log.Printf("[DuckDB] Failed to clear table %s: %v", table, err)
			return err
		}
		log.Printf("[DuckDB] Table %s cleared", table)
	}
	return nil
}

func parseDecimalScale(typ string) int32 {
	open := strings.IndexByte(typ, '(')
	if open < 0 || !strings.HasSuffix(typ, ")") {
		return 0
	}
	parts := strings.Split(typ[open+1:len(typ)-1], ",")
	if len(parts) != stringPartsCount {
		return 0
	}
	// Use ParseInt with bit size to prevent overflow
	if s, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 32); err == nil {
		return int32(s)
	}
	return 0
}

// normalizeFieldValue normalizes a single field value based on its type
func normalizeFieldValue(val any, typ string) any {
	lower := strings.ToLower(strings.TrimSpace(typ))

	switch {
	case strings.HasPrefix(lower, "decimal"), strings.HasPrefix(lower, "numeric"):
		scale := parseDecimalScale(lower)
		return normalizeDecimal(val, scale)
	case strings.HasPrefix(lower, "string"):
		return fmt.Sprintf("%v", val)
	case strings.HasPrefix(lower, "date"):
		return normalizeDateForDuck(val)
	case strings.HasPrefix(lower, "timestamp"):
		return normalizeTimestampForDuck(val)
	case strings.HasPrefix(lower, "time"):
		return normalizeTime(val)
	case isComplexType(lower):
		return normalizeJSON(val)
	case strings.HasPrefix(lower, "uuid"):
		return normalizeUUID(val)
	case strings.HasPrefix(lower, "interval"), strings.HasPrefix(lower, "duration"):
		return normalizeDuration(val)
	case strings.HasPrefix(lower, "int"):
		return normalizeInt(val)
	case strings.HasPrefix(lower, "float"):
		return normalizeFloat(val)
	case strings.HasPrefix(lower, "bool"):
		return normalizeBool(val)
	case strings.HasPrefix(lower, "bytes"), strings.HasPrefix(lower, "blob"):
		return normalizeBytesField(val)
	default:
		return val
	}
}

// isComplexType checks if the type is a complex Avro type
func isComplexType(lower string) bool {
	return strings.HasPrefix(lower, "list") ||
		strings.HasPrefix(lower, "array") ||
		strings.HasPrefix(lower, "map") ||
		strings.HasPrefix(lower, "struct") ||
		strings.HasPrefix(lower, "record")
}

// normalizeBytesField normalizes bytes/blob field values
func normalizeBytesField(val any) any {
	switch v := val.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	default:
		return []byte{}
	}
}

// NormalizeRecord adjusts the record types based on the given schema
func NormalizeRecord(record map[string]any, typMap map[string]string) map[string]any {
	normalized := make(map[string]any)

	for field, typ := range typMap {
		val, ok := record[field]
		if !ok {
			normalized[field] = defaultForType(typ)
			continue
		}

		normalized[field] = normalizeFieldValue(val, typ)
	}

	return normalized
}

func normalizeInt(val any) int64 {
	switch v := val.(type) {
	case float64:
		return int64(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return int64(i)
		}
		return 0
	case int:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

// normalizeDateForDuck converts an Avro/date value (int days since epoch,
// string or time.Time) into a Go time.Time at UTC midnight.
func normalizeDateForDuck(val any) time.Time {
	switch v := val.(type) {
	case int:
		return time.Unix(int64(v)*secondsPerDay, 0).UTC()
	case int32:
		return time.Unix(int64(v)*secondsPerDay, 0).UTC()
	case int64:
		return time.Unix(v*secondsPerDay, 0).UTC()
	case float64:
		return time.Unix(int64(v)*secondsPerDay, 0).UTC()
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.Unix(i*secondsPerDay, 0).UTC()
		}
	case time.Time:
		return v.UTC()
	}
	// fallback to today at UTC midnight
	return time.Now().UTC().Truncate(hoursPerDay * time.Hour)
}

// normalizeTimestampForDuck converts Avro timestamp (int64 micros since epoch,
// string or time.Time) into a Go time.Time.
func normalizeTimestampForDuck(val any) time.Time {
	switch v := val.(type) {
	case int:
		// interpret as seconds since epoch
		return time.Unix(int64(v), 0).UTC()
	case int32:
		return time.Unix(int64(v), 0).UTC()
	case int64:
		// if you store micros, divide; if you store seconds, adjust accordingly
		// here we assume v is microseconds:
		secs := v / microsecondsPerSecond
		nsec := (v % microsecondsPerSecond) * nanosecondsPerMicro
		return time.Unix(secs, nsec).UTC()
	case float64:
		secs := int64(v) / microsecondsPerSecond
		nsec := (int64(v) % microsecondsPerSecond) * nanosecondsPerMicro
		return time.Unix(secs, nsec).UTC()
	case string:
		// try RFC3339
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return t.UTC()
		}
		// fallback: parse as integer micros
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			secs := i / microsecondsPerSecond
			nsec := (i % microsecondsPerSecond) * nanosecondsPerMicro
			return time.Unix(secs, nsec).UTC()
		}
	case time.Time:
		return v.UTC()
	}
	// default to now
	return time.Now().UTC()
}

// ---------------------------------------------
// Normalizes Avro UUID coming as string or 16-byte array.
// Returns canonical 36-char string.
func normalizeUUID(val any) string {
	switch v := val.(type) {
	case string:
		return strings.ToLower(v)
	case []byte:
		if len(v) == uuidByteLength {
			return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
				binary.BigEndian.Uint32(v[0:4]),
				binary.BigEndian.Uint16(v[4:6]),
				binary.BigEndian.Uint16(v[6:8]),
				binary.BigEndian.Uint16(v[8:10]),
				v[10:16])
		}
	}
	return "" // fallback: empty UUID
}

// ---------------------------------------------
// Converts Avro logicalType "duration" (12-byte fixed) into ISO-8601 string
// or any representation DuckDB accepts for INTERVAL columns.
// DuckDB does accept strings like 'P2M3DT4H' etc.
func normalizeDuration(val any) string {
	// Avro duration layout: 3 x int32 (months, days, millis)
	if b, ok := val.([]byte); ok && len(b) == 12 {
		// Avro duration format specifies signed int32 values, safe conversion from uint32
		months := int32(binary.LittleEndian.Uint32(b[0:4]))  //nolint:gosec // Avro spec: duration uses signed int32
		days := int32(binary.LittleEndian.Uint32(b[4:8]))    //nolint:gosec // Avro spec: duration uses signed int32
		millis := int32(binary.LittleEndian.Uint32(b[8:12])) //nolint:gosec // Avro spec: duration uses signed int32

		// Build simple ISO-8601 string. All components optional.
		parts := []string{"P"}
		if months != 0 {
			parts = append(parts, fmt.Sprintf("%dM", months))
		}
		if days != 0 {
			parts = append(parts, fmt.Sprintf("%dD", days))
		}
		if millis != 0 {
			parts = append(parts, "T",
				fmt.Sprintf("%dS", millis/millisecondsPerSecond))
		}
		return strings.Join(parts, "")
	}
	// If it’s already string, just pass through.
	if s, ok := val.(string); ok {
		return s
	}
	return ""
}

// ---------------------------------------------
// Optional helper: flattens LIST / MAP / STRUCT unions
// into JSON string for storage in VARCHAR.
func normalizeJSON(val any) string {
	bytes, err := json.Marshal(val)
	if err != nil {
		return ""
	}
	return string(bytes)
}

func normalizeDecimal(val any, scale int32) float64 {
	var f float64

	switch v := val.(type) {
	case []byte:
		// Avro bytes → big.Int → *big.Rat → float64
		i := new(big.Int).SetBytes(v)
		rat := new(big.Rat).SetInt(i)
		f, _ = rat.Float64()
		f /= math.Pow10(int(scale))

	case *big.Int:
		rat := new(big.Rat).SetInt(v)
		f, _ = rat.Float64()
		f /= math.Pow10(int(scale))

	case *big.Rat:
		f, _ = v.Float64()
		f /= math.Pow10(int(scale))

	// case decimal.Decimal:
	//	// shopspring.Decimal → *big.Float → float64
	//	bf := v.BigFloat()
	//	f, _ = bf.Float64()

	case string:
		f, _ = strconv.ParseFloat(v, 64)
	}

	return f
}

func normalizeFloat(val any) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		return 0.0
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0.0
	}
}

func normalizeBool(val any) bool {
	switch v := val.(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1"
	default:
		return false
	}
}

// normalizeTime converts Avro time-micros or time-millis into int64 (Avro “long” branch).
// It treats any value < 86_400_000 as milliseconds and converts to microseconds.
func normalizeTime(val any) any {
	var micros int64

	switch v := val.(type) {
	case int:
		micros = int64(v)
	case int32:
		micros = int64(v)
	case int64:
		micros = v
	case float64:
		micros = int64(v)
	case string:
		if x, err := strconv.ParseInt(v, 10, 64); err == nil {
			micros = x
		}
	case time.Time:
		t := v.UTC()
		// hours, minutes, seconds -> microseconds
		micros = int64(t.Hour())*3_600_000_000 +
			int64(t.Minute())*60_000_000 +
			int64(t.Second())*1_000_000 +
			int64(t.Nanosecond()/nanosecondsPerMicro)
	default:
		micros = 0
	}

	// If looks like millis (< 1 day in ms), convert to micros
	if micros > 0 && micros < 86_400_000 {
		micros *= 1_000
	}

	return micros
}

func defaultForType(typ string) any {
	switch {
	case strings.HasPrefix(typ, "string"):
		return ""
	case strings.HasPrefix(typ, "int"):
		return 0
	case strings.HasPrefix(typ, "float"):
		return float64(0)
	case strings.HasPrefix(typ, "bool"):
		return false
	case strings.HasPrefix(typ, "timestamp"):
		return time.Now()
	default:
		return nil
	}
}

// typeMapping holds the mapping from Avro/Go types to DuckDB types
var typeMapping = map[string]string{
	"string":    "VARCHAR",
	"uuid":      "UUID",
	"bool":      "BOOLEAN",
	"int32":     "INTEGER",
	"int64":     "BIGINT",
	"long":      "BIGINT",
	"int":       "BIGINT",
	"float32":   "FLOAT",
	"float64":   "DOUBLE",
	"double":    "DOUBLE",
	"timestamp": "TIMESTAMP",
	"date":      "DATE",
	"time":      "TIME",
	"interval":  "INTERVAL",
	"duration":  "INTERVAL",
	"bytes":     "BLOB",
	"blob":      "BLOB",
	"list":      "LIST",
	"array":     "LIST",
	"map":       "MAP",
	"struct":    "STRUCT",
	"record":    "STRUCT",
}

// handleDecimalType processes decimal/numeric types with precision/scale
func handleDecimalType(t string) string {
	if open := strings.IndexByte(t, '('); open != -1 {
		return "DECIMAL" + strings.ToUpper(t[open:])
	}
	return "DECIMAL"
}

// findTypeMapping looks up type mapping using prefix matching
func findTypeMapping(t string) (string, bool) {
	for prefix, duckType := range typeMapping {
		if strings.HasPrefix(t, prefix) {
			return duckType, true
		}
	}
	return "", false
}

// mapToDuckDBType converts the generic type string coming from Avro
// mapping into the exact DuckDB type used in CREATE TABLE.
//
// It recognizes *all* Avro primitives + logical types and keeps
// precision/scale for decimals.
func mapToDuckDBType(typ string) string {
	t := strings.ToLower(strings.TrimSpace(typ))

	// Handle decimal/numeric types specially (need to preserve precision/scale)
	if strings.HasPrefix(t, "decimal") || strings.HasPrefix(t, "numeric") {
		return handleDecimalType(t)
	}

	// Look up in type mapping
	if duckType, found := findTypeMapping(t); found {
		return duckType
	}

	// Fallback
	return "VARCHAR"
}
