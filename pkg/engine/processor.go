// Code generated from chat-refactor – compile-ready.
// File: pkg/engine/processor.go  (no omissions)

package engine

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/riferrei/srclient"
	"github.com/siqueiraa/KaflowSQL/pkg/avro"
	"github.com/siqueiraa/KaflowSQL/pkg/duck"
	"github.com/siqueiraa/KaflowSQL/pkg/kafka"
	"github.com/siqueiraa/KaflowSQL/pkg/pipeline"
	"github.com/siqueiraa/KaflowSQL/pkg/schema"
	"github.com/siqueiraa/KaflowSQL/pkg/state"
	"github.com/siqueiraa/KaflowSQL/pkg/ttlindex"
)

const (
	numShards          = 256  // Number of shards for join index
	shardMask          = 0xff // Bit mask for shard calculation (256-1)
	purgeChannelSize   = 4096 // Buffer size for purge channel
	offsetInterval     = 10   // Offset persister interval in seconds
	memoryInterval     = 5    // Memory expirer interval in seconds
	memoryTTL          = 15   // Memory TTL in minutes
	persistentInterval = 30   // Persistent expirer interval in seconds
	persistentTTL      = 24   // Persistent TTL in hours
	bitShiftOne        = 1    // Bit shift value for mask calculation
)

/*─────────────────────────────────────────────────────────────────────────────*
| 1.  Types                                                                   |
*─────────────────────────────────────────────────────────────────────────────*/

// Row is the immutable payload for each Kafka event.
type Row struct {
	Table        uint8          `json:"t"`
	Data         map[string]any `json:"d"` // keep original map; zero-copy pointers
	DedupeKey    string         `json:"k"`
	PresenceMask uint64         `json:"m"`
	Tombstone    bool           `json:"x"`
}

// joinKey is a pre-hashed 64-bit composite join key.
type joinKey = uint64

// EdgeEntry keeps left/right rowID slices for one join key.
type EdgeEntry struct {
	mu       sync.RWMutex
	leftIDs  []uint64
	rightIDs []uint64
}

// shard for hash-table
type edgeShard struct {
	mu    sync.RWMutex
	table map[joinKey]*EdgeEntry
}

/*─────────────────────────────────────────────────────────────────────────────*
| 2.  Engine struct                                                           |
*─────────────────────────────────────────────────────────────────────────────*/

type Engine struct {
	// static config
	pipeline pipeline.Pipeline
	plan     *pipeline.QueryPlan
	topics   []string
	slowDims map[string]bool
	fastDims map[string]bool

	stateManager  *state.RocksDBState
	buffer        *duck.Buffer
	schemaManager *schema.Manager
	duckEngine    *duck.DBEngine
	producer      *kafka.Producer
	schemaClient  *srclient.SchemaRegistryClient

	/* zero-copy row store */
	rows   map[uint64]*Row // rowID -> Row
	rowsMu sync.RWMutex    // Protect concurrent map access
	nextID uint64          // atomic counter

	/* join index – numShards shards */
	edges [numShards]edgeShard

	/* TTL */
	memoryTTLIndex     *ttlindex.Index
	persistentTTLIndex *ttlindex.Index

	/* emission bitmask */
	requireMask uint64
	field2bit   map[string]uint64

	/* dedup + purge */
	processedOffsets sync.Map
	purgeCh          chan joinKey

	/* util */
	shutdown chan struct{}
}

/*─────────────────────────────────────────────────────────────────────────────*
| 3.  Constructor (same signature expected by main)                           |
*─────────────────────────────────────────────────────────────────────────────*/

func NewEngine(
	p pipeline.Pipeline,
	plan *pipeline.QueryPlan,
	sm *state.RocksDBState,
	buf *duck.Buffer,
	scm *schema.Manager,
	duckDB *duck.DBEngine,
	prod *kafka.Producer,
	topics []string,
	slow, fast []string,
	sr *srclient.SchemaRegistryClient,
) (*Engine, error) {

	e := &Engine{
		pipeline:           p,
		plan:               plan,
		topics:             topics,
		slowDims:           sliceToSet(slow),
		fastDims:           sliceToSet(fast),
		stateManager:       sm,
		buffer:             buf,
		schemaManager:      scm,
		duckEngine:         duckDB,
		producer:           prod,
		schemaClient:       sr,
		rows:               make(map[uint64]*Row),
		memoryTTLIndex:     ttlindex.New(),
		persistentTTLIndex: ttlindex.New(),
		purgeCh:            make(chan joinKey, purgeChannelSize),
		shutdown:           make(chan struct{}),
	}
	for i := range e.edges {
		e.edges[i].table = make(map[joinKey]*EdgeEntry)
	}

	// emission bitmask
	e.field2bit = make(map[string]uint64)
	for i, f := range p.Emission.Require {
		if i < 64 { // Ensure we don't overflow uint64 bit operations
			e.field2bit[f] = uint64(i)                //nolint:gosec // Bounded by 64-bit limit check above
			e.requireMask |= bitShiftOne << uint64(i) //nolint:gosec // Bounded by 64-bit limit check above
		}
	}

	// recover persisted state
	if err := e.recoverState(); err != nil {
		return nil, err
	}

	// background goroutines
	go e.offsetPersister(offsetInterval * time.Second)
	go e.PurgeWorker()
	go e.RunMemoryExpirer(memoryInterval*time.Second, memoryTTL*time.Minute)
	go e.RunPersistentExpirer(persistentInterval*time.Second, persistentTTL*time.Hour)

	return e, nil
}

/*─────────────────────────────────────────────────────────────────────────────*
| 4.  Utility                                                                 |
*─────────────────────────────────────────────────────────────────────────────*/

func sliceToSet(ss []string) map[string]bool {
	out := make(map[string]bool, len(ss))
	for _, s := range ss {
		out[s] = true
	}
	return out
}

// buildPresenceMask converts JSON map → bitmask once.
func (e *Engine) buildPresenceMask(m map[string]any) uint64 {
	var mask uint64
	for k := range m {
		if bit, ok := e.field2bit[k]; ok {
			mask |= 1 << bit
		}
	}
	return mask
}

func (e *Engine) meetsEmission(mask uint64) bool {
	return (mask & e.requireMask) == e.requireMask
}

// hashJoinKey returns xxhash64 of concatenated key values.
func hashJoinKey(parsed map[string]any, keys []string) joinKey {
	h := xxhash.New()
	for _, k := range keys {
		if v, ok := parsed[k]; ok {
			fmt.Fprint(h, v)
		}
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// shard index by low byte of joinKey
func (e *Engine) shardOf(jk joinKey) *edgeShard {
	return &e.edges[jk&shardMask]
}

/*─────────────────────────────────────────────────────────────────────────────*
| 6.  ProcessIncomingEvent (build + probe)                                    |
*─────────────────────────────────────────────────────────────────────────────*/

func (e *Engine) ProcessIncomingEvent(
	topic string,
	parsed map[string]any,
	ts time.Time,
	part int,
	off int64,
) error {

	start := time.Now()
	defer func() {
		log.Printf("[Proc] %v topic=%s part=%d off=%d", time.Since(start), topic, part, off)
	}()

	// 1) dedup
	kb := fmt.Sprintf("%s:%d", topic, part)
	if last, ok := e.processedOffsets.Load(kb); ok {
		if off <= last.(int64) {
			return nil
		}
	}
	e.processedOffsets.Store(kb, off)

	// 2) create Row
	id := atomic.AddUint64(&e.nextID, 1) - 1
	tableIndex := indexOf(topic, e.topics)
	if tableIndex > 255 { // Ensure table index fits in uint8
		return fmt.Errorf("too many topics: %d exceeds uint8 limit", tableIndex)
	}
	row := &Row{
		Table:        uint8(tableIndex), //nolint:gosec // Bounded by 255 check above
		Data:         parsed,
		DedupeKey:    fmt.Sprintf("dedupe:%s-%d-%d", topic, part, off),
		PresenceMask: e.buildPresenceMask(parsed),
	}
	// zero-copy store
	e.rowsMu.Lock()
	e.rows[id] = row
	e.rowsMu.Unlock()

	// 3) persist Row (cf rows)
	if enc, err := json.Marshal(row); err == nil {
		rk := make([]byte, 8)
		binary.BigEndian.PutUint64(rk, id)
		_ = e.stateManager.Put("rows", string(rk), enc, ts)
	}

	// 4) iterate plan edges for this topic
	for _, jc := range e.plan.JoinClauses {
		var (
			isLeft bool
			keys   []string
		)
		switch topic {
		case jc.LeftTable:
			isLeft = true
			keys = jc.LeftKeys
		case jc.RightTable:
			isLeft = false
			keys = jc.RightKeys
		default:
			continue
		}

		jk := hashJoinKey(parsed, keys)
		sh := e.shardOf(jk)

		if isLeft {
			// probe right side first
			entry := sh.getOrCreate(jk)
			entry.mu.RLock()
			for _, rid := range entry.rightIDs {
				e.emitJoin(id, rid)
			}
			entry.mu.RUnlock()

			// build left
			entry.mu.Lock()
			entry.leftIDs = append(entry.leftIDs, id)
			entry.mu.Unlock()
		} else {
			// probe left side
			entry := sh.getOrCreate(jk)
			entry.mu.RLock()
			for _, lid := range entry.leftIDs {
				e.emitJoin(lid, id)
			}
			entry.mu.RUnlock()

			// build right
			entry.mu.Lock()
			entry.rightIDs = append(entry.rightIDs, id)
			entry.mu.Unlock()
		}

		// persist edge slice (cf edges)
		go e.persistEdgeSlice(jc, isLeft, jk, ts)
	}
	return nil
}

/*─────────────────────────────────────────────────────────────────────────────*
| 7.  Edge helpers                                                            |
*─────────────────────────────────────────────────────────────────────────────*/

func (sh *edgeShard) getOrCreate(jk joinKey) *EdgeEntry {
	sh.mu.RLock()
	e, ok := sh.table[jk]
	sh.mu.RUnlock()
	if ok {
		return e
	}
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e = sh.table[jk]; e != nil {
		return e
	}
	e = &EdgeEntry{}
	sh.table[jk] = e
	return e
}

func encodeUint64Slice(ids []uint64) []byte {
	// 4-byte length + N*8 byte ids (big-endian)
	if len(ids) > 0xFFFFFFFF { // Ensure length fits in uint32
		panic("slice too large for uint32 length encoding")
	}
	buf := make([]byte, 4+8*len(ids))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(ids))) //nolint:gosec // Bounded by uint32 max check above
	for i, id := range ids {
		binary.BigEndian.PutUint64(buf[4+i*8:], id)
	}
	return buf
}

func decodeUint64Slice(b []byte) ([]uint64, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("too short")
	}
	n := binary.BigEndian.Uint32(b[:4])
	if len(b) != int(4+8*n) {
		return nil, fmt.Errorf("length mismatch")
	}
	out := make([]uint64, n)
	for i := uint32(0); i < n; i++ {
		out[i] = binary.BigEndian.Uint64(b[4+i*8:])
	}
	return out, nil
}

/*─────────────────────────────────────────────────────────────────────────────*
| 8.  Emit + Purge                                                            |
*─────────────────────────────────────────────────────────────────────────────*/

func (e *Engine) emitJoin(leftID, rightID uint64) {
	e.rowsMu.RLock()
	lr, leftOK := e.rows[leftID]
	rr, rightOK := e.rows[rightID]
	e.rowsMu.RUnlock()

	if !leftOK || !rightOK {
		return // Skip if rows don't exist
	}

	mergedPM := lr.PresenceMask | rr.PresenceMask
	if !e.meetsEmission(mergedPM) {
		return
	}

	// add to buffer (one record per table)
	e.buffer.Add(duck.Record{Table: e.topics[lr.Table], Data: lr.Data, UniqueKey: lr.DedupeKey})
	e.buffer.Add(duck.Record{Table: e.topics[rr.Table], Data: rr.Data, UniqueKey: rr.DedupeKey})

	// tombstone if full graph reached
	// simplistic: when both topics present, purge joinKey slice
	select {
	case e.purgeCh <- hashJoinKey(lr.Data, e.plan.JoinClauses[0].LeftKeys): // any key works
	default:
	}
}

func (e *Engine) PurgeWorker() {
	for jk := range e.purgeCh {
		sh := e.shardOf(jk)
		sh.mu.Lock()
		delete(sh.table, jk)
		sh.mu.Unlock()

		// remove from RocksDB + TTL
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], jk)
		rk := "edge:" + string(key[:])
		_ = e.stateManager.DeleteAsync([]byte(rk))
		e.memoryTTLIndex.Remove(rk)
		e.persistentTTLIndex.Remove(rk)
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
| 9.  Persist edge slice                                                      |
*─────────────────────────────────────────────────────────────────────────────*/

func (e *Engine) persistEdgeSlice(jc pipeline.JoinClause, isLeft bool, jk joinKey, ts time.Time) {
	sh := e.shardOf(jk)
	entry := sh.getOrCreate(jk)

	entry.mu.RLock()
	var ids []uint64
	if isLeft {
		ids = entry.leftIDs
	} else {
		ids = entry.rightIDs
	}
	entry.mu.RUnlock()

	//enc, _ := json.Marshal(ids)
	enc := encodeUint64Slice(ids)
	var keyBuf [8]byte
	binary.BigEndian.PutUint64(keyBuf[:], jk)

	side := "L"
	if !isLeft {
		side = "R"
	}
	rk := fmt.Sprintf("edge:%s:%s:%x", edgeID(jc.LeftTable, jc.RightTable), side, keyBuf)

	_ = e.stateManager.Put("edges", rk, enc, ts)
	e.memoryTTLIndex.Add(rk, ts)
	if !e.slowDims[jc.LeftTable] && !e.slowDims[jc.RightTable] {
		e.persistentTTLIndex.Add(rk, ts)
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
| 10. Recover state from RocksDB                                              |
*─────────────────────────────────────────────────────────────────────────────*/

func (e *Engine) recoverState() error {
	// rows
	if err := e.stateManager.ForEach("rows:", func(k string, v []byte, ts time.Time) error {
		var r Row
		if err := json.Unmarshal(v, &r); err != nil {
			return err
		}
		id := binary.BigEndian.Uint64([]byte(k))
		// store in map
		e.rowsMu.Lock()
		e.rows[id] = &r
		e.rowsMu.Unlock()
		if id >= e.nextID {
			e.nextID = id + 1
		}
		return nil
	}); err != nil {
		return err
	}

	// edges
	return e.stateManager.ForEach("edge:", func(k string, v []byte, ts time.Time) error {
		// parse key edge:<eid>:<L|R>:<hash>
		parts := strings.Split(k, ":")
		if len(parts) != 4 {
			return nil
		}
		side := parts[2]
		hashBytes, _ := strconv.ParseUint(parts[3], 16, 64)
		jk := joinKey(hashBytes)
		sh := e.shardOf(jk)
		entry := sh.getOrCreate(jk)

		ids, err := decodeUint64Slice(v)
		if err != nil {
			return err
		}

		entry.mu.Lock()
		if side == "L" {
			entry.leftIDs = ids
		} else {
			entry.rightIDs = ids
		}
		entry.mu.Unlock()

		// re-insert TTLs
		rk := "edge:" + k
		e.memoryTTLIndex.Add(rk, ts)
		e.persistentTTLIndex.Add(rk, ts)
		return nil
	})
}

/*─────────────────────────────────────────────────────────────────────────────*
| 11. Buffer-to-DuckDB-to-Kafka (unchanged)                                   |
*─────────────────────────────────────────────────────────────────────────────*/

func (e *Engine) ProcessBufferToDuckDB() error {
	start := time.Now()
	defer func() { log.Printf("[flush] %v", time.Since(start)) }()

	recs := e.buffer.Flush()
	if len(recs) == 0 {
		return nil
	}

	group := make(map[string][]map[string]any)
	for _, r := range recs {
		group[r.Table] = append(group[r.Table], r.Data)
	}
	for tbl, rows := range group {
		sch := e.schemaManager.GetSchemaForTable(tbl)
		if _, err := e.duckEngine.InsertBatch(context.Background(), tbl, rows, sch); err != nil {
			return err
		}
	}
	rows, err := e.duckEngine.ExecuteSQL(e.pipeline.Query)
	if err != nil {
		return err
	}
	defer rows.Close()

	num, err := PublishRowsToKafka(rows, e.pipeline.Output, e.producer, e.schemaClient)
	log.Printf("[flush] published %d rows", num)
	return err
}

func PublishRowsToKafka(rows *sql.Rows, out pipeline.OutputConfig,
	prod *kafka.Producer, sr *srclient.SchemaRegistryClient) (int, error) {

	cols, _ := rows.Columns()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	var batch []map[string]any
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}
		rec := make(map[string]any, len(cols))
		for i, c := range cols {
			rec[c] = vals[i]
		}
		payload, err := avro.NormalizeForAvroWithSchema(out.Topic, rec, sr)
		if err != nil {
			continue
		}
		if _, ok := payload[out.Key]; !ok {
			payload[out.Key] = rec[out.Key]
		}
		batch = append(batch, payload)
	}
	if len(batch) == 0 {
		return 0, nil
	}
	return len(batch), prod.PublishBatch(out.Topic, batch, out.Key)
}

/*─────────────────────────────────────────────────────────────────────────────*
| 12. TTL expirers & offset persister                                         |
*─────────────────────────────────────────────────────────────────────────────*/

func (e *Engine) RunMemoryExpirer(interval, ttl time.Duration) {
	tk := time.NewTicker(interval)
	for {
		select {
		case <-e.shutdown:
			return
		case now := <-tk.C:
			for _, rk := range e.memoryTTLIndex.GetExpired(now.Add(-ttl)) {
				e.rmSlice(rk)
			}
		}
	}
}

func (e *Engine) RunPersistentExpirer(interval, ttl time.Duration) {
	tk := time.NewTicker(interval)
	for {
		select {
		case <-e.shutdown:
			return
		case now := <-tk.C:
			for _, rk := range e.persistentTTLIndex.GetExpired(now.Add(-ttl)) {
				_ = e.stateManager.DeleteAsync([]byte(rk))
				e.rmSlice(rk)
			}
		}
	}
}

// rmSlice removes edge slice from RAM.
func (e *Engine) rmSlice(rk string) {
	parts := strings.Split(rk, ":")
	if len(parts) < 4 {
		return
	}
	hash, _ := strconv.ParseUint(parts[3], 16, 64)
	jk := joinKey(hash)
	sh := e.shardOf(jk)
	sh.mu.Lock()
	delete(sh.table, jk)
	sh.mu.Unlock()
}

func (e *Engine) offsetPersister(interval time.Duration) {
	tk := time.NewTicker(interval)
	for {
		select {
		case <-e.shutdown:
			return
		case <-tk.C:
			e.processedOffsets.Range(func(k, v any) bool {
				parts := strings.Split(k.(string), ":")
				if len(parts) != 2 {
					return true
				}
				part, _ := strconv.Atoi(parts[1])
				_ = e.stateManager.SaveOffset(parts[0], part, v.(int64))
				return true
			})
		}
	}
}

/*─────────────────────────────────────────────────────────────────────────────*
| 13. helpers                                                                 |
*─────────────────────────────────────────────────────────────────────────────*/

func indexOf(s string, arr []string) int {
	for i, v := range arr {
		if v == s {
			return i
		}
	}
	return -1
}

// edgeID returns a canonical "<smaller>|<greater>" identifier for a table pair.
func edgeID(a, b string) string {
	if a < b {
		return a + "|" + b
	}
	return b + "|" + a
}
