package duck

import (
	"log"
	"sync"
	"time"
)

type Record struct {
	Table     string
	Data      map[string]any
	UniqueKey string
}

type Buffer struct {
	mu               sync.Mutex
	Name             string
	records          []Record
	keysAlreadyAdded map[string]struct{}
	totalRecords     int
	flushCount       int
	lastFlush        time.Time
	lastInsert       time.Time
}

func NewBuffer(name string) *Buffer {
	return &Buffer{
		Name:             name,
		records:          make([]Record, 0),
		keysAlreadyAdded: make(map[string]struct{}),
		lastFlush:        time.Now(),
		lastInsert:       time.Now(),
	}
}

func (b *Buffer) Add(record Record) {
	b.mu.Lock()
	defer b.mu.Unlock()

	key := record.UniqueKey

	if _, exists := b.keysAlreadyAdded[key]; exists {
		return
	}

	b.records = append(b.records, record)
	b.keysAlreadyAdded[key] = struct{}{}
	b.totalRecords++
	b.lastInsert = time.Now()
}

func (b *Buffer) Flush() []Record {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.records) == 0 {
		return nil
	}

	flushed := b.records
	b.records = make([]Record, 0)

	b.keysAlreadyAdded = make(map[string]struct{})

	b.flushCount++
	b.lastFlush = time.Now()

	return flushed
}

func (b *Buffer) Size() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records)
}

func (b *Buffer) Metrics() {
	b.mu.Lock()
	defer b.mu.Unlock()

	sinceFlush := time.Since(b.lastFlush)

	log.Printf("[Metrics] Buffer for %s - size: %d, total: %d, flushes: %d, last_flush: %v ago",
		b.Name,
		len(b.records),
		b.totalRecords,
		b.flushCount,
		sinceFlush,
	)
}
