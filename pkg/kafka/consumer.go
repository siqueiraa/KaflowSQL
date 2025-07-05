package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
	jsoniter "github.com/json-iterator/go"

	"github.com/siqueiraa/KaflowSQL/pkg/avro"
	"github.com/siqueiraa/KaflowSQL/pkg/config"
	"github.com/siqueiraa/KaflowSQL/pkg/state"
)

const (
	// Maximum value for signed 32-bit integer
	maxInt32       = 0x7FFFFFFF
	defaultMapSize = 16 // Default size for payload maps
)

var (
	decodedMsgPool = sync.Pool{New: func() any { return new(DecodedMessage) }}
	payloadMapPool = sync.Pool{New: func() any { m := make(map[string]any, defaultMapSize); return &m }}
	json           = jsoniter.ConfigFastest
)

type Consumer struct {
	ctx            context.Context
	c              *ck.Consumer
	topic          string
	useAvro        bool
	schemaRegistry string
	stateManager   *state.RocksDBState
}

type DecodedMessage struct {
	Key        []byte
	Value      map[string]any
	Topic      string
	Time       time.Time
	Offset     int64
	Partition  int
	poolMapPtr *map[string]any
}

func (dm *DecodedMessage) Release() {
	if dm.poolMapPtr != nil {
		// Clear map and return to pool
		for k := range *dm.poolMapPtr {
			delete(*dm.poolMapPtr, k)
		}
		payloadMapPool.Put(dm.poolMapPtr)
		dm.poolMapPtr = nil
	}
	// Return DecodedMessage to pool
	decodedMsgPool.Put(dm)
}

// NewConsumer keeps the old 1‑return signature: it panics on unrecoverable
// config errors because the previous code path never returned an error either.
func NewConsumer(
	ctx context.Context,
	brokers []string,
	topic, groupID string,
	cfg config.KafkaConfig,
	stateManager *state.RocksDBState,
) *Consumer {
	cm := &ck.ConfigMap{
		"bootstrap.servers":               strings.Join(brokers, ","),
		"group.id":                        groupID,
		"enable.auto.commit":              false,
		"auto.offset.reset":               "earliest",
		"go.application.rebalance.enable": true,
	}
	c, err := ck.NewConsumer(cm)
	if err != nil {
		log.Fatalf("failed to create confluent consumer: %v", err)
	}

	cons := &Consumer{
		ctx:            ctx,
		c:              c,
		topic:          topic,
		useAvro:        cfg.UseAvro,
		schemaRegistry: cfg.SchemaRegistry,
		stateManager:   stateManager,
	}

	// Subscribe with onRebalance callback to set initial offsets from RocksDB
	err = c.SubscribeTopics([]string{topic}, func(con *ck.Consumer, ev ck.Event) error {
		switch e := ev.(type) {
		case ck.AssignedPartitions:
			parts := e.Partitions
			for i := range parts {
				off, offsetErr := stateManager.GetOffset(topic, int(parts[i].Partition))
				if offsetErr != nil {
					log.Printf("Offset not found for topic: %s partition: %d", topic, int(parts[i].Partition))
					parts[i].Offset = ck.OffsetBeginning
				} else {
					log.Printf("Offset found for topic: %s partition: %d offset: %d", topic, int(parts[i].Partition), off)
					parts[i].Offset = ck.Offset(off + 1)
				}
			}
			return con.Assign(parts)
		case ck.RevokedPartitions:
			return con.Unassign()
		default:
			return nil
		}
	})
	if err != nil {
		log.Fatalf("subscribe failed: %v", err)
	}

	return cons
}

// ReadMessage mirrors old behavior but uses Confluent ReadMessage(timeout).
func (c *Consumer) Read() (*DecodedMessage, error) {
	msg, err := c.c.ReadMessage(-1)
	if err != nil {
		var ke ck.Error
		if errors.As(err, &ke) && ke.Code() == ck.ErrTimedOut {
			return nil, nil // caller can skip on timeout
		}
		return nil, err
	}

	dm := decodedMsgPool.Get().(*DecodedMessage)
	dm.Topic = *msg.TopicPartition.Topic
	dm.Partition = int(msg.TopicPartition.Partition)
	dm.Offset = int64(msg.TopicPartition.Offset)
	dm.Key = msg.Key
	dm.Time = msg.Timestamp
	dm.poolMapPtr = nil

	if c.useAvro {
		m, err := avro.DecodeAvro(c.schemaRegistry, dm.Topic+"-value", msg.Value)
		if err != nil {
			decodedMsgPool.Put(dm)
			return nil, err
		}
		dm.Value = m
	} else {
		mp := payloadMapPool.Get().(*map[string]any)
		m := *mp
		for k := range m {
			delete(m, k)
		}
		if err := json.Unmarshal(msg.Value, &m); err != nil {
			payloadMapPool.Put(mp)
			decodedMsgPool.Put(dm)
			return nil, err
		}
		dm.Value = m
		dm.poolMapPtr = mp
	}

	return dm, nil
}

// CommitBatch commits a group of messages in one RPC to reduce overhead.
func (c *Consumer) CommitBatch(dms []*DecodedMessage) error {
	// determine highest offset+1 per partition
	byPart := make(map[int]int64)
	for _, dm := range dms {
		next := dm.Offset + 1
		if curr, ok := byPart[dm.Partition]; !ok || next > curr {
			byPart[dm.Partition] = next
		}
	}
	// build TopicPartition list
	tps := make([]ck.TopicPartition, 0, len(byPart))
	for p, off := range byPart {
		if p > maxInt32 { // Ensure partition fits in int32
			return fmt.Errorf("partition %d exceeds int32 limit", p)
		}
		tps = append(tps, ck.TopicPartition{
			Topic:     &c.topic,
			Partition: int32(p), //nolint:gosec // Bounded by int32 max check above
			Offset:    ck.Offset(off),
		})
	}
	// commit offsets
	_, err := c.c.CommitOffsets(tps)
	if err != nil {
		return fmt.Errorf("commit batch failed: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error { return c.c.Close() }

func (c *Consumer) LogStats() {
	if s := c.c.String(); s != "" {
		log.Printf("[Confluent] %s", s)
	}
}
