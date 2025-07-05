package kafka

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"

	"github.com/siqueiraa/KaflowSQL/pkg/avro"
	"github.com/siqueiraa/KaflowSQL/pkg/config"
)

const (
	batchTimeoutMillis = 100 // Batch timeout in milliseconds
	intKeyCapacity     = 12  // Buffer capacity for int keys
	int64KeyCapacity   = 20  // Buffer capacity for int64 keys
	batchTimeoutSecs   = 10  // Batch write timeout in seconds
	decimalBase        = 10  // Base for decimal number conversion
)

var (
	// jsonFast is our high-performance JSON API.
	jsonFast = jsoniter.ConfigFastest
)

// Producer wraps a kafka.Writer and optional Avro support.
type Producer struct {
	ctx            context.Context
	writer         *kafka.Writer
	useAvro        bool
	schemaRegistry string
	schemaClient   *srclient.SchemaRegistryClient
}

// NewProducer creates a new Kafka producer.
// Pass in a Context and your KafkaConfig.
func NewProducer(
	ctx context.Context,
	cfg config.KafkaConfig,
) (*Producer, error) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      cfg.Brokers,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: batchTimeoutMillis * time.Millisecond,
		// RequiredAcks is an int, so cast the constant.
		RequiredAcks: int(kafka.RequireAll),
	})

	var client *srclient.SchemaRegistryClient
	if cfg.UseAvro {
		client = srclient.CreateSchemaRegistryClient(cfg.SchemaRegistry)
	}

	return &Producer{
		ctx:            ctx,
		writer:         w,
		useAvro:        cfg.UseAvro,
		schemaRegistry: cfg.SchemaRegistry,
		schemaClient:   client,
	}, nil
}

// Publish sends a single message, encoding as Avro or JSON.
func (p *Producer) Publish(
	topic string,
	key []byte,
	value map[string]any,
) error {
	var (
		payload []byte
		err     error
	)

	if p.useAvro {
		// Avro path: fast, cached codec + singleflight
		payload, err = avro.EncodeAvro(p.schemaClient, topic+"-value", value)
		if err != nil {
			return fmt.Errorf("avro encode failed: %w", err)
		}
	} else {
		// JSON path: direct marshal via jsonFast
		payload, err = jsonFast.Marshal(value)
		if err != nil {
			return fmt.Errorf("json marshal failed: %w", err)
		}
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   key,
		Value: payload,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(p.ctx, msg); err != nil {
		log.Printf("[Kafka] publish failed topic=%s: %v", topic, err)
		return err
	}
	return nil
}

// PublishBatch serializes every record, builds the kafka.Message slice with
// minimal allocations and writes the batch with a context that can be
// canceled by the caller.
func (p *Producer) PublishBatch(
	topic string,
	records []map[string]any,
	keyField string,
) error {
	// Reserve exact capacity → no slice growth during append
	msgs := make([]kafka.Message, 0, len(records))

	now := time.Now() // One syscall instead of one per message

	for _, rec := range records {
		//------------------- payload serialization -------------------//
		var (
			payload []byte
			err     error
		)

		if p.useAvro {
			payload, err = avro.EncodeAvro(p.schemaClient, topic+"-value", rec)
		} else {
			payload, err = jsonFast.Marshal(rec)
		}
		if err != nil {
			log.Printf("[Kafka] encode failed: %v", err)
			continue // drop the faulty record, continue with the rest
		}

		//------------------- key extraction --------------------------//
		var key []byte
		if raw, ok := rec[keyField]; ok && raw != nil {
			switch v := raw.(type) {
			case string:
				// zero‑copy in Go 1.22+: string → []byte without allocation
				key = unsafe.Slice(unsafe.StringData(v), len(v))
			case []byte:
				key = v // already a byte slice
			case int:
				key = strconv.AppendInt(make([]byte, 0, intKeyCapacity), int64(v), decimalBase)
			case int64:
				key = strconv.AppendInt(make([]byte, 0, int64KeyCapacity), v, decimalBase)
			default:
				// Fallback: slower but rare for non‑primitive keys
				key = fmt.Append(nil, v)
			}
		}

		//------------------- build message ---------------------------//
		msgs = append(msgs, kafka.Message{
			Topic: topic,
			Key:   key,
			Value: payload,
			Time:  now,
		})
	}

	//------------------- write batch --------------------------------//
	ctx, cancel := context.WithTimeout(p.ctx, batchTimeoutSecs*time.Second)
	defer cancel()

	return p.writer.WriteMessages(ctx, msgs...)
}

// Close shuts down the writer cleanly.
func (p *Producer) Close() error {
	return p.writer.Close()
}
