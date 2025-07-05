package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/riferrei/srclient"

	"github.com/siqueiraa/KaflowSQL/pkg/avro"
	"github.com/siqueiraa/KaflowSQL/pkg/config"
	"github.com/siqueiraa/KaflowSQL/pkg/duck"
	"github.com/siqueiraa/KaflowSQL/pkg/engine"
	"github.com/siqueiraa/KaflowSQL/pkg/kafka"
	"github.com/siqueiraa/KaflowSQL/pkg/pipeline"
	"github.com/siqueiraa/KaflowSQL/pkg/schema"
	"github.com/siqueiraa/KaflowSQL/pkg/state"
)

// Constants for directory creation
const (
	defaultDirMode = 0o755 // Standard directory permissions
)

func main() {
	log.Println("[Engine] Starting KaflowSQL...")

	cfg := config.Load("config.yaml")

	pipelines, err := loadPipelines("pipelines")
	if err != nil {
		log.Fatalf("[Engine] Failed to load pipelines: %v", err)
	}

	log.Printf("[Engine] Loaded %d pipeline(s):\n", len(pipelines))
	for _, p := range pipelines {
		runPipeline(&p, &cfg)
	}

	select {}
}

// loadPipelines loads all pipeline YAML files from the specified directory.
func loadPipelines(dir string) ([]pipeline.Pipeline, error) {
	var pipelines []pipeline.Pipeline

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || filepath.Ext(path) != ".yaml" {
			return nil
		}

		p, err := pipeline.LoadFromFile(path)
		if err != nil {
			log.Printf("[Engine] Failed to load pipeline from %s: %v", path, err)
			return nil
		}

		pipelines = append(pipelines, p)
		return nil
	})

	return pipelines, err
}

// runPipeline initializes and starts processing for a single pipeline definition.
func runPipeline(p *pipeline.Pipeline, cfg *config.AppConfig) {
	fmt.Printf("→ Pipeline: %s\n", p.Name)
	ctx := context.Background()

	// Parse query plan
	plan, err := pipeline.NewQueryPlan(p.Query)
	if err != nil {
		log.Fatalf("Failed to parse query: %v", err)
	}
	p.Query = plan.SQL

	// Configure pipeline settings and dimensions
	ttlByTopic, slowDims, fastDims := configurePipelineSettings(p, plan)

	// Initialize core components
	stateManager := initializeStateManager(p.Name, cfg, ttlByTopic)
	schemaManager, duckEngine := initializeComponents(p.Name)
	schemaRegistryClient := setupSchemaRegistry(ctx, cfg, plan, schemaManager, duckEngine, p)

	// Create producer and engine
	producer, err := kafka.NewProducer(ctx, cfg.Kafka)
	if err != nil {
		log.Fatalf("[Kafka] Failed to create producer: %v", err)
	}

	buffer := duck.NewBuffer(p.Name)
	eng, err := engine.NewEngine(
		*p, plan, stateManager, buffer, schemaManager, duckEngine,
		producer, plan.Tables, slowDims, fastDims, schemaRegistryClient,
	)
	if err != nil {
		log.Fatalf("[Engine] Failed to initialize engine: %v", err)
	}

	// Start background services and consumers
	startBackgroundServices(cfg, stateManager, eng, buffer, p.Name)
	startKafkaConsumers(ctx, cfg, plan.Tables, p.Name, stateManager, eng)
}

// configurePipelineSettings sets up TTL configuration and dimensions for topics
func configurePipelineSettings(p *pipeline.Pipeline, plan *pipeline.QueryPlan) (
	ttlByTopic map[string]time.Duration, slowDims []string, fastDims []string) {
	defaultTTL, err := time.ParseDuration(p.State.Events.DefaultTTL)
	if err != nil {
		log.Fatalf("invalid default TTL %q: %v", p.State.Events.DefaultTTL, err)
	}

	ttlByTopic = map[string]time.Duration{}
	slowDims = make([]string, 0) // "dimension" topics
	fastDims = make([]string, 0) // everything else

	for _, topic := range plan.Tables {
		if slices.Contains(p.State.Dimensions, topic) {
			ttlByTopic[topic] = 0 // never expire
			slowDims = append(slowDims, topic)
		} else {
			if raw, ok := p.State.Events.Overrides[topic]; ok {
				d, parseErr := time.ParseDuration(raw)
				if parseErr != nil {
					log.Fatalf("invalid override TTL for %s: %v", topic, parseErr)
				}
				ttlByTopic[topic] = d
			} else {
				ttlByTopic[topic] = defaultTTL
			}
			fastDims = append(fastDims, topic)
		}
	}
	return ttlByTopic, slowDims, fastDims
}

// initializeStateManager creates and initializes the RocksDB state manager
func initializeStateManager(pipelineName string, cfg *config.AppConfig, ttlByTopic map[string]time.Duration) *state.RocksDBState {
	stateManager, err := state.NewRocksDBState(pipelineName, cfg, ttlByTopic)
	if err != nil {
		log.Fatalf("[Pipeline] Failed to init RocksDB: %v", err)
	}

	stats, err := stateManager.StatsByTopic()
	if err != nil {
		log.Printf("[State] Error getting RocksDB statistics: %v", err)
	} else {
		log.Println("[State] RocksDB initialized:")
		for topic, count := range stats {
			log.Printf("[State] Topic: %s | Records: %d", topic, count)
		}
	}
	return stateManager
}

// initializeComponents creates schema manager and DuckDB engine
func initializeComponents(pipelineName string) (*schema.Manager, *duck.DBEngine) {
	schemaManager := schema.NewSchemaManager()

	duckDir := filepath.Join(os.TempDir(), "kaflowsql", pipelineName, "duckdb")
	if mkdirErr := os.MkdirAll(duckDir, defaultDirMode); mkdirErr != nil {
		log.Fatalf("[DuckDB] Failed to create directory: %v", mkdirErr)
	}

	duckPath := filepath.Join(duckDir, fmt.Sprintf("%s.duckdb", pipelineName))
	duckEngine, err := duck.NewDuckDBEngine(duckPath)
	if err != nil {
		log.Fatalf("[DuckDB] Failed to init DuckDB: %v", err)
	}

	return schemaManager, duckEngine
}

// setupSchemaRegistry configures Avro schema registry if enabled
func setupSchemaRegistry(ctx context.Context, cfg *config.AppConfig, plan *pipeline.QueryPlan,
	schemaManager *schema.Manager, duckEngine *duck.DBEngine, p *pipeline.Pipeline) *srclient.SchemaRegistryClient {
	var schemaRegistryClient *srclient.SchemaRegistryClient

	if cfg.Kafka.UseAvro {
		schemaRegistryClient = srclient.CreateSchemaRegistryClient(cfg.Kafka.SchemaRegistry)
		for _, topic := range plan.Tables {
			// Convert Avro schema to TableSchema
			tableSchema, schemaErr := avro.SchemaToTableSchema(topic, schemaRegistryClient)
			if schemaErr != nil {
				log.Fatalf("[Init] Failed to get table schema for topic %s: %v", topic, schemaErr)
			}
			schemaManager.Update(topic, tableSchema)
			// Create table in DuckDB from the schema
			if createErr := duckEngine.CreateTableFromSchema(topic, tableSchema); createErr != nil {
				log.Fatalf("[Init] Failed to create table for %s: %v", topic, createErr)
			}
		}
		subject := fmt.Sprintf("%s-value", p.Output.Topic)
		avroJSON, schemaErr := duckEngine.GenerateAvroSchema(ctx, p.Output.Topic, plan.SQL)
		if schemaErr != nil {
			log.Fatalf("[Init] Failed to create avro schema %v", schemaErr)
		}

		regSchema, regErr := avro.CreateSchemaIfNotExists(schemaRegistryClient, subject, avroJSON, srclient.Avro)
		if regErr != nil {
			log.Fatalf("[SchemaRegistry] Failed to register schema for %s: %v", subject, regErr)
		}
		log.Printf("[SchemaRegistry] Subject=%s | ID=%d | Version=%d",
			subject, regSchema.ID(), regSchema.Version())
	}
	return schemaRegistryClient
}

// startBackgroundServices starts checkpoint and buffer processing routines
func startBackgroundServices(cfg *config.AppConfig, stateManager *state.RocksDBState,
	eng *engine.Engine, buffer *duck.Buffer, pipelineName string) {
	// Start checkpoint routine if enabled
	if cfg.State.RocksDB.Checkpoint.Enabled {
		go func() {
			ticker := time.NewTicker(cfg.State.RocksDB.Checkpoint.Interval)
			defer ticker.Stop()

			for range ticker.C {
				if checkpointErr := stateManager.CreateCheckpointIfEnabled(); checkpointErr != nil {
					log.Printf("[Checkpoint] Error creating checkpoint for %s: %v", pipelineName, checkpointErr)
				} else {
					log.Printf("[Checkpoint] Snapshot created for %s", pipelineName)
				}
			}
		}()
	}

	// Start periodic flush routine
	go func() {
		lastProcessed := time.Now() // Tracks last processing timestamp

		for {
			// Check size or time condition
			if buffer.Size() > 10000 || time.Since(lastProcessed) > 10*time.Second {
				log.Printf("[Engine] Buffer is ready to be processed (Size: %d bytes / Time: %v)", buffer.Size(), time.Since(lastProcessed))

				// Process buffer
				if err := eng.ProcessBufferToDuckDB(); err != nil {
					log.Printf("[Engine] Failed to process buffer for %s: %v", pipelineName, err)
				} else {
					log.Printf("[Engine] Buffer flushed to DuckDB for %s", pipelineName)
				}

				lastProcessed = time.Now()
				buffer.Metrics()
			}

			// Prevent tight loop with small sleep
			time.Sleep(1 * time.Second)
		}
	}()
}

// startKafkaConsumers launches consumer goroutines for all topics
func startKafkaConsumers(ctx context.Context, cfg *config.AppConfig, topics []string,
	pipelineName string, stateManager *state.RocksDBState, eng *engine.Engine) {
	for _, topic := range topics {
		go func(topic string) {
			reader := kafka.NewConsumer(
				ctx,
				cfg.Kafka.Brokers,
				topic,
				pipelineName,
				cfg.Kafka,
				stateManager,
			)

			lastLog := time.Now()
			var batch []*kafka.DecodedMessage
			lastCommit := time.Now()

			for {
				if time.Since(lastLog) >= 15*time.Second {
					reader.LogStats()
					lastLog = time.Now()
				}

				msg, err := reader.Read()
				if err != nil {
					log.Printf("[Kafka] Read error on topic %s: %v", topic, err)
					continue
				}

				if err := eng.ProcessIncomingEvent(topic, msg.Value, msg.Time, msg.Partition, msg.Offset); err != nil {
					log.Printf("[Engine] Error processing event: %v", err)
				}

				batch = append(batch, msg)

				if len(batch) >= 100 || time.Since(lastCommit) > 5*time.Second {
					if err := reader.CommitBatch(batch); err != nil {
						log.Printf("commit batch error: %v", err)
					}
					for _, m := range batch {
						m.Release()
					}
					batch = batch[:0]
					lastCommit = time.Now()
				}
			}
		}(topic)
	}
}
