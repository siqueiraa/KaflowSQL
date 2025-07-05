package main

import (
	"context"
	"log"
	"time"

	"github.com/siqueiraa/KaflowSQL/pkg/config"
	"github.com/siqueiraa/KaflowSQL/pkg/faker"
	"github.com/siqueiraa/KaflowSQL/pkg/kafka"
)

func main() {
	cfg := config.Load("../../config.yaml")
	ctx := context.Background() // or a cancellable / timeout context

	// Registra os schemas no Schema Registry se configurado
	if cfg.Kafka.SchemaRegistry != "" {
		log.Printf("[Fakegen] Registering schemas at %s", cfg.Kafka.SchemaRegistry)
		faker.RegisterSchemas(cfg.Kafka.SchemaRegistry)
	}

	producer, err := kafka.NewProducer(ctx, cfg.Kafka)
	if err != nil {
		log.Fatalf("[Fakegen] failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	log.Println("[Fakegen] Starting event generation...")
	for {
		faker.PublishUserEvent(producer)
		faker.PublishUserProfile(producer)
		faker.PublishTransaction(producer)
		time.Sleep(cfg.Emitter.Interval)
	}
}
