package faker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand" // Using weak random for test data generation only
	"net/http"
	"time"

	"github.com/siqueiraa/KaflowSQL/pkg/kafka"
)

const (
	maxUsers             = 50    // Maximum number of test users to generate
	httpOKStatus         = 200   // HTTP OK status code
	httpErrorThreshold   = 300   // HTTP error status threshold
	maxTransactionAmount = 100.0 // Maximum transaction amount for test data
)

var userIDs []string

func init() { //nolint:gochecknoinits // Required for test data initialization
	for i := 1; i <= maxUsers; i++ {
		userIDs = append(userIDs, fmt.Sprintf("u%d", i))
	}
}

func randomUserID() string {
	return userIDs[rand.Intn(len(userIDs))] //nolint:gosec // Using weak random for test data generation only
}

const userEventSchema = `{
  "type": "record",
  "name": "UserEvent",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "event_type", "type": "string"},
	    {
      "name": "event_time",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    }
  ]
}`

const userProfileSchema = `{
  "type": "record",
  "name": "UserProfile",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "plan", "type": "string"}
  ]
}`

const transactionSchema = `{
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "user_id", "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}`

const userEnrichedSchema = `{
  "type": "record",
  "name": "UserEnriched",
  "fields": [
    { "name": "user_id", "type": "string" },
    { "name": "event_type", "type": "string" },
    {
      "name": "event_time",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    { "name": "plan", "type": ["null", "string"], "default": null },
	{ "name": "last_tx_amount", "type": ["null", "double"], "default": null }
  ]
}
`

func RegisterSchemas(registryURL string) {
	schemas := map[string]string{
		"user_events-value":   userEventSchema,
		"user_profiles-value": userProfileSchema,
		"transactions-value":  transactionSchema,
		"user_enriched-value": userEnrichedSchema,
	}

	for subject, schema := range schemas {
		if err := registerSchema(subject, schema, registryURL); err != nil {
			log.Printf("[Schema] Failed to register schema for %s: %v", subject, err)
		} else {
			log.Printf("[Schema] Registered schema for %s", subject)
		}
	}
}

func registerSchema(subject, avroSchema, registryURL string) error {
	// First, try to get the latest schema to check if it already exists
	getURL := fmt.Sprintf("%s/subjects/%s/versions/latest", registryURL, subject)
	getReq, err := http.NewRequestWithContext(context.Background(), "GET", getURL, http.NoBody)
	if err != nil {
		return err
	}
	getReq.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		return err
	}
	defer getResp.Body.Close()

	// If schema exists (httpOKStatus), check if it matches
	if getResp.StatusCode == httpOKStatus {
		var existingSchema map[string]interface{}
		if decodeErr := json.NewDecoder(getResp.Body).Decode(&existingSchema); decodeErr != nil {
			return fmt.Errorf("failed to decode existing schema: %v", decodeErr)
		}

		if schema, ok := existingSchema["schema"].(string); ok && schema == avroSchema {
			// Schema already exists and matches, no need to register
			log.Printf("[Schema] Schema for %s already exists and matches, skipping registration", subject)
			return nil
		}
	}

	// Schema doesn't exist or doesn't match, proceed with registration
	payload := map[string]string{
		"schema": avroSchema,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", registryURL, subject)
	req, err := http.NewRequestWithContext(context.Background(), "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= httpErrorThreshold {
		return fmt.Errorf("failed to register schema: %s", resp.Status)
	}
	return nil
}

func PublishUserEvent(p *kafka.Producer) {
	payload := map[string]any{
		"user_id":    randomUserID(),
		"event_type": "login",
		"event_time": time.Now(),
	}
	send(p, "user_events", payload)
}

func PublishUserProfile(p *kafka.Producer) {
	payload := map[string]any{
		"user_id": randomUserID(),
		"plan":    "premium",
	}
	send(p, "user_profiles", payload)
}

func PublishTransaction(p *kafka.Producer) {
	payload := map[string]any{
		"user_id": randomUserID(),
		"amount":  rand.Float64() * maxTransactionAmount, //nolint:gosec // Using weak random for test data generation only
	}
	send(p, "transactions", payload)
}

func send(p *kafka.Producer, topic string, data map[string]any) {
	log.Printf("[Kafka] Preparing to send data to topic %s: %+v", topic, data)

	for key, v := range data {
		if v == "" || v == nil {
			log.Printf("[Kafka] Skipping field %s with nil or empty value", key)
			return
		}
	}

	log.Printf("[Kafka] All fields are valid. Publishing to topic %s", topic)

	if err := p.Publish(topic, nil, data); err != nil {
		log.Printf("[Kafka] Failed to publish message to topic %s: %v", topic, err)
		return
	}

	log.Printf("[Kafka] Successfully published message to topic %s", topic)
}
