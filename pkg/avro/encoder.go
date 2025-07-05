package avro

import (
	"github.com/riferrei/srclient"
)

// EncodeAvro encodes a Go record into the Confluent-wire-format Avro message
// by leveraging the shared helper for schema lookup and codec caching.
// Accepts a pre-initialized SchemaRegistryClient rather than a URL string.
func EncodeAvro(
	client *srclient.SchemaRegistryClient,
	topic string,
	record map[string]any,
) ([]byte, error) {
	subject := topic
	// Optionally normalize the record before encoding:
	// record = normalizeForAvro(record)
	return EncodeWithHelper(client, subject, record)
}
