package avro

// DecodeAvro decodes a Confluent-wire-format Avro message
// by leveraging a shared, cached codec helper.
func DecodeAvro(
	schemaRegistryURL string,
	subject string,
	payload []byte,
) (map[string]any, error) {
	client := getSchemaRegistryClient(schemaRegistryURL)
	return DecodeWithHelper(client, subject, payload)
}
