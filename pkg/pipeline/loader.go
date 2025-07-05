package pipeline

import (
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

func LoadFromFile(path string) (Pipeline, error) {
	var p Pipeline

	data, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}

	// Check for empty file
	if len(data) == 0 {
		return p, fmt.Errorf("empty pipeline file")
	}

	if unmarshalErr := yaml.Unmarshal(data, &p); unmarshalErr != nil {
		return p, unmarshalErr
	}

	// Validate required fields after unmarshaling
	if p.Name == "" {
		return p, fmt.Errorf("pipeline name is required")
	}

	if p.Query == "" {
		return p, fmt.Errorf("pipeline query is required")
	}

	if p.Output.Topic == "" {
		return p, fmt.Errorf("pipeline output topic is required")
	}

	if p.Output.Format == "" {
		return p, fmt.Errorf("pipeline output format is required")
	}

	// Validate SQL query syntax
	_, err = NewQueryPlan(p.Query)
	if err != nil {
		return p, fmt.Errorf("invalid SQL query: %w", err)
	}

	if p.Output.Schema == nil {
		log.Printf("[Pipeline] No schema defined in output. Will treat result as dynamic.")
	}

	return p, nil
}
